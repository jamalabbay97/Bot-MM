"""
tests/test_hft_risk_and_exits.py — Tests for HFT Risk Gating, Dynamic Exits, and RugCheck Bypass
================================================================================================
Tests cover:
  1. Concurrency, Max Active Positions (3), Portfolio Exposure (5%), Token Risk (1.5%), and Circuit Breakers.
  2. Bonding-Curve Tuned Dynamic Exits (+25%/+50% TP, -8% Hard SL, +15%/+8%/5% Trailing, 120s Timeout, Insolvency, Stale Tick).
  3. RugCheck age < 90s bypass, LRU 400 Cache, and >20% sniper bundle rejection.
  4. Monotonic lot state transitions and SQLite WAL recording.
  5. Telegram /trades, /news, and /whales telemetry verification.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import (
    MAX_ACTIVE_POSITIONS,
    MAX_PER_TOKEN_RISK_PCT,
    MAX_PORTFOLIO_EXPOSURE_PCT,
    EngineConfig,
)
from alpha_engine.execution.book import ExitDecision, OpenLot, PositionBook, RunningMetrics
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    LotStatus,
    OrderSide,
    SecurityTier,
    TradeExitReason,
)
from alpha_engine.models.state import PaperFill, PoolState
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.security.preflight import verify_solana_mint_preflight
from alpha_engine.security.rugcheck import (
    UNINDEXED_MINT_CACHE,
    UnindexedMintLRUCache,
    _fetch_rugcheck_report,
    _parse_rugcheck_report,
)


# =====================================================================
# DIRECTIVE 1: STRICT PORTFOLIO STATE MACHINE & DYNAMIC RISK GATING
# =====================================================================

def test_risk_gating_max_active_positions_cap():
    """Verify MAX_ACTIVE_POSITIONS = 3 hard cap rejects 4th open position."""
    book = PositionBook()
    total_equity = Decimal("100.0")  # 100 SOL

    # Open 3 lots
    for i in range(3):
        lot = book.open_lot(
            fill=PaperFill(
                token_address=f"Token{i}",
                chain=ChainIdentifier.SOLANA_MAINNET,
                effective_price=Decimal("1.0"),
                tokens_acquired=Decimal("1.0"),
                simulated_native_spent=Decimal("1.0"),
                side=OrderSide.BUY,
            ),
            signal_id=f"sig-{i}",
        )
        assert lot.status == LotStatus.OPEN

    assert book.open_position_count() == 3

    # Attempt to open 4th position
    can_open, reason = book.can_open_position(
        token_address="Token3",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.0"),
        total_equity_native=total_equity,
    )
    assert can_open is False
    assert "MAX_ACTIVE_POSITIONS" in reason


def test_risk_gating_exposure_and_token_risk_caps():
    """Verify 1.5% max per-token risk and 5% total portfolio exposure caps."""
    book = PositionBook()
    total_equity = Decimal("100.0")  # 100 SOL

    # 1. Per-token risk limit: 1.5% of 100 SOL = 1.5 SOL
    # Attempting 1.6 SOL should be rejected
    can_open, reason = book.can_open_position(
        token_address="TokenA",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.6"),
        total_equity_native=total_equity,
    )
    assert can_open is False
    assert "MAX_PER_TOKEN_RISK_PCT" in reason

    # 1.5 SOL should be permitted
    can_open, reason = book.can_open_position(
        token_address="TokenA",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.5"),
        total_equity_native=total_equity,
    )
    assert can_open is True

    # 2. Duplicate token or curve address check
    book.open_lot(
        fill=PaperFill(
            token_address="TokenA",
            chain=ChainIdentifier.SOLANA_MAINNET,
            effective_price=Decimal("1.0"),
            tokens_acquired=Decimal("1.5"),
            simulated_native_spent=Decimal("1.5"),
            side=OrderSide.BUY,
        ),
        signal_id="sig-a",
        pool_address="CurveA",
    )
    # Duplicate token rejected
    can_open, reason = book.can_open_position(
        token_address="TokenA",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.0"),
        total_equity_native=total_equity,
    )
    assert can_open is False
    assert "Duplicate" in reason

    # Duplicate curve address rejected
    can_open, reason = book.can_open_position(
        token_address="TokenB",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.0"),
        total_equity_native=total_equity,
        pool_address="CurveA",
    )
    assert can_open is False
    assert "Duplicate position already open for curve" in reason

    # 3. Total portfolio exposure cap (5% of 100 = 5.0 SOL)
    # Open second lot with 1.5 SOL (total = 3.0 SOL)
    book.open_lot(
        fill=PaperFill(
            token_address="TokenB",
            chain=ChainIdentifier.SOLANA_MAINNET,
            effective_price=Decimal("1.0"),
            tokens_acquired=Decimal("1.5"),
            simulated_native_spent=Decimal("1.5"),
            side=OrderSide.BUY,
        ),
        signal_id="sig-b",
    )
    # Open third lot with 1.5 SOL (total = 4.5 SOL)
    book.open_lot(
        fill=PaperFill(
            token_address="TokenC",
            chain=ChainIdentifier.SOLANA_MAINNET,
            effective_price=Decimal("1.0"),
            tokens_acquired=Decimal("1.5"),
            simulated_native_spent=Decimal("1.5"),
            side=OrderSide.BUY,
        ),
        signal_id="sig-c",
    )
    # 4.5 SOL open + 1.0 SOL attempted = 5.5 SOL > 5.0 SOL (5% max exposure)
    can_open, reason = book.can_open_position(
        token_address="TokenD",
        chain=ChainIdentifier.SOLANA_MAINNET,
        allocated_cost_native=Decimal("1.0"),
        total_equity_native=total_equity,
    )
    assert can_open is False


def test_circuit_breaker_streak_and_drawdown():
    """Verify circuit breaker trips on 3 consecutive losses OR 8.0% daily drawdown."""
    metrics = RunningMetrics(peak_equity_usd=Decimal("10000.0"))

    # Record 2 losses: should not trip
    metrics.record_closed_trade(Decimal("-10.0"), Decimal("0.01"), Decimal("9990.0"))
    metrics.record_closed_trade(Decimal("-20.0"), Decimal("0.01"), Decimal("9970.0"))
    assert metrics.consecutive_losses == 2
    assert metrics.circuit_breaker_active is False

    # 3rd consecutive loss trips circuit breaker
    metrics.record_closed_trade(Decimal("-15.0"), Decimal("0.01"), Decimal("9955.0"))
    assert metrics.consecutive_losses == 3
    assert metrics.circuit_breaker_active is True
    assert metrics.is_circuit_breaker_active(freeze_duration_s=3600.0) is True
    assert "Consecutive loss streak" in metrics.circuit_breaker_reason

    # Reset metrics for daily drawdown test
    metrics2 = RunningMetrics(peak_equity_usd=Decimal("10000.0"))
    metrics2.record_closed_trade(Decimal("500.0"), Decimal("0.01"), Decimal("10500.0"))
    assert metrics2.circuit_breaker_active is False

    # Equity drops from 10500 to 9600 (drawdown = 900 / 10500 = 8.57% >= 8.0%)
    metrics2.record_closed_trade(Decimal("-900.0"), Decimal("0.01"), Decimal("9600.0"))
    assert metrics2.circuit_breaker_active is True
    assert metrics2.daily_drawdown_pct >= 8.0
    assert "Daily drawdown reached" in metrics2.circuit_breaker_reason


# =====================================================================
# DIRECTIVE 2: ADAPTIVE HIGH-SPEED EXIT MONITOR LOOP
# =====================================================================

def test_stale_tick_guard_halts_triggers():
    """Verify that a price tick > 3.0 seconds old halts execution triggers."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenStale",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000"),
        tokens_held=Decimal("1000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("1000"),
        bonding_curve_mode=True,
    )
    # Price dropped -50% (would normally trigger stop-loss)
    current_price = Decimal("0.50")
    old_tick_timestamp = time.time() - 4.5  # 4.5 seconds old > 3.0s

    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=current_price,
        tick_timestamp_s=old_tick_timestamp,
        bonding_curve_mode=True,
    )
    assert decision is None  # Guard halted triggers


def test_bonding_curve_take_profit_ladder():
    """Verify TP at +25% (scale out 50%) and +50% (sell remaining 50%)."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenTP",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("10000"),
        bonding_curve_mode=True,
    )
    book._lots[(lot.chain.value, lot.token_address)] = [lot]
    now = time.time()

    # 1. Price reaches +25% (1.25)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.25"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.TP_25
    assert decision.exit_stage == ExitStage.TP1
    assert decision.tokens_to_sell == Decimal("5000")  # 50% scale out

    # Apply decision
    book.apply_exit_decision(decision, sell_price=Decimal("1.25"))
    assert lot.tokens_held == Decimal("5000")
    assert lot.scaled_out_50_pct is True

    # 2. Price reaches +50% (1.50)
    decision2 = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.50"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision2 is not None
    assert decision2.should_exit is True
    assert decision2.exit_reason == TradeExitReason.TP_50
    assert decision2.exit_stage == ExitStage.TP2
    assert decision2.tokens_to_sell == Decimal("5000")  # Sells remaining 50%


def test_bonding_curve_hard_stop_loss_and_prioritized_routing():
    """Verify -8% hard stop loss triggers and flags prioritized routing on high impact."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenSL",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("10000"),
        bonding_curve_mode=True,
    )
    now = time.time()

    # Price drops to 0.91 (-9% < -8%)
    # Curve reserve = 100000, position val = 10000 * 0.91 = 9100 (> 3% of reserve -> prioritized)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.91"),
        current_pool_reserve_native=Decimal("100000"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.SL_HARD
    assert decision.exit_stage == ExitStage.SL
    assert decision.prioritized is True


def test_bonding_curve_trailing_stop():
    """Verify trailing stop: activates at +15%, locks stop at +8%, trails by 5% as new highs form."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenTrail",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000"),
        tokens_held=Decimal("1000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("1000"),
        bonding_curve_mode=True,
    )
    now = time.time()

    # 1. Price hits +16% (1.16) -> Activates trailing stop locked at +8% (1.08)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.16"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision is None
    assert lot.trailing_stop_active is True
    assert lot.trailing_stop_price >= Decimal("1.08")

    # 2. Price makes new high at +20% (1.20) -> Trails 5% from peak (1.20 * 0.95 = 1.14)
    book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.20"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert lot.peak_price == Decimal("1.20")
    assert lot.trailing_stop_price == Decimal("1.14")

    # 3. Price drops below trailing stop to 1.13 -> Triggers SL_TRAILING
    decision_trail = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.13"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision_trail is not None
    assert decision_trail.should_exit is True
    assert decision_trail.exit_reason == TradeExitReason.SL_TRAILING
    assert decision_trail.exit_stage == ExitStage.TRAILING_SL


def test_velocity_time_decay_exit():
    """Verify position duration > 120s with unrealized PnL < +3.0% triggers TIMEOUT_VELOCITY_DECAY."""
    book = PositionBook()
    open_time_ns = time.time_ns() - int(130 * 1e9)  # 130s ago (> 120s)
    lot = OpenLot(
        token_address="TokenDecay",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000"),
        tokens_held=Decimal("1000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.01"),
        cost_basis_native=Decimal("1000"),
        open_timestamp_ns=open_time_ns,
        bonding_curve_mode=True,
    )

    # Current price is 1.01 (+1.0% < +3.0%)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.01"),
        tick_timestamp_s=time.time(),
        bonding_curve_mode=True,
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.TIMEOUT_VELOCITY_DECAY
    assert decision.exit_stage == ExitStage.TIMEOUT


def test_slippage_insolvency_guard():
    """Verify bonding curve liquidity check: > 15% price collapse risk triggers SLIPPAGE_INSOLVENCY."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenInsolvent",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("10000"),
        bonding_curve_mode=True,
    )
    # Available reserve in curve is 50,000 SOL
    # Position value is 10,000 * 1.0 = 10,000 SOL (20% of reserve > 15%)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.0"),
        current_pool_reserve_native=Decimal("50000"),
        tick_timestamp_s=time.time(),
        bonding_curve_mode=True,
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.SLIPPAGE_INSOLVENCY
    assert decision.exit_stage == ExitStage.RUGPULL
    assert decision.prioritized is True


# =====================================================================
# DIRECTIVE 3: GATEKEEPER & RUGCHECK CACHE/RPC BYPASS
# =====================================================================

@pytest.mark.anyio
async def test_rugcheck_age_bypass_and_lru_cache():
    """Verify age < 90s bypasses RugCheck, and LRU cache stores HTTP 400 responses."""
    cache = UnindexedMintLRUCache(ttl_seconds=60.0)
    mint = "NewMint1111111111111111111111111111111111111"
    assert cache.is_cached(mint) is False

    cache.put(mint)
    assert cache.is_cached(mint) is True

    # Test age < 90s skips RugCheck
    mock_session = MagicMock()
    mock_limiter = MagicMock()
    mock_limiter.helius.acquire = AsyncMock()

    with patch("alpha_engine.security.rugcheck.verify_solana_mint_preflight", new_callable=AsyncMock) as mock_preflight:
        mock_preflight.return_value = {
            "on_chain_fallback": True,
            "mint": mint,
            "mintAuthority": None,
            "freezeAuthority": None,
            "top10_concentration": 0.15,
            "developer_sniper_bundle": False,
        }

        # Call with age 45s (< 90s)
        res = await _fetch_rugcheck_report(
            session=mock_session,
            mint_address=mint,
            limiter=mock_limiter,
            rpc_url="https://solana.rpc.com",
            token_age_s=45.0,
        )
        assert res is not None
        assert res.get("on_chain_fallback") is True
        # RugCheck HTTP session.get should NOT have been called
        mock_session.get.assert_not_called()


def test_sybil_and_bundled_mint_detection():
    """Verify rejection of tokens where > 20% of supply was scooped in block 0/single account."""
    # Preflight dictionary with sniper bundle flag
    token_addr = "SniperToken11111111111111111111111111111111"
    raw_bundle = {
        "on_chain_fallback": True,
        "mint": token_addr,
        "mintAuthority": None,
        "freezeAuthority": None,
        "top10_concentration": 0.25,
        "developer_sniper_bundle": True,
        "max_single_holder_pct": 0.22,  # 22% > 20%
    }
    report = _parse_rugcheck_report(token_addr, raw_bundle)
    assert report.tier == SecurityTier.TIER1_REJECTED


# =====================================================================
# DIRECTIVE 4: IDEMPOTENT TRANSACTION LIFECYCLE & STATE RECOVERY
# =====================================================================

@pytest.mark.anyio
async def test_ledger_lot_lifecycle_transitions(tmp_path):
    """Verify monotonic lot lifecycle state transitions recorded in SQLite."""
    db_file = tmp_path / "test_lifecycle.db"
    async with SQLiteLedger(db_path=db_file) as ledger:
        lot_id = "lot-lifecycle-001"
        token = "TokenLife"
        chain = "solana_mainnet"

        # 1. PENDING_BUY -> OPEN
        await ledger.record_lot_transition(lot_id, token, chain, LotStatus.PENDING_BUY)
        await ledger.record_lot_transition(lot_id, token, chain, LotStatus.OPEN)

        # 2. PENDING_SELL
        await ledger.record_lot_transition(
            lot_id, token, chain, LotStatus.PENDING_SELL, exit_reason=TradeExitReason.TP_25
        )

        # 3. CLOSED -> SETTLED
        await ledger.record_lot_transition(
            lot_id, token, chain, LotStatus.CLOSED, exit_reason=TradeExitReason.TP_25, pnl_usd=Decimal("25.0")
        )
        await ledger.record_lot_transition(
            lot_id, token, chain, LotStatus.SETTLED, exit_reason=TradeExitReason.TP_25, pnl_usd=Decimal("25.0")
        )

        # Verify rows in lot_lifecycle table
        async with ledger._conn.execute(
            "SELECT status FROM lot_lifecycle WHERE lot_id = ? ORDER BY timestamp_ns ASC", (lot_id,)
        ) as cursor:
            rows = await cursor.fetchall()

        statuses = [r[0] for r in rows]
        assert statuses == [
            LotStatus.PENDING_BUY.value,
            LotStatus.OPEN.value,
            LotStatus.PENDING_SELL.value,
            LotStatus.CLOSED.value,
            LotStatus.SETTLED.value,
        ]


# =====================================================================
# DIRECTIVE 5: TELEGRAM /trades OUTPUT FORMATTING
# =====================================================================

@pytest.mark.anyio
async def test_telegram_trades_output_formatting():
    """Verify /trades command displays Entry, Current Price, Unrealized PnL %, Duration, and Trailing Stop status."""
    from alpha_engine.ingestion.telegram import TelegramIngester

    event_q: asyncio.Queue = asyncio.Queue()
    signal_q: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=event_q, signal_queue=signal_q, api_id=None, api_hash=None)

    # Mock status provider with detailed open trade info
    def mock_status():
        return {
            "open_trades": [
                {
                    "token_address": "TokenA1111111111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "entry_price": Decimal("0.001000"),
                    "current_price": Decimal("0.001250"),
                    "unrealized_pnl_usd": 25.0,
                    "unrealized_pnl_pct": 25.0,
                    "duration_s": 45.0,
                    "trailing_stop_status": "LOCKED (+8%)",
                    "tokens_held": Decimal("100000"),
                }
            ],
            "recent_news": [],
            "recent_whales": [],
        }

    ingester.set_status_provider(mock_status)

    mock_event = MagicMock()
    replies = []

    async def mock_reply(msg):
        replies.append(msg)

    mock_event.reply = AsyncMock(side_effect=mock_reply)

    await ingester._cmd_trades(mock_event)

    assert len(replies) == 1
    reply_text = replies[0]
    # Check column headers and fields
    assert "ENTRY" in reply_text
    assert "CURRENT" in reply_text
    assert "PNL %" in reply_text
    assert "DUR" in reply_text
    assert "TRAILING" in reply_text
    assert "+25.00%" in reply_text
    assert "45s" in reply_text
    assert "LOCKED (+8%)" in reply_text


@pytest.mark.anyio
async def test_executor_exit_paper_fill_validates_and_succeeds():
    """Verify that PaperExecutor.execute_exit generates a valid PaperFill with positive portfolio_equity_usd."""
    from alpha_engine.execution.executor import PaperExecutor
    from alpha_engine.models.state import PoolState

    book = PositionBook()
    executor = PaperExecutor(position_book=book, native_price_usd=Decimal("150.0"))
    pool = PoolState(
        pool_address="Pool1111111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="TokenA1111111111111111111111111111111111111",
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1000000.0"),
        k=Decimal("30.0") * Decimal("1000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=100,
    )

    # 1. Calling execute_exit with default portfolio equity
    fill = await executor.execute_exit(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="TokenA1111111111111111111111111111111111111",
        pool=pool,
        tokens_to_sell=Decimal("1000.0"),
        reason=TradeExitReason.TIMEOUT_VELOCITY_DECAY,
        apply_drag=True,
    )
    assert fill is not None
    assert fill.side == OrderSide.SELL
    assert fill.portfolio_equity_usd > Decimal(0)
    assert fill.effective_price > Decimal(0)

    # 2. Calling execute_exit with explicit portfolio equity
    fill2 = await executor.execute_exit(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="TokenA1111111111111111111111111111111111111",
        pool=pool,
        tokens_to_sell=Decimal("1000.0"),
        reason=TradeExitReason.TP_25,
        apply_drag=True,
        portfolio_equity_usd=Decimal("8500.0"),
    )
    assert fill2 is not None
    assert fill2.portfolio_equity_usd == Decimal("8500.0")


def test_solana_address_sanitization_and_ws_backoff():
    """Task 1: Verify Solana address sanitization strips malformed bytes/instruction slices and WS backoff limits."""
    from alpha_engine.ingestion.svm import sanitize_solana_pubkey, is_valid_solana_pubkey, SVMIngester
    from alpha_engine.rate_limiter.registry import RateLimiterRegistry

    # Valid 32-byte Base58 pubkeys
    valid_pubkey = "So11111111111111111111111111111111111111112"
    assert is_valid_solana_pubkey(valid_pubkey) is True
    assert sanitize_solana_pubkey(f"  '{valid_pubkey}'\n") == valid_pubkey

    # Malformed strings: raw byte slices, non-base58, instruction data, invalid lengths
    assert sanitize_solana_pubkey("0x1234") is None
    assert sanitize_solana_pubkey("invalid_base58_0OIl") is None
    assert sanitize_solana_pubkey("short") is None
    assert sanitize_solana_pubkey(None) is None
    assert sanitize_solana_pubkey("Program log: Instruction: InitializeMint") is None

    # WS Ingester ExponentialBackoff validation (initial=1.0s, max_delay=10.0s)
    event_q: asyncio.Queue = asyncio.Queue()
    limiter = RateLimiterRegistry.default()
    svm = SVMIngester(ws_url="wss://test.helius.xyz", pool_registry={}, event_queue=event_q, limiter=limiter)
    assert svm._backoff._initial == 1.0
    assert svm._backoff._max_delay == 10.0
    for _ in range(10):
        d = svm._backoff.next_delay()
        assert d <= 15.0  # With 50% max jitter, max delay is <= 15.0s (base <= 10.0s)


def test_pending_launch_buffer_smart_filters():
    """Task 2: Verify Smart Launch Filter: staging, 90-300s window, volume spike, and dump drop."""
    from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator
    from alpha_engine.models.enums import OrderSide, SecurityTier, SignalSource
    from alpha_engine.models.events import RawSignalEvent, SwapEvent
    from alpha_engine.models.state import PoolState, SecurityReport

    buffer = PendingLaunchBuffer(
        min_age_s=90.0,
        max_age_s=300.0,
        min_buys=15,
        min_volume_native=Decimal("15.0"),
        max_dump_pct=Decimal("0.30"),
    )
    sig_gen = SignalGenerator()
    token1 = "PumpMint111111111111111111111111111111111"
    report = SecurityReport(
        token_address=token1,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    pool_addr1 = "CurveAddr11111111111111111111111111111"
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token1,
        pool_address=pool_addr1,
        source=SignalSource.PUMP_FUN_MINT,
    )
    t_0 = 1000.0
    init_price = Decimal("0.000000028")

    # 1. Add launch at t_0 -> Staged, not yet graduated
    staged = buffer.add_launch(
        token_address=token1,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr1,
        initial_price=init_price,
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )
    assert buffer.is_staged(token1) is True
    assert staged.buy_count == 0

    pool = PoolState(
        pool_address=pool_addr1,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token1,
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1,
    )

    # 2. Swap arrives at t_0 + 30s (< 90s min age) -> Cannot graduate yet
    swap_early = SwapEvent(
        timestamp_ns=int((t_0 + 30.0) * 1e9),
        block_number=1,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr1,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token1,
        amount_in=Decimal("2.0"),
        amount_out=Decimal("50000000"),
        sender="Buyer11111111111111111111111111111",
        tx_hash="txEarly111111111111111111111111111",
    )
    sig = buffer.record_swap(swap_early, pool, sig_gen, current_time_s=t_0 + 30.0)
    assert sig is None
    assert staged.buy_count == 1
    assert staged.total_volume_native == Decimal("2.0")

    # 3. Test Dump Filter: price dumps > 30% within first 90s -> Dropped immediately!
    dumped_pool = PoolState(
        pool_address=pool_addr1,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token1,
        native_reserve=Decimal("15.0"),  # Reserve collapsed
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=2,
    )
    # price drops from 0.000000028 to 0.000000014 (>30% drop)
    sig_dump = buffer.record_swap(swap_early, dumped_pool, sig_gen, current_time_s=t_0 + 60.0)
    assert sig_dump is None
    assert staged.dropped is True
    assert buffer.is_staged(token1) is False

    # 4. Fresh Launch: successful graduation after 90s with >= 15 buys and >= 15 SOL
    token2 = "PumpMint222222222222222222222222222222222"
    pool_addr2 = "CurveAddr22222222222222222222222222222"
    report2 = SecurityReport(
        token_address=token2,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig2 = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token2,
        pool_address=pool_addr2,
        source=SignalSource.PUMP_FUN_MINT,
    )
    buffer.add_launch(
        token_address=token2,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr2,
        initial_price=init_price,
        report=report2,
        raw_signal=raw_sig2,
        t_0=t_0,
    )
    staged2 = buffer.get_staged(token2)
    assert staged2 is not None

    # Simulate 14 buys of 1.0 SOL
    for i in range(14):
        sw = SwapEvent(
            timestamp_ns=int((t_0 + 95.0) * 1e9),
            block_number=i + 1,
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool_address=pool_addr2,
            token_in="So11111111111111111111111111111111111111112",
            token_out=token2,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("20000000"),
            sender=f"Buyer{i:028d}",
            tx_hash=f"tx_{i:030d}",
        )
        buffer.record_swap(sw, pool, sig_gen, current_time_s=t_0 + 95.0)

    assert staged2.buy_count == 14
    assert staged2.graduated is False

    # 15th buy brings volume to 16.0 SOL (> 15 SOL threshold) at age 100s (>= 90s)
    sw_15 = SwapEvent(
        timestamp_ns=int((t_0 + 100.0) * 1e9),
        block_number=15,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr2,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token2,
        amount_in=Decimal("2.0"),
        amount_out=Decimal("40000000"),
        sender="Buyer1511111111111111111111111111",
        tx_hash="tx_15111111111111111111111111111",
    )
    grad_sig = buffer.record_swap(sw_15, pool, sig_gen, current_time_s=t_0 + 100.0)
    assert grad_sig is not None
    assert grad_sig.suggested_side == OrderSide.BUY
    assert grad_sig.token_address == token2
    assert staged2.graduated is True


def test_dynamic_trailing_stop_activation_at_50_pct():
    """Task 3: Verify trailing stop activation at +50% ROI locks stop at +30% and trails peak by dynamic drawdown."""
    book = PositionBook()
    lot = OpenLot(
        token_address="TokenPumpTrail",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000"),
        tokens_held=Decimal("1000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("1000"),
        bonding_curve_mode=True,
        exit_stage=ExitStage.TP2,  # TP scaled out, trailing stop manages remaining lot
    )
    now = time.time()

    # 1. Price climbs to +50% (1.50) -> Activates trailing stop and locks baseline at +30% (1.30)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.50"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert decision is None
    assert lot.trailing_stop_active is True
    assert lot.trailing_stop_price >= Decimal("1.30")
    assert lot.peak_pnl == Decimal("0.50")

    # 2. Price climbs to new peak +80% (1.80) -> Trails 12% drop from peak (1.80 * 0.88 = 1.584)
    book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.80"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert lot.peak_price == Decimal("1.80")
    assert lot.trailing_stop_price == Decimal("1.584").quantize(Decimal("1e-18"))

    # 3. Price drops below trailing stop to 1.55 (< 1.584) -> Auto-liquidates
    exit_dec = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.55"),
        tick_timestamp_s=now,
        bonding_curve_mode=True,
    )
    assert exit_dec is not None
    assert exit_dec.should_exit is True
    assert exit_dec.exit_reason == TradeExitReason.SL_TRAILING
    assert exit_dec.exit_stage == ExitStage.TRAILING_SL


@pytest.mark.anyio
async def test_telegram_trades_dynamic_pnl_and_state_indicators():
    """Task 4: Verify Telegram /trades formatting eliminates scientific notation, shows state indicators, and reflects exact duration."""
    from alpha_engine.ingestion.telegram import TelegramIngester

    event_q: asyncio.Queue = asyncio.Queue()
    ingester = TelegramIngester(event_queue=event_q, admin_ids=[999])

    entry_price = Decimal("0.00000002865")
    current_price = Decimal("0.000000042975")  # exact +50% gain
    now_ns = time.time_ns()

    status_data = {
        "open_trades": [
            {
                "token_address": "TokenMicro11111111111111111111111111111111",
                "chain": ChainIdentifier.SOLANA_MAINNET,
                "entry_price": entry_price,
                "current_price": current_price,
                "unrealized_pnl_usd": 15.50,
                "unrealized_pnl_pct": 50.0,
                "position_value_usd": 42.97,
                "open_timestamp_ns": now_ns - int(75 * 1e9),  # 75s ago
                "trailing_stop_status": "LOCKED (+30%)",
                "tokens_held": Decimal("1000000000"),
            }
        ]
    }
    ingester.set_status_provider(lambda: status_data)

    mock_event = MagicMock()
    replies = []
    async def mock_reply(msg):
        replies.append(msg)
    mock_event.reply = AsyncMock(side_effect=mock_reply)

    await ingester._cmd_trades(mock_event)

    assert len(replies) == 1
    text = replies[0]

    # No exponential notation like e-08
    assert "e-08" not in text
    assert "e-" not in text

    # Subscript notation or clean formatting
    assert "$0.0₇2.865" in text or "0.00000002865" in text

    # Color coded indicator for in profit
    assert "🟢" in text

    # Exact duration (~75s)
    assert "75s" in text

    # Dollar valuation of position displayed
    assert "Value: $42.970" in text

    # PnL % and Trailing status
    assert "+50.00%" in text
    assert "LOCKED (+30%)" in text


def test_signal_source_dynamic_exit():
    """Verify SignalSource.DYNAMIC_EXIT exists and is accepted by TradeReflection."""
    from alpha_engine.engine.feedback import TradeReflection
    from alpha_engine.models.enums import SignalSource

    assert hasattr(SignalSource, "DYNAMIC_EXIT")
    assert SignalSource.DYNAMIC_EXIT == "dynamic_exit"

    reflection = TradeReflection.from_trade(
        trade_id="exit_order_123",
        token_address="TokenExit11111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        signal_source=SignalSource.DYNAMIC_EXIT,
        entry_price=Decimal("1.0"),
        exit_price=Decimal("1.25"),
        realized_pnl_usd=Decimal("25.0"),
        realized_pnl_native=Decimal("0.15"),
        time_to_fill_ms=12.5,
    )
    assert reflection.signal_source == SignalSource.DYNAMIC_EXIT
    assert reflection.is_win is True



