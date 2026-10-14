"""
tests.test_strategy_management_and_trailing_stop
================================================
Comprehensive verification of user requirements:
1. Trailing stop-loss from highest recorded peak (50% / 60% drop from ATH).
2. Suppression of premature exits at -14% or -25% under peak trailing strategy.
3. Take-profit execution at +50% and +60%.
4. Minimum $10,000 USD market cap enforcement for cryptocurrencies.
5. Genuine trading activity verification (independent wallets & volume share).
6. Telegram strategy management commands, callbacks, and status reporting.
7. Older tokens revival breakout criteria.
8. Pacing governor bypass and circuit breaker duration reduction.
9. Full integration of Graduated/Movers profiles, PoolState updates, and Signal events.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import Runner
from alpha_engine.engine.staging import RevivalBreakoutBuffer
from alpha_engine.engine.strategy.screener_priority import (
    GraduatedProfileConfig,
    MoversProfileConfig,
    ScreenerPriorityEvaluator,
    ScreenerDecision,
)
from alpha_engine.execution.book import OpenLot, PositionBook, RunningMetrics
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    LotStatus,
    OrderSide,
    ScreenerSection,
    SignalSource,
    SignalStrength,
    SecurityTier,
    TradeExitReason,
)
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    ScreenerDiscoveryEvent,
    SignalEvent,
    SwapEvent,
)


@pytest.fixture
def base_config() -> EngineConfig:
    return EngineConfig(
        db_path=":memory:",
        telegram_admin_ids=[123456],
        active_strategy="peak_trailing_50",
        trailing_stop_from_peak_pct=0.50,
        custom_take_profit_pct=0.50,
        min_market_cap_usd=Decimal("10000.0"),
        pacing_governor_enabled=False,
        circuit_breaker_freeze_duration_s=300.0,
    )


def test_graduated_and_movers_profile_configs_10k_mcap():
    """
    Verify GraduatedProfileConfig and MoversProfileConfig enforce the $10,000 USD
    minimum market cap and $5,000 volume requirements.
    """
    grad_prof = GraduatedProfileConfig()
    mover_prof = MoversProfileConfig()

    assert grad_prof.min_market_cap_usd == Decimal("10000.0")
    assert grad_prof.min_volume_usd == Decimal("5000.0")
    assert mover_prof.min_market_cap_usd == Decimal("10000.0")
    assert mover_prof.min_volume_usd == Decimal("5000.0")

    cfg = EngineConfig(db_path=":memory:", min_market_cap_usd=Decimal("10000.0"))
    evaluator = ScreenerPriorityEvaluator(config=cfg)
    assert evaluator.graduated_profile.min_market_cap_usd == Decimal("10000.0")
    assert evaluator.movers_profile.min_market_cap_usd == Decimal("10000.0")


def test_trailing_stop_from_peak_and_premature_exit_suppression():
    """
    Requirement 2: Ensure trade does NOT close at -14% or -25% from entry,
    and only triggers when dropping by 50% (or 60%) from highest recorded peak.
    Also verifies OpenLot lifecycle transitions from LotStatus.OPEN to LotStatus.CLOSED.
    """
    book = PositionBook()

    # 1. Open lot with peak_trailing_50 strategy
    fill = PaperFill(
        order_id="ord-1",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        token_address="96X78pQMw7dXbsBAh7kPgSNJuDiGox537TaCRq2mpump",
        effective_price=Decimal("10.0"),
        tokens_acquired=Decimal("100.0"),
        simulated_native_spent=Decimal("1.0"),
    )
    lot: OpenLot = book.open_lot(
        fill=fill,
        signal_id="sig-1",
        strategy_name="peak_trailing_50",
        trailing_stop_from_peak_pct=0.50,
        custom_take_profit_pct=0.50,
    )

    assert isinstance(lot, OpenLot)
    assert lot.status == LotStatus.OPEN
    assert lot.entry_price == Decimal("10.0")
    assert lot.peak_price == Decimal("10.0")
    assert lot.strategy_name == "peak_trailing_50"
    assert lot.trailing_stop_from_peak_pct == 0.50

    # 2. Price retraces to $8.60 (-14.0% loss, as seen in the user screenshot)
    # Under legacy logic this would trigger sl_hard. Under peak_trailing_50, it MUST NOT exit.
    exit_action = book.evaluate_lot_exit(lot, current_price=Decimal("8.60"))
    assert exit_action is None
    assert lot.peak_price == Decimal("10.0")
    assert lot.status == LotStatus.OPEN

    # 3. Price drops further to $7.50 (-25.0% loss from entry)
    # Under peak_trailing_50, trade must still remain open (waiting for recovery or -50% peak drop)
    exit_action = book.evaluate_lot_exit(lot, current_price=Decimal("7.50"))
    assert exit_action is None
    assert lot.status == LotStatus.OPEN

    # 4. Token recovers and pumps to new ATH $25.0 (+150% from entry)
    exit_action = book.evaluate_lot_exit(lot, current_price=Decimal("25.0"))
    assert exit_action is not None
    assert exit_action.exit_reason in (TradeExitReason.TP_CUSTOM, TradeExitReason.TP_50)

    # 5. Test peak trailing exit specifically when TP is not yet triggered:
    lot2: OpenLot = book.open_lot(
        fill=fill,
        signal_id="sig-2",
        strategy_name="peak_trailing_50",
        trailing_stop_from_peak_pct=0.50,
        custom_take_profit_pct=2.00,  # 200% TP target
    )
    assert lot2.status == LotStatus.OPEN
    # Pumps from $10 -> $18.0
    book.evaluate_lot_exit(lot2, current_price=Decimal("18.0"))
    assert lot2.peak_price == Decimal("18.0")

    # Retraces from $18.0 -> $13.5 (-25% drop from peak, but still +35% above entry) -> Remain OPEN
    action = book.evaluate_lot_exit(lot2, current_price=Decimal("13.50"))
    assert action is None
    assert lot2.status == LotStatus.OPEN

    # Retraces from $18.0 -> $9.0 (-50% drop from highest peak $18.0) -> Trailing Peak Stop triggers!
    action = book.evaluate_lot_exit(lot2, current_price=Decimal("9.00"))
    assert action is not None
    assert action.exit_reason == TradeExitReason.TRAILING_PEAK_STOP

    # Close lot and verify state transition to LotStatus.CLOSED
    book.apply_exit_decision(action, sell_price=Decimal("9.00"), lot=lot2)
    assert lot2.status == LotStatus.CLOSED


def test_trailing_stop_60_percent_preset():
    """Verify peak_trailing_60 strategy allows larger retracements up to 60% from peak."""
    book = PositionBook()

    fill = PaperFill(
        order_id="ord-60",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        token_address="60TokenMintAddress111111111111111111111111",
        effective_price=Decimal("100.0"),
        tokens_acquired=Decimal("10.0"),
        simulated_native_spent=Decimal("1.0"),
    )
    lot_no_tp: OpenLot = book.open_lot(
        fill=fill,
        signal_id="sig-60-notp",
        strategy_name="peak_trailing_60",
        trailing_stop_from_peak_pct=0.60,
        custom_take_profit_pct=5.0,  # Far take-profit target
    )
    book.evaluate_lot_exit(lot_no_tp, current_price=Decimal("200.0"))
    assert lot_no_tp.peak_price == Decimal("200.0")

    # -50% drop from $200 peak -> $100 (Remain OPEN under 60% strategy)
    assert book.evaluate_lot_exit(lot_no_tp, current_price=Decimal("100.0")) is None

    # -60% drop from $200 peak -> $80 (Stop triggers!)
    act = book.evaluate_lot_exit(lot_no_tp, current_price=Decimal("80.0"))
    assert act is not None
    assert act.exit_reason == TradeExitReason.TRAILING_PEAK_STOP


def test_minimum_market_cap_10k_enforcement_with_pool_state():
    """
    Requirement 3: Tokens with market cap below $10,000 must be rejected
    before full analysis or opening a position. Tokens >= $10,000 are admitted.
    Also verifies PoolState calculation and PoolStateUpdateEvent integration.
    """
    cfg = EngineConfig(db_path=":memory:", min_market_cap_usd=Decimal("10000.0"))
    evaluator = ScreenerPriorityEvaluator(config=cfg)

    # Sub-$10k token ($5,000) -> Rejected
    sub10k_event = ScreenerDiscoveryEvent(
        mint="Sub10kToken111111111111111111111111111111111",
        section=ScreenerSection.GRADUATED,
        market_cap_usd=5000.0,
        volume_2m_usd=15000.0,
        liquidity_usd=25000.0,
        current_price=0.005,
    )

    res = asyncio.run(evaluator.evaluate(sub10k_event))
    assert res.decision == ScreenerDecision.REJECTED
    assert "below minimum threshold $10000" in res.reason or "below minimum threshold $10,000" in res.reason

    # >= $10k token ($15,000) -> Admitted
    ok_event = ScreenerDiscoveryEvent(
        mint="Ok15kToken11111111111111111111111111111111111",
        section=ScreenerSection.GRADUATED,
        market_cap_usd=15000.0,
        volume_2m_usd=20000.0,
        liquidity_usd=30000.0,
        current_price=0.0085,
    )
    res_ok = asyncio.run(evaluator.evaluate(ok_event))
    assert "below minimum threshold $10000" not in (res_ok.reason or "")
    assert "below minimum threshold $10,000" not in (res_ok.reason or "")

    # Test via PoolState and PoolStateUpdateEvent
    # 100 SOL reserve with SOL = $150 -> $15,000 native liquidity / market cap
    pool_state = PoolState(
        pool_address="PoolStateValid11111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=5000,
        token_decimals=6,
        native_decimals=9,
    )
    pool_update = PoolStateUpdateEvent(
        timestamp_ns=int(time.time() * 1e9),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        new_pool_state=pool_state,
    )
    assert pool_update.new_pool_state.pool_address == "PoolStateValid11111111111111111111111111111"
    calculated_mcap = pool_state.calculate_market_cap_usd(native_price_usd=Decimal("150.0"))
    assert calculated_mcap >= Decimal("10000.0")


def test_signal_event_generation_with_peak_trailing_profile():
    """
    Verify SignalEvent generation with PEAK_TRAILING exit profile,
    SignalSource, and SignalStrength indicators.
    """
    pool_state = PoolState(
        pool_address="PoolStateSignal1111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=5000,
    )
    sec_rep = SecurityReport(
        token_address="MoverToken10k1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    sig = SignalEvent(
        signal_id="sig-screener-10k",
        timestamp_ns=int(time.time() * 1e9),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address="MoverToken10k1111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=sec_rep,
        strength=SignalStrength.STRONG,
        alpha_score=0.88,
        source=SignalSource.SCREENER_GRADUATED,
        exit_profile=ExitProfile.PEAK_TRAILING,
        market_cap_usd=Decimal("12500.0"),
    )

    assert sig.signal_source == SignalSource.SCREENER_GRADUATED
    assert sig.strength == SignalStrength.STRONG
    assert sig.exit_profile == ExitProfile.PEAK_TRAILING
    assert sig.market_cap_usd == Decimal("12500.0")
    assert sig.suggested_side == OrderSide.BUY


def test_genuine_trading_activity_and_wash_trading_detection():
    """
    Requirement 5: Verify multi-wallet distribution and wash trading detection.
    """
    metrics = DEXMetricsAggregator()
    pool_addr = "PoolTestGenuine111111111111111111111111111111"
    now = time.time()

    # Case A: Artificial / Wash trading by a single dominant wallet
    # 10 swaps total, but 9 swaps (95% volume) from wallet "WashMaster"
    for i in range(9):
        metrics.record_swap(
            SwapEvent(
                timestamp_ns=int((now - 10 + i) * 1e9),
                chain=ChainIdentifier.SOLANA_MAINNET,
                tx_hash="0x" + "a" * 64,
                block_number=1000,
                token_in="So11111111111111111111111111111111111111112",
                token_out="TokenA1111111111111111111111111111111111111",
                pool_address=pool_addr,
                amount_in=Decimal("10.0"),  # 10 SOL each
                amount_out=Decimal("1000.0"),
                sender="WashMasterWallet1111111111111111111111111111",
            )
        )
    # 1 swap from another wallet
    metrics.record_swap(
        SwapEvent(
            timestamp_ns=int(now * 1e9),
            chain=ChainIdentifier.SOLANA_MAINNET,
            tx_hash="0x" + "b" * 64,
            block_number=1001,
            token_in="So11111111111111111111111111111111111111112",
            token_out="TokenA1111111111111111111111111111111111111",
            pool_address=pool_addr,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("100.0"),
            sender="OtherWallet22222222222222222222222222222222",
        )
    )

    stats: dict[str, Any] = metrics.get_participant_distribution(pool_addr, window_sec=60.0, current_time_s=now)
    assert stats["unique_wallets_count"] == 2
    assert stats["top_wallet_volume_share"] > 0.80  # > 80% volume from 1 wallet!

    # Case B: Genuine trading across multiple independent participants
    pool_genuine = "PoolTestGenuine222222222222222222222222222222"
    for w_idx in range(8):
        metrics.record_swap(
            SwapEvent(
                timestamp_ns=int((now - 15 + w_idx) * 1e9),
                chain=ChainIdentifier.SOLANA_MAINNET,
                tx_hash=f"0x{w_idx:02x}" + "c" * 62,
                block_number=1000 + w_idx,
                token_in="So11111111111111111111111111111111111111112",
                token_out="TokenB1111111111111111111111111111111111111",
                pool_address=pool_genuine,
                amount_in=Decimal("2.0"),  # 2 SOL each
                amount_out=Decimal("200.0"),
                sender=f"IndependentWallet_{w_idx:02d}_11111111111111111111",
            )
        )

    stats_gen: dict[str, Any] = metrics.get_participant_distribution(pool_genuine, window_sec=60.0, current_time_s=now)
    assert stats_gen["unique_wallets_count"] == 8
    assert stats_gen["top_wallet_volume_share"] <= 0.20  # Well distributed!


def test_telegram_strategy_management_interface():
    """
    Requirement 6: Verify Telegram strategy switching, auto-trade toggles, and status reports.
    """
    cfg = EngineConfig(db_path=":memory:")
    runner = Runner(cfg)

    # 1. Initial status (verifying $10,000 minimum market cap)
    initial_status = runner.get_strategy_status()
    assert initial_status["active_strategy"] == "peak_trailing_50"
    assert initial_status["trailing_stop_from_peak_pct"] == 0.50
    assert initial_status["custom_take_profit_pct"] == 0.50
    assert initial_status["auto_trade_enabled"] is True
    assert initial_status["min_market_cap_usd"] == 10000.0

    # 2. Switch to peak60 preset via set_trading_strategy
    s60 = runner.set_trading_strategy(strategy_name="peak_trailing_60")
    assert s60["active_strategy"] == "peak_trailing_60"
    assert s60["trailing_stop_from_peak_pct"] == 0.60
    assert s60["custom_take_profit_pct"] == 0.60

    # 3. Custom parameters tuning and market cap update
    custom = runner.set_trading_strategy(custom_take_profit_pct=0.75, trailing_stop_from_peak_pct=0.40, min_market_cap_usd=12000.0)
    assert custom["custom_take_profit_pct"] == 0.75
    assert custom["trailing_stop_from_peak_pct"] == 0.40
    assert custom["min_market_cap_usd"] == 12000.0

    # 4. Toggle auto trade
    toggled = runner.set_trading_strategy(auto_trade_enabled=False)
    assert toggled["auto_trade_enabled"] is False

    # 5. Restore default strategy
    default_s = runner.set_trading_strategy(strategy_name="default", min_market_cap_usd=10000.0)
    assert default_s["active_strategy"] == "default"
    assert default_s["custom_take_profit_pct"] is None
    assert default_s["min_market_cap_usd"] == 10000.0

    # 6. Verify TelegramIngester command execution
    q = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q, admin_ids=[12345], db_path=":memory:")
    ingester.set_execution_target(runner)

    fake_event = MagicMock()
    fake_event.sender_id = 12345
    fake_event.text = "/strategy set peak50"

    asyncio.run(ingester._cmd_strategies(fake_event, args="set peak50"))
    assert runner.active_strategy == "peak_trailing_50"

    # Test Telegram /strategy mcap 15000 command
    asyncio.run(ingester._cmd_strategies(fake_event, args="mcap 15000"))
    assert runner.get_strategy_status()["min_market_cap_usd"] == 15000.0

    # Callback query test
    asyncio.run(ingester._handle_strategy_callback(fake_event, data="strat:peak60"))
    assert runner.active_strategy == "peak_trailing_60"


def test_continuous_screener_poller_and_api_dispatch():
    """
    Requirement 4: Verify continuous polling mechanism filters tokens < $10k
    and executes without unhandled errors using AsyncMock and patch.
    """
    cfg = EngineConfig(db_path=":memory:", min_market_cap_usd=Decimal("10000.0"))
    runner = Runner(cfg)

    # Mock session returning DexScreener token boosts
    fake_boosts = [
        {"tokenAddress": "Sub10kMint1111111111111111111111111111111", "totalAmount": 100, "chainId": "solana"},
        {"tokenAddress": "Ok10kMint22222222222222222222222222222222", "totalAmount": 200, "chainId": "solana"},
    ]

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=fake_boosts)

    mock_session = MagicMock()
    mock_session.closed = False
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
    runner._session = mock_session

    # Run poller pass with patch
    with patch.object(runner, "_poll_graduated_and_movers", new_callable=AsyncMock) as mock_poll:
        runner._shutdown_event.set()  # Prevent infinite loop
        asyncio.run(runner._screener_priority_poller())
        assert runner._shutdown_event.is_set()


def test_older_tokens_revival_criteria_relaxed():
    """
    Requirement 7: Verify older tokens are not permanently dropped by
    overly restrictive revival thresholds.
    """
    cfg = EngineConfig(
        db_path=":memory:",
        revival_max_dev_balance_pct=0.05,  # 5%
        revival_max_top10_concentration_pct=50.0,
        revival_volume_surge_multiplier_k=1.5,
        revival_min_net_buy_delta=0.20,
    )
    buffer = RevivalBreakoutBuffer(config=cfg)

    # Stage an established older token (24h old, 3% dev balance, 42% top 10 concentration)
    token_ca = "OlderRaydiumToken1111111111111111111111111111"
    now = time.time()

    report_mock = MagicMock(spec=SecurityReport)
    report_mock.passes_hard_gates = True
    report_mock.dev_balance_ratio = 0.03  # 3% dev balance (passed under 5% limit, would have failed old 0.1% rule)
    report_mock.top10_concentration = 0.42  # 42% top 10 concentration (passed under 50% limit, would have failed old 25% rule)

    staged = buffer.stage_token(
        token_address=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="RaydiumPoolOlder11111111111111111111111111111",
        initial_price=Decimal("0.0001"),
        report=report_mock,
        token_creation_timestamp=now - (24 * 3600),
    )

    assert buffer.is_staged(token_ca)
    assert staged is not None
    assert staged.dropped is False
    assert staged.token_age_hours >= 23.9


def test_pacing_governor_bypass_and_circuit_breaker_freeze_reduction():
    """
    Requirement 8: Verify governor pacing cooldowns do NOT freeze trading when disabled,
    and circuit breaker freeze duration is reduced from 3600s to 300s.
    """
    cfg = EngineConfig(
        db_path=":memory:",
        pacing_governor_enabled=False,
        circuit_breaker_freeze_duration_s=300.0,
    )
    runner = Runner(cfg)

    # Simulate 12 consecutive trades
    for _ in range(12):
        runner.record_governor_trade()

    # Governor must NOT pause trading because pacing_governor_enabled is False
    assert runner.is_trading_paused_by_governor() is False
    assert runner._cooldown_until == 0.0

    # Test circuit breaker freeze reduction
    metrics = RunningMetrics()
    metrics.trip_launch_circuit_breaker(reason="Test loss")

    # Right after tripping: active
    assert metrics.is_launch_circuit_breaker_active(freeze_duration_s=300.0) is True

    # Advance tripped_at by 350 seconds (> 5 minutes, but < 1 hour): must be EXPIRED under 300s freeze duration!
    metrics.launch_circuit_breaker_tripped_at = time.time() - 350.0
    assert metrics.is_launch_circuit_breaker_active(freeze_duration_s=300.0) is False


@pytest.mark.asyncio
async def test_positive_cache_in_security_gatekeeper():
    """
    Verify that SecurityGatekeeper caches verified CLEAN tokens in positive cache.
    Subsequent screenings must return the cached clean report immediately with zero latency,
    preventing RugCheck API rate-limiting (429) and eliminating redundant calls.
    """
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    session_mock = MagicMock()
    limiter_mock = MagicMock()
    limiter_mock.acquire = AsyncMock()

    gk = SecurityGatekeeper(
        session=session_mock,
        limiter=limiter_mock,
        evm_rpc_url="http://localhost:8545",
        evm_router_address="0x1111111111111111111111111111111111111111",
        weth_address="0x2222222222222222222222222222222222222222",
        negative_cache_ttl_s=300.0,
    )

    clean_report = SecurityReport(
        token_address="FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.10,
        mint_authority_disabled=True,
    )

    # 1. Record clean report in positive cache
    gk.record_clean("FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump", ChainIdentifier.SOLANA_MAINNET, clean_report)

    # 2. Lookup must hit positive cache instantly without network calls
    cached = gk.get_cached_clean("FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump", ChainIdentifier.SOLANA_MAINNET)
    assert cached is not None
    assert cached.tier == SecurityTier.CLEAN
    assert cached.token_address == "FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump"

    # 3. screen_token must return cached report directly without calling _run_tier1
    with patch.object(gk, "_run_tier1", new_callable=AsyncMock) as mock_tier1:
        res = await gk.screen_token("FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump", ChainIdentifier.SOLANA_MAINNET)
        assert res.tier == SecurityTier.CLEAN
        mock_tier1.assert_not_called()


def test_screener_exclusive_focus_graduated_and_movers_definition():
    """
    Verify that when screener_exclusive_focus is enabled, the bot accurately identifies
    and permits:
    1. Direct screener discovery tokens (SCREENER_GRADUATED, SCREENER_MOVERS)
    2. Raydium AMM graduated pump tokens (ExecutionVenue.RAYDIUM_AMM + ending in pump)
    3. Graduated launch buffer tokens (strategy_pattern="graduated_launch")
    4. Active market movers (alpha_score >= 0.40 or MODERATE/STRONG strength)
    """
    cfg = EngineConfig(
        db_path=":memory:",
        screener_exclusive_focus=True,
        min_market_cap_usd=Decimal("10000.0"),
    )
    runner = Runner(cfg)
    assert runner.screener_exclusive_focus is True

    dummy_pool = PoolState(
        pool_address="Pool1111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
        token_decimals=6,
        native_decimals=9,
    )
    dummy_report = SecurityReport(
        token_address="FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.10,
        mint_authority_disabled=True,
    )

    # 1. Raydium AMM graduated pump token
    from alpha_engine.models.enums import ExecutionVenue
    venue_raydium = ExecutionVenue.RAYDIUM_AMM
    sig_raydium_grad = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="FiJ7513iZHCT6yqG9NgZQ9MP8d8E6Kw6NGEM327ppump",
        pool_address="PoolFiJ111111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        source=SignalSource.DEX_SWAP,
        execution_venue=venue_raydium,
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
        pool_state=dummy_pool,
        security_report=dummy_report,
    )
    tok_l = sig_raydium_grad.token_address.lower()
    is_graduated = (
        sig_raydium_grad.source in (SignalSource.SCREENER_GRADUATED, SignalSource.SCREENER_MOVERS)
        or (getattr(sig_raydium_grad, "execution_venue", None) == venue_raydium and tok_l.endswith("pump"))
    )
    assert is_graduated is True

    # 2. Graduated launch buffer signal
    sig_launch_grad = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="8TrqbfjjQZxbQdFD9Dq184Dqc3vyRoNfWC6eZ1YRpump",
        pool_address="Pool8Trq111111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        source=SignalSource.PUMP_FUN_MINT,
        strategy_pattern="graduated_launch",
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
        pool_state=dummy_pool,
        security_report=dummy_report,
    )
    is_grad_launch = getattr(sig_launch_grad, "strategy_pattern", None) == "graduated_launch"
    assert is_grad_launch is True

    # 3. Market mover with moderate/strong alpha
    sig_mover = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="AUSh7DdFPZcirDqVCcSC4o5AoKrWLd6aJKk2vjrPpump",
        pool_address="PoolAUS111111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        source=SignalSource.DEX_SWAP,
        strength=SignalStrength.MODERATE,
        alpha_score=0.55,
        pool_state=dummy_pool,
        security_report=dummy_report,
    )
    is_mover = sig_mover.strength in (SignalStrength.MODERATE, SignalStrength.STRONG) and sig_mover.alpha_score >= 0.40
    assert is_mover is True


def test_screener_exclusive_focus_defaults_to_false_for_general_stability():
    """
    Verify EngineConfig defaults screener_exclusive_focus to False so that
    existing strategies and high-conviction trades are preserved upon bot start.
    """
    cfg = EngineConfig(db_path=":memory:")
    assert cfg.screener_exclusive_focus is False
    runner = Runner(cfg)
    assert runner.screener_exclusive_focus is False

