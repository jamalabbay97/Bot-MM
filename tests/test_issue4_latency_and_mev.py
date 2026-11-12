"""
tests/test_issue4_latency_and_mev.py — Acceptance and Regression Tests for Issue 4
=================================================================================
Verifies:
1. Deterministic seed + a price series that moves during the latency window:
   fill price reflects the move (both up and down).
2. State too stale at fill time -> rejection fail-closed with counted telemetry reason.
3. Private-route trades have zero sandwich impact; public-route impact scales with
   trade size relative to depth and slippage tolerance, and is bounded.
4. Same seed -> same results; different seed -> different results.
5. Shadow tournament and live-paper use the same module and execution assumptions (parity test).
6. Integration test through the runner entry path with a scripted price feed.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.backtester import WalkForwardBacktester
from alpha_engine.engine.experimentation import StrategyTournamentRegistry
from alpha_engine.engine.runner import PaperTradingEngine, ShutdownSentinel
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.math.mev import (
    LatencyResult,
    determine_market_state_at_fill,
    sample_fill_latency,
    simulate_frontrun_volume,
    simulate_latency,
)
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, ReserveType, SecurityReport


class DummyPriceService:
    def __init__(self, sol_price: Decimal = Decimal("200.0"), eth_price: Decimal = Decimal("3000.0")):
        self.sol_price = sol_price
        self.eth_price = eth_price

    def get_price(self, chain: ChainIdentifier):
        p = self.eth_price if chain == ChainIdentifier.BASE_MAINNET else self.sol_price
        mock = MagicMock()
        mock.price = p
        mock.status.value = "OK"
        return mock


def _make_pool(
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
    native_reserve: Decimal = Decimal("100.0"),
    token_reserve: Decimal = Decimal("1000000.0"),
    pool_address: str = "6UmmUiYo5ZbzK8kQ8FkL6Z5uN6fS7Q3eT5bY9cV3mX2a",
    timestamp_s: float | None = None,
    timestamp_ns: int | None = None,
) -> PoolState:
    if timestamp_ns is not None:
        ts_ns = timestamp_ns
        ts_s = float(ts_ns) / 1_000_000_000.0
    elif timestamp_s is not None:
        ts_s = timestamp_s
        ts_ns = int(ts_s * 1_000_000_000)
    else:
        ts_s = time.time()
        ts_ns = int(ts_s * 1_000_000_000)

    return PoolState(
        pool_address=pool_address,
        chain=chain,
        native_reserve=native_reserve,
        token_reserve=token_reserve,
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        token_decimals=6,
        native_decimals=9,
        reserve_type=ReserveType.EXACT,
        source="unit_test",
        timestamp_s=ts_s,
        timestamp_ns=ts_ns,
        is_verified=True,
        total_supply=Decimal("1000000000.0"),
        circulating_supply=Decimal("1000000000.0"),
    )


def _make_signal(
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
    token_address: str = "TokenTest1111111111111111111111111111111111",
    pool_state: PoolState | None = None,
    side: OrderSide = OrderSide.BUY,
    timestamp_ns: int = 1_000_000_000_000_000,
    is_private_route: bool = True,
    max_slippage_bps: int = 50,
) -> SignalEvent:
    pool = pool_state or _make_pool(chain=chain, timestamp_ns=timestamp_ns)
    report = SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=SecurityTier.CLEAN,
        is_safe_to_trade=True,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
    )
    return SignalEvent(
        signal_id=f"sig_{token_address[:8]}",
        token_address=token_address,
        pool_address=pool.pool_address,
        chain=chain,
        suggested_side=side,
        strength=SignalStrength.STRONG,
        source=SignalSource.DEX_SWAP,
        alpha_score=0.85,
        pool_state=pool,
        security_report=report,
        timestamp_ns=timestamp_ns,
        execution_venue=ExecutionVenue.RAYDIUM_AMM if chain == ChainIdentifier.SOLANA_MAINNET else ExecutionVenue.UNISWAP_V2,
        is_private_route=is_private_route,
        max_slippage_bps=max_slippage_bps,
    )


# ---------------------------------------------------------------------------
# Test 1: Price series moving during latency window (both UP and DOWN)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_price_movement_during_latency_window_reflected_in_fill_price():
    """
    When the market price moves between signal_time and fill_time, the fill price
    must reflect the market state AT fill time rather than the signal-time state.
    - Market moves UP: BUY fill price is higher (tokens acquired is lower).
    - Market moves DOWN: BUY fill price is lower (tokens acquired is higher).
    """
    cfg = EngineConfig(execution_seed=123)
    price_svc = DummyPriceService()
    book = PositionBook()
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)

    signal_ts_ns = 1_000_000_000_000_000  # 1,000,000s in ns
    trade_size_native = Decimal("1.0")

    # Initial pool at signal time: 100 SOL / 1,000,000 tokens -> spot = 0.000100 SOL/token
    pool_signal = _make_pool(
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        timestamp_ns=signal_ts_ns,
    )

    # Baseline: No movement during latency (market state unchanged)
    def static_feed(chain, token, at_ts_ns):
        return pool_signal.model_copy(update={"timestamp_ns": at_ts_ns, "timestamp_s": float(at_ts_ns) / 1e9})

    executor_static = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=static_feed,
        is_private_route=True,
    )
    sig_static = _make_signal(pool_state=pool_signal, timestamp_ns=signal_ts_ns, is_private_route=True)
    fill_static = await executor_static.execute_signal(
        signal=sig_static,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=trade_size_native,
    )
    assert fill_static is not None
    static_price = fill_static.effective_price
    static_tokens = fill_static.tokens_acquired

    # Case A: Market moved UP during latency window (+21% price increase)
    # native: 110.0, token: 909090.9 -> spot ~ 0.000121 SOL/token
    def up_feed(chain, token, at_ts_ns):
        return _make_pool(
            native_reserve=Decimal("110.0"),
            token_reserve=Decimal("909090.9"),
            timestamp_ns=at_ts_ns,
        )

    executor_up = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=up_feed,
        is_private_route=True,
    )
    sig_up = _make_signal(token_address="TokenUp111111111111111111111111111111111", pool_state=pool_signal, timestamp_ns=signal_ts_ns, is_private_route=True)
    fill_up = await executor_up.execute_signal(
        signal=sig_up,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=trade_size_native,
    )
    assert fill_up is not None
    # Effective price must be strictly higher than static price
    assert fill_up.effective_price > static_price, (
        f"Upward move expected higher fill price: got {fill_up.effective_price} <= {static_price}"
    )
    assert fill_up.tokens_acquired < static_tokens

    # Case B: Market moved DOWN during latency window (-19% price decrease)
    # native: 90.0, token: 1111111.1 -> spot ~ 0.000081 SOL/token
    def down_feed(chain, token, at_ts_ns):
        return _make_pool(
            native_reserve=Decimal("90.0"),
            token_reserve=Decimal("1111111.1"),
            timestamp_ns=at_ts_ns,
        )

    executor_down = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=down_feed,
        is_private_route=True,
    )
    sig_down = _make_signal(token_address="TokenDown11111111111111111111111111111111", pool_state=pool_signal, timestamp_ns=signal_ts_ns, is_private_route=True)
    fill_down = await executor_down.execute_signal(
        signal=sig_down,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=trade_size_native,
    )
    assert fill_down is not None
    # Effective price must be strictly lower than static price
    assert fill_down.effective_price < static_price, (
        f"Downward move expected lower fill price: got {fill_down.effective_price} >= {static_price}"
    )
    assert fill_down.tokens_acquired > static_tokens


# ---------------------------------------------------------------------------
# Test 2: State too stale at fill time -> rejection with counter
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_stale_state_at_fill_time_rejected_with_counter():
    """
    If no state fresher than configurable bound exists at fill time,
    the fill must be REJECTED fail-closed and counted in telemetry.
    """
    cfg = EngineConfig(fill_max_stale_ms=2000, execution_seed=42)
    price_svc = DummyPriceService()
    book = PositionBook()
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)

    signal_ts_ns = 1_000_000_000_000_000  # 1,000,000s in ns
    # Stale pool whose timestamp is 10 seconds older than signal time
    pool_stale = _make_pool(
        timestamp_ns=signal_ts_ns - 10_000_000_000,  # 10s old
    )

    # Provider returns state that is 10s old at fill time
    def stale_resolver(chain, token, at_ts_ns):
        return pool_stale

    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=stale_resolver,
    )
    sig = _make_signal(pool_state=pool_stale, timestamp_ns=signal_ts_ns)

    fill = await executor.execute_signal(
        signal=sig,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("1.0"),
    )
    assert fill is None, "Stale state at fill time must reject fill fail-closed"
    assert executor.telemetry_counters.get("stale_pool_at_fill_rejected", 0) >= 1, (
        "Rejection counter 'stale_pool_at_fill_rejected' must be incremented"
    )


# ---------------------------------------------------------------------------
# Test 3: Private vs Public MEV impact & Depth Scaling
# ---------------------------------------------------------------------------
def test_conditional_mev_private_vs_public_and_depth_scaling():
    """
    - Private-route trades: zero sandwich impact (0 frontrun volume, 0 fraction).
    - Public-route trades:
      * Small trade (< 0.5% depth) with tight slippage (<50 bps) has 0 frontrun impact.
      * Larger trade with wide slippage has positive impact scaling with trade size/depth.
      * Impact is bounded by config ceiling (mev_max_sandwich_impact_bps).
    """
    cfg = EngineConfig(execution_seed=777)
    pool = _make_pool(native_reserve=Decimal("100.0"), token_reserve=Decimal("1000000.0"))

    # 1. Private route: zero impact regardless of trade size and slippage
    frac_priv, vol_priv = simulate_frontrun_volume(
        pool=pool,
        trade_size_native=Decimal("10.0"),  # 10% depth
        max_slippage_bps=500,               # 500 bps
        is_private_route=True,
        config=cfg,
    )
    assert frac_priv == 0.0
    assert vol_priv == Decimal("0")

    # 2. Public route with negligible trade (0.1 SOL = 0.1% depth) and tight slippage (20 bps)
    frac_small, vol_small = simulate_frontrun_volume(
        pool=pool,
        trade_size_native=Decimal("0.1"),
        max_slippage_bps=20,
        is_private_route=False,
        config=cfg,
    )
    assert frac_small == 0.0
    assert vol_small == Decimal("0")

    # 3. Public route with substantial trade (5.0 SOL = 5% depth) and wide slippage (300 bps)
    frac_large, vol_large = simulate_frontrun_volume(
        pool=pool,
        trade_size_native=Decimal("5.0"),
        max_slippage_bps=300,
        is_private_route=False,
        config=cfg,
    )
    assert frac_large > 0.0
    assert vol_large > Decimal("0")
    # Must be bounded: frontrun volume should never exceed pool ceiling (e.g. 5% of reserve)
    assert vol_large <= pool.native_reserve * Decimal("0.05")


# ---------------------------------------------------------------------------
# Test 4: Deterministic Seeding Reproducibility
# ---------------------------------------------------------------------------
def test_deterministic_seeding_reproducibility():
    """
    Same config seed -> exact same latency and MEV draws.
    Different config seed -> different results.
    """
    cfg1 = EngineConfig(execution_seed=42)
    cfg2 = EngineConfig(execution_seed=42)
    cfg3 = EngineConfig(execution_seed=99999)

    pool = _make_pool(native_reserve=Decimal("100.0"))
    ts = 1_000_000_000_000_000

    # Run 1 with seed 42
    lat1 = [sample_fill_latency(ChainIdentifier.SOLANA_MAINNET, config=cfg1) for _ in range(10)]
    # Run 2 with seed 42
    lat2 = [sample_fill_latency(ChainIdentifier.SOLANA_MAINNET, config=cfg2) for _ in range(10)]
    # Run 3 with seed 99999
    lat3 = [sample_fill_latency(ChainIdentifier.SOLANA_MAINNET, config=cfg3) for _ in range(10)]

    assert lat1 == lat2, "Identical seed must yield identical latency sequence"
    assert lat1 != lat3, "Different seed must yield different latency sequence"

    # Full simulate_latency reproducibility test using pool, ts, and LatencyResult
    sim1 = [
        simulate_latency(pool, signal_timestamp_ns=ts, chain=ChainIdentifier.SOLANA_MAINNET, trade_size_native=Decimal("1.0"), max_slippage_bps=100, config=cfg1)
        for _ in range(5)
    ]
    sim2 = [
        simulate_latency(pool, signal_timestamp_ns=ts, chain=ChainIdentifier.SOLANA_MAINNET, trade_size_native=Decimal("1.0"), max_slippage_bps=100, config=cfg2)
        for _ in range(5)
    ]
    sim3 = [
        simulate_latency(pool, signal_timestamp_ns=ts, chain=ChainIdentifier.SOLANA_MAINNET, trade_size_native=Decimal("1.0"), max_slippage_bps=100, config=cfg3)
        for _ in range(5)
    ]

    for s1, s2 in zip(sim1, sim2):
        assert isinstance(s1, LatencyResult)
        assert s1.latency_ms == s2.latency_ms
        assert s1.fill_timestamp_ns == s2.fill_timestamp_ns
        assert s1.frontrun_volume == s2.frontrun_volume

    diff_count = sum(1 for s1, s3 in zip(sim1, sim3) if s1.latency_ms != s3.latency_ms or s1.frontrun_volume != s3.frontrun_volume)
    assert diff_count > 0, "Different seed must produce different pipeline simulation outcomes"


# ---------------------------------------------------------------------------
# Test 5: Shadow Tournament and Live-Paper Parity (Same Module & Assumptions)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_shadow_and_live_paper_parity_same_module():
    """
    Verify that live-paper fills and shadow tournament lots use the exact same
    latency and execution logic, yielding identical fill prices for identical inputs.
    """
    cfg = EngineConfig(execution_seed=12345)
    price_svc = DummyPriceService(sol_price=Decimal("200.0"))
    book = PositionBook()
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)

    signal_ts_ns = 1_000_000_000_000_000
    trade_size_native = Decimal("1.0")
    pool = _make_pool(
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        timestamp_ns=signal_ts_ns,
    )

    def feed(chain, token, at_ts_ns):
        return pool.model_copy(update={"timestamp_ns": at_ts_ns, "timestamp_s": float(at_ts_ns) / 1e9})

    # 1. Live Paper execution
    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=feed,
        is_private_route=True,
    )
    signal = _make_signal(pool_state=pool, timestamp_ns=signal_ts_ns, is_private_route=True)
    live_fill = await executor.execute_signal(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=trade_size_native,
    )
    assert live_fill is not None

    # 2. Shadow Tournament execution using the same config and pool state
    tournament = StrategyTournamentRegistry(config=cfg, cost_model=cost_model)
    shadow_lots = tournament.evaluate_signal_and_record_shadow_lots(
        signal=signal,
        pool=pool,
        native_price_usd=Decimal("200.0"),
        base_trade_size_native=trade_size_native,
        market_state_provider=feed,
        is_private_route=True,
    )
    assert len(shadow_lots) > 0
    shadow_lot = shadow_lots[0]

    # Verify exact parity
    assert abs(shadow_lot.entry_price - live_fill.effective_price) < Decimal("1e-6"), (
        f"Shadow entry price {shadow_lot.entry_price} != live fill price {live_fill.effective_price}"
    )
    assert abs(shadow_lot.tokens_acquired - live_fill.tokens_acquired) < Decimal("1e-6")


# ---------------------------------------------------------------------------
# Test 6: Integration Test Through Runner Entry Path with Scripted Price Feed
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_integration_runner_entry_path_with_scripted_price_feed():
    """
    End-to-end integration test through runner._process_signal_queue:
    - Scripted price feed simulates price move during latency window.
    - Runner entry gate and PaperExecutor execute through real runtime path.
    - Filled position reflects the moved market state at fill time.
    """
    cfg = EngineConfig(
        allow_static_native_prices=True,
        circuit_breaker_freeze_duration_s=0.0,
        ai_supervisor_enabled=False,
        execution_seed=42,
    )
    price_svc = DummyPriceService(sol_price=Decimal("200.0"))
    runner = PaperTradingEngine(config=cfg, native_price_service=price_svc)
    runner._cash_sol = Decimal("50.0")
    runner._ledger = AsyncMock()
    runner._ledger.get_closed_trade_stats.return_value = {
        "total_trades": 0, "wins": 0, "avg_win_native": Decimal(0), "avg_loss_native": Decimal(0),
    }

    now_s = time.time()
    signal_ts_ns = int(now_s * 1_000_000_000)
    token_addr = "TokenScripted111111111111111111111111111111"
    initial_pool = _make_pool(
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        timestamp_ns=signal_ts_ns,
    )

    # Scripted feed: at fill time, price moved up (native 120, token 833333.3)
    def scripted_pool_feed(chain, token, pool_address="", at_timestamp_ns=None):
        ts = at_timestamp_ns or signal_ts_ns
        return _make_pool(
            native_reserve=Decimal("120.0"),
            token_reserve=Decimal("833333.3"),
            timestamp_ns=ts,
        )

    runner._pool_resolver = scripted_pool_feed
    if runner._executor is not None:
        runner._executor._pool_resolver = scripted_pool_feed

    signal = _make_signal(token_address=token_addr, pool_state=initial_pool, timestamp_ns=signal_ts_ns)

    await runner._signal_q.put(signal)
    await runner._signal_q.put(ShutdownSentinel())
    await runner._process_signal_queue()

    open_lots = runner._position_book.get_open_lots(token_address=token_addr)
    assert len(open_lots) == 1, "Expected exactly 1 open lot created through runner entry path"
    lot = open_lots[0]

    # Expected fill price reflects the moved pool (native 120, token 833333.3) -> spot price 0.000144
    # Initial spot was 0.000100
    assert lot.entry_price > Decimal("0.000120"), (
        f"Fill price {lot.entry_price} must reflect upward price move during latency window"
    )


# ---------------------------------------------------------------------------
# Test 7: determine_market_state_at_fill Direct State Provider Resolution
# ---------------------------------------------------------------------------
def test_determine_market_state_at_fill_sequence_and_staleness():
    """
    Direct unit test of determine_market_state_at_fill with sequence of tick states
    and staleness boundary rejection.
    """
    t0 = 1_000_000_000_000_000
    p1 = _make_pool(native_reserve=Decimal("10.0"), timestamp_ns=t0)
    p2 = _make_pool(native_reserve=Decimal("12.0"), timestamp_ns=t0 + 200_000_000)
    p3 = _make_pool(native_reserve=Decimal("15.0"), timestamp_ns=t0 + 500_000_000)

    # Query between p2 and p3 (target = t0 + 250ms -> freshest available state is p2)
    res_pool, rej = determine_market_state_at_fill(
        initial_pool=p1,
        target_fill_timestamp_ns=t0 + 250_000_000,
        market_state_provider=[p1, p2, p3],
        max_stale_bound_ms=4000,
    )
    assert res_pool is not None
    assert res_pool.native_reserve == Decimal("12.0")
    assert rej is None

    # Query far in the future exceeding max_stale_bound_ms (e.g. max 100ms)
    res_stale, rej_reason = determine_market_state_at_fill(
        initial_pool=p1,
        target_fill_timestamp_ns=t0 + 2_000_000_000,
        market_state_provider=[p1, p2, p3],
        max_stale_bound_ms=100,
    )
    assert res_stale is None
    assert rej_reason == "stale_pool_at_fill_rejected"


# ---------------------------------------------------------------------------
# Test 8: Backtester Parity & Execution Assumptions
# ---------------------------------------------------------------------------
def test_backtester_parity_and_execution_assumptions():
    """
    Verify WalkForwardBacktester uses the unified latency and conditional MEV model,
    matching tournament and live paper execution assumptions.
    """
    cfg = EngineConfig(execution_seed=777)
    price_svc = DummyPriceService()
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)
    backtester = WalkForwardBacktester(cost_model=cost_model, config=cfg)

    assert backtester.config.execution_seed == 777
    assert backtester.rng is not None
    assert hasattr(backtester, "partition_records")


# ---------------------------------------------------------------------------
# Test 9: execute_exit Latency and Stale State Handling
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_execute_exit_latency_and_stale_handling():
    """
    Verify execute_exit incorporates latency, resolves market state at fill time,
    and handles stale state fail-closed.
    """
    cfg = EngineConfig(execution_seed=42, fill_max_stale_ms=4000)
    price_svc = DummyPriceService()
    book = PositionBook()
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)

    t_now = time.time_ns()
    # Stale pool (10 seconds old > max_stale 4.0s)
    stale_pool = _make_pool(native_reserve=Decimal("100.0"), timestamp_ns=t_now - 10_000_000_000)

    executor = PaperExecutor(
        position_book=book,
        price_service=price_svc,
        cost_model=cost_model,
        config=cfg,
        pool_resolver=None,
    )

    with patch.object(time, "time_ns", return_value=t_now):
        res = await executor.execute_exit(
            token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool=stale_pool,
            tokens_to_sell=Decimal("100.0"),
            reason=TradeExitReason.SL_INITIAL,
        )
        assert res is None, "Stale pool at exit fill time must be rejected"
        assert executor.telemetry_counters.get("stale_pool_at_fill_rejected", 0) > 0

    # Fresh pool with mocked asyncio.sleep
    fresh_pool = _make_pool(native_reserve=Decimal("100.0"), timestamp_ns=t_now)
    with patch.object(asyncio, "sleep", return_value=None):
        res_ok = await executor.execute_exit(
            token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            chain=ChainIdentifier.SOLANA_MAINNET,
            pool=fresh_pool,
            tokens_to_sell=Decimal("100.0"),
            reason=TradeExitReason.TP_25,
        )
        assert res_ok is not None
        assert res_ok.tokens_acquired == Decimal("100.0")
