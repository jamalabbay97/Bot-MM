import asyncio
import time
from decimal import Decimal
from typing import Any, Optional
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator, StagedLaunch
from alpha_engine.engine.strategy.screener_priority import (
    GraduatedProfileConfig,
    MoversProfileConfig,
    ScreenerDecision,
    ScreenerEvaluationResult,
    ScreenerPriorityEvaluator,
)
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator, PoolRollingMetrics
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    ExitStage,
    OrderSide,
    ScreenerSection,
    SecurityTier,
    SignalSource,
    SignalStrength,
    StrategyType,
    TradeExitReason,
)
from alpha_engine.models.events import RawSignalEvent, ScreenerDiscoveryEvent, SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.security.gatekeeper import SecurityGatekeeper


def create_mock_security_report(
    token_address: str,
    tier: SecurityTier = SecurityTier.CLEAN,
    is_honeypot: bool = False,
    is_rug: bool = False,
    mint_auth_disabled: bool = True,
    score: float = 95.0,
) -> SecurityReport:
    return SecurityReport(
        token_address=token_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=tier,
        is_honeypot=is_honeypot,
        is_rug=is_rug,
        score=score,
        flags=[] if not is_honeypot else ["HONEYPOT"],
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.10,
        mint_authority_disabled=mint_auth_disabled,
        verified_source_code=True,
        checked_at_ts=int(time.time()),
    )


class MockSecurityGatekeeper(SecurityGatekeeper):
    def __init__(self, default_report: Optional[SecurityReport] = None):
        self.default_report = default_report
        self.call_count = 0

    async def screen_token(self, token_address: str, chain: Any, pool_address: str = "") -> SecurityReport:
        self.call_count += 1
        if self.default_report is not None:
            return self.default_report.model_copy(update={"token_address": token_address})
        return create_mock_security_report(token_address)


def make_swap_event(
    pool_address: str,
    amount_in: Decimal,
    amount_out: Decimal,
    timestamp_ns: int,
    is_buy: bool = True,
) -> SwapEvent:
    return SwapEvent(
        timestamp_ns=timestamp_ns,
        tx_hash="0x" + "a" * 64,
        block_number=1000,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_address,
        token_in="So11111111111111111111111111111111111111112",
        token_out="GraduatedToken1111111111111111111111111",
        amount_in=amount_in,
        amount_out=amount_out,
        sender="1" * 32,
        is_buy=is_buy,
        source=SignalSource.DEX_SWAP,
    )


# ==============================================================================
# TEST 1: Graduated Token Admitted Immediately Without Observation Delay
# ==============================================================================
@pytest.mark.anyio
async def test_01_graduated_token_admitted_immediately():
    """1. A Graduated token that meets every entry condition is evaluated and admitted immediately without artificial delay."""
    config = EngineConfig(screener_min_volume_usd=8000.0, strategy_v2_enabled=True)
    metrics_agg = DEXMetricsAggregator()
    gatekeeper = MockSecurityGatekeeper()
    grad_cfg = GraduatedProfileConfig(
        min_liquidity_usd=Decimal("10000.0"),
        max_price_impact_pct=0.05,
        min_volume_usd=Decimal("8000.0"),
    )
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=gatekeeper,
        metrics_aggregator=metrics_agg,
        graduated_config=grad_cfg,
    )
    assert isinstance(evaluator.gatekeeper, SecurityGatekeeper)

    token = "GraduatedMint111111111111111111111111111111"
    pool = "GradPoolRaydium11111111111111111111111111111"

    # Inject 2-minute rolling volume above $8,000 threshold ($12,500)
    now = time.time()
    for i in range(10):
        metrics_agg.record_swap(
            make_swap_event(
                pool_address=pool,
                amount_in=Decimal("10.0"),  # 10 SOL * $150 = $1,500 each
                amount_out=Decimal("50000.0"),
                timestamp_ns=int((now - 10 + i) * 1e9),
                is_buy=True,
            ),
            native_price_usd=Decimal("150.0"),
        )
    vol_2m = metrics_agg.get_rolling_volume_usd(pool, window_sec=120.0, native_price_usd=Decimal("150.0"))
    assert vol_2m >= 8000.0

    pool_metrics = metrics_agg.get_metrics(pool)
    assert isinstance(pool_metrics, PoolRollingMetrics)
    assert pool_metrics.volume_2m_usd >= 8000.0

    discovery = ScreenerDiscoveryEvent(
        mint=token,
        pool_address=pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=now,
        liquidity_usd=35000.0,
        volume_2m_usd=vol_2m,
        current_price=0.00045,
        breakout_price=0.00040,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert isinstance(result, ScreenerEvaluationResult)
    assert result.decision == ScreenerDecision.ACCEPTED
    assert result.signal is not None
    assert result.signal.source == SignalSource.SCREENER_GRADUATED
    assert result.signal.strategy_type == StrategyType.GRADUATED_PRIORITY
    assert result.signal.suggested_side == OrderSide.BUY
    assert result.signal.execution_venue == ExecutionVenue.RAYDIUM_AMM
    assert isinstance(result.signal.strength, SignalStrength)

    # Verify that in PendingLaunchBuffer, screener priority enables immediate graduation without waiting min_age_s (30s-120s)
    buffer = PendingLaunchBuffer(min_age_s=60.0, max_age_s=180.0, min_unique_signers=1)
    raw_sig = RawSignalEvent(
        source=SignalSource.SCREENER_GRADUATED,
        raw_data={},
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool,
        token_address=token,
    )
    buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool,
        initial_price=Decimal("0.0001"),
        report=create_mock_security_report(token),
        raw_signal=raw_sig,
        t_0=now - 2.0,  # Only 2 seconds old! Much less than 60s min_age_s
    )
    staged = buffer.get_staged(token)
    assert staged is not None
    staged.is_screener_priority = True
    staged.screener_section = ScreenerSection.GRADUATED
    staged.rolling_volume_2m_usd = Decimal("8500.0")

    pool_state = PoolState(
        pool_address=pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("100.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
        token_decimals=6,
        native_decimals=9,
    )
    sig_gen = SignalGenerator(config=config)
    grad_signal = buffer.record_trade(
        token_address=token,
        sol_amount=Decimal("1.0"),
        buyer="Buyer11111111111111111111111111111111",
        slot=1001,
        is_buy=True,
        pool=pool_state,
        signal_generator=sig_gen,
        current_time_s=now,
    )
    assert grad_signal is not None
    assert staged.graduated is True, "Graduated priority token must graduate immediately without 60s delay"


# ==============================================================================
# TEST 2: Movers Token Admitted Immediately Without Observation Delay
# ==============================================================================
@pytest.mark.anyio
async def test_02_movers_token_admitted_immediately():
    """2. A Movers token that meets every entry condition is evaluated and admitted immediately."""
    config = EngineConfig(screener_min_volume_usd=8000.0, strategy_v2_enabled=True)
    metrics_agg = DEXMetricsAggregator()
    gatekeeper = MockSecurityGatekeeper()
    movers_cfg = MoversProfileConfig(
        min_buy_pressure_ratio=0.55,
        max_extension_pct=1.50,
        min_volume_usd=Decimal("8000.0"),
    )
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=gatekeeper,
        metrics_aggregator=metrics_agg,
        movers_config=movers_cfg,
    )

    token = "MoversMint22222222222222222222222222222222"
    pool = "MoversPoolRaydium222222222222222222222222222"

    now = time.time()
    # Inject $9,000 volume into metrics
    for i in range(6):
        metrics_agg.record_swap(
            make_swap_event(
                pool_address=pool,
                amount_in=Decimal("10.0"),  # 10 SOL * $150 = $1,500 each -> $9,000
                amount_out=Decimal("10000.0"),
                timestamp_ns=int((now - 20 + i) * 1e9),
                is_buy=True,
            ),
            native_price_usd=Decimal("150.0"),
        )

    discovery = ScreenerDiscoveryEvent(
        mint=token,
        pool_address=pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.MOVERS,
        timestamp=now,
        liquidity_usd=25000.0,
        volume_2m_usd=9000.0,
        current_price=0.0012,
        breakout_price=0.0010,  # 20% extension
        price_change_pct_2m=15.0,
        volume_trend_fading=False,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert isinstance(result, ScreenerEvaluationResult)
    assert result.decision == ScreenerDecision.ACCEPTED
    assert result.signal is not None
    assert result.signal.source == SignalSource.SCREENER_MOVERS
    assert result.signal.strategy_type == StrategyType.MOVERS_PRIORITY
    assert result.signal.execution_venue == ExecutionVenue.RAYDIUM_AMM
    assert isinstance(result.signal.strength, SignalStrength)
    assert result.volume_2m_usd >= 8000.0


# ==============================================================================
# TEST 3: Token with $7,999 Qualifying Volume is Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_03_token_with_7999_volume_is_rejected():
    """3. A token with $7,999 qualifying volume is strictly rejected."""
    config = EngineConfig(screener_min_volume_usd=8000.0)
    gatekeeper = MockSecurityGatekeeper()
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=gatekeeper)

    discovery = ScreenerDiscoveryEvent(
        mint="UnderVolumeMint33333333333333333333333333",
        pool_address="UnderVolumePool333333333333333333333333",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=50000.0,
        volume_2m_usd=7999.00,  # Strictly below 8000
        current_price=0.0005,
        breakout_price=0.00045,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert result.decision == ScreenerDecision.REJECTED
    assert "below threshold" in result.reason
    assert result.signal is None


# ==============================================================================
# TEST 4: $8,000 Volume Passes Volume Check but Requires All Other Conditions
# ==============================================================================
@pytest.mark.anyio
async def test_04_token_with_8000_volume_requires_all_other_conditions():
    """4. A token with exactly $8,000 volume passes volume check but still requires every other condition (security, honeypot, etc.)."""
    config = EngineConfig(screener_min_volume_usd=8000.0)
    # Gatekeeper reports a honeypot!
    bad_report = create_mock_security_report(
        "HoneypotMint444444444444444444444444444444",
        tier=SecurityTier.TIER2_HONEYPOT,
        is_honeypot=True,
        mint_auth_disabled=False,
    )
    gatekeeper = MockSecurityGatekeeper(default_report=bad_report)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=gatekeeper)

    discovery = ScreenerDiscoveryEvent(
        mint="HoneypotMint444444444444444444444444444444",
        pool_address="HoneypotPool444444444444444444444444444444",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=50000.0,
        volume_2m_usd=8000.00,  # Exact threshold met!
        current_price=0.0005,
        breakout_price=0.00045,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    # Must be rejected because honeypot fails security gatekeeper despite $8,000 volume
    assert result.decision == ScreenerDecision.REJECTED
    assert "Security Gatekeeper rejected" in result.reason
    assert result.signal is None


# ==============================================================================
# TEST 5: Insufficient or Unverifiable Volume is Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_05_unverifiable_volume_rejected():
    """5. A token with insufficient or unverifiable volume is rejected even if it appears in either section."""
    config = EngineConfig(screener_min_volume_usd=8000.0)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=MockSecurityGatekeeper())

    # Case A: volume is None / NaN / 0.0
    discovery = ScreenerDiscoveryEvent(
        mint="UnverifiableMint55555555555555555555555555",
        pool_address="UnverifiablePool555555555555555555555555",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.MOVERS,
        timestamp=time.time(),
        liquidity_usd=50000.0,
        volume_2m_usd=0.0,  # No verifiable volume
        current_price=0.001,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert result.decision == ScreenerDecision.REJECTED
    assert result.signal is None


# ==============================================================================
# TEST 6: Inadequate Liquidity is Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_06_inadequate_liquidity_rejected():
    """6. A token with inadequate liquidity (< $10,000) is rejected despite meeting volume."""
    config = EngineConfig(screener_min_volume_usd=8000.0, graduated_min_liquidity_usd=10000.0)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=MockSecurityGatekeeper())

    discovery = ScreenerDiscoveryEvent(
        mint="LowLiqMint666666666666666666666666666666",
        pool_address="LowLiqPool666666666666666666666666666666",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=4500.0,  # Far below $10,000 executable liquidity requirement
        volume_2m_usd=12000.0,  # Has volume!
        current_price=0.0005,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert result.decision == ScreenerDecision.REJECTED
    assert "Inadequate liquidity" in result.reason
    assert result.signal is None


# ==============================================================================
# TEST 7: Excessively Extended Movers Token is Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_07_excessively_extended_movers_rejected():
    """7. An excessively extended Movers token (> 150% extension) is rejected, not bought simply because of ranking."""
    config = EngineConfig(screener_min_volume_usd=8000.0, movers_max_extension_pct=150.0)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=MockSecurityGatekeeper())

    discovery = ScreenerDiscoveryEvent(
        mint="ExtendedMoversMint7777777777777777777777777",
        pool_address="ExtendedMoversPool777777777777777777777777",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.MOVERS,
        timestamp=time.time(),
        liquidity_usd=40000.0,
        volume_2m_usd=25000.0,
        current_price=0.0030,
        breakout_price=0.0010,  # +200% extension above base level!
        price_change_pct_2m=40.0,
        volume_trend_fading=False,
        market_data_fresh=True,
    )

    result = await evaluator.evaluate(discovery)
    assert result.decision == ScreenerDecision.REJECTED
    assert "Excessively extended move" in result.reason
    assert result.signal is None


# ==============================================================================
# TEST 8: Token in Both Sections Evaluated Once and No Duplicate Buys
# ==============================================================================
@pytest.mark.anyio
async def test_08_both_sections_deduplication_and_no_duplicate_buys():
    """8. A token listed in both sections is evaluated once with merged signals and cannot trigger duplicate buys."""
    config = EngineConfig(screener_min_volume_usd=8000.0)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=MockSecurityGatekeeper())

    mint = "DualListedMint88888888888888888888888888888"
    pool = "DualListedPool88888888888888888888888888888"

    # Evaluation 1: Appears in both (or first as GRADUATED)
    discovery1 = ScreenerDiscoveryEvent(
        mint=mint,
        pool_address=pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.BOTH,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=10000.0,
        current_price=0.001,
        breakout_price=0.0009,
        market_data_fresh=True,
    )

    res1 = await evaluator.evaluate(discovery1)
    assert res1.decision == ScreenerDecision.ACCEPTED
    assert res1.signal is not None

    # Immediate second event for the same token (e.g. from MOVERS feed)
    discovery2 = ScreenerDiscoveryEvent(
        mint=mint,
        pool_address=pool,
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.MOVERS,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=10000.0,
        current_price=0.001,
        breakout_price=0.0009,
        market_data_fresh=True,
    )

    res2 = await evaluator.evaluate(discovery2)
    # Must be rejected because atomic lock is already held and evaluation is in progress or completed
    assert res2.decision == ScreenerDecision.REJECTED
    assert "already active or in-flight" in res2.reason
    assert res2.signal is None


# ==============================================================================
# TEST 9: Missing, Stale, or Inconsistent Market Data Safely Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_09_stale_or_inconsistent_market_data_safely_handled():
    """9. Missing, stale, or inconsistent market data causes a safe rejection or deferral."""
    config = EngineConfig(screener_min_volume_usd=8000.0)
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=MockSecurityGatekeeper())

    # Case A: Market data explicitly marked stale
    disc_stale = ScreenerDiscoveryEvent(
        mint="StaleMint99999999999999999999999999999999",
        pool_address="StalePool99999999999999999999999999999999",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time() - 400.0,  # 400 seconds ago
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_data_fresh=False,  # Not fresh
    )
    res_stale = await evaluator.evaluate(disc_stale)
    assert res_stale.decision == ScreenerDecision.DEFERRED
    assert "Stale or unconfirmed" in res_stale.reason

    # Case B: Inconsistent price (<= 0)
    disc_invalid_price = ScreenerDiscoveryEvent(
        mint="InvalidPriceMint9999999999999999999999999",
        pool_address="InvalidPricePool999999999999999999999999",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        current_price=0.0,  # Invalid price
        market_data_fresh=True,
    )
    res_price = await evaluator.evaluate(disc_invalid_price)
    assert res_price.decision == ScreenerDecision.REJECTED
    assert "Invalid price structure" in res_price.reason


# ==============================================================================
# TEST 10: High Volume of Listings Bounded Concurrency & Backpressure
# ==============================================================================
@pytest.mark.anyio
async def test_10_bounded_concurrency_and_backpressure():
    """10. High volume of new listings does not cause unbounded task queues, excessive memory use, or RPC storms."""
    config = EngineConfig(screener_min_volume_usd=8000.0, screener_concurrency_limit=5)
    gatekeeper = MockSecurityGatekeeper()
    evaluator = ScreenerPriorityEvaluator(config=config, gatekeeper=gatekeeper)

    # Track max concurrent active evaluations
    current_active = 0
    max_active = 0
    lock = asyncio.Lock()

    original_sem = evaluator._semaphore

    class InstrumentedSemaphore:
        def __init__(self, sem: asyncio.Semaphore):
            self._sem = sem

        async def __aenter__(self):
            nonlocal current_active, max_active
            await self._sem.__aenter__()
            async with lock:
                current_active += 1
                if current_active > max_active:
                    max_active = current_active

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            nonlocal current_active
            async with lock:
                current_active -= 1
            await self._sem.__aexit__(exc_type, exc_val, exc_tb)

    evaluator._semaphore = InstrumentedSemaphore(original_sem)  # type: ignore

    # Send 30 simultaneous events
    events = [
        ScreenerDiscoveryEvent(
            mint=f"BurstMint_{i:04d}1111111111111111111111111",
            pool_address=f"BurstPool_{i:04d}11111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            section=ScreenerSection.GRADUATED,
            timestamp=time.time(),
            liquidity_usd=20000.0,
            volume_2m_usd=8500.0,
            current_price=0.001,
            breakout_price=0.0009,
            market_data_fresh=True,
        )
        for i in range(30)
    ]

    results = await asyncio.gather(*(evaluator.evaluate(e) for e in events))
    assert len(results) == 30
    assert max_active <= 5, f"Expected bounded concurrency <= 5, but reached {max_active}"
    assert all(r.decision == ScreenerDecision.ACCEPTED for r in results)


# ==============================================================================
# TEST 11: Position Monitoring and Emergency Exits Continue Normally
# ==============================================================================
@pytest.mark.anyio
async def test_11_position_monitoring_and_emergency_exits_unaffected():
    """11. Existing position monitoring and emergency exits continue operating normally alongside screener evaluations."""
    config = EngineConfig(screener_min_volume_usd=8000.0, strategy_v2_enabled=True)
    engine = PaperTradingEngine(config)

    from alpha_engine.models.state import PaperFill
    fill = PaperFill(
        order_id="order_123",
        token_address="OpenPosToken11111111111111111111111111",
        pool_address="OpenPosPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("100000.0"),
        effective_price=Decimal("0.00001"),
        simulated_gas_cost_usd=Decimal("0.03"),
        fill_latency_ms=30,
    )
    lot = engine.position_book.open_lot(
        fill=fill,
        signal_id="sig_existing_open",
        initial_pool_reserve_native=Decimal("100.0"),
        pool_address="OpenPosPool11111111111111111111111111",
        exit_profile=ExitProfile.FAST_SNIPE,
    )
    assert engine.position_book.open_position_count() == 1
    assert lot.exit_profile == ExitProfile.FAST_SNIPE

    # Simulate concurrent screener evaluations
    screener_event = ScreenerDiscoveryEvent(
        mint="NewScreenerToken222222222222222222222222",
        pool_address="NewScreenerPool222222222222222222222222",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=25000.0,
        volume_2m_usd=9500.0,
        current_price=0.0002,
        breakout_price=0.00018,
        market_data_fresh=True,
    )
    screener_task = asyncio.create_task(engine.evaluate_screener_token(screener_event))

    # Meanwhile, check exit engine condition for existing position (e.g. emergency rug or hard stop-loss)
    should_exit, reason = engine.exit_engine.check_exit_conditions(
        current_price=Decimal("0.000005"),  # 50% loss
        entry_price=Decimal("0.00001"),
        liquidity_drop_pct=0.90,  # 90% liquidity rug drop
        age_seconds=60.0,
    )
    assert should_exit is True
    assert reason is not None

    await screener_task
    # Position monitoring and exit triggers worked immediately without being delayed by screener traffic


# ==============================================================================
# TEST 12: Regression Testing for Unrelated Engine Functionality
# ==============================================================================
@pytest.mark.anyio
async def test_12_regression_testing():
    """12. Existing functionality unrelated to the strategy passes regression tests."""
    config = EngineConfig(strategy_v2_enabled=True)
    sig_gen = SignalGenerator(config=config)

    # Verify standard bonding curve mint launch path still operates normally
    pool_state = PoolState(
        pool_address="StandardBondingCurvePool111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("30.0"),
        fee_numerator=10,
        fee_denominator=10000,
        last_updated_block=100,
        token_decimals=6,
        native_decimals=9,
    )
    raw_sig = RawSignalEvent(
        source=SignalSource.PUMP_FUN_MINT,
        raw_data={},
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address="StandardBondingCurveToken111111111111",
    )
    staged = StagedLaunch(
        token_address=raw_sig.token_address,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        t_0=time.time(),
        initial_price=Decimal("0.00003"),
        latest_price=Decimal("0.000035"),
        min_price=Decimal("0.00003"),
        report=create_mock_security_report(raw_sig.token_address),
        raw_signal=raw_sig,
    )

    launch_signal = sig_gen.generate_launch_signal(staged, pool_state)
    assert launch_signal is not None
    assert launch_signal.source == SignalSource.PUMP_FUN_MINT
    assert launch_signal.alpha_score >= 0.0


# ==============================================================================
# TEST 13: Strict Market Cap Gate ($8,000 threshold strictly rejected)
# ==============================================================================
@pytest.mark.anyio
async def test_13_market_cap_below_8000_strictly_rejected():
    """13. Enforce MIN_MARKET_CAP_USD = 8000: tokens with MC < $8,000 are strictly rejected."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    gatekeeper = MockSecurityGatekeeper()
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=gatekeeper,
        graduated_config=GraduatedProfileConfig(min_market_cap_usd=Decimal("8000.0")),
        movers_config=MoversProfileConfig(min_market_cap_usd=Decimal("8000.0")),
    )

    # 1. Graduated token with Market Cap $7,999.00
    disc_grad = ScreenerDiscoveryEvent(
        mint="LowMcGradToken1111111111111111111111111",
        pool_address="LowMcGradPool1111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=25000.0,
        volume_2m_usd=12000.0,
        market_cap_usd=7999.0,
        current_price=0.000007999,
        breakout_price=0.0000079,
        market_data_fresh=True,
    )
    res_grad = await evaluator.evaluate(disc_grad)
    assert res_grad.decision == ScreenerDecision.REJECTED
    assert res_grad.admitted is False
    assert "below minimum threshold" in res_grad.reason
    assert "$7999.00" in res_grad.reason

    # 2. Movers token with Market Cap $7,999.99
    disc_mov = ScreenerDiscoveryEvent(
        mint="LowMcMoverToken111111111111111111111111",
        pool_address="LowMcMoverPool111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.MOVERS,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=7999.99,
        current_price=0.00000799999,
        breakout_price=0.0000079,
        market_data_fresh=True,
    )
    res_mov = await evaluator.evaluate(disc_mov)
    assert res_mov.decision == ScreenerDecision.REJECTED
    assert res_mov.admitted is False
    assert "below minimum threshold" in res_mov.reason


# ==============================================================================
# TEST 14: Market Cap Gate Only Satisfies Gate (No Automatic Buys)
# ==============================================================================
@pytest.mark.anyio
async def test_14_market_cap_at_8000_passes_mc_gate_only():
    """14. MC >= $8,000 only satisfies the MC gate; does not automatically trigger buys if security/liquidity fails."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    # Honeypot gatekeeper: security fails!
    bad_gk = MockSecurityGatekeeper(
        default_report=create_mock_security_report("HoneypotToken111111111111111111111111111", is_honeypot=True)
    )
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=bad_gk,
        graduated_config=GraduatedProfileConfig(min_market_cap_usd=Decimal("8000.0")),
    )

    # Token with exact MC $8,000.00 but is a honeypot -> strictly rejected!
    disc_hp = ScreenerDiscoveryEvent(
        mint="HoneypotToken111111111111111111111111111",
        pool_address="HoneypotPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=50000.0,
        volume_2m_usd=20000.0,
        market_cap_usd=8000.0,
        current_price=0.000008,
        breakout_price=0.0000078,
        market_data_fresh=True,
    )
    res_hp = await evaluator.evaluate(disc_hp)
    assert res_hp.decision == ScreenerDecision.REJECTED
    assert "Hard security gates failed" in res_hp.reason or "HONEYPOT" in res_hp.reason

    # Token with MC $8,000.00 but low liquidity ($2,000 < $10,000) -> strictly rejected!
    clean_gk = MockSecurityGatekeeper()
    evaluator_clean = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=clean_gk,
        graduated_config=GraduatedProfileConfig(
            min_market_cap_usd=Decimal("8000.0"),
            min_liquidity_usd=Decimal("10000.0"),
        ),
    )
    disc_low_liq = ScreenerDiscoveryEvent(
        mint="LowLiqToken11111111111111111111111111111",
        pool_address="LowLiqPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=2000.0,  # Below $10k required!
        volume_2m_usd=20000.0,
        market_cap_usd=8000.0,
        current_price=0.000008,
        breakout_price=0.0000078,
        market_data_fresh=True,
    )
    res_low_liq = await evaluator_clean.evaluate(disc_low_liq)
    assert res_low_liq.decision == ScreenerDecision.REJECTED
    assert "liquidity" in res_low_liq.reason.lower()

    # Token with MC $8,001.00 and all criteria met -> admitted!
    disc_clean = ScreenerDiscoveryEvent(
        mint="CleanGradToken1111111111111111111111111",
        pool_address="CleanGradPool1111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=25000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=8001.0,
        current_price=0.000008001,
        breakout_price=0.0000078,
        market_data_fresh=True,
    )
    res_clean = await evaluator_clean.evaluate(disc_clean)
    assert res_clean.decision == ScreenerDecision.ACCEPTED
    assert res_clean.admitted is True


# ==============================================================================
# TEST 15: Missing or Stale Market Cap Strictly Rejected
# ==============================================================================
@pytest.mark.anyio
async def test_15_market_cap_missing_or_stale_strictly_rejected():
    """15. Tokens with missing, non-positive, or stale market cap data are strictly rejected."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=MockSecurityGatekeeper(),
        graduated_config=GraduatedProfileConfig(min_market_cap_usd=Decimal("8000.0")),
    )

    # 1. Missing MC (None with no liquidity)
    disc_none = ScreenerDiscoveryEvent(
        mint="NoneMcToken1111111111111111111111111111",
        pool_address="NoneMcPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=None,
        volume_2m_usd=15000.0,
        market_cap_usd=None,
        current_price=0.000008,
        breakout_price=0.0000078,
        market_data_fresh=True,
    )
    res_none = await evaluator.evaluate(disc_none)
    assert res_none.decision == ScreenerDecision.REJECTED
    assert "missing" in res_none.reason.lower() or "uninitialized pool" in res_none.reason.lower()

    # 2. Zero MC
    disc_zero = ScreenerDiscoveryEvent(
        mint="ZeroMcToken1111111111111111111111111111",
        pool_address="ZeroMcPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=0.0,
        current_price=0.000008,
        breakout_price=0.0000078,
        market_data_fresh=True,
    )
    res_zero = await evaluator.evaluate(disc_zero)
    assert res_zero.decision == ScreenerDecision.REJECTED

    # 3. Stale market data
    disc_stale = ScreenerDiscoveryEvent(
        mint="StaleDataToken1111111111111111111111111",
        pool_address="StaleDataPool1111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=15000.0,
        current_price=0.000015,
        breakout_price=0.000014,
        market_data_fresh=False,  # STALE!
    )
    res_stale = await evaluator.evaluate(disc_stale)
    assert res_stale.decision in (ScreenerDecision.DEFERRED, ScreenerDecision.REJECTED)
    assert res_stale.admitted is False
    assert "stale" in res_stale.reason.lower()


# ==============================================================================
# TEST 16: Failed Breakout Rejected (>2% breakdown or failed flag)
# ==============================================================================
@pytest.mark.anyio
async def test_16_failed_breakout_rejected():
    """16. Reject failed breakouts (>2% drop below breakout price or failed_breakout flag)."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=MockSecurityGatekeeper(),
        graduated_config=GraduatedProfileConfig(min_market_cap_usd=Decimal("8000.0")),
    )

    # Price dropped to 0.00038 vs breakout 0.00040 (5% breakdown > 2% threshold)
    disc_failed = ScreenerDiscoveryEvent(
        mint="FailedBreakoutToken1111111111111111111",
        pool_address="FailedBreakoutPool1111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=12000.0,
        current_price=0.00038,
        breakout_price=0.00040,
        failed_breakout=True,
        market_data_fresh=True,
    )
    res = await evaluator.evaluate(disc_failed)
    assert res.decision == ScreenerDecision.REJECTED
    assert "breakout failed" in res.reason.lower() or "breakout invalidated" in res.reason.lower()


# ==============================================================================
# TEST 17: Breakout Volume Confirmation (1.5x 5-candle median)
# ==============================================================================
@pytest.mark.anyio
async def test_17_breakout_volume_confirmation():
    """17. Breakout requires volume >= 1.5x median of last 5 candles."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=MockSecurityGatekeeper(),
        graduated_config=GraduatedProfileConfig(
            min_market_cap_usd=Decimal("8000.0"),
            breakout_volume_multiplier=1.5,
        ),
    )

    # 1. Breakout with insufficient volume (< 1.5x median: surge=1.2x)
    disc_low_vol = ScreenerDiscoveryEvent(
        mint="LowVolBreakoutToken1111111111111111111",
        pool_address="LowVolBreakoutPool1111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=12000.0,
        current_price=0.000405,  # +1.25% breakout
        breakout_price=0.000400,
        volume_surge_multiplier=1.2,  # < 1.5x required!
        market_data_fresh=True,
    )
    res_low = await evaluator.evaluate(disc_low_vol)
    assert res_low.decision == ScreenerDecision.REJECTED
    assert "Breakout volume" in res_low.reason

    # 2. Breakout with strong volume (>= 1.5x median: surge=2.1x)
    disc_high_vol = ScreenerDiscoveryEvent(
        mint="HighVolBreakoutToken111111111111111111",
        pool_address="HighVolBreakoutPool111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=15000.0,
        market_cap_usd=12000.0,
        current_price=0.000405,  # +1.25% breakout
        breakout_price=0.000400,
        volume_surge_multiplier=2.1,  # >= 1.5x!
        market_data_fresh=True,
    )
    res_high = await evaluator.evaluate(disc_high_vol)
    assert res_high.decision == ScreenerDecision.ACCEPTED


# ==============================================================================
# TEST 18: Breakout Retest Confirmation (Holding within ±1% of breakout price)
# ==============================================================================
@pytest.mark.anyio
async def test_18_retest_confirmation():
    """18. Retest confirmation: price holding within ±1% of breakout level is admitted."""
    config = EngineConfig(min_market_cap_usd=8000.0, strategy_v2_enabled=True)
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=MockSecurityGatekeeper(),
        graduated_config=GraduatedProfileConfig(
            min_market_cap_usd=Decimal("8000.0"),
            retest_tolerance_pct=0.01,
        ),
    )

    # Breakout was at 0.000400. Price retests at 0.000402 (+0.5%, well within ±1% tolerance)
    disc_retest = ScreenerDiscoveryEvent(
        mint="RetestToken111111111111111111111111111",
        pool_address="RetestPool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=35000.0,
        volume_2m_usd=18000.0,
        market_cap_usd=15000.0,
        current_price=0.000402,
        breakout_price=0.000400,
        retest_confirmed=True,
        market_data_fresh=True,
    )
    res = await evaluator.evaluate(disc_retest)
    assert res.decision == ScreenerDecision.ACCEPTED
    assert res.admitted is True


# ==============================================================================
# TEST 19: Anti-Chase Guard (Reject chasing without confirmed retest)
# ==============================================================================
@pytest.mark.anyio
async def test_19_chase_without_retest_rejected():
    """19. Reject chasing overextended moves without retest when continuation is disabled."""
    config = EngineConfig(
        min_market_cap_usd=8000.0,
        allow_continuation_without_retest=False,
        strategy_v2_enabled=True,
    )
    evaluator = ScreenerPriorityEvaluator(
        config=config,
        gatekeeper=MockSecurityGatekeeper(),
        graduated_config=GraduatedProfileConfig(
            min_market_cap_usd=Decimal("8000.0"),
            allow_continuation_without_retest=False,
            max_price_extension_pct=0.15,
        ),
    )

    # Price extended +20% with needs_retest=True
    disc_chase = ScreenerDiscoveryEvent(
        mint="ChaseToken1111111111111111111111111111",
        pool_address="ChasePool11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        section=ScreenerSection.GRADUATED,
        timestamp=time.time(),
        liquidity_usd=30000.0,
        volume_2m_usd=16000.0,
        market_cap_usd=20000.0,
        current_price=0.00048,  # +20% above breakout
        breakout_price=0.00040,
        needs_retest=True,
        retest_confirmed=False,
        market_data_fresh=True,
    )
    res = await evaluator.evaluate(disc_chase)
    assert res.decision == ScreenerDecision.REJECTED
    assert "Overextended" in res.reason


# ==============================================================================
# TEST 20: Partial Scale-Out at +2R (+2R Take Profit with 25% default)
# ==============================================================================
def test_20_partial_scale_out_at_2r():
    """20. Dynamic Exits: scale out partial position (default 25%) at +2R."""
    book = PositionBook()
    lot = OpenLot(
        token_address="Token2R",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        peak_price=Decimal("1.0"),
        cost_basis_native=Decimal("10000"),
        initial_risk_r=Decimal("0.08"),  # 8% initial risk
        partial_exit_enabled=True,
        bonding_curve_mode=False,
    )
    book._lots[(lot.chain.value, lot.token_address)] = [lot]

    # Target is +2R: 1.0 + (0.08 * 2.0) = 1.16 (+16% gain)
    # Price reaches 1.16: triggers partial exit
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.16"),
        partial_exit_fraction=Decimal("0.25"),
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.TP_25
    assert decision.exit_stage == ExitStage.TP1
    assert decision.tokens_to_sell == Decimal("2500")  # exactly 25% sold!
    assert lot.scaled_out_2r is True

    # Next tick at 1.18 does not trigger 2R scale-out again (already scaled out)
    lot.tokens_held = Decimal("7500")
    decision_next = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("1.18"),
        partial_exit_fraction=Decimal("0.25"),
    )
    assert decision_next is None

    # Test fraction variations: 10% vs 50%
    lot_10 = OpenLot(
        token_address="Token10",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        initial_risk_r=Decimal("0.08"),
        partial_exit_enabled=True,
    )
    dec_10 = book.evaluate_lot_exit(lot=lot_10, current_price=Decimal("1.16"), partial_exit_fraction=Decimal("0.10"))
    assert dec_10 is not None and dec_10.tokens_to_sell == Decimal("1000")  # 10%

    lot_50 = OpenLot(
        token_address="Token50",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        entry_price=Decimal("1.0"),
        initial_risk_r=Decimal("0.08"),
        partial_exit_enabled=True,
    )
    dec_50 = book.evaluate_lot_exit(lot=lot_50, current_price=Decimal("1.16"), partial_exit_fraction=Decimal("0.50"))
    assert dec_50 is not None and dec_50.tokens_to_sell == Decimal("5000")  # 50%


# ==============================================================================
# TEST 21: Trailing Stop Anchored to Confirmed Higher Low (HL)
# ==============================================================================
def test_21_trailing_stop_anchored_to_confirmed_higher_low():
    """21. Trailing stop updates only when a confirmed Higher Low is formed, with 1% safety buffer."""
    lot = OpenLot(
        token_address="TokenHL",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("1000"),
        tokens_held=Decimal("1000"),
        entry_price=Decimal("1.00"),
        peak_price=Decimal("1.00"),
        cost_basis_native=Decimal("1000"),
        initial_risk_r=Decimal("0.08"),
    )

    now = time.time()
    # Price progresses: 1.00 -> 1.15 (swing high) -> 1.08 (swing low) -> 1.25 (new swing high confirming 1.08 as HL)
    lot.record_tick(price=Decimal("1.00"), volume_native=Decimal("5.0"), timestamp_s=now)
    lot.record_tick(price=Decimal("1.15"), volume_native=Decimal("10.0"), timestamp_s=now + 5)
    lot.record_tick(price=Decimal("1.08"), volume_native=Decimal("8.0"), timestamp_s=now + 10)
    lot.record_tick(price=Decimal("1.25"), volume_native=Decimal("15.0"), timestamp_s=now + 15)

    # A confirmed Higher Low should have been detected at 1.08
    assert len(lot.confirmed_higher_lows) > 0
    assert lot.last_confirmed_hl == Decimal("1.08")
    assert lot.trailing_stop_active is True
    # Trailing stop anchored at HL * (1 - 0.01 buffer) = 1.08 * 0.99 = 1.0692
    expected_stop = (Decimal("1.08") * Decimal("0.99")).quantize(Decimal("1e-18"))
    assert lot.trailing_stop_price == expected_stop

