"""
tests.test_enterprise_engine_hardening — Comprehensive Test Suite for Enterprise Multi-Chain Trading Engine
===========================================================================================================
Validates:
1. Dual-horizon StrategyHorizon and ExecutionVenue enums & DecisionSignal model.
2. DEXMetricsAggregator rolling metrics, volume spike, and order-flow imbalance over 15 blocks.
3. Dual-horizon signal generation (Short-Term Scalp vs. Long-Term Swing).
4. Strict preflight gatekeeping (<= 3% tax, >= 99% LP burn, <= 15% top 10, mint/freeze disabled) and cabal detection.
5. Smart-money scoring formula, single-dev > 80% disqualifier, and 120s copy-trade gating.
6. Execution engine: private relay scalp routing, swing DCA batching, and dynamic tips.
7. Autonomous feedback: Bayesian stop-loss tuning, 14-day whitelist auto-deprecation, sentiment correlation tuning, and post-entry honeypot mutation emergency exit.
"""

from __future__ import annotations

import time
from decimal import Decimal

import pytest

from alpha_engine.engine.feedback import AdaptiveFeedbackEngine, TradeReflection
from alpha_engine.engine.signals import (
    generate_long_term_swing_signal,
    generate_short_term_scalp_signal,
)
from alpha_engine.engine.staging import StagedToken, VolumeBucket
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.bundle import DynamicTipAllocator, execute_scalp_via_private_relay
from alpha_engine.execution.executor import PaperExecutor, PrivateTxRouter
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.models.decisions import DecisionSignal
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SecurityTier,
    SignalSource,
    StrategyHorizon,
    TradeExitReason,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import SwapEvent
from alpha_engine.models.profiler import WalletTradeRecord
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.profiler.evaluator import WalletEvaluator
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.security.gatekeeper import detect_insider_cabal, evaluate_strict_hard_fails


# =============================================================================
# 1. DATA MODELS & ENUMS
# =============================================================================

def test_strategy_horizon_and_execution_venues():
    assert StrategyHorizon.SHORT_TERM_SCALP == "short_term_scalp"
    assert StrategyHorizon.LONG_TERM_SWING == "long_term_swing"

    assert ExecutionVenue.UNISWAP_V2.value == "uniswap_v2"
    assert ExecutionVenue.UNISWAP_V3.value == "uniswap_v3"
    assert ExecutionVenue.RAYDIUM_AMM.value == "raydium_amm"
    assert ExecutionVenue.RAYDIUM_CPMM.value == "raydium_cpmm"
    assert ExecutionVenue.RAYDIUM_CLMM.value == "raydium_clmm"
    assert ExecutionVenue.ORCA_WHIRLPOOL.value == "orca_whirlpool"
    assert ExecutionVenue.PUMP_FUN.value == "pump_fun"

    assert TradeExitReason.EMERGENCY_HONEYPOT_MUTATION == "emergency_honeypot_mutation"
    assert WhitelistStatus.ACTIVE.value == "active"
    assert WhitelistStatus.BANNED.value == "banned"
    assert WhitelistStatus.SUSPENDED.value == "suspended"


def test_decision_signal_instantiation_and_validation():
    signal = DecisionSignal(
        token_address="0x" + "a" * 40,
        pool_address="0x" + "b" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        suggested_side=OrderSide.BUY,
        strategy_horizon=StrategyHorizon.SHORT_TERM_SCALP,
        execution_venue=ExecutionVenue.UNISWAP_V3,
        trigger_reason="Test scalp signal with OFI > 75%",
        confidence_interval=(0.80, 0.95),
        alpha_score=0.88,
        metadata={"use_private_mempool": True},
    )
    assert signal.strategy_horizon == StrategyHorizon.SHORT_TERM_SCALP
    assert signal.execution_venue == ExecutionVenue.UNISWAP_V3
    assert signal.metadata["use_private_mempool"] is True
    assert signal.confidence_interval == (0.80, 0.95)


# =============================================================================
# 2. DEX METRICS AGGREGATOR
# =============================================================================

def test_dex_metrics_aggregator_volume_and_order_flow_imbalance():
    aggregator = DEXMetricsAggregator()
    pool_id = "0x" + "b" * 40
    token_id = "0x" + "d" * 40
    now_ns = time.time_ns()

    pool = PoolState(
        pool_address=pool_id,
        token_address=token_id,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=1000,
    )
    aggregator.record_pool_state(pool)

    # Record 15 buy swaps across sequential blocks
    for i in range(15):
        swap = SwapEvent(
            timestamp_ns=now_ns - (15 - i) * 2 * 1_000_000_000,
            block_number=1000 + i,
            chain=ChainIdentifier.BASE_MAINNET,
            pool_address=pool_id,
            token_in="0x4200000000000000000000000000000000000006",  # WETH
            token_out=token_id,
            amount_in=Decimal("0.5"),
            amount_out=Decimal("5000.0"),
            sender="0x" + "1" * 40,
            tx_hash="0x" + f"{i:064x}",
        )
        aggregator.record_swap(swap=swap, pool_state=pool)

    # 100% buy pressure -> OFI = 1.0 > 0.75
    ofi = aggregator.get_order_flow_imbalance(pool_id, num_blocks=15)
    assert ofi == 1.0

    # Volume spike check with baseline
    has_spike, ratio = aggregator.check_volume_spike(pool_id, baseline_1h_volume=Decimal("1.0"), multiplier=3.0)
    assert bool(has_spike) is True
    assert ratio >= 3.0


# =============================================================================
# 3. SIGNAL GENERATION: SCALP VS SWING
# =============================================================================

def test_short_term_scalp_signal_generation():
    aggregator = DEXMetricsAggregator()
    pool_addr = "0x" + "c" * 40
    token_addr = "0x" + "d" * 40
    now_ns = time.time_ns()

    pool = PoolState(
        pool_address=pool_addr,
        token_address=token_addr,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=2000,
    )
    aggregator.record_pool_state(pool)

    # Record 15 strong buy blocks
    for i in range(15):
        swap = SwapEvent(
            timestamp_ns=now_ns - (15 - i) * 2 * 1_000_000_000,
            block_number=2000 - 15 + i,
            chain=ChainIdentifier.BASE_MAINNET,
            pool_address=pool_addr,
            token_in="0x4200000000000000000000000000000000000006",
            token_out=token_addr,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("20000.0"),
            sender="0x" + "1" * 40,
            tx_hash="0x" + f"{(i+10):064x}",
        )
        aggregator.record_swap(swap=swap, pool_state=pool)

    # 1. Normal Scalp Signal Generation with private relay enabled (zero sandwich risk)
    signal = generate_short_term_scalp_signal(
        pool=pool,
        metrics_aggregator=aggregator,
        trade_size_native=Decimal("0.5"),
        baseline_1h_volume=Decimal("2.0"),
        mempool_is_public=False,
    )
    assert signal is not None
    assert signal.strategy_horizon == StrategyHorizon.SHORT_TERM_SCALP
    assert signal.execution_venue in (ExecutionVenue.UNISWAP_V2, ExecutionVenue.UNISWAP_V3)
    assert signal.confidence_interval[0] >= 0.80

    # 2. Blocked if public mempool creates severe sandwich risk with excessive slippage
    blocked_signal = generate_short_term_scalp_signal(
        pool=pool,
        metrics_aggregator=aggregator,
        trade_size_native=Decimal("30.0"),  # 60% of 50 SOL reserve -> massive sandwich risk
        baseline_1h_volume=Decimal("2.0"),
        mempool_is_public=True,
        max_slippage_bps=500,
    )
    assert blocked_signal is None


def test_long_term_swing_signal_generation():
    token_addr = "0x" + "e" * 40
    pool_addr = "0x" + "f" * 40
    now = time.time()

    valid_report = SecurityReport(
        token_address=token_addr,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        buy_tax_bps=100,
        sell_tax_bps=100,
        lp_burned_ratio=0.995,
        top10_concentration=0.10,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        is_honeypot=False,
    )

    # Create staged token aged 48 hours (in [24h, 72h] window)
    staged = StagedToken(
        token_address=token_addr,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address=pool_addr,
        t_staged=now - 48 * 3600,
        ttl_seconds=86400.0 * 7,
        initial_price=Decimal("0.001"),
        latest_price=Decimal("0.00102"),
        peak_price=Decimal("0.00105"),
        trough_price=Decimal("0.00098"),
        security_report=valid_report,
        token_age_seconds=48 * 3600.0,
    )

    # Populate 12 hours of tight consolidation (std dev < 10%)
    for i in range(144):
        ts = now - (144 - i) * 300
        p = Decimal("0.001") * (Decimal("1.0") + Decimal(str((i % 3) * 0.01)))
        staged.record_5m_price(p, volume_native=Decimal("0.5"), timestamp_s=ts)

    # Populate 3 consecutive hours of awakening volume increase
    for h in range(3):
        vol = Decimal(str(10.0 * (h + 1)))
        staged.volume_buckets.append(
            VolumeBucket(
                timestamp_s=int(now - (3 - h) * 3600 + 100),
                buy_volume=vol,
                sell_volume=Decimal(0),
                buy_count=5,
                sell_count=0,
            )
        )

    pool = PoolState(
        pool_address=pool_addr,
        token_address=token_addr,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("10000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=5000,
    )

    # Verified smart money inflow of 15.0 SOL
    signal = generate_long_term_swing_signal(
        staged_token=staged,
        pool=pool,
        report=valid_report,
        smart_money_inflows_native=Decimal("15.0"),
    )
    assert signal is not None
    assert signal.strategy_horizon == StrategyHorizon.LONG_TERM_SWING
    assert signal.execution_venue == ExecutionVenue.UNISWAP_V2

    # Should fail if smart money inflow is below threshold (< 5 SOL)
    no_flow_signal = generate_long_term_swing_signal(
        staged_token=staged,
        pool=pool,
        report=valid_report,
        smart_money_inflows_native=Decimal("2.0"),
    )
    assert no_flow_signal is None


# =============================================================================
# 4. STRICT PREFLIGHT GATEKEEPING & CABAL DETECTION
# =============================================================================

def test_strict_hard_fails_and_cabal_detection():
    # 1. Passing report (<= 3% tax, >= 99% LP burn, <= 15% top 10, mint/freeze disabled)
    valid_report = SecurityReport(
        token_address="0x" + "1" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        buy_tax_bps=200,   # 2% <= 3%
        sell_tax_bps=250,  # 2.5% <= 3%
        lp_burned_ratio=0.995,  # >= 99%
        top10_concentration=0.12,  # 12% <= 15%
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        is_honeypot=False,
    )
    assert valid_report.passes_strict_hard_fails is True
    passes, reason = evaluate_strict_hard_fails(valid_report)
    assert passes is True
    assert reason == ""

    # 2. Failing: Buy tax > 3% (e.g. 400 bps / 4%)
    high_tax_report = valid_report.model_copy(update={"buy_tax_bps": 400})
    assert high_tax_report.passes_strict_hard_fails is False
    p, r = evaluate_strict_hard_fails(high_tax_report)
    assert p is False
    assert "BUY_TAX_EXCEEDED" in r

    # 3. Failing: LP burn < 99%
    low_burn_report = valid_report.model_copy(update={"lp_burned_ratio": 0.98})
    assert low_burn_report.passes_strict_hard_fails is False

    # 4. Failing: Top 10 concentration > 15%
    high_conc_report = valid_report.model_copy(update={"top10_concentration": 0.18})
    assert high_conc_report.passes_strict_hard_fails is False

    # 5. Failing: Freeze authority enabled
    freeze_enabled = valid_report.model_copy(update={"freeze_authority_disabled": False})
    assert freeze_enabled.passes_strict_hard_fails is False

    # 6. Insider Cabal Detection:
    # 4 wallets funded by the same address "0xdeployer" buying within 30s of each other
    funding_sources = {
        "0xuser1": "0xdeployer",
        "0xuser2": "0xdeployer",
        "0xuser3": "0xdeployer",
        "0xuser4": "0xdeployer",
    }
    buy_times = {
        "0xuser1": 1000.0,
        "0xuser2": 1005.0,
        "0xuser3": 1012.0,
        "0xuser4": 1025.0,
    }
    is_cabal, cabal_reason, cabal_wallets = detect_insider_cabal(funding_sources, buy_times, simultaneous_window_s=60.0)
    assert is_cabal is True
    assert len(cabal_wallets) >= 3
    assert "INSIDER_CABAL_DETECTED" in cabal_reason


# =============================================================================
# 5. SMART MONEY SCORING & 120s COPY-TRADE GATING
# =============================================================================

def test_smart_money_score_formula_and_single_dev_filter():
    evaluator = WalletEvaluator()

    # Test formula: S_wallet = (WinRate * 0.4) + (SharpeRatio * 0.3) + (HoldingDiscipline * 0.2) + (Longevity * 0.1)
    score = evaluator.calculate_smart_money_score(
        win_rate=0.80,          # 0.80 * 0.4 = 0.32
        sharpe_ratio=0.70,      # 0.70 * 0.3 = 0.21
        holding_discipline=0.90,# 0.90 * 0.2 = 0.18
        longevity=0.60,         # 0.60 * 0.1 = 0.06 -> total = 0.77
    )
    assert score == 0.77

    # Test single-developer > 80% volume concentration filter (with 25 trades spanning 30 days)
    now_ts = 1700000000
    trades = [
        WalletTradeRecord(
            token_address=f"0x{i:040x}",
            buy_tx_hash=f"0x{i:064x}",
            sell_tx_hash=f"0x{(i+100):064x}",
            buy_timestamp=now_ts - (30 - i) * 86400 - 400,
            sell_timestamp=now_ts - (30 - i) * 86400,
            holding_time_seconds=400.0,
            invested_native=Decimal("10.0"),
            realized_native_pnl=Decimal("5.0"),
            realized_pnl_usd=Decimal("5000.0"),
            roi_pct=50.0,
            custom_metadata={"developer_address": "0xmaliciousdev"},
        )
        for i in range(25)
    ]
    # 100% of trades originate from tokens deployed by "0xmaliciousdev"
    profile = evaluator.evaluate(
        wallet_address="0x" + "2" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=now_ts,
    )
    assert profile.classification == WalletClassification.INSIDER
    assert "Single-developer token concentration" in profile.rejection_reasons[0]


def test_120s_two_wallet_copy_trade_gating():
    profiler = SmartMoneyProfiler(db_path=":memory:")
    token = "0x" + "3" * 40
    now = 1000.0

    # 1. First high-scoring wallet buys (S = 0.82 >= 0.75)
    triggered, wallets = profiler.record_wallet_purchase(
        token_address=token,
        wallet_address="0xwalletA",
        smart_money_score=0.82,
        timestamp_s=now,
        funding_source="0xexchange1",
    )
    assert triggered is False  # Only 1 wallet -> gating prevents copy-trade

    # 2. Correlated wallet buys within 30s (shares funding source with Wallet A)
    triggered_correl, _ = profiler.record_wallet_purchase(
        token_address=token,
        wallet_address="0xwalletA_clone",
        smart_money_score=0.85,
        timestamp_s=now + 30.0,
        funding_source="0xexchange1",  # Same funding source -> correlated Sybil
    )
    assert triggered_correl is False  # Sybil/cluster filter prevents copy-trade

    # 3. Second distinct, uncorrelated high-scoring wallet buys within 120s window (at t=60s)
    triggered_valid, qualifying = profiler.record_wallet_purchase(
        token_address=token,
        wallet_address="0xwalletB",
        smart_money_score=0.79,
        timestamp_s=now + 60.0,
        funding_source="0xexchange2",  # Uncorrelated
    )
    assert triggered_valid is True
    assert "0xwalleta" in qualifying
    assert "0xwalletb" in qualifying


# =============================================================================
# 6. EXECUTION ENGINE: PRIVATE RELAY SCALPING & SWING DCA BATCHING
# =============================================================================

@pytest.mark.anyio
async def test_scalp_private_relay_execution():
    router = PrivateTxRouter()
    book = PositionBook()
    executor = PaperExecutor(position_book=book, native_price_usd=Decimal("200.0"), private_router=router)

    pool = PoolState(
        pool_address="0x" + "9" * 40,
        token_address="0x" + "4" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=2000,
    )

    signal = DecisionSignal(
        token_address="0x" + "4" * 40,
        pool_address="0x" + "9" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        suggested_side=OrderSide.BUY,
        strategy_horizon=StrategyHorizon.SHORT_TERM_SCALP,
        execution_venue=ExecutionVenue.UNISWAP_V3,
        trigger_reason="Scalp entry via private relay",
        confidence_interval=(0.85, 0.98),
        alpha_score=0.92,
        pool_state=pool,
        metadata={"use_private_mempool": True},
    )

    # Execute scalp pipeline enforcing private relay routing
    fill = await executor.execute_scalp_pipeline(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        priority_percentile="p95",
    )
    assert fill is not None
    assert fill.token_address == signal.token_address
    assert fill.simulated_native_spent > Decimal(0)

    # Dynamic tip allocator verification
    allocator = DynamicTipAllocator()
    tip_evm = await allocator.get_dynamic_tip(ChainIdentifier.BASE_MAINNET, percentile="p95")
    assert tip_evm >= 1_000_000_000  # >= 1 gwei
    escalated = allocator.escalate_tip(tip_evm, escalation_factor=1.25)
    assert escalated == int(tip_evm * 1.25)

    # Standalone execute_scalp_via_private_relay function verification
    relay_direct = await execute_scalp_via_private_relay(
        chain=ChainIdentifier.BASE_MAINNET,
        transactions=[f"simulated_private_tx_{fill.order_id}"],
        priority_percentile="p95",
        router=router,
    )
    assert relay_direct.get("relay") == "flashbots_titan"


@pytest.mark.anyio
async def test_swing_dca_batching_execution():
    book = PositionBook()
    executor = PaperExecutor(position_book=book, native_price_usd=Decimal("200.0"))

    pool = PoolState(
        pool_address="0x" + "9" * 40,
        token_address="0x" + "5" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=3000,
    )

    signal = DecisionSignal(
        token_address="0x" + "5" * 40,
        pool_address="0x" + "9" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        suggested_side=OrderSide.BUY,
        strategy_horizon=StrategyHorizon.LONG_TERM_SWING,
        execution_venue=ExecutionVenue.UNISWAP_V2,
        trigger_reason="Swing DCA entry",
        confidence_interval=(0.80, 0.95),
        alpha_score=0.88,
        pool_state=pool,
    )

    # Execute swing DCA across 3 blocks
    fills = await executor.execute_swing_dca_pipeline(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        num_batches=3,
    )
    assert len(fills) == 3
    total_spent = sum(f.simulated_native_spent for f in fills)
    assert total_spent > Decimal(0)


# =============================================================================
# 7. AUTONOMOUS FEEDBACK, SELF-LEARNING & EMERGENCY HONEYPOT EXITS
# =============================================================================

@pytest.mark.anyio
async def test_bayesian_stop_loss_and_14d_whitelist_deprecation():
    feedback = AdaptiveFeedbackEngine()

    # 1. Volatility-adjusted stop-loss:
    # Short-term scalp: bounded strictly between 4% and 7%
    sl_scalp_low_vol = feedback.optimize_stop_loss_bayesian(realized_volatility=0.01, horizon=StrategyHorizon.SHORT_TERM_SCALP)
    assert 4.0 <= sl_scalp_low_vol <= 7.0
    sl_scalp_high_vol = feedback.optimize_stop_loss_bayesian(realized_volatility=0.15, horizon=StrategyHorizon.SHORT_TERM_SCALP)
    assert sl_scalp_high_vol == 7.0

    # Long-term swing: bounded strictly between 15% and 25%
    sl_swing_low_vol = feedback.optimize_stop_loss_bayesian(realized_volatility=0.05, horizon=StrategyHorizon.LONG_TERM_SWING)
    assert 15.0 <= sl_swing_low_vol <= 25.0
    sl_swing_high_vol = feedback.optimize_stop_loss_bayesian(realized_volatility=0.40, horizon=StrategyHorizon.LONG_TERM_SWING)
    assert sl_swing_high_vol == 25.0

    # 2. 14-day rolling whitelist auto-deprecation
    now_ns = time.time_ns()
    # Add 4 losing trades for wallet C within last 14 days (win rate 0% < 45%, negative PnL)
    for i in range(4):
        ref = TradeReflection.from_trade(
            trade_id=f"trade_{i}",
            token_address="0x" + "6" * 40,
            chain=ChainIdentifier.BASE_MAINNET,
            signal_source=SignalSource.WHALE_WALLET,
            entry_price=Decimal("1.0"),
            exit_price=Decimal("0.8"),
            realized_pnl_usd=Decimal("-200.0"),
            realized_pnl_native=Decimal("-0.2"),
            wallet_address="0xunderperforming_wallet",
        )
        ref.timestamp_ns = now_ns - (i + 1) * 86400 * 1_000_000_000
        await feedback.record_closed_trade(ref)

    deprecated = await feedback.evaluate_14d_whitelist_deprecation(current_time_ns=now_ns)
    assert "0xunderperforming_wallet" in deprecated


def test_sentiment_sensitivity_weight_tuning():
    feedback = AdaptiveFeedbackEngine(initial_social_weight=1.0)

    # 1. Social volume spikes that DO NOT retain price over 2 hours (low/negative correlation)
    social_vols = [100.0, 300.0, 500.0, 900.0, 1500.0]
    price_retention = [1.05, 0.95, 0.85, 0.70, 0.60]  # Dumped despite social volume -> r < 0.35

    new_weight = feedback.tune_sentiment_sensitivity_weights(social_vols, price_retention)
    assert new_weight < 1.0  # Decayed weight


@pytest.mark.anyio
async def test_post_entry_contract_mutation_and_emergency_exit():
    feedback = AdaptiveFeedbackEngine()
    book = PositionBook()
    executor = PaperExecutor(position_book=book, native_price_usd=Decimal("200.0"))
    token = "0x" + "7" * 40
    pool = PoolState(
        pool_address="0x" + "8" * 40,
        token_address=token,
        chain=ChainIdentifier.BASE_MAINNET,
        native_reserve=Decimal("20.0"),
        token_reserve=Decimal("500000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=8000,
    )

    # Contract variable mutates after confirmation: dev raises sell tax to 25% (2500 bps)
    mutated_security_report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        buy_tax_bps=100,
        sell_tax_bps=2500,  # Mutated > 300 bps!
        lp_burned_ratio=0.99,
        top10_concentration=0.10,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        is_honeypot=False,
    )

    is_mutated, reason = await feedback.check_contract_mutation_and_emergency_exit(
        token_address=token,
        chain=ChainIdentifier.BASE_MAINNET,
        current_security_report=mutated_security_report,
        tokens_to_sell=Decimal("10000.0"),
        pool=pool,
        executor=executor,
    )
    assert is_mutated is True
    assert "sell_tax_bps=2500 > 300" in reason
