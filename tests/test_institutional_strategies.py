"""
tests.test_institutional_strategies — Unit & Integration Tests for Institutional Strategies
==========================================================================================
Validates:
1. Momentum Breakout (Volume Profile & Order Flow)
2. Mean Reversion (Z-Score & Dynamic Bollinger Bands)
3. Liquidity Sweep & Reversal
4. Whale Accumulation Shadow (Anti-Sybil Protection)
5. Narrative Velocity (Social Momentum & Reach)
6. Multi-Timeframe Confluence Matrix
7. Adaptive Regime Meta-Engine & Dynamic Routing
8. StrategyRegistry & StrategyManager
"""

import time
from decimal import Decimal
import pytest

from alpha_engine.engine.strategy import (
    AdaptiveRegimeStrategy,
    LiquiditySweepStrategy,
    MarketRegime,
    MeanReversionStrategy,
    MomentumBreakoutStrategy,
    MultiTimeframeConfluenceStrategy,
    NarrativeVelocityStrategy,
    StrategyManager,
    StrategyRegistry,
    WhaleAccumulationShadowStrategy,
)
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    OrderSide,
    SecurityTier,
    StrategyType,
    TradeExitReason,
)
from alpha_engine.models.state import PoolState, SecurityReport


@pytest.fixture
def clean_security_report() -> SecurityReport:
    return SecurityReport(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.12,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
    )


@pytest.fixture
def mock_pool_state() -> PoolState:
    return PoolState(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PoolTest11111111111111111111111111111111111",
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100000,
        market_cap_usd=Decimal("150000.0"),
        timestamp_s=time.time(),
    )


class MockPosition:
    def __init__(
        self,
        entry_price: Decimal,
        exit_stage: int = 0,
        stop_loss_pct: float = -12.0,
        strategy_type: str = "momentum_breakout",
        indicators: dict | None = None,
    ):
        self.entry_price = entry_price
        self.exit_stage = exit_stage
        self.stop_loss_pct = stop_loss_pct
        self.strategy_type = strategy_type
        self.indicators = indicators or {}


# ------------------------------------------------------------------------------
# 1. Momentum Breakout Strategy Tests
# ------------------------------------------------------------------------------

def test_momentum_breakout_volume_profile_and_entry(clean_security_report, mock_pool_state):
    strategy = MomentumBreakoutStrategy(min_volume_surge=2.0, min_order_flow_imbalance=0.58)

    # Simulated recent swaps showing a Value Area around 0.000100 to 0.000120
    swaps = [
        {"price": 0.000100, "volume": 10.0},
        {"price": 0.000105, "volume": 25.0},
        {"price": 0.000110, "volume": 50.0},  # Peak POC
        {"price": 0.000115, "volume": 30.0},
        {"price": 0.000120, "volume": 15.0},
        {"price": 0.000125, "volume": 5.0},
    ]

    poc, vah, val = strategy.compute_volume_profile(swaps, num_bins=10)
    assert poc > Decimal("0")
    assert vah >= poc
    assert val <= poc

    # Test breakout above VAH with strong volume surge and positive delta
    breakout_price = vah * Decimal("1.04")
    market_data = {
        "recent_swaps": swaps,
        "volume_surge_multiplier": 3.2,
        "net_buy_delta": 0.72,
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=breakout_price,
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.MOMENTUM_BREAKOUT
    assert signal.side == OrderSide.BUY
    assert signal.confidence >= 0.75
    assert len(signal.take_profit_ladder) == 3
    assert signal.recommended_position_pct > 0.35

    # Test rejection if order flow delta is weak
    weak_delta_data = dict(market_data, net_buy_delta=0.45)
    rejected_signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=breakout_price,
        security_report=clean_security_report,
        market_data=weak_delta_data,
    )
    assert rejected_signal is None


def test_momentum_breakout_exit_ladder():
    strategy = MomentumBreakoutStrategy()
    pos = MockPosition(entry_price=Decimal("1.0"), exit_stage=0)

    # 1. Hard Stop-Loss
    exit_sl = strategy.evaluate_exit(pos, current_price=Decimal("0.85"), peak_price=Decimal("1.0"), elapsed_seconds=60.0)
    assert exit_sl.should_exit is True
    assert exit_sl.reason == TradeExitReason.SL_HARD

    # 2. TP Stage 1 (+30%)
    exit_tp1 = strategy.evaluate_exit(pos, current_price=Decimal("1.32"), peak_price=Decimal("1.32"), elapsed_seconds=120.0)
    assert exit_tp1.should_exit is True
    assert exit_tp1.exit_ratio == 0.40
    assert exit_tp1.stage_index == 1

    # 3. Dynamic Trailing Stop from peak
    pos_stage1 = MockPosition(entry_price=Decimal("1.0"), exit_stage=1)
    # Peak reached 1.50 (+50%), then dropped to 1.20 (20% drawdown > 18% trailing stop)
    exit_trail = strategy.evaluate_exit(pos_stage1, current_price=Decimal("1.20"), peak_price=Decimal("1.50"), elapsed_seconds=300.0)
    assert exit_trail.should_exit is True
    assert exit_trail.reason == TradeExitReason.SL_TRAILING


# ------------------------------------------------------------------------------
# 2. Mean Reversion Strategy Tests
# ------------------------------------------------------------------------------

def test_mean_reversion_z_score_and_absorption(clean_security_report, mock_pool_state):
    strategy = MeanReversionStrategy(z_score_entry_threshold=-2.0, min_net_buy_delta=0.45)

    # Normal price baseline around 100, then sharp plunge to 70
    prices = [Decimal(str(100.0 + (i % 3))) for i in range(25)]
    prices[-1] = Decimal("70.0")  # Extreme oversold drop

    z_score, mean_p, upper_b, lower_b = strategy.compute_z_score_and_bands(prices)
    assert z_score < -2.0
    assert Decimal("70.0") < lower_b

    # Entry with buyer absorption
    market_data = {
        "price_series": prices,
        "net_buy_delta": 0.52,  # Buyers absorbing at discount
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("70.0"),
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.MEAN_REVERSION
    assert signal.indicators["z_score"] < -2.0
    assert signal.exit_profile == ExitProfile.MEAN_REVERSION_EXIT

    # Reject falling knife without absorption
    no_absorb_data = dict(market_data, net_buy_delta=0.30)
    assert strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("70.0"),
        security_report=clean_security_report,
        market_data=no_absorb_data,
    ) is None


def test_mean_reversion_exit_targets():
    strategy = MeanReversionStrategy()
    pos = MockPosition(
        entry_price=Decimal("70.0"),
        exit_stage=0,
        indicators={"mean_price": 95.0, "upper_band": 115.0},
    )

    # Target 1: Reversion to Mean (price reaches 96.0)
    exit_target1 = strategy.evaluate_exit(pos, current_price=Decimal("96.0"), peak_price=Decimal("96.0"), elapsed_seconds=300.0)
    assert exit_target1.should_exit is True
    assert exit_target1.exit_ratio == 0.50
    assert exit_target1.stage_index == 1

    # Target 2: Expansion to Upper Band (price reaches 116.0)
    pos_stage1 = MockPosition(
        entry_price=Decimal("70.0"),
        exit_stage=1,
        indicators={"mean_price": 95.0, "upper_band": 115.0},
    )
    exit_target2 = strategy.evaluate_exit(pos_stage1, current_price=Decimal("116.0"), peak_price=Decimal("116.0"), elapsed_seconds=600.0)
    assert exit_target2.should_exit is True
    assert exit_target2.exit_ratio == 1.0


# ------------------------------------------------------------------------------
# 3. Liquidity Sweep & Reversal Strategy Tests
# ------------------------------------------------------------------------------

def test_liquidity_sweep_reclaim_detection(clean_security_report, mock_pool_state):
    strategy = LiquiditySweepStrategy(sweep_tolerance_pct=4.0, min_delta_absorption=0.55)

    # Create synthetic candles: Swing Low at 10.0, recent candle swept to 9.7 (-3%), then reclaimed to 10.2
    candles = [
        {"high": 12.0, "low": 10.0, "price": 11.0},
        {"high": 11.5, "low": 10.1, "price": 10.8},
        {"high": 11.8, "low": 10.0, "price": 11.2},
        {"high": 11.0, "low": 10.2, "price": 10.5},
        {"high": 11.4, "low": 10.0, "price": 10.7},
        {"high": 11.2, "low": 10.1, "price": 10.4},
        {"high": 10.8, "low": 10.0, "price": 10.3},
        {"high": 10.5, "low": 9.7, "price": 10.0},   # Sweep candle (trough 9.7 < 10.0)
        {"high": 10.4, "low": 9.9, "price": 10.2},   # Reclaim candle
    ]

    is_sweep, swing_low, trough, swing_high = strategy.detect_sweep_and_reclaim(candles, current_price=Decimal("10.2"))
    assert is_sweep is True
    assert swing_low == Decimal("10.0")
    assert trough == Decimal("9.7")

    market_data = {
        "candles": candles,
        "net_buy_delta": 0.62,
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("10.2"),
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.LIQUIDITY_SWEEP
    assert signal.indicators["sweep_trough"] == 9.7
    assert signal.stop_loss_pct < 0


# ------------------------------------------------------------------------------
# 4. Whale Accumulation Shadow Strategy Tests
# ------------------------------------------------------------------------------

def test_whale_shadow_anti_sybil_and_entry(clean_security_report, mock_pool_state):
    strategy = WhaleAccumulationShadowStrategy(min_whale_inflow_native=Decimal("1.5"), min_distinct_whales=2)

    # 1. Clean whale accumulation entry
    market_data = {
        "accumulating_whales": ["WhaleWalletA111", "WhaleWalletB222"],
        "whale_inflow_native": "2.8",
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("0.05"),
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.WHALE_SHADOW
    assert signal.indicators["whale_count"] == 2
    assert signal.indicators["whale_inflow_native"] == 2.8

    # 2. Reject if dev holding is excessive
    class MockStagedDevCluster:
        dev_balance_pct = 4.5  # > 1.0% limit
        whale_cluster_detected = False

    rejected = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("0.05"),
        security_report=clean_security_report,
        staged_data=MockStagedDevCluster(),
        market_data=market_data,
    )
    assert rejected is None


# ------------------------------------------------------------------------------
# 5. Narrative Velocity Strategy Tests
# ------------------------------------------------------------------------------

def test_narrative_velocity_social_surge(clean_security_report, mock_pool_state):
    strategy = NarrativeVelocityStrategy(min_reach_score=70.0, min_velocity_index=65.0)

    class MockReachVerdict:
        score = 82.0
        flags = []

    market_data = {
        "reach_verdict": MockReachVerdict(),
        "reach_score": 82.0,
        "velocity_index": 78.0,
        "distinct_channels_count": 4,
        "template_duplicate_count": 0,
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("0.0025"),
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.NARRATIVE_VELOCITY
    assert signal.confidence >= 0.80

    # Reject bot raid template spam
    bot_farm_data = dict(market_data, template_duplicate_count=4)
    assert strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("0.0025"),
        security_report=clean_security_report,
        market_data=bot_farm_data,
    ) is None


# ------------------------------------------------------------------------------
# 6. Multi-Timeframe Confluence Strategy Tests
# ------------------------------------------------------------------------------

def test_multi_timeframe_confluence(clean_security_report, mock_pool_state):
    strategy = MultiTimeframeConfluenceStrategy(min_confluence_score=75.0)

    market_data = {
        "net_buy_delta": 0.60,       # 1m: 25 pts
        "volume_surge_multiplier": 2.5, # 5m: 25 pts
        "rsi_5m": 58.0,
        "ema_15m_21": "0.0090",       # 15m: 25 pts (current 0.0100 >= 0.0090)
        "token_age_hours": 2.5,       # 1h: 25 pts
        "one_hour_volume": 45.0,
    }

    signal = strategy.evaluate_entry(
        token_address="TokenTest1111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        current_price=Decimal("0.0100"),
        pool_state=mock_pool_state,
        security_report=clean_security_report,
        market_data=market_data,
    )

    assert signal is not None
    assert signal.strategy_type == StrategyType.MULTI_TIMEFRAME
    assert signal.indicators["confluence_score"] >= 75.0


# ------------------------------------------------------------------------------
# 7. Adaptive Regime Strategy & StrategyManager Tests
# ------------------------------------------------------------------------------

def test_adaptive_regime_classification_and_routing(clean_security_report, mock_pool_state):
    adaptive = AdaptiveRegimeStrategy()

    # Synthetic trending series
    trending_prices = [Decimal(str(10.0 + i * 0.5)) for i in range(25)]
    regime, hurst, vol = adaptive.classify_regime(trending_prices)
    assert regime in (MarketRegime.TRENDING_BULL, MarketRegime.HIGH_VOLATILITY_BURST)

    # Synthetic ranging/oscillating series
    ranging_prices = [Decimal(str(10.0 + (1.0 if i % 2 == 0 else -1.0))) for i in range(30)]
    regime_range, hurst_range, vol_range = adaptive.classify_regime(ranging_prices)
    assert hurst_range < 0.50

    # Test StrategyRegistry and StrategyManager
    registry = StrategyRegistry()
    assert len(registry.get_all_strategies()) >= 7
    active = registry.get_active_strategies()
    assert len(active) >= 7

    manager = StrategyManager(registry=registry)
    summary = manager.get_status_summary()
    assert len(summary) >= 7
    strat_types = {s["strategy_type"] for s in summary}
    assert "momentum_breakout" in strat_types
    assert "mean_reversion" in strat_types
    assert "liquidity_sweep" in strat_types
    assert "whale_shadow" in strat_types
    assert "narrative_velocity" in strat_types
    assert "multi_timeframe" in strat_types
    assert "adaptive_regime" in strat_types
