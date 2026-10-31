"""
alpha_engine.engine.strategy.adaptive_regime — Adaptive Market Regime Strategy
=============================================================================
Implements an autonomous meta-strategy that classifies real-time market regimes
using Realized Volatility and the Hurst Exponent, then dynamically routes execution
to the optimal underlying sub-strategy.
"""

from __future__ import annotations

import logging
import math
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.engine.strategy.base import (
    BaseStrategy,
    MarketRegime,
    StrategyExitSignal,
    StrategySignal,
)
from alpha_engine.engine.strategy.liquidity_sweep import LiquiditySweepStrategy
from alpha_engine.engine.strategy.mean_reversion import MeanReversionStrategy
from alpha_engine.engine.strategy.momentum_breakout import MomentumBreakoutStrategy
from alpha_engine.engine.strategy.multi_timeframe import MultiTimeframeConfluenceStrategy
from alpha_engine.engine.strategy.whale_shadow import WhaleAccumulationShadowStrategy
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    OrderSide,
    StrategyType,
    TradeExitReason,
)
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class AdaptiveRegimeStrategy(BaseStrategy):
    """
    Adaptive Regime Strategy.
    Dynamically classifies current market regime into:
      - TRENDING_BULL: Routes to Momentum Breakout / Multi-Timeframe
      - RANGING_SIDEWAYS: Routes to Mean-Reversion / Liquidity Sweep
      - HIGH_VOLATILITY_BURST: Routes to Whale Accumulation Shadow with tightened risk
    """

    def __init__(
        self,
        hurst_lookback: int = 30,
        high_vol_threshold_pct: float = 8.0,
        enabled: bool = True,
        weight: float = 1.3,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.ADAPTIVE_REGIME,
            name="Adaptive Regime Engine",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [
            MarketRegime.TRENDING_BULL,
            MarketRegime.RANGING_SIDEWAYS,
            MarketRegime.HIGH_VOLATILITY_BURST,
        ]
        self._hyperparameters = {
            "hurst_lookback": hurst_lookback,
            "high_vol_threshold_pct": high_vol_threshold_pct,
        }

        # Sub-strategies managed by the regime router
        self.momentum_strat = MomentumBreakoutStrategy()
        self.mean_reversion_strat = MeanReversionStrategy()
        self.liquidity_sweep_strat = LiquiditySweepStrategy()
        self.whale_strat = WhaleAccumulationShadowStrategy()
        self.multi_tf_strat = MultiTimeframeConfluenceStrategy()

    def classify_regime(
        self,
        prices: list[Decimal],
    ) -> tuple[MarketRegime, float, float]:
        """
        Classifies market regime based on Realized Volatility and Rescaled Range (Hurst Exponent).
        Returns (MarketRegime, hurst_exponent, realized_vol_pct).
        """
        if not prices or len(prices) < 10:
            return MarketRegime.TRENDING_BULL, 0.55, 5.0

        n = len(prices)
        float_prices = [float(p) for p in prices]

        # 1. Log returns and Realized Volatility
        returns = []
        for i in range(1, n):
            if float_prices[i - 1] > 0:
                returns.append(math.log(float_prices[i] / float_prices[i - 1]))

        if not returns:
            return MarketRegime.TRENDING_BULL, 0.55, 5.0

        mean_ret = sum(returns) / len(returns)
        var_ret = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        std_ret = math.sqrt(var_ret)
        realized_vol_pct = std_ret * 100.0

        # 2. Rescaled Range (Hurst Exponent approximation)
        # H = log(R / S) / log(N)
        cum_deviations = []
        running_sum = 0.0
        for r in returns:
            running_sum += (r - mean_ret)
            cum_deviations.append(running_sum)

        r_range = max(cum_deviations) - min(cum_deviations)
        s_std = std_ret if std_ret > 0 else 0.0001
        rescaled_range = max(0.001, r_range / s_std)

        log_n = math.log(len(returns)) if len(returns) > 1 else 1.0
        hurst = max(0.1, min(0.95, math.log(rescaled_range) / log_n))

        # 3. Regime Classification
        high_vol_thresh = self._hyperparameters["high_vol_threshold_pct"]

        if realized_vol_pct >= high_vol_thresh:
            regime = MarketRegime.HIGH_VOLATILITY_BURST
        elif hurst > 0.54:
            regime = MarketRegime.TRENDING_BULL
        elif hurst < 0.46:
            regime = MarketRegime.RANGING_SIDEWAYS
        else:
            regime = MarketRegime.TRENDING_BULL

        return regime, round(hurst, 3), round(realized_vol_pct, 2)

    def evaluate_entry(
        self,
        token_address: str,
        chain: ChainIdentifier,
        current_price: Decimal,
        pool_state: Optional[PoolState] = None,
        security_report: Optional[SecurityReport] = None,
        staged_data: Optional[Any] = None,
        market_data: Optional[dict[str, Any]] = None,
    ) -> Optional[StrategySignal]:
        if not self.enabled or current_price <= Decimal("0"):
            return None

        m_data = market_data or {}
        raw_prices = m_data.get("price_series", [])
        if not raw_prices and staged_data:
            recent_swaps = getattr(staged_data, "recent_swaps", [])
            raw_prices = [Decimal(str(s.get("price", 0))) for s in recent_swaps if s.get("price", 0) > 0]

        regime, hurst, vol_pct = self.classify_regime(raw_prices)

        # Route to appropriate specialized sub-strategy
        selected_signal: Optional[StrategySignal] = None
        delegated_strategy_name = ""

        if regime == MarketRegime.TRENDING_BULL:
            # Delegate to Momentum Breakout or Multi-Timeframe
            selected_signal = self.momentum_strat.evaluate_entry(
                token_address, chain, current_price, pool_state, security_report, staged_data, market_data
            )
            delegated_strategy_name = "MomentumBreakout"
            if not selected_signal:
                selected_signal = self.multi_tf_strat.evaluate_entry(
                    token_address, chain, current_price, pool_state, security_report, staged_data, market_data
                )
                delegated_strategy_name = "MultiTimeframe"

        elif regime == MarketRegime.RANGING_SIDEWAYS:
            # Delegate to Mean Reversion or Liquidity Sweep
            selected_signal = self.mean_reversion_strat.evaluate_entry(
                token_address, chain, current_price, pool_state, security_report, staged_data, market_data
            )
            delegated_strategy_name = "MeanReversion"
            if not selected_signal:
                selected_signal = self.liquidity_sweep_strat.evaluate_entry(
                    token_address, chain, current_price, pool_state, security_report, staged_data, market_data
                )
                delegated_strategy_name = "LiquiditySweep"

        elif regime == MarketRegime.HIGH_VOLATILITY_BURST:
            # Delegate to Whale Accumulation Shadow with dampened sizing
            selected_signal = self.whale_strat.evaluate_entry(
                token_address, chain, current_price, pool_state, security_report, staged_data, market_data
            )
            delegated_strategy_name = "WhaleShadow"
            if selected_signal:
                # Dampen position size by 25% for high-volatility regime
                selected_signal.recommended_position_pct = max(0.35, round(selected_signal.recommended_position_pct * 0.75, 2))

        if not selected_signal:
            return None

        # Wrap signal with Adaptive Regime metadata
        selected_signal.strategy_type = self.strategy_type
        selected_signal.side = selected_signal.side or OrderSide.BUY
        selected_signal.exit_profile = selected_signal.exit_profile or ExitProfile.DYNAMIC_TRAILING
        selected_signal.indicators["detected_regime"] = regime.value
        selected_signal.indicators["hurst_exponent"] = hurst
        selected_signal.indicators["realized_vol_pct"] = vol_pct
        selected_signal.indicators["delegated_strategy"] = delegated_strategy_name
        selected_signal.rationale = f"Adaptive Regime [{regime.value.upper()}] (Hurst={hurst:.2f}, Vol={vol_pct:.1f}%): Delegated to {delegated_strategy_name}. {selected_signal.rationale}"

        return selected_signal

    def evaluate_exit(
        self,
        position: Any,
        current_price: Decimal,
        peak_price: Decimal,
        elapsed_seconds: float,
        pool_state: Optional[PoolState] = None,
    ) -> StrategyExitSignal:
        # 0. Emergency Pool Depletion Exit safeguard
        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) <= Decimal("0"):
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.EMERGENCY_DRAIN,
                    rationale="Adaptive Regime Emergency: Pool liquidity reserves depleted",
                )

        # Route exit to corresponding sub-strategy if recorded in indicators
        indicators = getattr(position, "indicators", {})
        delegated = indicators.get("delegated_strategy", "")

        if delegated == "MeanReversion":
            return self.mean_reversion_strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)
        elif delegated == "LiquiditySweep":
            return self.liquidity_sweep_strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)
        elif delegated == "WhaleShadow":
            return self.whale_strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)
        elif delegated == "MultiTimeframe":
            return self.multi_tf_strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)
        else:
            return self.momentum_strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)
