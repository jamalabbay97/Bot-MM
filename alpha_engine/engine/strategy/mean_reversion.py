"""
alpha_engine.engine.strategy.mean_reversion — Short-Term Statistical Mean-Reversion Strategy
========================================================================================
Implements a quantitative mean-reversion engine using:
1. Rolling Z-Score and Dynamic Volatility Bollinger Bands.
2. Absorption dip verification to filter falling-knife rugpulls.
3. Multi-staged mean-reversion TP ladder (Mean reversion at Z=0, Upper Band at Z=+2).
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
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    OrderSide,
    StrategyType,
    TradeExitReason,
)
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class MeanReversionStrategy(BaseStrategy):
    """
    Statistical Mean-Reversion Strategy using Rolling Z-Score & Dynamic Bollinger Bands.
    Executes when a liquid, verified token experiences extreme short-term oversold deviations
    (Z < -2.0) accompanied by order flow absorption at structural support.
    """

    def __init__(
        self,
        lookback_periods: int = 20,
        z_score_entry_threshold: float = -2.0,
        bollinger_num_std: float = 2.0,
        hard_stop_loss_pct: float = -9.0,
        z_score_stop_threshold: float = -3.2,
        time_exit_seconds: float = 2700.0,
        min_net_buy_delta: float = 0.45,
        enabled: bool = True,
        weight: float = 1.0,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.MEAN_REVERSION,
            name="Mean Reversion (Z-Score & Dynamic Bollinger Bands)",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [MarketRegime.RANGING_SIDEWAYS, MarketRegime.TRENDING_BULL]
        self._hyperparameters = {
            "lookback_periods": lookback_periods,
            "z_score_entry_threshold": z_score_entry_threshold,
            "bollinger_num_std": bollinger_num_std,
            "hard_stop_loss_pct": hard_stop_loss_pct,
            "z_score_stop_threshold": z_score_stop_threshold,
            "time_exit_seconds": time_exit_seconds,
            "min_net_buy_delta": min_net_buy_delta,
        }

    def compute_z_score_and_bands(
        self,
        price_history: list[Decimal],
    ) -> tuple[float, Decimal, Decimal, Decimal]:
        """
        Computes (z_score, mean, upper_band, lower_band) over rolling lookback.
        """
        if not price_history or len(price_history) < 5:
            return 0.0, Decimal("0"), Decimal("0"), Decimal("0")

        n = len(price_history)
        mean_p = sum(price_history) / Decimal(str(n))
        variance = sum((p - mean_p) ** 2 for p in price_history) / Decimal(str(n))
        std_p = Decimal(str(math.sqrt(float(variance))))

        if std_p <= Decimal("0"):
            return 0.0, mean_p, mean_p, mean_p

        latest = price_history[-1]
        z_score = float((latest - mean_p) / std_p)
        num_std = Decimal(str(self._hyperparameters["bollinger_num_std"]))
        upper_band = mean_p + (num_std * std_p)
        lower_band = mean_p - (num_std * std_p)

        return z_score, mean_p, upper_band, lower_band

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

        # 1. Security Gatekeeper Integrity & Pool Verification
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled:
                return None
            if security_report.lp_burned_ratio < 0.85:
                return None

        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) < Decimal("0"):
                return None

        # 2. Extract Price Series
        m_data = market_data or {}
        raw_prices = m_data.get("price_series", [])
        if not raw_prices and staged_data:
            # Build prices from volume buckets or recent swaps
            recent_swaps = getattr(staged_data, "recent_swaps", [])
            raw_prices = [Decimal(str(s.get("price", 0))) for s in recent_swaps if s.get("price", 0) > 0]

        if len(raw_prices) < 8:
            return None

        lookback = self._hyperparameters["lookback_periods"]
        window_prices = raw_prices[-lookback:]
        z_score, mean_p, upper_band, lower_band = self.compute_z_score_and_bands(window_prices)

        z_threshold = self._hyperparameters["z_score_entry_threshold"]

        # 3. Entry Condition: Extreme statistical deviation below mean (Z <= -2.0)
        # Price is at or below the Lower Bollinger Band
        if z_score > z_threshold and current_price > lower_band:
            return None

        # 4. Anti-Falling-Knife Absorption Check:
        # Require net buy delta stabilization (buyers stepping in at the extreme discount)
        net_buy_delta = float(getattr(staged_data, "net_buy_delta", m_data.get("net_buy_delta", 0.50)))
        if net_buy_delta < self._hyperparameters["min_net_buy_delta"]:
            return None

        # Calculate high-probability bounce Kelly sizing
        confidence = min(0.92, 0.72 + abs(z_score + 2.0) * 0.08)
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=64.0,
            win_loss_payoff_ratio=1.65,
            fraction=0.5,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": float(mean_p / current_price), "sell_pct": 50.0},
            {"trigger_multiplier": float(upper_band / current_price), "sell_pct": 50.0},
        ]

        # Sanitize TP triggers in case spread is tight
        for tp in tp_ladder:
            if tp["trigger_multiplier"] < 1.10:
                tp["trigger_multiplier"] = 1.15

        rationale = (
            f"Mean Reversion Dip detected: Z-Score={z_score:.2f} <= {z_threshold:.1f}, "
            f"Current={current_price:.8f} <= LowerBand={lower_band:.8f}, Mean={mean_p:.8f}. "
            f"Buyer absorption net delta={net_buy_delta:.0%}."
        )

        return StrategySignal(
            token_address=token_address,
            chain=chain,
            strategy_type=self.strategy_type,
            side=OrderSide.BUY,
            confidence=round(confidence, 3),
            entry_price=current_price,
            expected_slippage_bps=90,
            stop_loss_pct=self._hyperparameters["hard_stop_loss_pct"],
            take_profit_ladder=tp_ladder,
            trailing_stop_pct=15.0,
            trailing_activation_pct=18.0,
            max_hold_seconds=self._hyperparameters["time_exit_seconds"],
            kelly_fraction=0.5,
            recommended_position_pct=kelly_pct,
            rationale=rationale,
            indicators={
                "z_score": z_score,
                "mean_price": float(mean_p),
                "upper_band": float(upper_band),
                "lower_band": float(lower_band),
                "net_buy_delta": net_buy_delta,
                "pool_liquidity_usd": float(getattr(pool_state, "liquidity_usd", 0.0)) if pool_state else 0.0,
            },
            exit_profile=ExitProfile.MEAN_REVERSION_EXIT,
        )

    def evaluate_exit(
        self,
        position: Any,
        current_price: Decimal,
        peak_price: Decimal,
        elapsed_seconds: float,
        pool_state: Optional[PoolState] = None,
    ) -> StrategyExitSignal:
        entry_price = getattr(position, "entry_price", Decimal("0"))
        if entry_price <= Decimal("0"):
            return StrategyExitSignal(should_exit=False)

        pnl_pct = float((current_price - entry_price) / entry_price * Decimal("100"))
        hard_stop = self._hyperparameters["hard_stop_loss_pct"]
        max_hold = self._hyperparameters["time_exit_seconds"]

        # 0. Emergency Pool Depletion Exit
        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) <= Decimal("0"):
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.EMERGENCY_DRAIN,
                    rationale="Mean Reversion Emergency: Pool liquidity reserves depleted",
                )

        # 0b. Peak Drawdown Protection
        peak_p = max(peak_price, current_price)
        if peak_p > entry_price * Decimal("1.10"):
            drawdown_from_peak = float((peak_p - current_price) / peak_p * Decimal("100"))
            if drawdown_from_peak >= 15.0:
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.SL_TRAILING,
                    rationale=f"Mean Reversion Trailing Drawdown from peak ({drawdown_from_peak:.1f}% >= 15.0%)",
                )

        # 1. Hard Stop Loss
        if pnl_pct <= hard_stop:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Mean-Reversion Hard Stop: {pnl_pct:.2f}% <= {hard_stop:.2f}%",
            )

        # 2. Staggered Mean-Reversion TP
        stage = getattr(position, "exit_stage", 0)
        indicators = getattr(position, "indicators", {})
        mean_p = Decimal(str(indicators.get("mean_price", 0)))
        upper_band = Decimal(str(indicators.get("upper_band", 0)))

        # Target 1: Reversion to Statistical Mean (Z=0)
        if stage == 0 and ((mean_p > Decimal("0") and current_price >= mean_p) or pnl_pct >= 15.0):
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_25,
                rationale=f"Mean Reversion Target 1 Hit (Reverted to Mean, +{pnl_pct:.1f}%): Scaling out 50%",
                stage_index=1,
            )

        # Target 2: Expansion to Upper Bollinger Band (Z=+2.0)
        if stage == 1 and ((upper_band > Decimal("0") and current_price >= upper_band) or pnl_pct >= 35.0):
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_50,
                rationale=f"Mean Reversion Target 2 Hit (Reached Upper Band, +{pnl_pct:.1f}%): Closing remaining position",
                stage_index=2,
            )

        # 3. Time Invalidation Exit
        if elapsed_seconds >= max_hold:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT,
                rationale=f"Mean Reversion Time Limit reached ({elapsed_seconds/60:.1f}m): Exiting position",
            )

        return StrategyExitSignal(should_exit=False)
