"""
alpha_engine.engine.strategy.multi_timeframe — Multi-Timeframe Confluence Strategy
=================================================================================
Implements an institutional multi-timeframe trend & momentum matrix:
1. Synthesizes directional signals across 1m, 5m, 15m, and 1h horizons.
2. Computes an aggregate Confluence Score (0-100) requiring multi-horizon alignment.
3. Guards against overextended top-wicks while riding sustained structural macro trends.
"""

from __future__ import annotations

import logging
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


class MultiTimeframeConfluenceStrategy(BaseStrategy):
    """
    Multi-Timeframe Confluence Strategy.
    Requires multi-horizon alignment across short-term execution triggers (1m/5m)
    and intermediate structural trend filters (15m/1h) before committing risk.
    """

    def __init__(
        self,
        min_confluence_score: float = 75.0,
        max_overextension_ratio: float = 1.85,
        hard_stop_loss_pct: float = -11.0,
        trailing_activation_pct: float = 25.0,
        trailing_stop_pct: float = 22.0,
        time_exit_seconds: float = 5400.0,
        enabled: bool = True,
        weight: float = 1.2,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.MULTI_TIMEFRAME,
            name="Multi-Timeframe Confluence Matrix",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [MarketRegime.TRENDING_BULL, MarketRegime.LIQUIDITY_EXPANSION]
        self._hyperparameters = {
            "min_confluence_score": min_confluence_score,
            "max_overextension_ratio": max_overextension_ratio,
            "hard_stop_loss_pct": hard_stop_loss_pct,
            "trailing_activation_pct": trailing_activation_pct,
            "trailing_stop_pct": trailing_stop_pct,
            "time_exit_seconds": time_exit_seconds,
        }

    def compute_confluence_matrix(
        self,
        current_price: Decimal,
        market_data: dict[str, Any],
        staged_data: Optional[Any] = None,
    ) -> tuple[float, dict[str, Any]]:
        """
        Calculates the Multi-Timeframe Confluence Score (0 to 100):
          - 1m Horizon (Microstructure): 25 pts
          - 5m Horizon (Local Trend): 25 pts
          - 15m Horizon (Structural Support): 25 pts
          - 1h Horizon (Macro Direction): 25 pts
        """
        score = 0.0
        details: dict[str, Any] = {}

        # 1m Horizon: Net buy delta and micro momentum
        net_buy_delta = float(getattr(staged_data, "net_buy_delta", market_data.get("net_buy_delta", 0.50)))
        if net_buy_delta >= 0.55:
            score += 25.0
            details["1m_status"] = "BULLISH_DELTA"
        elif net_buy_delta >= 0.50:
            score += 15.0
            details["1m_status"] = "NEUTRAL_DELTA"
        else:
            details["1m_status"] = "BEARISH_DELTA"

        # 5m Horizon: 5m volume surge & candle trend
        vol_surge = float(getattr(staged_data, "volume_surge_multiplier", market_data.get("volume_surge_multiplier", 1.0)))
        rsi_5m = float(getattr(staged_data, "rsi_5m", market_data.get("rsi_5m", 50.0)))
        if vol_surge >= 2.0 and 45.0 <= rsi_5m <= 75.0:
            score += 25.0
            details["5m_status"] = "BULLISH_SURGE"
        elif vol_surge >= 1.3:
            score += 15.0
            details["5m_status"] = "MODERATE_VOLUME"
        else:
            details["5m_status"] = "LOW_VOLUME"

        # 15m Horizon: Intermediate structure & EMA 21 alignment
        ema_15m_21 = Decimal(str(market_data.get("ema_15m_21", current_price * Decimal("0.95"))))
        if current_price >= ema_15m_21:
            score += 25.0
            details["15m_status"] = "ABOVE_EMA21"
        else:
            details["15m_status"] = "BELOW_EMA21"

        # 1h Horizon: Macro consolidation base & age
        token_age_h = float(getattr(staged_data, "token_age_hours", market_data.get("token_age_hours", 1.0)))
        one_h_vol = float(getattr(staged_data, "one_hour_volume", market_data.get("one_hour_volume", 10.0)))
        if token_age_h >= 0.5 and one_h_vol > 0:
            score += 25.0
            details["1h_status"] = "SUSTAINED_LIQUIDITY"
        else:
            score += 15.0
            details["1h_status"] = "EARLY_FORMATION"

        details["confluence_score"] = score
        return score, details

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

        # 1. Security Check
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled or not getattr(security_report, "freeze_authority_disabled", True):
                return None

        # 2. Compute Confluence Matrix
        m_data = market_data or {}
        score, details = self.compute_confluence_matrix(current_price, m_data, staged_data)
        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) < Decimal("0"):
                return None
            details["pool_liquidity_usd"] = float(getattr(pool_state, "liquidity_usd", 0.0))

        min_score = self._hyperparameters["min_confluence_score"]
        if score < min_score:
            return None

        # 3. Anti-Overextension Guard (Avoid buying extreme vertical blow-offs)
        base_p = Decimal(str(m_data.get("ema_15m_21", getattr(staged_data, "initial_price", current_price))))
        if base_p > Decimal("0"):
            extension_ratio = float(current_price / base_p)
            if extension_ratio > self._hyperparameters["max_overextension_ratio"]:
                return None

        # 4. Position Sizing & Staggered TP
        confidence = min(0.95, (score / 100.0))
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=66.0,
            win_loss_payoff_ratio=2.25,
            fraction=0.5,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": 1.35, "sell_pct": 40.0},
            {"trigger_multiplier": 1.85, "sell_pct": 30.0},
            {"trigger_multiplier": 3.00, "sell_pct": 30.0},
        ]

        rationale = (
            f"Multi-Timeframe Confluence Confirmed: Score={score:.0f}/100 >= {min_score:.0f}. "
            f"Matrix: 1m={details.get('1m_status')}, 5m={details.get('5m_status')}, "
            f"15m={details.get('15m_status')}, 1h={details.get('1h_status')}. Price={current_price:.8f}."
        )

        return StrategySignal(
            token_address=token_address,
            chain=chain,
            strategy_type=self.strategy_type,
            side=OrderSide.BUY,
            confidence=round(confidence, 3),
            entry_price=current_price,
            expected_slippage_bps=120,
            stop_loss_pct=self._hyperparameters["hard_stop_loss_pct"],
            take_profit_ladder=tp_ladder,
            trailing_stop_pct=self._hyperparameters["trailing_stop_pct"],
            trailing_activation_pct=self._hyperparameters["trailing_activation_pct"],
            max_hold_seconds=self._hyperparameters["time_exit_seconds"],
            kelly_fraction=0.5,
            recommended_position_pct=kelly_pct,
            rationale=rationale,
            indicators=details,
            exit_profile=ExitProfile.DYNAMIC_TRAILING,
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
        trailing_act = self._hyperparameters["trailing_activation_pct"]
        trailing_stop_pct = self._hyperparameters["trailing_stop_pct"]
        max_hold = self._hyperparameters["time_exit_seconds"]

        # 0. Emergency Pool Depletion Exit
        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) <= Decimal("0"):
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.EMERGENCY_DRAIN,
                    rationale="Multi-Timeframe Emergency: Pool liquidity reserves depleted",
                )

        # 1. Structural Trend Breakdown Exit
        indicators = getattr(position, "indicators", {})
        if indicators.get("trend_invalidation_5m", False):
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_TRAILING,
                rationale="Multi-Timeframe Trend Breakdown: 5m EMA 9 crossed below EMA 21. Closing position.",
            )

        # 2. Hard Stop Loss
        if pnl_pct <= hard_stop:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Multi-Timeframe Hard Stop: {pnl_pct:.2f}% <= {hard_stop:.2f}%",
            )

        # 3. Dynamic Trailing Stop
        peak_p = max(peak_price, current_price)
        peak_gain_pct = float((peak_p - entry_price) / entry_price * Decimal("100"))
        if peak_gain_pct >= trailing_act:
            drawdown = float((peak_p - current_price) / peak_p * Decimal("100"))
            if drawdown >= trailing_stop_pct:
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.SL_TRAILING,
                    rationale=f"Multi-Timeframe Trailing Stop: Peak Gain={peak_gain_pct:.1f}%, Drawdown={drawdown:.1f}%",
                )

        # 4. Multi-Stage Take Profit
        stage = getattr(position, "exit_stage", 0)
        if stage == 0 and pnl_pct >= 35.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.40,
                reason=TradeExitReason.TP_25,
                rationale="Multi-Timeframe TP1 Hit (+35.0%): Scaling out 40%",
                stage_index=1,
            )
        elif stage == 1 and pnl_pct >= 85.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_50,
                rationale="Multi-Timeframe TP2 Hit (+85.0%): Scaling out 50% of remainder",
                stage_index=2,
            )
        elif stage >= 2 and pnl_pct >= 200.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_2X,
                rationale="Multi-Timeframe TP3 Runner Hit (+200.0%): Closing position",
                stage_index=3,
            )

        # 5. Time Limit
        if elapsed_seconds >= max_hold:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT,
                rationale=f"Multi-Timeframe Time Limit ({elapsed_seconds/3600:.1f}h) reached",
            )

        return StrategyExitSignal(should_exit=False)
