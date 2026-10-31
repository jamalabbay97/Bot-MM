"""
alpha_engine.engine.strategy.narrative_velocity — Narrative Velocity Strategy
=============================================================================
Implements a social momentum & viral narrative engine:
1. Interfaces with ReachAnalyst, Telegram firehose, and X/Twitter stream ingestion.
2. Computes Narrative Velocity (mention acceleration & channel diversity entropy).
3. Applies anti-bot raid & copycat dilution filters before entering explosive social metas.
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


class NarrativeVelocityStrategy(BaseStrategy):
    """
    Narrative Velocity Strategy.
    Identifies early parabolic virality when authentic social mindshare and mention velocity
    accelerate across distinct community channels ahead of broader retail discovery.
    """

    def __init__(
        self,
        min_reach_score: float = 70.0,
        min_velocity_index: float = 65.0,
        min_organic_channels: int = 3,
        max_template_duplicates: int = 2,
        min_market_cap_usd: float = 15_000.0,
        max_market_cap_usd: float = 3_500_000.0,
        hard_stop_loss_pct: float = -13.0,
        trailing_activation_pct: float = 30.0,
        trailing_stop_pct: float = 25.0,
        time_exit_seconds: float = 7200.0,
        enabled: bool = True,
        weight: float = 1.15,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.NARRATIVE_VELOCITY,
            name="Narrative Velocity (Reach & Social Momentum)",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [
            MarketRegime.TRENDING_BULL,
            MarketRegime.HIGH_VOLATILITY_BURST,
            MarketRegime.LIQUIDITY_EXPANSION,
        ]
        self._hyperparameters = {
            "min_reach_score": min_reach_score,
            "min_velocity_index": min_velocity_index,
            "min_organic_channels": min_organic_channels,
            "max_template_duplicates": max_template_duplicates,
            "min_market_cap_usd": min_market_cap_usd,
            "max_market_cap_usd": max_market_cap_usd,
            "hard_stop_loss_pct": hard_stop_loss_pct,
            "trailing_activation_pct": trailing_activation_pct,
            "trailing_stop_pct": trailing_stop_pct,
            "time_exit_seconds": time_exit_seconds,
        }

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

        # 1. Security Baseline
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled or not getattr(security_report, "freeze_authority_disabled", True):
                return None

        # 2. Extract Social & Sentiment Data
        m_data = market_data or {}
        reach_verdict = getattr(staged_data, "reach_verdict", None) or m_data.get("reach_verdict")
        reach_score = float(getattr(reach_verdict, "score", getattr(staged_data, "reach_score", m_data.get("reach_score", 0.0))))
        reach_flags = list(getattr(reach_verdict, "flags", getattr(staged_data, "reach_flags", m_data.get("reach_flags", []))))

        # Anti-Raid Filter
        if "PAID_BOT_RAID_DETECTED" in reach_flags or "TEMPLATE_FARM_DETECTED" in reach_flags:
            return None

        template_duplicates = int(m_data.get("template_duplicate_count", 0))
        if template_duplicates > self._hyperparameters["max_template_duplicates"]:
            return None

        channel_count = int(m_data.get("distinct_channels_count", 1))
        velocity_index = float(m_data.get("velocity_index", reach_score))

        # Check thresholds
        min_reach = self._hyperparameters["min_reach_score"]
        min_velocity = self._hyperparameters["min_velocity_index"]
        min_channels = self._hyperparameters["min_organic_channels"]

        if reach_score < min_reach and velocity_index < min_velocity:
            return None

        if channel_count < min_channels and velocity_index < 85.0:
            return None

        # 3. Market Cap Sweet Spot Filter
        mcap = float(getattr(pool_state, "market_cap_usd", 0.0))
        if mcap > 0:
            if mcap < self._hyperparameters["min_market_cap_usd"] or mcap > self._hyperparameters["max_market_cap_usd"]:
                return None

        # 4. Position Sizing & Staggered TP
        confidence = min(0.94, 0.70 + (reach_score / 100.0) * 0.15 + (velocity_index / 100.0) * 0.10)
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=63.0,
            win_loss_payoff_ratio=2.6,
            fraction=0.5,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": 1.50, "sell_pct": 40.0},
            {"trigger_multiplier": 2.50, "sell_pct": 30.0},
            {"trigger_multiplier": 5.00, "sell_pct": 30.0},
        ]

        rationale = (
            f"Narrative Velocity Surge: ReachScore={reach_score:.1f}/100, VelocityIndex={velocity_index:.1f}, "
            f"DistinctChannels={channel_count}. Organic mindshare verified, Mcap=${mcap:,.0f}."
        )

        return StrategySignal(
            token_address=token_address,
            chain=chain,
            strategy_type=self.strategy_type,
            side=OrderSide.BUY,
            confidence=round(confidence, 3),
            entry_price=current_price,
            expected_slippage_bps=140,
            stop_loss_pct=self._hyperparameters["hard_stop_loss_pct"],
            take_profit_ladder=tp_ladder,
            trailing_stop_pct=self._hyperparameters["trailing_stop_pct"],
            trailing_activation_pct=self._hyperparameters["trailing_activation_pct"],
            max_hold_seconds=self._hyperparameters["time_exit_seconds"],
            kelly_fraction=0.5,
            recommended_position_pct=kelly_pct,
            rationale=rationale,
            indicators={
                "reach_score": reach_score,
                "velocity_index": velocity_index,
                "channel_count": channel_count,
                "market_cap_usd": mcap,
            },
            exit_profile=ExitProfile.MOMENTUM_RUNNER,
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
                    rationale="Narrative Velocity Emergency: Pool liquidity reserves depleted",
                )

        # 1. Social Narrative Decay Exit
        indicators = getattr(position, "indicators", {})
        if indicators.get("social_velocity_collapsed", False) and pnl_pct < 10.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT_VELOCITY_DECAY,
                rationale="Narrative Velocity collapsed: Social engagement dropped > 65%. Exiting position.",
            )

        # 2. Hard Stop Loss
        if pnl_pct <= hard_stop:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Narrative Velocity Hard Stop: {pnl_pct:.2f}% <= {hard_stop:.2f}%",
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
                    rationale=f"Narrative Velocity Trailing Stop: Peak Gain={peak_gain_pct:.1f}%, Drawdown={drawdown:.1f}%",
                )

        # 4. Multi-Stage Take Profit
        stage = getattr(position, "exit_stage", 0)
        if stage == 0 and pnl_pct >= 50.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.40,
                reason=TradeExitReason.TP_25,
                rationale="Narrative Velocity TP1 Hit (+50.0%): Scaling out 40%",
                stage_index=1,
            )
        elif stage == 1 and pnl_pct >= 150.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_50,
                rationale="Narrative Velocity TP2 Hit (+150.0%): Scaling out 50% of remainder",
                stage_index=2,
            )
        elif stage >= 2 and pnl_pct >= 400.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_2X,
                rationale="Narrative Velocity TP3 Viral Runner Hit (+400.0%): Closing position",
                stage_index=3,
            )

        # 5. Time Limit
        if elapsed_seconds >= max_hold:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT,
                rationale=f"Narrative Velocity Time Expiration ({elapsed_seconds/3600:.1f}h)",
            )

        return StrategyExitSignal(should_exit=False)
