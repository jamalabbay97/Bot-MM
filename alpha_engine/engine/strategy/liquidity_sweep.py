"""
alpha_engine.engine.strategy.liquidity_sweep — Liquidity Sweep & Structural Reversal Strategy
============================================================================================
Implements an institutional smart-money liquidity sweep engine:
1. Detects clustered swing lows and liquidity pools.
2. Identifies stop-hunt sweeps where price penetrates the level and immediately reclaims it.
3. Confirms seller exhaustion and aggressive delta absorption before executing long entries.
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


class LiquiditySweepStrategy(BaseStrategy):
    """
    Liquidity Sweep & Reversal Strategy.
    Capitalizes on stop-loss cascades and liquidity runs where automated sellers are trapped
    and institutional buyers absorb supply below key support, followed by an immediate reclaim.
    """

    def __init__(
        self,
        lookback_swings: int = 30,
        sweep_tolerance_pct: float = 3.5,
        min_reclaim_pct: float = 0.5,
        min_delta_absorption: float = 0.55,
        hard_stop_loss_pct: float = -10.0,
        trailing_activation_pct: float = 25.0,
        trailing_stop_pct: float = 20.0,
        time_exit_seconds: float = 3600.0,
        enabled: bool = True,
        weight: float = 1.1,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.LIQUIDITY_SWEEP,
            name="Liquidity Sweep & Reversal",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [MarketRegime.RANGING_SIDEWAYS, MarketRegime.TRENDING_BULL, MarketRegime.HIGH_VOLATILITY_BURST]
        self._hyperparameters = {
            "lookback_swings": lookback_swings,
            "sweep_tolerance_pct": sweep_tolerance_pct,
            "min_reclaim_pct": min_reclaim_pct,
            "min_delta_absorption": min_delta_absorption,
            "hard_stop_loss_pct": hard_stop_loss_pct,
            "trailing_activation_pct": trailing_activation_pct,
            "trailing_stop_pct": trailing_stop_pct,
            "time_exit_seconds": time_exit_seconds,
        }

    def detect_sweep_and_reclaim(
        self,
        candles: list[dict[str, Any]],
        current_price: Decimal,
    ) -> tuple[bool, Decimal, Decimal, Decimal]:
        """
        Scans candle history to locate the prior key swing low and determine
        if a liquidity sweep has occurred and is currently being reclaimed.
        Returns (is_sweep, swing_low, sweep_trough, swing_high).
        """
        if not candles or len(candles) < 8:
            return False, Decimal("0"), Decimal("0"), Decimal("0")

        # Find Swing Low and Swing High in lookback excluding the most recent 2 candles
        prior_candles = candles[:-2]
        lows = [Decimal(str(c.get("low", c.get("price", 0)))) for c in prior_candles if c.get("low", c.get("price", 0)) > 0]
        highs = [Decimal(str(c.get("high", c.get("price", 0)))) for c in prior_candles if c.get("high", c.get("price", 0)) > 0]

        if not lows or not highs:
            return False, Decimal("0"), Decimal("0"), Decimal("0")

        swing_low = min(lows)
        swing_high = max(highs)

        # Inspect the recent 2 candles for the sweep wick
        recent = candles[-2:]
        recent_troughs = [Decimal(str(c.get("low", c.get("price", 0)))) for c in recent if c.get("low", c.get("price", 0)) > 0]
        if not recent_troughs:
            return False, swing_low, Decimal("0"), swing_high

        sweep_trough = min(recent_troughs)

        # A sweep requires the trough to dip strictly below the prior swing low
        if sweep_trough >= swing_low:
            return False, swing_low, sweep_trough, swing_high

        # Check that the sweep was within tolerance (not a complete structural collapse)
        sweep_depth_pct = float((swing_low - sweep_trough) / swing_low * Decimal("100"))
        max_sweep = self._hyperparameters["sweep_tolerance_pct"]
        if sweep_depth_pct > max_sweep:
            return False, swing_low, sweep_trough, swing_high

        # Check Reclaim: Current price must have bounced back ABOVE the swing low
        min_reclaim = Decimal(str(self._hyperparameters["min_reclaim_pct"])) / Decimal("100")
        is_reclaimed = current_price >= (swing_low * (Decimal("1") + min_reclaim))

        return is_reclaimed, swing_low, sweep_trough, swing_high

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

        # 1. Security Check & Pool State
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled:
                return None

        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) < Decimal("0"):
                return None

        # 2. Extract Candles
        m_data = market_data or {}
        candles = list(getattr(staged_data, "five_min_candles", [])) if staged_data else m_data.get("candles", [])
        if not candles:
            # Synthetic candles from volume buckets
            buckets = list(getattr(staged_data, "volume_buckets", [])) if staged_data else []
            if len(buckets) >= 8:
                candles = [
                    {
                        "open": float(b.total_volume),
                        "close": float(b.total_volume),
                        "low": float(staged_data.trough_price if hasattr(staged_data, "trough_price") else current_price),
                        "high": float(staged_data.peak_price if hasattr(staged_data, "peak_price") else current_price),
                        "price": float(current_price),
                    }
                    for b in buckets
                ]

        is_reclaim, swing_low, sweep_trough, swing_high = self.detect_sweep_and_reclaim(candles, current_price)
        if not is_reclaim:
            # Secondary check: staged token trough bounce
            trough_p = getattr(staged_data, "trough_price", Decimal("0"))
            initial_p = getattr(staged_data, "initial_price", Decimal("0"))
            if trough_p > Decimal("0") and initial_p > Decimal("0"):
                if trough_p < initial_p and current_price >= initial_p * Decimal("1.02"):
                    is_reclaim = True
                    swing_low = initial_p
                    sweep_trough = trough_p
                    swing_high = getattr(staged_data, "peak_price", initial_p * Decimal("1.5"))

        if not is_reclaim or swing_low <= Decimal("0"):
            return None

        # 3. Order Flow Absorption Check
        net_buy_delta = float(getattr(staged_data, "net_buy_delta", m_data.get("net_buy_delta", 0.50)))
        if net_buy_delta < self._hyperparameters["min_delta_absorption"]:
            return None

        # Calculate Invalidation Stop Loss based on the swept wick low
        inval_sl_pct = float((sweep_trough - current_price) / current_price * Decimal("100")) - 1.5
        inval_sl_pct = max(self._hyperparameters["hard_stop_loss_pct"], min(-4.0, inval_sl_pct))

        # Confidence & Position Sizing
        confidence = min(0.94, 0.74 + (net_buy_delta - 0.50) * 0.30)
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=65.0,
            win_loss_payoff_ratio=2.2,
            fraction=0.5,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": 1.35, "sell_pct": 40.0},
            {"trigger_multiplier": 1.80, "sell_pct": 30.0},
            {"trigger_multiplier": 2.60, "sell_pct": 30.0},
        ]

        rationale = (
            f"Liquidity Sweep Reversal confirmed: Swept swing low {swing_low:.8f} (trough {sweep_trough:.8f}) "
            f"and reclaimed at {current_price:.8f}. Net buy absorption={net_buy_delta:.0%}. "
            f"Invalidation SL set at {inval_sl_pct:.1f}%."
        )

        return StrategySignal(
            token_address=token_address,
            chain=chain,
            strategy_type=self.strategy_type,
            side=OrderSide.BUY,
            confidence=round(confidence, 3),
            entry_price=current_price,
            expected_slippage_bps=110,
            stop_loss_pct=inval_sl_pct,
            take_profit_ladder=tp_ladder,
            trailing_stop_pct=self._hyperparameters["trailing_stop_pct"],
            trailing_activation_pct=self._hyperparameters["trailing_activation_pct"],
            max_hold_seconds=self._hyperparameters["time_exit_seconds"],
            kelly_fraction=0.5,
            recommended_position_pct=kelly_pct,
            rationale=rationale,
            indicators={
                "swing_low": float(swing_low),
                "sweep_trough": float(sweep_trough),
                "swing_high": float(swing_high),
                "net_buy_delta": net_buy_delta,
                "pool_liquidity_usd": float(getattr(pool_state, "liquidity_usd", 0.0)) if pool_state else 0.0,
            },
            exit_profile=ExitProfile.LIQUIDITY_TARGET,
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
                    rationale="Liquidity Sweep Emergency: Pool liquidity reserves depleted",
                )

        # 1. Stop Loss
        pos_sl = getattr(position, "stop_loss_pct", hard_stop)
        if pnl_pct <= pos_sl:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Liquidity Sweep Invalidation Stop: {pnl_pct:.2f}% <= {pos_sl:.2f}%",
            )

        # 2. Dynamic Trailing Stop
        peak_p = max(peak_price, current_price)
        peak_gain_pct = float((peak_p - entry_price) / entry_price * Decimal("100"))
        if peak_gain_pct >= trailing_act:
            drawdown = float((peak_p - current_price) / peak_p * Decimal("100"))
            if drawdown >= trailing_stop_pct:
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.SL_TRAILING,
                    rationale=f"Trailing Stop hit: Peak Gain={peak_gain_pct:.1f}%, Drawdown={drawdown:.1f}%",
                )

        # 3. Take Profit Staged Ladder
        stage = getattr(position, "exit_stage", 0)
        if stage == 0 and pnl_pct >= 35.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.40,
                reason=TradeExitReason.TP_25,
                rationale="Liquidity Sweep TP1 hit (+35.0%): Scaling out 40%",
                stage_index=1,
            )
        elif stage == 1 and pnl_pct >= 80.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_50,
                rationale="Liquidity Sweep TP2 hit (+80.0%): Scaling out 50% of remainder",
                stage_index=2,
            )
        elif stage >= 2 and pnl_pct >= 160.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_2X,
                rationale="Liquidity Sweep Final Target hit (+160.0%): Closing position",
                stage_index=3,
            )

        # 4. Time Expiration
        if elapsed_seconds >= max_hold:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT,
                rationale=f"Liquidity Sweep Time Limit ({elapsed_seconds/60:.1f}m) reached",
            )

        return StrategyExitSignal(should_exit=False)
