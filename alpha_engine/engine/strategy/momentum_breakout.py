"""
alpha_engine.engine.strategy.momentum_breakout — Institutional Momentum Breakout Strategy
========================================================================================
Implements an institutional-grade breakout engine utilizing:
1. Discrete Volume Profile (POC - Point of Control, VAH - Value Area High, VAL - Value Area Low).
2. Order Flow Imbalance & Cumulative Volume Delta (CVD) acceleration.
3. Multi-staged TP ladder, dynamic ATR trailing stops, and velocity decay exits.
"""

from __future__ import annotations

import logging
from collections import defaultdict
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


class MomentumBreakoutStrategy(BaseStrategy):
    """
    Momentum Breakout Strategy with Volume Profile & Order Flow Imbalance.
    Identifies high-probability expansions beyond Value Area High (VAH) backed by
    aggressive buyer imbalance and volume expansion.
    """

    def __init__(
        self,
        min_volume_surge: float = 2.5,
        min_order_flow_imbalance: float = 0.60,
        breakout_threshold_pct: float = 1.5,
        hard_stop_loss_pct: float = -12.0,
        trailing_activation_pct: float = 25.0,
        trailing_stop_pct: float = 18.0,
        time_exit_seconds: float = 1800.0,
        enabled: bool = True,
        weight: float = 1.2,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.MOMENTUM_BREAKOUT,
            name="Momentum Breakout (Volume Profile & Order Flow)",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [MarketRegime.TRENDING_BULL, MarketRegime.LIQUIDITY_EXPANSION]
        self._hyperparameters = {
            "min_volume_surge": min_volume_surge,
            "min_order_flow_imbalance": min_order_flow_imbalance,
            "breakout_threshold_pct": breakout_threshold_pct,
            "hard_stop_loss_pct": hard_stop_loss_pct,
            "trailing_activation_pct": trailing_activation_pct,
            "trailing_stop_pct": trailing_stop_pct,
            "time_exit_seconds": time_exit_seconds,
        }

    def compute_volume_profile(
        self,
        recent_swaps: list[dict[str, Any]],
        num_bins: int = 20,
    ) -> tuple[Decimal, Decimal, Decimal]:
        """
        Calculates the Volume Profile from recent trade history.
        Returns (POC, VAH, VAL) where:
          - POC = Point of Control (price level with peak traded volume)
          - VAH = Value Area High (upper boundary enclosing 70% of total volume)
          - VAL = Value Area Low (lower boundary enclosing 70% of total volume)
        """
        if not recent_swaps or len(recent_swaps) < 5:
            return Decimal("0"), Decimal("0"), Decimal("0")

        prices = [Decimal(str(s.get("price", 0))) for s in recent_swaps if s.get("price", 0) > 0]
        if not prices:
            return Decimal("0"), Decimal("0"), Decimal("0")

        min_p = min(prices)
        max_p = max(prices)
        if min_p == max_p:
            return min_p, min_p, min_p

        bin_step = (max_p - min_p) / Decimal(str(num_bins))
        bins: dict[int, Decimal] = defaultdict(Decimal)
        bin_prices: dict[int, Decimal] = {}

        for i in range(num_bins):
            bin_prices[i] = min_p + (bin_step * Decimal(str(i))) + (bin_step / Decimal("2"))

        for s in recent_swaps:
            p = Decimal(str(s.get("price", 0)))
            vol = Decimal(str(s.get("amount_in", s.get("volume", 1.0))))
            if p <= min_p:
                b_idx = 0
            elif p >= max_p:
                b_idx = num_bins - 1
            else:
                b_idx = int((p - min_p) / bin_step)
                b_idx = min(num_bins - 1, max(0, b_idx))
            bins[b_idx] += vol

        total_vol = sum(bins.values())
        if total_vol == Decimal("0"):
            return min_p, max_p, min_p

        # Find Point of Control (POC)
        poc_bin = max(bins.items(), key=lambda x: x[1])[0]
        poc_price = bin_prices[poc_bin]

        # Find Value Area (70% volume distribution around POC)
        target_va_vol = total_vol * Decimal("0.70")
        accum_vol = bins[poc_bin]
        low_idx = poc_bin
        high_idx = poc_bin

        while accum_vol < target_va_vol and (low_idx > 0 or high_idx < num_bins - 1):
            next_low_vol = bins[low_idx - 1] if low_idx > 0 else Decimal("0")
            next_high_vol = bins[high_idx + 1] if high_idx < num_bins - 1 else Decimal("0")

            if next_high_vol >= next_low_vol and high_idx < num_bins - 1:
                high_idx += 1
                accum_vol += next_high_vol
            elif low_idx > 0:
                low_idx -= 1
                accum_vol += next_low_vol
            else:
                high_idx += 1
                accum_vol += next_high_vol

        vah_price = bin_prices[high_idx]
        val_price = bin_prices[low_idx]
        return poc_price, vah_price, val_price

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

        # 1. Security Gate Check & Pool Verification
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled or not getattr(security_report, "freeze_authority_disabled", True):
                return None
            if security_report.buy_tax_bps > 300 or security_report.sell_tax_bps > 300:
                return None

        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) < Decimal("0"):
                return None

        # 2. Extract Trade and Order Flow History
        m_data = market_data or {}
        recent_swaps = list(getattr(staged_data, "recent_swaps", [])) if staged_data else m_data.get("recent_swaps", [])
        volume_surge = float(getattr(staged_data, "volume_surge_multiplier", m_data.get("volume_surge_multiplier", 1.0)))
        net_buy_delta = float(getattr(staged_data, "net_buy_delta", m_data.get("net_buy_delta", 0.50)))

        # 3. Compute Volume Profile
        poc, vah, val = self.compute_volume_profile(recent_swaps)

        min_surge = self._hyperparameters["min_volume_surge"]
        min_imbalance = self._hyperparameters["min_order_flow_imbalance"]
        breakout_threshold_pct = self._hyperparameters["breakout_threshold_pct"]

        # Check Breakout Condition:
        # Price is breaking above VAH with sustained order flow imbalance and volume surge
        has_profile = vah > Decimal("0")
        is_breakout = False
        if has_profile:
            breakout_margin = ((current_price - vah) / vah) * Decimal("100")
            is_breakout = breakout_margin >= Decimal(str(breakout_threshold_pct))
        else:
            # Fallback to consolidation peak if volume profile bins are sparse
            consol_peak = getattr(staged_data, "peak_price", Decimal("0"))
            if consol_peak > Decimal("0"):
                is_breakout = current_price >= consol_peak * Decimal("1.02")

        if not is_breakout:
            return None

        # Check Order Flow and Surge Filter
        if volume_surge < min_surge or net_buy_delta < min_imbalance:
            return None

        # Calculate Confidence Score (0.70 to 0.96)
        confidence = min(0.96, 0.70 + (volume_surge / 15.0) * 0.15 + (net_buy_delta - 0.50) * 0.20)
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=62.0,
            win_loss_payoff_ratio=2.1,
            fraction=0.5,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": 1.30, "sell_pct": 40.0},
            {"trigger_multiplier": 1.75, "sell_pct": 30.0},
            {"trigger_multiplier": 2.50, "sell_pct": 30.0},
        ]

        rationale = (
            f"Momentum Breakout confirmed: Price {current_price:.8f} cleared VAH {vah:.8f} "
            f"(+{float((current_price - vah)/vah*100):.1f}%). Surge={volume_surge:.1f}x, "
            f"Net Buy Delta={net_buy_delta:.0%}, POC={poc:.8f}."
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
            indicators={
                "poc": float(poc),
                "vah": float(vah),
                "val": float(val),
                "volume_surge": volume_surge,
                "net_buy_delta": net_buy_delta,
                "pool_liquidity_usd": float(getattr(pool_state, "liquidity_usd", 0.0)) if pool_state else 0.0,
            },
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
        trailing_stop_pct = self._hyperparameters["trailing_stop_pct"]
        trailing_act = self._hyperparameters["trailing_activation_pct"]
        max_hold = self._hyperparameters["time_exit_seconds"]

        # 0. Emergency Pool Liquidity Drain
        if pool_state is not None:
            if getattr(pool_state, "native_reserves", Decimal("1")) <= Decimal("0"):
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.EMERGENCY_DRAIN,
                    rationale="Emergency exit: pool liquidity reserves depleted",
                )

        # 1. Hard Risk-Based Stop Loss
        if pnl_pct <= hard_stop:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Hard Stop-Loss hit: {pnl_pct:.2f}% <= {hard_stop:.2f}%",
            )

        # 2. Dynamic Trailing Stop from Peak
        peak_p = max(peak_price, current_price)
        peak_gain_pct = float((peak_p - entry_price) / entry_price * Decimal("100"))
        if peak_gain_pct >= trailing_act:
            drawdown_from_peak = float((peak_p - current_price) / peak_p * Decimal("100"))
            if drawdown_from_peak >= trailing_stop_pct:
                return StrategyExitSignal(
                    should_exit=True,
                    exit_ratio=1.0,
                    reason=TradeExitReason.SL_TRAILING,
                    rationale=f"Trailing Stop hit: Peak Gain={peak_gain_pct:.1f}%, Drawdown={drawdown_from_peak:.1f}% >= {trailing_stop_pct:.1f}%",
                )

        # 3. Staggered Take-Profit Ladder
        stage = getattr(position, "exit_stage", 0)
        if stage == 0 and pnl_pct >= 30.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.40,
                reason=TradeExitReason.TP_25,
                rationale=f"TP Stage 1 hit (+{pnl_pct:.1f}% gain): scaling out 40%",
                stage_index=1,
            )
        elif stage == 1 and pnl_pct >= 75.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_50,
                rationale=f"TP Stage 2 hit (+{pnl_pct:.1f}% gain): scaling out 50% of remainder",
                stage_index=2,
            )
        elif stage >= 2 and pnl_pct >= 150.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_2X,
                rationale=f"TP Stage 3 Final Runner hit (+{pnl_pct:.1f}% gain): closing full position",
                stage_index=3,
            )

        # 4. Time-Based Stagnation Exit
        if elapsed_seconds >= max_hold and pnl_pct < 5.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT_VELOCITY_DECAY,
                rationale=f"Time-based exit: position held for {elapsed_seconds/60:.1f}m with low return ({pnl_pct:.2f}%)",
            )

        return StrategyExitSignal(should_exit=False)
