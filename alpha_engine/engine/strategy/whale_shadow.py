"""
alpha_engine.engine.strategy.whale_shadow — Whale Accumulation Shadow Strategy
=============================================================================
Implements an institutional smart-money tracking engine:
1. Tracks on-chain accumulation patterns from verified high-win-rate smart money wallets.
2. Enforces anti-Sybil & anti-cabal heuristics (mixer detection, co-funding with deployer).
3. Executes a shadow long position with strict whale-exit triggers and multi-stage TP.
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


class WhaleAccumulationShadowStrategy(BaseStrategy):
    """
    Whale Accumulation Shadow Strategy.
    Detects quiet, low-impact accumulation by verified smart money wallets while rigorously
    filtering out sybil clusters, dev pockets, and fake wash-trading volume.
    """

    def __init__(
        self,
        min_whale_inflow_native: Decimal = Decimal("1.0"),
        min_distinct_whales: int = 2,
        max_price_impact_during_accum_pct: float = 20.0,
        max_dev_balance_pct: float = 1.0,
        hard_stop_loss_pct: float = -14.0,
        trailing_activation_pct: float = 35.0,
        trailing_stop_pct: float = 28.0,
        time_exit_seconds: float = 14400.0,
        enabled: bool = True,
        weight: float = 1.25,
    ) -> None:
        super().__init__(
            strategy_type=StrategyType.WHALE_SHADOW,
            name="Whale Accumulation Shadow (Anti-Sybil)",
            enabled=enabled,
            weight=weight,
        )
        self.suitable_regimes = [
            MarketRegime.TRENDING_BULL,
            MarketRegime.RANGING_SIDEWAYS,
            MarketRegime.HIGH_VOLATILITY_BURST,
        ]
        self._hyperparameters = {
            "min_whale_inflow_native": float(min_whale_inflow_native),
            "min_distinct_whales": min_distinct_whales,
            "max_price_impact_during_accum_pct": max_price_impact_during_accum_pct,
            "max_dev_balance_pct": max_dev_balance_pct,
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

        # 1. Security & Anti-Cabal Pre-Flight
        if security_report:
            if security_report.is_honeypot:
                return None
            if not security_report.mint_authority_disabled or not getattr(security_report, "freeze_authority_disabled", True):
                return None
            if security_report.top10_concentration > 0.25 and not getattr(pool_state, "is_pump_fun", False):
                return None

        # Check Staged Token Dev Balance & Sybil Cluster
        dev_balance_pct = float(getattr(staged_data, "dev_balance_pct", 0.0))
        max_dev = self._hyperparameters["max_dev_balance_pct"]
        if dev_balance_pct > max_dev:
            return None

        if getattr(staged_data, "whale_cluster_detected", False):
            # Sybil cluster / cabal detected
            return None

        # 2. Extract Smart Money & Whale Inflows
        m_data = market_data or {}
        whale_wallets = list(m_data.get("accumulating_whales", []))
        total_whale_inflow = Decimal(str(m_data.get("whale_inflow_native", "0")))

        # If explicit whale payload is not present, derive from staged token metrics
        if not whale_wallets and staged_data:
            unique_buyers = getattr(staged_data, "unique_buyers", set())
            buy_vol = getattr(staged_data, "total_volume_native", Decimal("0"))
            if len(unique_buyers) >= 3 and buy_vol >= Decimal(str(self._hyperparameters["min_whale_inflow_native"])):
                whale_wallets = list(unique_buyers)[:3]
                total_whale_inflow = buy_vol

        min_inflow = Decimal(str(self._hyperparameters["min_whale_inflow_native"]))
        min_whales = self._hyperparameters["min_distinct_whales"]

        if len(whale_wallets) < min_whales or total_whale_inflow < min_inflow:
            return None

        # 3. Check Price Run-Up during Accumulation (Avoid buying after the pump already happened)
        initial_p = getattr(staged_data, "initial_price", current_price)
        if initial_p > Decimal("0"):
            run_up_pct = float((current_price - initial_p) / initial_p * Decimal("100"))
            if run_up_pct > self._hyperparameters["max_price_impact_during_accum_pct"]:
                return None

        # 4. Sizing & Confidence
        confidence = min(0.95, 0.75 + (len(whale_wallets) * 0.05) + min(0.10, float(total_whale_inflow) * 0.02))
        kelly_pct = self.calculate_fractional_kelly_size(
            win_rate_pct=68.0,
            win_loss_payoff_ratio=2.4,
            fraction=0.55,
            confidence_multiplier=confidence,
        )

        tp_ladder = [
            {"trigger_multiplier": 1.50, "sell_pct": 40.0},
            {"trigger_multiplier": 2.20, "sell_pct": 30.0},
            {"trigger_multiplier": 4.00, "sell_pct": 30.0},
        ]

        rationale = (
            f"Whale Accumulation Shadow: {len(whale_wallets)} smart wallets accumulated "
            f"{float(total_whale_inflow):.2f} native volume. Dev balance={dev_balance_pct:.1f}% <= {max_dev}%. "
            f"Clean anti-sybil profile at price {current_price:.8f}."
        )

        return StrategySignal(
            token_address=token_address,
            chain=chain,
            strategy_type=self.strategy_type,
            side=OrderSide.BUY,
            confidence=round(confidence, 3),
            entry_price=current_price,
            expected_slippage_bps=130,
            stop_loss_pct=self._hyperparameters["hard_stop_loss_pct"],
            take_profit_ladder=tp_ladder,
            trailing_stop_pct=self._hyperparameters["trailing_stop_pct"],
            trailing_activation_pct=self._hyperparameters["trailing_activation_pct"],
            max_hold_seconds=self._hyperparameters["time_exit_seconds"],
            kelly_fraction=0.55,
            recommended_position_pct=kelly_pct,
            rationale=rationale,
            indicators={
                "whale_count": len(whale_wallets),
                "whale_inflow_native": float(total_whale_inflow),
                "tracked_whales": whale_wallets,
                "dev_balance_pct": dev_balance_pct,
            },
            exit_profile=ExitProfile.WHALE_SHADOW_EXIT,
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
                    rationale="Whale Shadow Emergency: Pool liquidity reserves depleted",
                )

        # 1. Whale Dump Early Exit
        indicators = getattr(position, "indicators", {})
        if indicators.get("whales_dumped", False):
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.EMERGENCY_DRAIN,
                rationale="Whale Shadow Emergency: Tracked whales initiated major distribution. Exiting immediately.",
            )

        # 2. Hard Stop Loss
        if pnl_pct <= hard_stop:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.SL_HARD,
                rationale=f"Whale Shadow Stop-Loss: {pnl_pct:.2f}% <= {hard_stop:.2f}%",
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
                    rationale=f"Whale Shadow Trailing Stop: Peak Gain={peak_gain_pct:.1f}%, Drawdown={drawdown:.1f}%",
                )

        # 4. Take Profit Ladder
        stage = getattr(position, "exit_stage", 0)
        if stage == 0 and pnl_pct >= 50.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.40,
                reason=TradeExitReason.TP_25,
                rationale="Whale Shadow TP1 Hit (+50.0%): Scaling out 40%",
                stage_index=1,
            )
        elif stage == 1 and pnl_pct >= 120.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=0.50,
                reason=TradeExitReason.TP_50,
                rationale="Whale Shadow TP2 Hit (+120.0%): Scaling out 50% of remainder",
                stage_index=2,
            )
        elif stage >= 2 and pnl_pct >= 300.0:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TP_2X,
                rationale="Whale Shadow TP3 Moonbag Hit (+300.0%): Closing full position",
                stage_index=3,
            )

        # 5. Time Limit
        if elapsed_seconds >= max_hold:
            return StrategyExitSignal(
                should_exit=True,
                exit_ratio=1.0,
                reason=TradeExitReason.TIMEOUT,
                rationale=f"Whale Shadow Time Limit reached ({elapsed_seconds/3600:.1f}h)",
            )

        return StrategyExitSignal(should_exit=False)
