"""
alpha_engine.engine.strategy.risk_manager
=========================================
Quantitative Risk & Position Sizing Engine.
Single authority for position sizing across autonomous, manual-USD, and manual-native modes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.math.sizing import (
    GAS_MULTIPLE_GATE,
    gas_cost_usd,
    half_kelly_fraction,
    shrink_win_rate,
)
from alpha_engine.models.enums import ChainIdentifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizingResult:
    """
    Output of the single position sizing authority.
    """

    native_trade_size: Decimal
    usd_at_risk: Decimal
    fraction_of_equity: float
    kelly_fraction: float
    is_calibrated: bool
    gas_rejected: bool
    edge_rejected: bool
    is_manual: bool = False
    rejection_reason: Optional[str] = None

    @property
    def is_approved(self) -> bool:
        return (
            not self.gas_rejected
            and not self.edge_rejected
            and self.native_trade_size > Decimal("0")
        )


class RiskEngine:
    """
    Evaluates execution risk, correlated exposure, and calculates optimal position sizing.
    Acts as the single sizing authority for all trades in the engine.
    """

    def __init__(self, config: EngineConfig) -> None:
        self.config = config
        self._ledger_stats: dict[str, Any] = {
            "total_trades": 0,
            "wins": 0,
            "win_rate": 0.0,
            "avg_win_native": Decimal("0.0"),
            "avg_loss_native": Decimal("0.0"),
        }
        self._logged_uncalibrated: bool = False

    def update_ledger_stats(self, stats: dict[str, Any]) -> None:
        """Update historical closed trade statistics from the ledger."""
        self._ledger_stats = stats or {}

    def check_daily_limits(self, running_metrics: dict) -> bool:
        """
        Check if daily loss limit or consecutive losses are breached.
        """
        consecutive_losses = running_metrics.get("consecutive_losses", 0)
        daily_drawdown = running_metrics.get("daily_drawdown_pct", 0.0)

        if consecutive_losses >= self.config.circuit_breaker_max_losses:
            logger.warning("Circuit breaker tripped: %d consecutive losses.", consecutive_losses)
            return False

        if daily_drawdown >= self.config.circuit_breaker_drawdown_pct:
            logger.warning("Circuit breaker tripped: %.2f%% drawdown > limit.", daily_drawdown * 100)
            return False

        return True

    def calculate_position_size(
        self,
        current_capital: Decimal,
        score: float,
        is_new_launch: bool = False,
    ) -> Decimal:
        """
        Backwards compatibility facade for dynamic position sizing.
        Calculates position size based on 0-100 score and max_per_token_risk_pct.
        """
        max_risk_pct = Decimal(str(self.config.max_per_token_risk_pct))

        min_score = 50.0 if is_new_launch else 55.0
        if score < min_score:
            return Decimal("0.0")

        if score >= 70.0:
            scaling_factor = Decimal(str(min(1.0, score / 100.0)))
        else:
            score_range = max(1.0, 70.0 - min_score)
            interp = (score - min_score) / score_range
            factor = 0.35 + 0.35 * interp
            scaling_factor = Decimal(str(factor))

        target_risk_pct = max_risk_pct * scaling_factor
        return current_capital * target_risk_pct

    def calculate_order_size(
        self,
        portfolio_equity_usd: Decimal,
        native_price_usd: Decimal,
        chain: ChainIdentifier,
        score: float = 80.0,
        is_new_launch: bool = False,
        manual_amount: Optional[Decimal] = None,
        manual_currency: str = "SOL",
        custom_amount_native: Optional[Decimal] = None,
    ) -> SizingResult:
        """
        Single Sizing Authority:
        Computes the exact native order size and USD at risk.
        Ensures sized amount == balance-checked amount == filled amount across all modes.
        """
        if portfolio_equity_usd <= Decimal("0") or native_price_usd <= Decimal("0"):
            return SizingResult(
                native_trade_size=Decimal("0"),
                usd_at_risk=Decimal("0"),
                fraction_of_equity=0.0,
                kelly_fraction=0.0,
                is_calibrated=False,
                gas_rejected=False,
                edge_rejected=False,
                rejection_reason="Invalid portfolio equity or native price <= 0",
            )

        trade_gas_usd = gas_cost_usd(chain)
        gas_threshold_usd = trade_gas_usd * Decimal(str(GAS_MULTIPLE_GATE))

        # -------------------------------------------------------------
        # 1. Custom amount on signal (e.g. DM / automated override)
        # -------------------------------------------------------------
        if custom_amount_native is not None and custom_amount_native > Decimal("0"):
            native_size = custom_amount_native
            usd_at_risk = native_size * native_price_usd
            frac = float(usd_at_risk / portfolio_equity_usd)
            if usd_at_risk < gas_threshold_usd:
                return SizingResult(
                    native_trade_size=Decimal("0"),
                    usd_at_risk=usd_at_risk,
                    fraction_of_equity=frac,
                    kelly_fraction=frac,
                    is_calibrated=True,
                    gas_rejected=True,
                    edge_rejected=False,
                    is_manual=True,
                    rejection_reason=f"Gas drag: usd_at_risk=${usd_at_risk:.4f} < ${gas_threshold_usd:.4f}",
                )
            return SizingResult(
                native_trade_size=native_size,
                usd_at_risk=usd_at_risk,
                fraction_of_equity=frac,
                kelly_fraction=frac,
                is_calibrated=True,
                gas_rejected=False,
                edge_rejected=False,
                is_manual=True,
            )

        # -------------------------------------------------------------
        # 2. Manual trade amount configured by user (USD or native)
        # -------------------------------------------------------------
        if manual_amount is not None and manual_amount > Decimal("0"):
            curr = str(manual_currency).upper()
            if curr == "USD":
                usd_at_risk = manual_amount
                native_size = usd_at_risk / native_price_usd
            else:
                native_size = manual_amount
                usd_at_risk = native_size * native_price_usd

            frac = float(usd_at_risk / portfolio_equity_usd)
            if usd_at_risk < gas_threshold_usd:
                return SizingResult(
                    native_trade_size=Decimal("0"),
                    usd_at_risk=usd_at_risk,
                    fraction_of_equity=frac,
                    kelly_fraction=frac,
                    is_calibrated=True,
                    gas_rejected=True,
                    edge_rejected=False,
                    is_manual=True,
                    rejection_reason=f"Gas drag: usd_at_risk=${usd_at_risk:.4f} < ${gas_threshold_usd:.4f}",
                )
            return SizingResult(
                native_trade_size=native_size,
                usd_at_risk=usd_at_risk,
                fraction_of_equity=frac,
                kelly_fraction=frac,
                is_calibrated=True,
                gas_rejected=False,
                edge_rejected=False,
                is_manual=True,
            )

        # -------------------------------------------------------------
        # 3. Autonomous sizing: Kelly Criterion / Conservative Calibration
        # -------------------------------------------------------------
        total_closed = int(self._ledger_stats.get("total_trades", 0) or 0)
        min_closed = getattr(self.config, "kelly_min_closed_trades", 30)

        if total_closed < min_closed:
            is_calibrated = False
            if not self._logged_uncalibrated:
                logger.info(
                    "Kelly sizing not yet calibrated (%d closed trades < %d minimum); using conservative fixed fraction %.4f",
                    total_closed,
                    min_closed,
                    self.config.kelly_uncalibrated_fraction,
                )
                self._logged_uncalibrated = True
            base_fraction = Decimal(str(self.config.kelly_uncalibrated_fraction))
            raw_kelly = float(base_fraction)
        else:
            is_calibrated = True
            wins = int(self._ledger_stats.get("wins", 0) or 0)
            shrinkage_n = getattr(self.config, "kelly_shrinkage_trades", 20)
            shrunk_p = shrink_win_rate(
                sample_wins=wins,
                sample_total=total_closed,
                prior_win_rate=0.5,
                shrinkage_trades=shrinkage_n,
            )
            avg_win = Decimal(str(self._ledger_stats.get("avg_win_native", Decimal("0.0")) or 0.0))
            avg_loss = Decimal(str(self._ledger_stats.get("avg_loss_native", Decimal("0.0")) or 0.0))
            k_mult = getattr(self.config, "kelly_multiplier", 0.5)
            raw_kelly = half_kelly_fraction(
                win_rate=shrunk_p,
                avg_win_native=avg_win,
                avg_loss_native=avg_loss,
                multiplier=k_mult,
            )
            if raw_kelly <= 0.0:
                return SizingResult(
                    native_trade_size=Decimal("0"),
                    usd_at_risk=Decimal("0"),
                    fraction_of_equity=0.0,
                    kelly_fraction=0.0,
                    is_calibrated=True,
                    gas_rejected=False,
                    edge_rejected=True,
                    rejection_reason=f"Kelly edge <= 0 ({raw_kelly:.4f}) — no statistical edge to buy",
                )
            base_fraction = Decimal(str(raw_kelly))

        # Adaptive score qualification
        min_score = 50.0 if is_new_launch else 55.0
        if score < min_score:
            return SizingResult(
                native_trade_size=Decimal("0"),
                usd_at_risk=Decimal("0"),
                fraction_of_equity=0.0,
                kelly_fraction=float(base_fraction),
                is_calibrated=is_calibrated,
                gas_rejected=False,
                edge_rejected=False,
                rejection_reason=f"Alpha score {score:.1f} < min_score {min_score:.1f}",
            )

        # Conviction scaling
        if score >= 70.0:
            scaling_factor = Decimal(str(min(1.0, score / 100.0)))
        else:
            score_range = max(1.0, 70.0 - min_score)
            interp = (score - min_score) / score_range
            scaling_factor = Decimal(str(0.35 + 0.35 * interp))

        # Maximum portfolio risk cap:
        max_cap = Decimal(
            str(self.config.max_autonomous_risk_pct if is_new_launch else self.config.max_portfolio_risk_pct)
        )

        if not is_calibrated:
            # Below calibration threshold: size equals conservative fixed fraction directly
            target_fraction = min(base_fraction, max_cap)
        else:
            # Above calibration threshold: scale Kelly fraction by conviction score
            target_fraction = min(base_fraction * scaling_factor, max_cap)

        usd_at_risk = target_fraction * portfolio_equity_usd

        # Gas-drag threshold validation
        if usd_at_risk < gas_threshold_usd:
            return SizingResult(
                native_trade_size=Decimal("0"),
                usd_at_risk=usd_at_risk,
                fraction_of_equity=float(target_fraction),
                kelly_fraction=float(base_fraction),
                is_calibrated=is_calibrated,
                gas_rejected=True,
                edge_rejected=False,
                rejection_reason=f"usd_at_risk=${usd_at_risk:.4f} < gas_threshold=${gas_threshold_usd:.4f}",
            )

        native_size = usd_at_risk / native_price_usd
        return SizingResult(
            native_trade_size=native_size,
            usd_at_risk=usd_at_risk,
            fraction_of_equity=float(target_fraction),
            kelly_fraction=float(base_fraction),
            is_calibrated=is_calibrated,
            gas_rejected=False,
            edge_rejected=False,
        )

    def check_portfolio_exposure(self, total_exposure: Decimal, current_capital: Decimal) -> bool:
        """
        Ensure we don't breach max_portfolio_exposure_pct.
        """
        max_exp_pct = Decimal(str(self.config.max_portfolio_exposure_pct))
        if total_exposure > (current_capital * max_exp_pct):
            logger.warning("Portfolio exposure limit reached.")
            return False
        return True
