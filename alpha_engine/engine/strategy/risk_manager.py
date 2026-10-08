"""
alpha_engine.engine.strategy.risk_manager
=========================================
Quantitative Risk & Position Sizing Engine.
"""

import logging
from decimal import Decimal
from alpha_engine.config import EngineConfig

logger = logging.getLogger(__name__)

class RiskEngine:
    """
    Evaluates execution risk, correlated exposure, and calculates optimal position sizing.
    """

    def __init__(self, config: EngineConfig) -> None:
        self.config = config

    def check_daily_limits(self, running_metrics: dict) -> bool:
        """
        Check if daily loss limit or consecutive losses are breached.
        """
        consecutive_losses = running_metrics.get("consecutive_losses", 0)
        daily_drawdown = running_metrics.get("daily_drawdown_pct", 0.0)

        if consecutive_losses >= self.config.circuit_breaker_max_losses:
            logger.warning(f"Circuit breaker tripped: {consecutive_losses} consecutive losses.")
            return False

        if daily_drawdown >= self.config.circuit_breaker_drawdown_pct:
            logger.warning(f"Circuit breaker tripped: {daily_drawdown*100:.2f}% drawdown > limit.")
            return False

        return True

    def calculate_position_size(self, current_capital: Decimal, score: float) -> Decimal:
        """
        Calculates position size dynamically based on the 0-100 score.
        """
        max_risk_pct = Decimal(str(self.config.max_per_token_risk_pct))
        
        # Scale risk by score. e.g. Score 100 = 100% of max_risk_pct. Score 70 = 70%
        # Below min_entry_score (e.g. 70), risk is 0.
        min_score = self.config.min_entry_score
        if score < min_score:
            return Decimal("0.0")

        scaling_factor = Decimal(str(score / 100.0))
        target_risk_pct = max_risk_pct * scaling_factor
        
        return current_capital * target_risk_pct

    def check_portfolio_exposure(self, total_exposure: Decimal, current_capital: Decimal) -> bool:
        """
        Ensure we don't breach max_portfolio_exposure_pct.
        """
        max_exp_pct = Decimal(str(self.config.max_portfolio_exposure_pct))
        if total_exposure > (current_capital * max_exp_pct):
            logger.warning("Portfolio exposure limit reached.")
            return False
        return True
