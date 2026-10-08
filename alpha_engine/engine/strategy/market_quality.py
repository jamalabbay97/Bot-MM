"""
alpha_engine.engine.strategy.market_quality
===========================================
Market Quality, Volume Momentum, Buy/Sell Flow, and Holder Dynamics.
"""

from typing import Any, Dict

class MarketAnalyticsEngine:
    """
    Evaluates market quality using volume momentum, buy/sell flow,
    and holder dynamics based on recent swap events.
    """

    def __init__(self) -> None:
        pass

    def evaluate_volume_momentum(self, staging_data: Any) -> float:
        """
        Calculates a 0-100 score for volume momentum.
        Expects staging_data (StagedLaunch or Wave2 buffer object).
        """
        # Simple heuristic: higher native volume yields higher momentum score.
        try:
            vol_sol = float(staging_data.total_volume_native)
        except AttributeError:
            vol_sol = 0.0

        if vol_sol > 5.0:
            return 100.0
        if vol_sol > 2.0:
            return 80.0
        if vol_sol > 0.8:
            return 60.0
        return max(0.0, min(100.0, (vol_sol / 0.8) * 50.0))

    def evaluate_flow(self, staging_data: Any) -> float:
        """
        Calculates a 0-100 score based on buy/sell flow.
        """
        # In a staging buffer, we track buy_count.
        try:
            buys = staging_data.buy_count
        except AttributeError:
            buys = 0
            
        if buys > 20:
            return 100.0
        if buys > 10:
            return 80.0
        if buys > 3:
            return 60.0
        return max(0.0, min(100.0, (buys / 3) * 50.0))

    def evaluate_holder_dynamics(self, staging_data: Any) -> float:
        """
        Calculates a 0-100 score based on unique buyers (entropy).
        """
        try:
            unique_buyers = len(staging_data.unique_buyers)
        except AttributeError:
            unique_buyers = 0
            
        if unique_buyers > 15:
            return 100.0
        if unique_buyers > 8:
            return 80.0
        if unique_buyers >= 2:
            return 60.0
        return max(0.0, min(100.0, (unique_buyers / 2) * 50.0))

    def get_market_quality_scores(self, staging_data: Any) -> Dict[str, float]:
        """
        Returns a dictionary of all market quality sub-scores.
        """
        return {
            "volume_momentum": self.evaluate_volume_momentum(staging_data),
            "flow": self.evaluate_flow(staging_data),
            "holders": self.evaluate_holder_dynamics(staging_data),
        }
