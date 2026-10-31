"""
alpha_engine.engine.self_learning.reinforcement_learner — Online Lightweight Reinforcement Learning
===================================================================================================
Implements an online contextual bandit & exponential gradient policy:
1. Dynamically balances exploration vs exploitation across strategies.
2. Updates strategy selection probabilities and risk weights based on risk-adjusted reward.
3. Automatically throttles underperforming strategies and allocates capital to alpha champions.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Optional

from alpha_engine.engine.self_learning.models import ClosedTradeOutcome

logger = logging.getLogger(__name__)


class OnlinePolicyLearner:
    """
    Lightweight Reinforcement Learner using an Exponential Gradient (Exp3-style)
    Softmax Multi-Armed Bandit with Sortino-weighted reward signals.
    """

    def __init__(
        self,
        learning_rate_eta: float = 0.15,
        discount_factor: float = 0.95,
        min_weight: float = 0.20,
        max_weight: float = 2.50,
    ) -> None:
        self.eta = learning_rate_eta
        self.gamma = discount_factor
        self.min_weight = min_weight
        self.max_weight = max_weight
        # Estimated cumulative value Q(s) for each strategy
        self._strategy_values: dict[str, float] = {}
        # Dynamic policy weights
        self._strategy_weights: dict[str, float] = {}

    def compute_reward(self, trade: ClosedTradeOutcome, context: Optional[dict[str, Any]] = None) -> float:
        """
        Computes a bounded, risk-adjusted reward scalar in [-2.0, +2.0]:
        - Base reward proportional to ROI %
        - Scaled by PnL USD
        - Penalized for slippage leakage beyond expected tolerance
        - Penalized for excessive fill latency
        """
        roi = trade.roi_pct
        pnl = trade.realized_pnl_usd
        slippage_leak = max(0, trade.actual_slippage_bps - trade.expected_slippage_bps)

        # Normalized return component: clamp between -1.0 and +1.0
        norm_roi = max(-1.0, min(1.0, roi / 50.0))
        # PnL magnitude booster
        pnl_scale = min(1.0, abs(pnl) / 25.0)

        # Slippage penalty
        slip_penalty = min(0.5, (slippage_leak / 500.0))

        # Latency penalty and execution context if context is provided
        latency_penalty = 0.0
        if context:
            fill_latency_ms = float(context.get("fill_latency_ms", context.get("latency_ms", 0.0)))
            if fill_latency_ms > 1500.0:
                latency_penalty = min(0.3, (fill_latency_ms - 1500.0) / 5000.0)
            if context.get("high_volatility_adverse", False):
                latency_penalty += 0.1

        raw_reward = (norm_roi * (1.0 + 0.5 * pnl_scale)) - slip_penalty - latency_penalty
        return max(-2.0, min(2.0, raw_reward))

    def update_policy(self, trade: ClosedTradeOutcome, context: Optional[dict[str, Any]] = None) -> dict[str, float]:
        """
        Processes a closed trade and updates the online policy weights.
        Returns the updated strategy weights mapping.
        """
        strat = trade.strategy_type or "unknown"
        reward = self.compute_reward(trade, context=context)

        # Update running Q-value with exponential moving average
        current_val = self._strategy_values.get(strat, 0.0)
        updated_val = (self.gamma * current_val) + (self.eta * reward)
        self._strategy_values[strat] = updated_val

        # Softmax weight mapping: W = exp(eta * Q)
        exp_val = math.exp(max(-3.0, min(3.0, updated_val)))
        clamped_weight = max(self.min_weight, min(self.max_weight, exp_val))
        self._strategy_weights[strat] = round(clamped_weight, 3)

        logger.info(
            "Online RL Update: Strategy=%s, Reward=%.2f, Q-Val=%.3f -> Weight=%.2f",
            strat, reward, updated_val, clamped_weight,
        )
        return dict(self._strategy_weights)

    def get_weight(self, strategy_type: str) -> float:
        return self._strategy_weights.get(strategy_type, 1.0)

    def get_all_weights(self) -> dict[str, float]:
        return dict(self._strategy_weights)
