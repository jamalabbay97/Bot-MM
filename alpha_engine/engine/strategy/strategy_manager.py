"""
alpha_engine.engine.strategy.strategy_manager — Unified Strategy Registry & Execution Manager
=============================================================================================
Orchestrates all institutional quantitative strategies:
1. Dynamic registration, weighting, and performance tracking of strategies.
2. Signal arbitration, priority routing, and deduplication.
3. Position exit monitoring across active strategy profiles.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.engine.strategy.adaptive_regime import AdaptiveRegimeStrategy
from alpha_engine.engine.strategy.base import BaseStrategy, StrategyExitSignal, StrategySignal
from alpha_engine.engine.strategy.liquidity_sweep import LiquiditySweepStrategy
from alpha_engine.engine.strategy.mean_reversion import MeanReversionStrategy
from alpha_engine.engine.strategy.momentum_breakout import MomentumBreakoutStrategy
from alpha_engine.engine.strategy.multi_timeframe import MultiTimeframeConfluenceStrategy
from alpha_engine.engine.strategy.narrative_velocity import NarrativeVelocityStrategy
from alpha_engine.engine.strategy.whale_shadow import WhaleAccumulationShadowStrategy
from alpha_engine.models.enums import ChainIdentifier, StrategyType
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class StrategyRegistry:
    """
    Central registry and performance arbiter for all trading strategies.
    Manages active state, dynamic weights, and execution counters.
    """

    def __init__(self) -> None:
        self._strategies: dict[StrategyType, BaseStrategy] = {}
        self._performance_scores: dict[StrategyType, float] = {}
        self._init_default_strategies()

    def _init_default_strategies(self) -> None:
        """Instantiate and register the 7 core institutional strategies."""
        strats: list[BaseStrategy] = [
            MomentumBreakoutStrategy(),
            MeanReversionStrategy(),
            LiquiditySweepStrategy(),
            WhaleAccumulationShadowStrategy(),
            NarrativeVelocityStrategy(),
            MultiTimeframeConfluenceStrategy(),
            AdaptiveRegimeStrategy(),
        ]
        for s in strats:
            self.register_strategy(s)

    def register_strategy(self, strategy: BaseStrategy) -> None:
        """Register a strategy into the active registry."""
        self._strategies[strategy.strategy_type] = strategy
        if strategy.strategy_type not in self._performance_scores:
            self._performance_scores[strategy.strategy_type] = 1.0
        logger.info("Registered strategy: %s (Type: %s)", strategy.name, strategy.strategy_type.value)

    def get_strategy(self, strategy_type: StrategyType) -> Optional[BaseStrategy]:
        return self._strategies.get(strategy_type)

    def get_all_strategies(self) -> list[BaseStrategy]:
        return list(self._strategies.values())

    def get_active_strategies(self) -> list[BaseStrategy]:
        """Returns enabled strategies sorted by dynamic weight and performance score."""
        active = [s for s in self._strategies.values() if s.enabled]
        return sorted(active, key=lambda s: s.weight * self._performance_scores.get(s.strategy_type, 1.0), reverse=True)

    def update_strategy_weight(self, strategy_type: StrategyType, weight: float) -> None:
        s = self._strategies.get(strategy_type)
        if s:
            s.set_weight(weight)

    def update_performance_score(self, strategy_type: StrategyType, score: float) -> None:
        self._performance_scores[strategy_type] = max(0.1, score)

    def set_strategy_enabled(self, strategy_type: StrategyType, enabled: bool) -> None:
        s = self._strategies.get(strategy_type)
        if s:
            s.set_enabled(enabled)


class StrategyManager:
    """
    Strategy Orchestration and Execution Coordinator.
    Evaluates incoming tokens across active institutional strategies and
    monitors open positions for multi-criteria staged exits.
    """

    def __init__(self, registry: Optional[StrategyRegistry] = None, config: Optional[Any] = None) -> None:
        self.registry = registry or StrategyRegistry()
        self.config = config

    def evaluate_token_opportunity(
        self,
        token_address: str,
        chain: ChainIdentifier,
        current_price: Decimal,
        pool_state: Optional[PoolState] = None,
        security_report: Optional[SecurityReport] = None,
        staged_data: Optional[Any] = None,
        market_data: Optional[dict[str, Any]] = None,
    ) -> list[StrategySignal]:
        """
        Evaluates a token against all active registered strategies.
        Returns a list of qualified StrategySignals sorted by weighted confidence.
        """
        signals: list[StrategySignal] = []
        active_strats = self.registry.get_active_strategies()

        for strat in active_strats:
            try:
                sig = strat.evaluate_entry(
                    token_address=token_address,
                    chain=chain,
                    current_price=current_price,
                    pool_state=pool_state,
                    security_report=security_report,
                    staged_data=staged_data,
                    market_data=market_data,
                )
                if sig is not None:
                    signals.append(sig)
            except Exception as exc:
                logger.error("Error evaluating strategy %s on %s: %s", strat.name, token_address[:8], exc)

        # Sort signals by weighted confidence
        signals.sort(
            key=lambda s: s.confidence * self.registry._performance_scores.get(s.strategy_type, 1.0),
            reverse=True,
        )
        return signals

    def evaluate_position_exit(
        self,
        position: Any,
        current_price: Decimal,
        peak_price: Decimal,
        elapsed_seconds: float,
        pool_state: Optional[PoolState] = None,
    ) -> StrategyExitSignal:
        """
        Evaluates an open lot/position against its originating strategy's exit engine.
        Falls back to adaptive regime evaluation if strategy type is unassigned.
        """
        strat_type_raw = getattr(position, "strategy_type", None)
        strat = None
        if strat_type_raw:
            try:
                st = StrategyType(strat_type_raw) if not isinstance(strat_type_raw, StrategyType) else strat_type_raw
                strat = self.registry.get_strategy(st)
            except Exception:
                pass

        if not strat:
            # Default to Adaptive Regime Exit Engine
            strat = self.registry.get_strategy(StrategyType.ADAPTIVE_REGIME)

        if strat:
            return strat.evaluate_exit(position, current_price, peak_price, elapsed_seconds, pool_state)

        return StrategyExitSignal(should_exit=False)

    def get_status_summary(self) -> list[dict[str, Any]]:
        """Provides status summary of all registered strategies for dashboard and telemetry."""
        summary = []
        for s in self.registry.get_all_strategies():
            score = self.registry._performance_scores.get(s.strategy_type, 1.0)
            summary.append({
                "strategy_type": s.strategy_type.value,
                "name": s.name,
                "enabled": s.enabled,
                "weight": s.weight,
                "performance_score": round(score, 3),
                "hyperparameters": s.get_hyperparameters(),
            })
        return summary
