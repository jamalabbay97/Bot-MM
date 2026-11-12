"""
alpha_engine.engine.self_learning.self_learning_engine — Comprehensive Self-Learning Engine
==========================================================================================
Unified institutional orchestration of:
1. Per-strategy statistical tracking & loss attribution (StrategyPerformanceTracker).
2. Online reinforcement learning & policy rebalancing (OnlinePolicyLearner).
3. Autonomous hyperparameter recalibration (AutonomousHyperparameterTuner).
4. Direct closed-trade event hooks and Ruflo episodic memory integration.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable, Optional, Union

from alpha_engine.engine.self_learning.hyperparameter_tuner import (
    AutonomousHyperparameterTuner,
    TunedHyperparameters,
)
from alpha_engine.engine.self_learning.metrics_tracker import StrategyPerformanceTracker
from alpha_engine.engine.self_learning.models import (
    ClosedTradeOutcome,
    ComprehensiveLossAttribution,
    StrategyPerformanceMetrics,
)
from alpha_engine.engine.self_learning.reinforcement_learner import OnlinePolicyLearner

logger = logging.getLogger(__name__)


class SelfLearningEngine:
    """
    Unified Self-Learning and Continuous Adaptation Engine.
    Processes every closed trade in real-time, adjusts strategy weights via online RL,
    tunes Council debate weights, and updates system risk posture.
    """

    def __init__(
        self,
        strategy_registry: Optional[Any] = None,
        council_arbiter: Optional[Any] = None,
        episodic_memory: Optional[Any] = None,
        settings_path: str = "bot_runtime_settings.json",
        config: Optional[Any] = None,
        ledger: Optional[Any] = None,
    ) -> None:
        self.strategy_registry = strategy_registry
        self.council_arbiter = council_arbiter
        self.episodic_memory = episodic_memory
        self.config = config
        self.ledger = ledger

        self.metrics_tracker = StrategyPerformanceTracker()
        self.policy_learner = OnlinePolicyLearner()
        self.hyperparameter_tuner = AutonomousHyperparameterTuner(settings_file_path=settings_path)

        self._on_tune_callbacks: list[Callable[[TunedHyperparameters], Any]] = []

    async def start(self) -> None:
        """Lifecycle start hook."""
        pass

    async def stop(self) -> None:
        """Lifecycle stop hook."""
        pass

    def register_on_tune_callback(self, callback: Callable[[TunedHyperparameters], Any]) -> None:
        """Register listener for hyperparameter changes."""
        if callback not in self._on_tune_callbacks:
            self._on_tune_callbacks.append(callback)

    async def on_trade_closed(self, trade_outcome: Union[ClosedTradeOutcome, dict[str, Any]]) -> dict[str, Any]:
        """
        Primary closed-trade hook. Invoked immediately upon trade settlement.
        Executes the complete self-learning lifecycle:
          1. Record trade & attribute loss root-cause
          2. Update online RL policy weights
          3. Propagate updated weights to StrategyRegistry
          4. Recalibrate Council weights and entry thresholds
          5. Dispatch episodic memory entry to Ruflo
        """
        outcome_obj = self._normalize_trade_outcome(trade_outcome)
        return await self._process_trade_closed(outcome_obj)

    def on_trade_closed_sync(self, trade_outcome: Union[ClosedTradeOutcome, dict[str, Any]]) -> dict[str, Any]:
        """
        Synchronous variant of closed-trade hook for non-async callers.
        """
        outcome_obj = self._normalize_trade_outcome(trade_outcome)
        return self._process_trade_closed_sync(outcome_obj)

    def _normalize_trade_outcome(self, trade_outcome: Union[ClosedTradeOutcome, dict[str, Any]]) -> ClosedTradeOutcome:
        if isinstance(trade_outcome, dict):
            strat_val = trade_outcome.get("strategy_type", "MOMENTUM_BREAKOUT")
            if hasattr(strat_val, "value"):
                strat_val = strat_val.value
            hold_time = float(trade_outcome.get("holding_time_seconds", trade_outcome.get("duration_s", trade_outcome.get("holding_duration_s", 60.0))))
            return ClosedTradeOutcome(
                trade_id=str(trade_outcome.get("trade_id", uuid.uuid4().hex[:8])),
                token_address=str(trade_outcome.get("token_address", "Unknown")),
                chain=str(trade_outcome.get("chain", "solana_mainnet")),
                strategy_type=str(strat_val),
                entry_price=float(trade_outcome.get("entry_price", 1.0)),
                exit_price=float(trade_outcome.get("exit_price", 1.0)),
                realized_pnl_usd=float(trade_outcome.get("realized_pnl_usd", 0.0)),
                realized_pnl_native=float(trade_outcome.get("realized_pnl_native", 0.0)),
                roi_pct=float(trade_outcome.get("roi_pct", trade_outcome.get("realized_pnl_pct", 0.0))),
                actual_slippage_bps=int(trade_outcome.get("actual_slippage_bps", 50)),
                fill_latency_ms=float(trade_outcome.get("fill_latency_ms", 35.0)),
                exit_reason=str(trade_outcome.get("exit_reason", "UNKNOWN")),
                holding_time_seconds=hold_time,
                holding_duration_s=hold_time,
                is_win=bool(trade_outcome.get("is_win", trade_outcome.get("realized_pnl_usd", 0.0) > 0)),
                timestamp_ns=int(trade_outcome.get("timestamp_ns", time.time_ns())),
            )
        return trade_outcome

    async def _process_trade_closed(self, trade_outcome: ClosedTradeOutcome) -> dict[str, Any]:
        result = self._execute_core_lifecycle(trade_outcome)
        # 5. Record to Ruflo Episodic Memory asynchronously
        if self.episodic_memory and hasattr(self.episodic_memory, "record_episode"):
            await self._record_to_ruflo_memory_async(trade_outcome)
        return result

    def _process_trade_closed_sync(self, trade_outcome: ClosedTradeOutcome) -> dict[str, Any]:
        result = self._execute_core_lifecycle(trade_outcome)
        # 5. Record to Ruflo Episodic Memory if event loop running
        if self.episodic_memory and hasattr(self.episodic_memory, "record_episode"):
            self._record_to_ruflo_memory(trade_outcome)
        return result

    def _execute_core_lifecycle(self, trade_outcome: ClosedTradeOutcome) -> dict[str, Any]:
        logger.info(
            "SelfLearningEngine: Processing closed trade %s (%s) PnL=$%.2f (ROI=%.2f%%)",
            trade_outcome.trade_id, trade_outcome.strategy_type,
            trade_outcome.realized_pnl_usd, trade_outcome.roi_pct,
        )

        # 1. Update Metrics Tracker
        metrics = self.metrics_tracker.record_closed_trade(trade_outcome)

        # 2. Update Online RL Policy
        updated_weights = self.policy_learner.update_policy(trade_outcome)

        # 3. Propagate weights to StrategyRegistry
        strat_key = trade_outcome.strategy_type
        if self.strategy_registry and strat_key:
            rl_weight = updated_weights.get(strat_key, 1.0)
            combined_weight = round(rl_weight * metrics.dynamic_weight, 3)
            # Find matching StrategyType
            for s in self.strategy_registry.get_all_strategies():
                st_val = s.strategy_type.value if hasattr(s.strategy_type, "value") else str(s.strategy_type)
                if st_val.lower() == strat_key.lower():
                    s.set_weight(combined_weight)
                    self.strategy_registry.update_performance_score(s.strategy_type, combined_weight)

        # 4. Tune Hyperparameters
        tuned = self.hyperparameter_tuner.record_trade_outcome(trade_outcome)
        if tuned:
            self._apply_tuned_hyperparameters(tuned)

        return {
            "strategy_metrics": metrics.to_dict(),
            "policy_weights": updated_weights,
            "tuned_hyperparameters": tuned.to_dict() if tuned else None,
        }

    async def _record_to_ruflo_memory_async(self, trade: ClosedTradeOutcome) -> None:
        """Saves trade outcome into Ruflo episodic memory."""
        if self.episodic_memory is None:
            return
        try:
            from alpha_engine.ruflo.memory import MemoryEpisode

            fv = trade.feature_vector if len(trade.feature_vector) == 7 else [0.5] * 7
            ep = MemoryEpisode(
                episode_id=trade.trade_id,
                token_address=trade.token_address,
                chain=trade.chain,
                decision_type="ENTER",
                signal_source=trade.strategy_type,
                confidence_score=trade.council_confidence,
                feature_vector=fv,
                realized_pnl_usd=trade.realized_pnl_usd,
                roi_pct=trade.roi_pct,
                actual_slippage_bps=trade.actual_slippage_bps,
                fill_latency_ms=trade.fill_latency_ms,
                root_cause_tag="PROFIT_TARGET_HIT" if trade.is_win else "STOP_LOSS_HIT",
                summary_reflection=f"Closed with ROI {trade.roi_pct:.2f}% (PnL ${trade.realized_pnl_usd:.2f})",
            )
            res = self.episodic_memory.record_episode(ep)
            if asyncio.iscoroutine(res):
                await res
        except Exception as exc:
            logger.error("Failed recording episode to Ruflo episodic memory: %s", exc, exc_info=True)
            raise

    def _apply_tuned_hyperparameters(self, tuned: TunedHyperparameters) -> None:
        """Apply newly tuned hyperparameters to Council and Strategy engines."""
        if self.council_arbiter and hasattr(self.council_arbiter, "council_weights"):
            self.council_arbiter.council_weights = dict(tuned.council_weights)

        for cb in self._on_tune_callbacks:
            try:
                cb(tuned)
            except Exception as exc:
                logger.warning("Error in tuned callback: %s", exc)

    def _record_to_ruflo_memory(self, trade: ClosedTradeOutcome) -> None:
        """Saves trade outcome into Ruflo episodic memory."""
        try:
            from alpha_engine.ruflo.memory import MemoryEpisode
            import asyncio

            fv = trade.feature_vector if len(trade.feature_vector) == 7 else [0.5] * 7
            ep = MemoryEpisode(
                episode_id=trade.trade_id,
                token_address=trade.token_address,
                chain=trade.chain,
                decision_type="ENTER",
                signal_source=trade.strategy_type,
                confidence_score=trade.council_confidence,
                feature_vector=fv,
                realized_pnl_usd=trade.realized_pnl_usd,
                roi_pct=trade.roi_pct,
                actual_slippage_bps=trade.actual_slippage_bps,
                fill_latency_ms=trade.fill_latency_ms,
                root_cause_tag="PROFIT_TARGET_HIT" if trade.is_win else "STOP_LOSS_HIT",
                summary_reflection=f"Closed with ROI {trade.roi_pct:.2f}% (PnL ${trade.realized_pnl_usd:.2f})",
            )
            loop = asyncio.get_running_loop()
            loop.create_task(self.episodic_memory.record_episode(ep))
        except Exception:
            pass

    def get_ranked_strategy_report(self) -> list[dict[str, Any]]:
        """Returns leaderboard of all strategies sorted by mathematical expectancy."""
        ranked = self.metrics_tracker.get_ranked_strategies()
        return [m.to_dict() for m in ranked]

    def get_strategy_scorecard(self, strategy_type: str) -> Optional[StrategyPerformanceMetrics]:
        """Returns statistical performance scorecard for a specific strategy."""
        return self.metrics_tracker.get_metrics(strategy_type)

    def attribute_trade_loss(self, trade: ClosedTradeOutcome) -> ComprehensiveLossAttribution:
        """Classify loss attribution taxonomy for an underperforming closed trade."""
        return self.metrics_tracker._attribute_trade_outcome(trade)

