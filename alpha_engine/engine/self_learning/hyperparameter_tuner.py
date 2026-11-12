"""
alpha_engine.engine.self_learning.hyperparameter_tuner — Autonomous Hyperparameter Auto-Tuner
=============================================================================================
Continuously learns from closed trade post-mortems and dynamically tunes:
1. Council agent debate weights (ChainForensic, QuantTechnicals, ReachAnalyst).
2. Entry barriers and breakout surge multipliers.
3. Dynamic slippage tolerances and MEV sandwich buffer limits.
4. Fractional Kelly position sizing bounds.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.engine.self_learning.models import ClosedTradeOutcome, ComprehensiveLossAttribution

logger = logging.getLogger(__name__)


@dataclass
class TunedHyperparameters:
    """Active auto-tuned system parameters."""
    council_weights: dict[str, float] = field(default_factory=lambda: {
        "ChainForensic": 0.40,
        "QuantTechnicals": 0.40,
        "ReachAnalyst": 0.20,
    })
    max_slippage_bps: int = 150
    min_volume_surge_multiplier: float = 2.5
    min_alpha_score_entry: float = 0.65
    hard_stop_loss_pct: float = -12.0
    kelly_fraction_multiplier: float = 0.50
    trailing_stop_pct: float = 20.0
    min_entry_price_sol: Decimal = Decimal("0.0001")
    last_tuned_timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["min_entry_price_sol"] = str(self.min_entry_price_sol)
        return d


class AutonomousHyperparameterTuner:
    """
    Self-Learning Hyperparameter Tuner.
    Inspects trade outcomes and execution telemetry to autonomously tune
    system parameters, writing state changes to runtime settings.
    """

    def __init__(
        self,
        settings_file_path: str = "bot_runtime_settings.json",
        min_trades_between_tuning: int = 3,
        min_slippage_samples: int = 10,
    ) -> None:
        self.settings_file_path = settings_file_path
        self.min_trades_between_tuning = min_trades_between_tuning
        self.min_slippage_samples = min_slippage_samples
        self.current_params = TunedHyperparameters()
        self._trades_since_last_tune: int = 0
        self._agent_accuracy: dict[str, list[int]] = {
            "ChainForensic": [],
            "QuantTechnicals": [],
            "ReachAnalyst": [],
        }
        self._slippage_observations: list[int] = []
        self._load_persisted_settings()

    def _load_persisted_settings(self) -> None:
        """Loads existing settings if available."""
        if not os.path.exists(self.settings_file_path):
            return
        try:
            with open(self.settings_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                auto_tuned = data.get("autonomous_hyperparameters", {})
                if auto_tuned:
                    self.current_params.max_slippage_bps = auto_tuned.get("max_slippage_bps", 150)
                    self.current_params.min_volume_surge_multiplier = auto_tuned.get("min_volume_surge_multiplier", 2.5)
                    self.current_params.hard_stop_loss_pct = auto_tuned.get("hard_stop_loss_pct", -12.0)
                    self.current_params.kelly_fraction_multiplier = auto_tuned.get("kelly_fraction_multiplier", 0.5)
                    self.current_params.council_weights = auto_tuned.get("council_weights", self.current_params.council_weights)
        except Exception as exc:
            logger.warning("Could not load persisted hyperparameters: %s", exc)

    def record_trade_outcome(self, trade: ClosedTradeOutcome) -> Optional[TunedHyperparameters]:
        """
        Ingests a closed trade, audits agent predictive accuracy,
        and triggers a hyperparameter recalibration cycle if batch threshold is met.
        """
        self._trades_since_last_tune += 1
        self._slippage_observations.append(trade.actual_slippage_bps)
        if len(self._slippage_observations) > 50:
            self._slippage_observations.pop(0)

        # Audit Agent Verdict Alignment
        is_win = trade.is_win
        for agent_name, verdict in trade.council_verdicts.items():
            if agent_name in self._agent_accuracy:
                vote = verdict.get("vote", "") if isinstance(verdict, dict) else str(getattr(verdict, "vote", ""))
                # If agent approved a winner or rejected a loser, mark as accurate (1), else (0)
                is_accurate = 1 if (is_win and "APPROVE" in vote) or (not is_win and "REJECT" in vote) else 0
                self._agent_accuracy[agent_name].append(is_accurate)
                if len(self._agent_accuracy[agent_name]) > 30:
                    self._agent_accuracy[agent_name].pop(0)

        if self._trades_since_last_tune >= self.min_trades_between_tuning:
            return self._recalibrate_hyperparameters(latest_trade=trade)

        return None

    def _recalibrate_hyperparameters(self, latest_trade: ClosedTradeOutcome) -> TunedHyperparameters:
        """Executes full autonomous recalibration across weights, thresholds, and risk bounds."""
        self._trades_since_last_tune = 0

        # 1. Calibrate Council Weights based on empirical accuracy
        acc_scores = {}
        for agent_name, history in self._agent_accuracy.items():
            if history:
                acc_scores[agent_name] = max(0.20, sum(history) / len(history))
            else:
                acc_scores[agent_name] = 0.50

        total_acc = sum(acc_scores.values())
        if total_acc > 0:
            self.current_params.council_weights = {
                k: round(v / total_acc, 3) for k, v in acc_scores.items()
            }

        # 2. Calibrate Max Slippage Tolerance from empirical distribution
        # Requires minimum sample size (min_slippage_samples) to avoid calibrating from insufficient observations.
        # Calibrates to 75th percentile with 20% safety margin, bounded within [75, 300] bps.
        if len(self._slippage_observations) >= self.min_slippage_samples:
            sorted_slip = sorted(self._slippage_observations)
            p75_idx = int(len(sorted_slip) * 0.75)
            p75_slip = sorted_slip[min(p75_idx, len(sorted_slip) - 1)]
            # Provide 20% safety margin above 75th percentile slippage, bounded [75, 300] bps
            calibrated_slip = max(75, min(300, int(p75_slip * 1.20)))
            self.current_params.max_slippage_bps = calibrated_slip

        # 3. Calibrate Entry Thresholds and Stop Loss based on failure modes
        if not latest_trade.is_win:
            if latest_trade.roi_pct <= -15.0:
                # Tighten hard stop-loss to contain catastrophic drawdowns
                self.current_params.hard_stop_loss_pct = max(-15.0, min(-8.0, self.current_params.hard_stop_loss_pct + 1.0))
                # Elevate minimum alpha entry requirement
                self.current_params.min_alpha_score_entry = min(0.78, self.current_params.min_alpha_score_entry + 0.02)
                # Dampen Kelly multiplier
                self.current_params.kelly_fraction_multiplier = max(0.30, self.current_params.kelly_fraction_multiplier * 0.90)
        else:
            if latest_trade.roi_pct >= 40.0:
                # Loosen Kelly dampener slightly during winning regimes
                self.current_params.kelly_fraction_multiplier = min(0.85, self.current_params.kelly_fraction_multiplier * 1.05)

        self.current_params.last_tuned_timestamp = time.time()
        self._persist_settings()

        logger.info(
            "Hyperparameter Recalibration Complete: Council=%s, MaxSlippage=%dbps, StopLoss=%.1f%%, KellyMult=%.2f",
            self.current_params.council_weights,
            self.current_params.max_slippage_bps,
            self.current_params.hard_stop_loss_pct,
            self.current_params.kelly_fraction_multiplier,
        )
        return self.current_params

    def _persist_settings(self) -> None:
        """Persists auto-tuned parameters into the runtime settings JSON file safely."""
        try:
            data = {}
            if os.path.exists(self.settings_file_path):
                with open(self.settings_file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            data["autonomous_hyperparameters"] = self.current_params.to_dict()
            data["updated_at"] = int(time.time())

            with open(self.settings_file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as exc:
            logger.error("Failed to persist tuned hyperparameters to %s: %s", self.settings_file_path, exc)

    def record_loss_attribution(
        self,
        reason: ComprehensiveLossAttribution,
        trade: ClosedTradeOutcome,
    ) -> None:
        """Adapts parameters directly based on loss attribution categorization."""
        pnl_dec = Decimal(str(trade.realized_pnl_usd))
        if reason == ComprehensiveLossAttribution.MEV_SANDWICH:
            self.current_params.max_slippage_bps = min(300, self.current_params.max_slippage_bps + 25)
        elif reason == ComprehensiveLossAttribution.RUG_PULL:
            self.current_params.min_alpha_score_entry = min(0.85, self.current_params.min_alpha_score_entry + 0.05)
        elif reason == ComprehensiveLossAttribution.SLIPPAGE_LEAKAGE and pnl_dec < Decimal("0"):
            self.current_params.max_slippage_bps = max(75, self.current_params.max_slippage_bps - 15)
        self._persist_settings()

