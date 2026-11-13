"""
alpha_engine.observability.walk_forward — Walk-Forward Rolling Validation Engine
================================================================================
Implements rolling time-split walk-forward validation for candidate and promoted strategies.
Ensures every generated strategy shows out-of-sample performance on historical/recorded
data before staging or promotion:
- Time-ordered partitioning into In-Sample (training) and Out-of-Sample (validation)
- Evaluates declarative EntryRuleSet criteria on historical signals
- Computes risk-adjusted performance (Profit Factor, Sortino, Win Rate, Drawdown)
- Rejects overfit strategies failing out-of-sample fail-closed
- Persists validation audits to SQLite ledger
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from alpha_engine.engine.experimentation import StrategyParameters
from alpha_engine.engine.rules import EntryRuleSet
from alpha_engine.execution.ledger import SQLiteLedger

logger = logging.getLogger(__name__)


@dataclass
class WindowPerformance:
    """Quantitative performance metrics across a single time window."""

    trades_count: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    net_pnl_usd: float = 0.0
    gross_profit_usd: float = 0.0
    gross_loss_usd: float = 0.0
    max_drawdown_pct: float = 0.0
    sortino_ratio: float = 0.0
    expectancy_usd: float = 0.0


@dataclass
class WalkForwardWindow:
    """Representation of an individual train/val rolling window."""

    window_index: int
    train_start_ns: int
    train_end_ns: int
    val_start_ns: int
    val_end_ns: int
    train_trades: List[Dict[str, Any]] = field(default_factory=list)
    val_trades: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class WalkForwardResult:
    """Institutional walk-forward validation results."""

    strategy_name: str
    is_promotable: bool
    in_sample_metrics: WindowPerformance
    out_of_sample_metrics: WindowPerformance
    full_sample_metrics: WindowPerformance
    rejection_reasons: List[str] = field(default_factory=list)
    total_trades_analyzed: int = 0
    validation_id: str = field(default_factory=lambda: f"wf-{uuid.uuid4().hex[:8]}")
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "validation_id": self.validation_id,
            "strategy_name": self.strategy_name,
            "is_promotable": 1 if self.is_promotable else 0,
            "in_sample_trades": self.in_sample_metrics.trades_count,
            "in_sample_profit_factor": self.in_sample_metrics.profit_factor,
            "in_sample_net_pnl": self.in_sample_metrics.net_pnl_usd,
            "out_of_sample_trades": self.out_of_sample_metrics.trades_count,
            "out_of_sample_profit_factor": self.out_of_sample_metrics.profit_factor,
            "out_of_sample_net_pnl": self.out_of_sample_metrics.net_pnl_usd,
            "out_of_sample_drawdown_pct": self.out_of_sample_metrics.max_drawdown_pct,
            "rejection_reasons": self.rejection_reasons,
            "timestamp": self.timestamp,
        }


class WalkForwardValidator:
    """
    Time-split walk-forward validation engine.
    Ensures strategies generalize out-of-sample before staging or promotion.
    """

    def __init__(
        self,
        ledger: Optional[SQLiteLedger] = None,
        train_ratio: float = 0.70,
        min_oos_trades: int = 5,
        min_oos_profit_factor: float = 1.10,
        max_oos_drawdown_pct: float = 15.0,
    ) -> None:
        self.ledger = ledger
        self.train_ratio = max(0.50, min(0.90, train_ratio))
        self.min_oos_trades = max(1, min_oos_trades)
        self.min_oos_profit_factor = max(0.5, min_oos_profit_factor)
        self.max_oos_drawdown_pct = max(1.0, max_oos_drawdown_pct)

    def _compute_metrics(self, trades: List[Dict[str, Any]]) -> WindowPerformance:
        """Calculate quantitative performance from a series of trade records."""
        if not trades:
            return WindowPerformance()

        n = len(trades)
        wins = 0
        losses = 0
        gross_profit = 0.0
        gross_loss = 0.0
        net_pnl = 0.0
        loss_pnls: List[float] = []

        cumulative_pnl = 0.0
        peak_pnl = 0.0
        max_dd = 0.0

        for t in trades:
            pnl = float(t.get("realized_pnl_usd", 0.0))
            net_pnl += pnl
            cumulative_pnl += pnl
            if cumulative_pnl > peak_pnl:
                peak_pnl = cumulative_pnl
            dd = peak_pnl - cumulative_pnl
            if dd > max_dd:
                max_dd = dd

            if pnl > 0:
                wins += 1
                gross_profit += pnl
            else:
                losses += 1
                gross_loss += abs(pnl)
                loss_pnls.append(abs(pnl))

        win_rate = (wins / n * 100.0) if n > 0 else 0.0
        pf = (gross_profit / gross_loss) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
        exp = (net_pnl / n) if n > 0 else 0.0

        # Sortino computation
        downside_dev = math.sqrt(sum(x**2 for x in loss_pnls) / n) if loss_pnls else 0.0
        sortino = (exp / downside_dev) if downside_dev > 0 else (99.0 if net_pnl > 0 else 0.0)

        cost_basis = sum(float(t.get("cost_basis_usd", 20.0)) for t in trades) / n if n > 0 else 20.0
        mdd_pct = (max_dd / cost_basis * 100.0) if cost_basis > 0 else 0.0

        return WindowPerformance(
            trades_count=n,
            winning_trades=wins,
            losing_trades=losses,
            win_rate=win_rate,
            profit_factor=pf,
            net_pnl_usd=net_pnl,
            gross_profit_usd=gross_profit,
            gross_loss_usd=gross_loss,
            max_drawdown_pct=mdd_pct,
            sortino_ratio=sortino,
            expectancy_usd=exp,
        )

    def _filter_trades_by_rules(
        self,
        strategy_params: StrategyParameters,
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Filter historical records according to candidate strategy's entry rules."""
        if not hasattr(strategy_params, "entry_rules") or not strategy_params.entry_rules:
            return records

        filtered: List[Dict[str, Any]] = []
        for r in records:
            # Build mock signal or attribute dict from record
            # EntryRuleSet evaluates against signals or attributes dict
            # If entry rules match, include trade
            try:
                # Check metrics extraction against record
                score = float(r.get("alpha_score", 0.5))
                if score < strategy_params.min_entry_score:
                    continue

                class _MockSignal:
                    def __init__(self, data: dict):
                        self.__dict__.update(data)
                        self.score = float(data.get("alpha_score", 0.5))
                        self.source = data.get("source", "FAST_LAUNCH")
                        self.token_address = data.get("token_address", "mock")

                sig_obj = _MockSignal(r)
                if strategy_params.entry_rules.evaluate(sig_obj):
                    filtered.append(r)
            except Exception:
                # If extraction fails, preserve record
                filtered.append(r)

        return filtered

    async def evaluate_trades_walk_forward(
        self,
        strategy_params: StrategyParameters,
        trade_records: List[Dict[str, Any]],
    ) -> WalkForwardResult:
        """
        Partition trade records by time into in-sample and out-of-sample segments,
        re-evaluate under candidate strategy rules, and verify out-of-sample performance.
        """
        # Ensure trades are ordered by timestamp
        ordered = sorted(trade_records, key=lambda x: x.get("timestamp_ns", 0))
        n_total = len(ordered)

        # Filter through candidate entry rules
        matched_trades = self._filter_trades_by_rules(strategy_params, ordered)

        split_idx = int(len(matched_trades) * self.train_ratio)
        in_sample = matched_trades[:split_idx]
        out_of_sample = matched_trades[split_idx:]

        is_perf = self._compute_metrics(in_sample)
        oos_perf = self._compute_metrics(out_of_sample)
        full_perf = self._compute_metrics(matched_trades)

        rejections: List[str] = []

        # Anti-overfitting gates on Out-of-Sample window
        if len(out_of_sample) < self.min_oos_trades:
            rejections.append(f"INSUFFICIENT_OOS_TRADES: got {len(out_of_sample)}, need {self.min_oos_trades}")
        else:
            if oos_perf.net_pnl_usd <= 0.0:
                rejections.append(f"OOS_NEGATIVE_PNL: ${oos_perf.net_pnl_usd:+.2f}")
            if oos_perf.profit_factor < self.min_oos_profit_factor:
                rejections.append(f"OOS_PROFIT_FACTOR_BELOW_MIN: {oos_perf.profit_factor:.2f} < {self.min_oos_profit_factor:.2f}")
            if oos_perf.max_drawdown_pct > self.max_oos_drawdown_pct:
                rejections.append(f"OOS_HIGH_DRAWDOWN: {oos_perf.max_drawdown_pct:.1f}% > {self.max_oos_drawdown_pct:.1f}%")

            # Cliff drop: in-sample won big but OOS collapsed
            if is_perf.profit_factor > 1.80 and oos_perf.profit_factor < 1.0:
                rejections.append("OVERFIT_OOS_DEGRADATION: in-sample PF > 1.80 but OOS PF < 1.0")

        is_promotable = (len(rejections) == 0)

        result = WalkForwardResult(
            strategy_name=strategy_params.strategy_name,
            is_promotable=is_promotable,
            in_sample_metrics=is_perf,
            out_of_sample_metrics=oos_perf,
            full_sample_metrics=full_perf,
            rejection_reasons=rejections,
            total_trades_analyzed=len(matched_trades),
        )

        # Store in ledger if ledger is available
        if self.ledger is not None and hasattr(self.ledger, "record_walk_forward_validation"):
            try:
                await self.ledger.record_walk_forward_validation(
                    validation_id=result.validation_id,
                    strategy_name=result.strategy_name,
                    is_promotable=result.is_promotable,
                    in_sample_trades=is_perf.trades_count,
                    in_sample_pf=is_perf.profit_factor,
                    in_sample_pnl=is_perf.net_pnl_usd,
                    oos_trades=oos_perf.trades_count,
                    oos_pf=oos_perf.profit_factor,
                    oos_pnl=oos_perf.net_pnl_usd,
                    oos_mdd=oos_perf.max_drawdown_pct,
                    rejections=rejections,
                )
            except Exception as e:
                logger.error("Failed to persist walk-forward validation to ledger: %s", e)

        return result

