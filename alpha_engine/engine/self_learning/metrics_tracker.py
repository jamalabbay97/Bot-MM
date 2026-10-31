"""
alpha_engine.engine.self_learning.metrics_tracker — Per-Strategy Performance & Loss Attribution Tracker
=======================================================================================================
Maintains institutional-grade rolling statistical scorecards for every trading strategy:
- Win Rate, Profit Factor, Sharpe Ratio, Sortino Ratio, Max Drawdown, Mathematical Expectancy.
- Granular loss root-cause attribution.
- Strategy performance ranking and quarantine enforcement.
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict, deque
from typing import Optional

from alpha_engine.engine.self_learning.models import (
    ClosedTradeOutcome,
    ComprehensiveLossAttribution,
    StrategyPerformanceMetrics,
    StrategyStatus,
)

logger = logging.getLogger(__name__)


class StrategyPerformanceTracker:
    """
    Real-time rolling performance tracker for all institutional strategies.
    Computes Sharpe, Sortino, Drawdown, and mathematical expectancy incrementally.
    """

    def __init__(self, history_window: int = 200, risk_free_rate: float = 0.0) -> None:
        self.history_window = history_window
        self.risk_free_rate = risk_free_rate
        self._strategy_trades: dict[str, deque[ClosedTradeOutcome]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )
        self._strategy_metrics: dict[str, StrategyPerformanceMetrics] = {}
        self._equity_curves: dict[str, list[float]] = defaultdict(lambda: [0.0])
        self._global_trades: deque[ClosedTradeOutcome] = deque(maxlen=1000)

    def record_closed_trade(self, trade: ClosedTradeOutcome) -> StrategyPerformanceMetrics:
        """
        Record a closed trade, attribute loss root cause if applicable,
        and update the strategy's statistical performance scorecard.
        """
        strat_key = trade.strategy_type or "unknown"
        self._strategy_trades[strat_key].append(trade)
        self._global_trades.append(trade)

        # Update Cumulative Equity Curve for Drawdown calculation
        prior_equity = self._equity_curves[strat_key][-1]
        new_equity = prior_equity + trade.realized_pnl_usd
        self._equity_curves[strat_key].append(new_equity)

        # Diagnose and attribute loss reason if unprofitable
        loss_attribution = self._attribute_trade_outcome(trade)

        # Recompute all metrics for this strategy
        metrics = self._recompute_strategy_metrics(strat_key, trade, loss_attribution)
        self._strategy_metrics[strat_key] = metrics
        return metrics

    def _attribute_trade_outcome(self, trade: ClosedTradeOutcome) -> ComprehensiveLossAttribution:
        """
        Diagnoses root cause of the trade outcome using execution latency,
        slippage leakage, holding duration, and drawdown severity.
        """
        if trade.is_win:
            return ComprehensiveLossAttribution.PROFIT_TARGET_HIT

        roi = trade.roi_pct
        slippage_leak = max(0, trade.actual_slippage_bps - trade.expected_slippage_bps)
        hold_s = trade.holding_time_seconds
        exit_r = trade.exit_reason.lower()

        if "rug" in exit_r or roi <= -80.0:
            return ComprehensiveLossAttribution.RUG_PULL
        if "drain" in exit_r or roi <= -50.0:
            return ComprehensiveLossAttribution.LIQUIDITY_DRAIN
        if slippage_leak >= 250 or "sandwich" in exit_r:
            return ComprehensiveLossAttribution.MEV_SANDWICH
        if slippage_leak >= 150:
            return ComprehensiveLossAttribution.SLIPPAGE_LEAKAGE
        if hold_s >= 1800.0 and abs(roi) < 8.0:
            return ComprehensiveLossAttribution.VOLUME_DRYOUT
        if hold_s <= 180.0 and roi <= -15.0:
            return ComprehensiveLossAttribution.CABAL_DUMP
        if "timeout" in exit_r or hold_s >= 3600.0:
            return ComprehensiveLossAttribution.TIME_DECAY
        if roi <= -10.0 and hold_s < 600.0:
            return ComprehensiveLossAttribution.FALSE_BREAKOUT

        return ComprehensiveLossAttribution.LATE_ENTRY

    def _recompute_strategy_metrics(
        self,
        strategy_key: str,
        latest_trade: ClosedTradeOutcome,
        latest_loss_reason: ComprehensiveLossAttribution,
    ) -> StrategyPerformanceMetrics:
        trades = list(self._strategy_trades[strategy_key])
        total = len(trades)
        if total == 0:
            return StrategyPerformanceMetrics(strategy_type=strategy_key, name=strategy_key)

        if latest_trade and latest_trade.realized_pnl_usd < 0:
            logger.debug(
                "MetricsTracker: Strategy %s registered loss trade %s (pnl=%.2f, reason=%s)",
                strategy_key, latest_trade.trade_id, latest_trade.realized_pnl_usd, latest_loss_reason.value,
            )

        wins = [t for t in trades if t.is_win]
        losses = [t for t in trades if not t.is_win]

        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / total) * 100.0

        gross_profits = sum(t.realized_pnl_usd for t in wins)
        gross_losses = abs(sum(t.realized_pnl_usd for t in losses))

        if gross_losses > 0:
            profit_factor = gross_profits / gross_losses
        else:
            profit_factor = 3.0 if gross_profits > 0 else 1.0

        avg_win = (gross_profits / win_count) if win_count > 0 else 0.0
        avg_loss = (gross_losses / loss_count) if loss_count > 0 else 0.0

        # Mathematical Expectancy: E = (P_win * Avg_Win) - (P_loss * Avg_Loss)
        p_win = win_count / total
        p_loss = loss_count / total
        expectancy = (p_win * avg_win) - (p_loss * avg_loss)

        total_pnl = sum(t.realized_pnl_usd for t in trades)
        avg_hold = sum(t.holding_time_seconds for t in trades) / total

        # Return Series & Downside Risk for Sharpe & Sortino
        returns = [t.roi_pct / 100.0 for t in trades]
        mean_return = sum(returns) / total

        if total >= 3:
            variance = sum((r - mean_return) ** 2 for r in returns) / total
            std_dev = math.sqrt(variance)

            # Downside deviation (returns below target/risk-free)
            downside_var = sum((min(0.0, r - self.risk_free_rate)) ** 2 for r in returns) / total
            downside_std = math.sqrt(downside_var)

            sharpe = ((mean_return - self.risk_free_rate) / std_dev) * math.sqrt(252) if std_dev > 0 else 0.0
            sortino = ((mean_return - self.risk_free_rate) / downside_std) * math.sqrt(252) if downside_std > 0 else 0.0
        else:
            sharpe = 1.0 if mean_return > 0 else -0.5
            sortino = 1.5 if mean_return > 0 else -0.5

        # Maximum Peak-to-Trough Drawdown
        equity = self._equity_curves[strategy_key]
        peak = 0.0
        max_dd = 0.0
        for eq in equity:
            if eq > peak:
                peak = eq
            dd = peak - eq
            if dd > max_dd:
                max_dd = dd

        max_dd_pct = (max_dd / peak * 100.0) if peak > 0 else (abs(max_dd) if max_dd > 0 else 0.0)

        # Maintain Loss Reasons Count
        prior_metrics = self._strategy_metrics.get(strategy_key)
        reasons = dict(prior_metrics.loss_reasons) if prior_metrics else {}
        reasons[latest_loss_reason.value] = reasons.get(latest_loss_reason.value, 0) + 1

        # Dynamic Status Evaluation (Quarantine / Boost)
        if total < 5:
            status = StrategyStatus.INSUFFICIENT_DATA
            weight = 1.0
        elif expectancy < 0 and (win_rate < 35.0 or profit_factor < 0.80 or max_dd_pct >= 25.0):
            status = StrategyStatus.QUARANTINED
            weight = 0.20  # Heavily damped to preserve capital
        elif expectancy > 0 and win_rate >= 60.0 and profit_factor >= 1.70:
            status = StrategyStatus.BOOSTED
            weight = 1.40  # Boost allocation
        elif profit_factor < 1.10:
            status = StrategyStatus.DAMPENED
            weight = 0.65
        else:
            status = StrategyStatus.ACTIVE
            weight = 1.0

        return StrategyPerformanceMetrics(
            strategy_type=strategy_key,
            name=strategy_key,
            total_trades=total,
            winning_trades=win_count,
            losing_trades=loss_count,
            win_rate_pct=round(win_rate, 2),
            profit_factor=round(profit_factor, 2),
            sharpe_ratio=round(sharpe, 2),
            sortino_ratio=round(sortino, 2),
            max_drawdown_pct=round(max_dd_pct, 2),
            mathematical_expectancy_usd=round(expectancy, 2),
            total_realized_pnl_usd=round(total_pnl, 2),
            average_win_usd=round(avg_win, 2),
            average_loss_usd=round(avg_loss, 2),
            average_hold_time_seconds=round(avg_hold, 1),
            status=status,
            dynamic_weight=weight,
            loss_reasons=reasons,
            last_updated=time.time(),
        )

    def get_strategy_metrics(self, strategy_type: str) -> Optional[StrategyPerformanceMetrics]:
        return self._strategy_metrics.get(strategy_type)

    def get_all_strategy_metrics(self) -> dict[str, StrategyPerformanceMetrics]:
        return dict(self._strategy_metrics)

    def get_ranked_strategies(self) -> list[StrategyPerformanceMetrics]:
        """Rank strategies descending by Expectancy and Profit Factor."""
        items = list(self._strategy_metrics.values())
        return sorted(items, key=lambda m: (m.mathematical_expectancy_usd, m.profit_factor), reverse=True)
