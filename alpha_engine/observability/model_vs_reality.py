"""
alpha_engine.observability.model_vs_reality — Simulator vs. Observed Market Reality Report
==========================================================================================
Audits paper execution model fidelity against observed on-chain market reality:
- For each paper trade fill, compares modeled fill price vs. observed tick price at fill time
- Calculates signed execution bias (bps): positive = optimistic (modeled fill better than reality)
- Computes per-venue and per-size distribution metrics (mean bias, median bias, % fills better)
- Raises WARNING when aggregate or venue bias exceeds configured bound (e.g. > 25 bps)
- Exposes CLI summary and dashboard telemetry integration

CALIBRATION PROTOCOL (WITH TINY REAL TRADES):
--------------------------------------------
To calibrate the paper simulator against genuine on-chain execution without risking material capital:
1. Submit a series of tiny real test transactions (e.g. 0.01 SOL on Solana, 0.005 ETH on Base).
2. For each real trade, record the exact submission slot/block, landing slot/block, base network fee,
   priority fee paid, and realized on-chain DEX swap fill price from transaction logs.
3. Compare the simulator's predicted latency to observed landing latency:
   - Adjust `LATENCY_MIN_MS_*` and `LATENCY_MAX_MS_*` to match empirical 5th and 95th percentiles.
4. Compare simulator's modeled fill price against the genuine on-chain fill price:
   - If real trades consistently execute worse than modeled (positive bias):
     * Increase `EXTRA_SLIPPAGE_BPS` (default: 0 bps).
     * Adjust `MEV_MAX_SANDWICH_IMPACT_BPS` if public mempool transactions suffered frontrunning.
5. Re-run this ModelVsReality report until mean simulator bias drops below 10 bps.
NOTE: Order submission remains strictly PAPER ONLY in code. Never enable live submission without full calibration.
"""

from __future__ import annotations

import logging
import math
import statistics
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from alpha_engine.models.enums import ExecutionVenue, OrderSide
from alpha_engine.models.state import PaperFill

logger = logging.getLogger(__name__)


@dataclass
class FillRealityComparison:
    """Comparison point between a simulated paper fill and observed market reality."""

    trade_id: str
    venue: ExecutionVenue
    side: OrderSide
    size_usd: Decimal
    modeled_fill_price: Decimal
    observed_market_price: Decimal
    quote_price: Optional[Decimal] = None
    timestamp_ns: int = 0

    @property
    def signed_error_bps(self) -> float:
        """
        Signed error in basis points:
        Positive (+) means the simulator was optimistic (filled at a better price than reality).
        Negative (-) means the simulator was pessimistic (filled at a worse price than reality).
        """
        obs = float(self.observed_market_price)
        mod = float(self.modeled_fill_price)
        if obs <= 0.0:
            return 0.0

        if self.side == OrderSide.BUY:
            # For BUY: lower fill price is better for trader
            return (obs - mod) / obs * 10000.0
        else:
            # For SELL: higher fill price is better for trader
            return (mod - obs) / obs * 10000.0

    @property
    def is_better_than_reality(self) -> bool:
        """True if the modeled fill gave a more favorable execution than the observed market."""
        return self.signed_error_bps > 0.0

    @property
    def size_bucket(self) -> str:
        """Categorize order size into standardized buckets."""
        s = float(self.size_usd)
        if s < 25.0:
            return "MICRO (< $25)"
        elif s < 100.0:
            return "MEDIUM ($25 - $100)"
        else:
            return "LARGE (>= $100)"


@dataclass
class ModelRealityReport:
    """Comprehensive institutional model-vs-reality bias report."""

    total_fills: int = 0
    mean_bias_bps: float = 0.0
    median_bias_bps: float = 0.0
    fraction_better_than_reality: float = 0.0
    worst_discrepancy_bps: float = 0.0
    bias_warning_threshold_bps: float = 25.0
    warning_triggered: bool = False
    warning_messages: List[str] = field(default_factory=list)
    per_venue_bias: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    per_size_bias: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def format_cli_summary(self) -> str:
        """Format a clear institutional terminal report."""
        status_line = (
            f"⚠️  WARNING: Simulator bias ({self.mean_bias_bps:+.1f} bps) exceeds {self.bias_warning_threshold_bps:.1f} bps bound!"
            if self.warning_triggered
            else "✅ PASS: Simulator execution model matches observed market within tolerance."
        )

        lines = [
            "\n" + "=" * 80,
            "📊 MODEL-VS-REALITY EXECUTION FIDELITY REPORT",
            "=" * 80,
            f"Status: {status_line}",
            f"Total Fills Analyzed: {self.total_fills}",
            f"Mean Bias: {self.mean_bias_bps:+.2f} bps  |  Median Bias: {self.median_bias_bps:+.2f} bps",
            f"Fills Better than Reality: {self.fraction_better_than_reality * 100.0:.1f}%  |  Worst Discrepancy: {self.worst_discrepancy_bps:+.1f} bps",
            "-" * 80,
            f"{'Execution Venue':<22} | {'Fills':<6} | {'Mean Bias':<12} | {'Median Bias':<12} | {'% Optimistic':<12} | {'Status'}",
            "-" * 80,
        ]

        for venue, stats in self.per_venue_bias.items():
            warn = "⚠️ HIGH" if stats["mean_bias_bps"] > self.bias_warning_threshold_bps else "✅ OK"
            lines.append(
                f"{venue:<22} | {stats['count']:<6} | {stats['mean_bias_bps']:>+9.2f} bps | {stats['median_bias_bps']:>+9.2f} bps | {stats['fraction_better']*100:>10.1f}% | {warn}"
            )

        lines.extend([
            "-" * 80,
            f"{'Size Bucket':<22} | {'Fills':<6} | {'Mean Bias':<12} | {'Median Bias':<12} | {'% Optimistic':<12} | {'Status'}",
            "-" * 80,
        ])
        for size_b, stats in self.per_size_bias.items():
            warn = "⚠️ HIGH" if stats["mean_bias_bps"] > self.bias_warning_threshold_bps else "✅ OK"
            lines.append(
                f"{size_b:<22} | {stats['count']:<6} | {stats['mean_bias_bps']:>+9.2f} bps | {stats['median_bias_bps']:>+9.2f} bps | {stats['fraction_better']*100:>10.1f}% | {warn}"
            )

        lines.append("=" * 80 + "\n")
        return "\n".join(lines)


class ModelVsRealityReporter:
    """Computes execution simulator bias against observed ticks."""

    def __init__(self, bias_warning_threshold_bps: float = 25.0) -> None:
        self.bias_warning_threshold_bps = max(1.0, bias_warning_threshold_bps)

    def generate_report(self, comparisons: List[FillRealityComparison]) -> ModelRealityReport:
        """Compute statistical bias distribution from a set of fill comparisons."""
        if not comparisons:
            return ModelRealityReport(bias_warning_threshold_bps=self.bias_warning_threshold_bps)

        n = len(comparisons)
        errors = [c.signed_error_bps for c in comparisons]
        better_count = sum(1 for c in comparisons if c.is_better_than_reality)

        mean_err = statistics.mean(errors) if errors else 0.0
        median_err = statistics.median(errors) if errors else 0.0
        worst_err = max(errors, key=abs) if errors else 0.0
        frac_better = (better_count / n) if n > 0 else 0.0

        # Per-venue breakdown
        venues: Dict[str, List[FillRealityComparison]] = {}
        for c in comparisons:
            v_key = c.venue.value if hasattr(c.venue, "value") else str(c.venue)
            venues.setdefault(v_key, []).append(c)

        venue_stats: Dict[str, Dict[str, Any]] = {}
        warnings: List[str] = []

        for v_name, v_items in venues.items():
            v_errs = [c.signed_error_bps for c in v_items]
            v_mean = statistics.mean(v_errs)
            v_med = statistics.median(v_errs)
            v_better = sum(1 for c in v_items if c.is_better_than_reality) / len(v_items)
            venue_stats[v_name] = {
                "count": len(v_items),
                "mean_bias_bps": v_mean,
                "median_bias_bps": v_med,
                "fraction_better": v_better,
            }
            if v_mean > self.bias_warning_threshold_bps:
                warnings.append(f"Venue '{v_name}' mean bias of {v_mean:+.1f} bps exceeds {self.bias_warning_threshold_bps:.1f} bps threshold.")

        # Per-size breakdown
        sizes: Dict[str, List[FillRealityComparison]] = {}
        for c in comparisons:
            sizes.setdefault(c.size_bucket, []).append(c)

        size_stats: Dict[str, Dict[str, Any]] = {}
        for s_name, s_items in sizes.items():
            s_errs = [c.signed_error_bps for c in s_items]
            s_mean = statistics.mean(s_errs)
            s_med = statistics.median(s_errs)
            s_better = sum(1 for c in s_items if c.is_better_than_reality) / len(s_items)
            size_stats[s_name] = {
                "count": len(s_items),
                "mean_bias_bps": s_mean,
                "median_bias_bps": s_med,
                "fraction_better": s_better,
            }

        # Overall warning check
        warning_triggered = (mean_err > self.bias_warning_threshold_bps) or bool(warnings)
        if mean_err > self.bias_warning_threshold_bps:
            logger.warning(
                "ModelVsRealityReporter: Aggregate simulator bias is optimistic by %+.1f bps (threshold=%0.1f bps)",
                mean_err,
                self.bias_warning_threshold_bps,
            )

        return ModelRealityReport(
            total_fills=n,
            mean_bias_bps=mean_err,
            median_bias_bps=median_err,
            fraction_better_than_reality=frac_better,
            worst_discrepancy_bps=worst_err,
            bias_warning_threshold_bps=self.bias_warning_threshold_bps,
            warning_triggered=warning_triggered,
            warning_messages=warnings,
            per_venue_bias=venue_stats,
            per_size_bias=size_stats,
        )

