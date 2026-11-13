"""
alpha_engine.engine.rules — Declarative Entry-Rule Schema & Metric Registry
===========================================================================
Defines typed, validated, and evaluated rule structures for quantitative trading strategies:
- ComparisonOperator & LogicalOperator enums
- MetricRegistry mapping known metric names to real extractors against live signal/pool state
- RuleCondition, RuleGroup, and EntryRuleSet
- Strict validation (unknown metrics fail validation, out-of-bound thresholds fail validation)
- Fail-closed evaluation (missing data returns False)
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SchemaValidationError(ValueError):
    """Raised when an entry rule condition or schema fails validation."""
    pass


class ComparisonOperator(str, Enum):
    """Supported comparison operators for rule conditions."""
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    NEQ = "!="

    @classmethod
    def from_str(cls, val: Any) -> "ComparisonOperator":
        if isinstance(val, cls):
            return val
        for op in cls:
            if op.value == val or op.name == str(val).upper():
                return op
        raise SchemaValidationError(f"Invalid comparison operator: {val!r}. Valid operators: {[op.value for op in cls]}")


class LogicalOperator(str, Enum):
    """Supported logical aggregation operators for rule groups."""
    AND = "AND"
    OR = "OR"

    @classmethod
    def from_str(cls, val: Any) -> "LogicalOperator":
        if isinstance(val, cls):
            return val
        val_upper = str(val).upper().strip()
        if val_upper in ("AND", "OR"):
            return cls(val_upper)
        raise SchemaValidationError(f"Invalid logical operator: {val!r}. Valid: AND, OR")


@dataclass(frozen=True)
class MetricDefinition:
    """Specification of an observable market/signal metric."""
    name: str
    description: str
    allowed_range: Tuple[float, float]
    extractor: Callable[[Any, Optional[Any], Optional[Decimal], Optional[Dict[str, Any]]], Optional[float]]


# ---------------------------------------------------------------------------
# Metric Registry & Extractor Implementations
# ---------------------------------------------------------------------------

def _extract_metric_context(
    metric_name: str,
    ctx: Optional[Dict[str, Any]],
    pool: Optional[Any] = None,
    native_price_usd: Optional[Decimal] = None,
) -> Optional[float]:
    """Helper to extract metric override or fallback from context, pool, or pricing."""
    if ctx and metric_name in ctx and ctx[metric_name] is not None:
        try:
            return float(ctx[metric_name])
        except (TypeError, ValueError):
            pass
    if pool is not None and hasattr(pool, metric_name) and getattr(pool, metric_name) is not None:
        try:
            return float(getattr(pool, metric_name))
        except (TypeError, ValueError):
            pass
    if native_price_usd is not None and ctx and f"{metric_name}_native" in ctx:
        try:
            return float(Decimal(str(ctx[f"{metric_name}_native"])) * native_price_usd)
        except (TypeError, ValueError):
            pass
    return None


def _extract_mcap(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract market cap in USD."""
    ctx_val = _extract_metric_context("mcap_usd", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    mcap = getattr(signal, "market_cap_usd", None)
    if mcap is not None:
        try:
            return float(mcap)
        except (TypeError, ValueError):
            pass
    if pool is not None and native_price_usd is not None and hasattr(pool, "calculate_market_cap_usd"):
        try:
            calc = pool.calculate_market_cap_usd(native_price_usd)
            if calc is not None:
                return float(calc)
        except Exception:
            pass
    base_mcap = getattr(signal, "base_market_cap", None)
    if base_mcap is not None:
        try:
            return float(base_mcap)
        except (TypeError, ValueError):
            pass
    return None


def _extract_net_buy_delta(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract net buy volume ratio (0.0 to 1.0)."""
    ctx_val = _extract_metric_context("net_buy_delta", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    delta = getattr(signal, "net_buy_delta", None)
    if delta is not None:
        try:
            return float(delta)
        except (TypeError, ValueError):
            pass
    # Try pattern feature extraction if available
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.net_buy_delta)
    except Exception:
        return None


def _extract_volume_surge(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract volume surge multiplier over baseline."""
    ctx_val = _extract_metric_context("volume_surge_multiplier", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    surge = getattr(signal, "volume_surge_multiplier", None)
    if surge is not None:
        try:
            return float(surge)
        except (TypeError, ValueError):
            pass
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.volume_surge_multiplier)
    except Exception:
        return None


def _extract_dormancy_hours(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract consolidation / dormancy / token age duration in hours."""
    ctx_val = _extract_metric_context("dormancy_hours", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    cons_h = getattr(signal, "consolidation_length_hours", None)
    if cons_h is not None:
        try:
            return float(cons_h)
        except (TypeError, ValueError):
            pass
    age_h = getattr(signal, "token_age_hours", None)
    if age_h is not None:
        try:
            return float(age_h)
        except (TypeError, ValueError):
            pass
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.consolidation_duration_s) / 3600.0
    except Exception:
        return None


def _extract_slippage_bps(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract estimated or configured slippage tolerance in basis points."""
    ctx_val = _extract_metric_context("max_slippage_bps", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    slip = getattr(signal, "max_slippage_bps", None)
    if slip is not None:
        try:
            return float(slip)
        except (TypeError, ValueError):
            pass
    # If pool available, estimate CPMM buy price impact for quote size
    if pool is not None and hasattr(pool, "native_reserve") and pool.native_reserve > Decimal("0"):
        from alpha_engine.math.cpmm import cpmm_buy_quote
        try:
            quote_size = (Decimal("50.0") / native_price_usd) if (native_price_usd is not None and native_price_usd > Decimal("0")) else Decimal("0.1")
            q = cpmm_buy_quote(pool, quote_size)
            return float(q.price_impact_bps)
        except Exception:
            pass
    return None


def _extract_wallet_score(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract wallet profile score or sortino ratio."""
    ctx_val = _extract_metric_context("wallet_score", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    profile = getattr(signal, "wallet_profile", None)
    if profile is not None:
        score = getattr(profile, "sortino_ratio", None)
        if score is None:
            score = getattr(profile, "wallet_score", None)
        if score is None:
            score = getattr(profile, "win_rate", None)
        if score is not None:
            try:
                return float(score)
            except (TypeError, ValueError):
                pass
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.smart_wallet_inflows)
    except Exception:
        return None


def _extract_social_conviction(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract social or organic conviction score (0.0 to 100.0)."""
    ctx_val = _extract_metric_context("social_conviction", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    conv = getattr(signal, "social_conviction", None)
    if conv is None:
        conv = getattr(signal, "organic_conviction", None)
    if conv is not None:
        try:
            return float(conv)
        except (TypeError, ValueError):
            pass
    alpha = getattr(signal, "alpha_score", None)
    if alpha is not None:
        try:
            val = float(alpha)
            return val * 100.0 if val <= 1.0 else val
        except (TypeError, ValueError):
            pass
    return None


def _extract_liquidity_usd(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract total pool liquidity depth in USD."""
    ctx_val = _extract_metric_context("liquidity_usd", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    sec = getattr(signal, "security_report", None)
    if sec is not None:
        liq = getattr(sec, "liquidity_usd", None)
        if liq is not None:
            try:
                return float(liq)
            except (TypeError, ValueError):
                pass
    if pool is not None and native_price_usd is not None and hasattr(pool, "native_reserve"):
        try:
            return float(pool.native_reserve * native_price_usd * Decimal("2"))
        except Exception:
            pass
    return None


def _extract_holder_concentration(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract top 10 non-pool holder concentration (0.0 to 1.0)."""
    ctx_val = _extract_metric_context("holder_concentration", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    sec = getattr(signal, "security_report", None)
    if sec is not None:
        conc = getattr(sec, "top10_concentration", None)
        if conc is not None:
            try:
                return float(conc)
            except (TypeError, ValueError):
                pass
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.top10_concentration)
    except Exception:
        return None


def _extract_alpha_score(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract normalized alpha score (0.0 to 100.0)."""
    ctx_val = _extract_metric_context("alpha_score", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    score = getattr(signal, "alpha_score", None)
    if score is not None:
        try:
            val = float(score)
            return val * 100.0 if val <= 1.0 else val
        except (TypeError, ValueError):
            pass
    return None


def _extract_dip_depth_pct(signal: Any, pool: Optional[Any], native_price_usd: Optional[Decimal], ctx: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract dip depth from initial peak in percent."""
    ctx_val = _extract_metric_context("dip_depth_pct", ctx, pool, native_price_usd)
    if ctx_val is not None:
        return ctx_val
    dip = getattr(signal, "dip_depth_pct", None)
    if dip is not None:
        try:
            return float(dip)
        except (TypeError, ValueError):
            pass
    from alpha_engine.models.decisions import PatternFeatureVector
    try:
        vec = PatternFeatureVector.from_signal(signal)
        return float(vec.dip_depth_pct)
    except Exception:
        return None


METRIC_REGISTRY: Dict[str, MetricDefinition] = {
    "mcap_usd": MetricDefinition(
        name="mcap_usd",
        description="Market capitalization in USD",
        allowed_range=(0.0, 100_000_000_000.0),
        extractor=_extract_mcap,
    ),
    "net_buy_delta": MetricDefinition(
        name="net_buy_delta",
        description="Ratio of buy volume to total volume (0.0 to 1.0)",
        allowed_range=(0.0, 1.0),
        extractor=_extract_net_buy_delta,
    ),
    "volume_surge_multiplier": MetricDefinition(
        name="volume_surge_multiplier",
        description="Volume surge multiplier relative to baseline moving average",
        allowed_range=(0.0, 1000.0),
        extractor=_extract_volume_surge,
    ),
    "dormancy_hours": MetricDefinition(
        name="dormancy_hours",
        description="Consolidation length or token age in hours",
        allowed_range=(0.0, 87600.0),
        extractor=_extract_dormancy_hours,
    ),
    "slippage_estimate_bps": MetricDefinition(
        name="slippage_estimate_bps",
        description="Estimated execution slippage or max slippage in basis points",
        allowed_range=(0.0, 5000.0),
        extractor=_extract_slippage_bps,
    ),
    "wallet_score": MetricDefinition(
        name="wallet_score",
        description="Associated smart wallet score or Sortino ratio",
        allowed_range=(-10.0, 100.0),
        extractor=_extract_wallet_score,
    ),
    "social_conviction": MetricDefinition(
        name="social_conviction",
        description="Organic social / sentiment conviction score (0.0 to 100.0)",
        allowed_range=(0.0, 100.0),
        extractor=_extract_social_conviction,
    ),
    "liquidity_usd": MetricDefinition(
        name="liquidity_usd",
        description="Pool liquidity depth in USD",
        allowed_range=(0.0, 100_000_000_000.0),
        extractor=_extract_liquidity_usd,
    ),
    "holder_concentration": MetricDefinition(
        name="holder_concentration",
        description="Top 10 non-pool holder concentration fraction (0.0 to 1.0)",
        allowed_range=(0.0, 1.0),
        extractor=_extract_holder_concentration,
    ),
    "alpha_score": MetricDefinition(
        name="alpha_score",
        description="Composite signal alpha score (0.0 to 100.0)",
        allowed_range=(0.0, 100.0),
        extractor=_extract_alpha_score,
    ),
    "dip_depth_pct": MetricDefinition(
        name="dip_depth_pct",
        description="Drawdown percentage from peak (0.0 to 100.0)",
        allowed_range=(0.0, 100.0),
        extractor=_extract_dip_depth_pct,
    ),
}

# Canonical Metric Aliases
METRIC_ALIASES: dict[str, str] = {
    "mcap": "mcap_usd",
    "max_mcap_usd": "mcap_usd",
    "min_net_buy_delta": "net_buy_delta",
    "surge_factor": "volume_surge_multiplier",
    "min_surge_k": "volume_surge_multiplier",
    "min_surge_multiplier": "volume_surge_multiplier",
    "dormancy": "dormancy_hours",
    "min_dormancy_hours": "dormancy_hours",
    "token_age_hours": "dormancy_hours",
    "consolidation_length_hours": "dormancy_hours",
    "slippage_bps": "slippage_estimate_bps",
    "max_slippage_bps": "slippage_estimate_bps",
    "slippage_estimate": "slippage_estimate_bps",
    "min_wallet_sortino": "wallet_score",
    "wallet_sortino": "wallet_score",
    "organic_conviction": "social_conviction",
    "organic_conviction_min": "social_conviction",
    "liquidity": "liquidity_usd",
    "top10_concentration": "holder_concentration",
    "min_entry_score": "alpha_score",
}


def resolve_canonical_metric(name: str) -> str:
    """Resolve an incoming metric name or alias to its canonical registry key."""
    canonical = METRIC_ALIASES.get(name, name)
    if canonical not in METRIC_REGISTRY:
        raise SchemaValidationError(
            f"Unknown metric '{name}'. Registered metrics: {sorted(METRIC_REGISTRY.keys())} "
            f"(aliases: {sorted(METRIC_ALIASES.keys())})"
        )
    return canonical


# ---------------------------------------------------------------------------
# Rule Structures: RuleCondition, RuleGroup, EntryRuleSet
# ---------------------------------------------------------------------------

@dataclass
class RuleCondition:
    """
    Atomic predicate comparing an extracted market metric against a threshold.
    """
    metric: str
    operator: ComparisonOperator
    threshold: float
    allowed_range: Optional[Tuple[float, float]] = None

    def __post_init__(self) -> None:
        if isinstance(self.operator, str):
            self.operator = ComparisonOperator.from_str(self.operator)
        self.threshold = float(self.threshold)
        self.validate()

    def validate(self) -> None:
        """Validate metric existence, operator, and threshold bounds."""
        canonical_metric = resolve_canonical_metric(self.metric)
        defn = METRIC_REGISTRY[canonical_metric]

        bounds = self.allowed_range or defn.allowed_range
        min_v, max_v = bounds
        if not (min_v <= self.threshold <= max_v):
            raise SchemaValidationError(
                f"Threshold {self.threshold} for metric '{self.metric}' (canonical '{canonical_metric}') "
                f"is outside allowed range [{min_v}, {max_v}]."
            )

    def evaluate(
        self,
        signal: Any,
        pool: Optional[Any] = None,
        native_price_usd: Optional[Decimal] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Evaluate condition against live state.
        Fails closed (returns False) if metric data is missing or unverified.
        """
        canonical = resolve_canonical_metric(self.metric)
        defn = METRIC_REGISTRY[canonical]
        val = defn.extractor(signal, pool, native_price_usd, context)
        if val is None:
            logger.debug("RuleCondition[%s]: Data unavailable, failing closed (False)", self.metric)
            return False

        op = self.operator
        thresh = self.threshold
        if op == ComparisonOperator.GT:
            return val > thresh
        elif op == ComparisonOperator.GTE:
            return val >= thresh
        elif op == ComparisonOperator.LT:
            return val < thresh
        elif op == ComparisonOperator.LTE:
            return val <= thresh
        elif op == ComparisonOperator.EQ:
            return abs(val - thresh) < 1e-9
        elif op == ComparisonOperator.NEQ:
            return abs(val - thresh) >= 1e-9
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "operator": self.operator.value,
            "threshold": self.threshold,
            "allowed_range": list(self.allowed_range) if self.allowed_range else None,
        }

    def as_dict(self) -> Dict[str, Any]:
        """Convert condition to dict representation via asdict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RuleCondition":
        rng = data.get("allowed_range")
        range_tuple = (float(rng[0]), float(rng[1])) if rng and len(rng) == 2 else None
        return cls(
            metric=str(data["metric"]),
            operator=ComparisonOperator.from_str(str(data["operator"])),
            threshold=float(data["threshold"]),
            allowed_range=range_tuple,
        )


@dataclass
class RuleGroup:
    """
    A collection of RuleConditions aggregated by a LogicalOperator (AND / OR).
    """
    conditions: List[RuleCondition] = field(default_factory=list)
    operator: LogicalOperator = LogicalOperator.AND

    def __post_init__(self) -> None:
        if isinstance(self.operator, str):
            self.operator = LogicalOperator.from_str(self.operator)
        self.validate()

    def validate(self) -> None:
        for c in self.conditions:
            c.validate()

    def evaluate(
        self,
        signal: Any,
        pool: Optional[Any] = None,
        native_price_usd: Optional[Decimal] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        if not self.conditions:
            return True

        if self.operator == LogicalOperator.AND:
            return all(c.evaluate(signal, pool, native_price_usd, context) for c in self.conditions)
        elif self.operator == LogicalOperator.OR:
            return any(c.evaluate(signal, pool, native_price_usd, context) for c in self.conditions)
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator": self.operator.value,
            "conditions": [c.to_dict() for c in self.conditions],
        }

    def as_dict(self) -> Dict[str, Any]:
        """Convert group to dict representation via asdict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RuleGroup":
        conds = [RuleCondition.from_dict(c) for c in data.get("conditions", [])]
        op = LogicalOperator.from_str(str(data.get("operator", "AND")))
        return cls(conditions=conds, operator=op)


@dataclass
class EntryRuleSet:
    """
    Top-level declarative entry rule specification for a StrategyVariant:
    - List of atomic conditions (evaluated with top-level operator)
    - Optional nested RuleGroups (AND / OR)
    - Full serialization, validation, and canonical fingerprinting for deduplication.
    """
    conditions: List[RuleCondition] = field(default_factory=list)
    groups: List[RuleGroup] = field(default_factory=list)
    operator: LogicalOperator = LogicalOperator.AND

    def __post_init__(self) -> None:
        if isinstance(self.operator, str):
            self.operator = LogicalOperator.from_str(self.operator)
        self.validate()

    def validate(self) -> None:
        for c in self.conditions:
            c.validate()
        for g in self.groups:
            g.validate()

    def evaluate(
        self,
        signal: Any,
        pool: Optional[Any] = None,
        native_price_usd: Optional[Decimal] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Evaluate all conditions and groups. If no rules defined, returns True (permissive).
        """
        if not self.conditions and not self.groups:
            return True

        results: list[bool] = []
        for c in self.conditions:
            results.append(c.evaluate(signal, pool, native_price_usd, context))
        for g in self.groups:
            results.append(g.evaluate(signal, pool, native_price_usd, context))

        if self.operator == LogicalOperator.AND:
            return all(results)
        elif self.operator == LogicalOperator.OR:
            return any(results)
        return False

    def fingerprint(self) -> str:
        """
        Produce a canonical hashable string representation for deduplication.
        Conditions and groups are sorted canonically so rule ordering does not produce false duplicates.
        """
        cond_strings = []
        for c in self.conditions:
            can = resolve_canonical_metric(c.metric)
            cond_strings.append(f"{can}_{c.operator.value}_{round(c.threshold, 4)}")
        cond_strings.sort()

        group_strings = []
        for g in self.groups:
            g_conds = []
            for gc in g.conditions:
                can = resolve_canonical_metric(gc.metric)
                g_conds.append(f"{can}_{gc.operator.value}_{round(gc.threshold, 4)}")
            g_conds.sort()
            group_strings.append(f"{g.operator.value}:({','.join(g_conds)})")
        group_strings.sort()

        return f"OP:{self.operator.value}|CONDS:{';'.join(cond_strings)}|GROUPS:{';'.join(group_strings)}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator": self.operator.value,
            "conditions": [c.to_dict() for c in self.conditions],
            "groups": [g.to_dict() for g in self.groups],
        }

    def as_dict(self) -> Dict[str, Any]:
        """Convert ruleset to dict representation via asdict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EntryRuleSet":
        if not data:
            return cls()
        conds = [RuleCondition.from_dict(c) for c in data.get("conditions", [])]
        grps = [RuleGroup.from_dict(g) for g in data.get("groups", [])]
        op = LogicalOperator.from_str(str(data.get("operator", "AND")))
        return cls(conditions=conds, groups=grps, operator=op)
