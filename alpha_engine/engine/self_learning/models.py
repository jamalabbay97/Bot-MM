"""
alpha_engine.engine.self_learning.models — Domain Models for the Self-Learning Engine
===================================================================================
Defines structured performance metrics, loss attribution categories,
and closed-trade events for online continuous reinforcement learning.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional


class ComprehensiveLossAttribution(str, Enum):
    """Granular post-mortem loss attribution taxonomy."""
    LATE_ENTRY = "LATE_ENTRY"
    RUG_PULL = "RUG_PULL"
    MEV_SANDWICH = "MEV_SANDWICH"
    VOLUME_DRYOUT = "VOLUME_DRYOUT"
    CABAL_DUMP = "CABAL_DUMP"
    LIQUIDITY_DRAIN = "LIQUIDITY_DRAIN"
    FALSE_BREAKOUT = "FALSE_BREAKOUT"
    SLIPPAGE_LEAKAGE = "SLIPPAGE_LEAKAGE"
    TIME_DECAY = "TIME_DECAY"
    CONTRACT_MUTATION = "CONTRACT_MUTATION"
    PROFIT_TARGET_HIT = "PROFIT_TARGET_HIT"  # Non-loss outcome
    UNKNOWN = "UNKNOWN"


class StrategyStatus(str, Enum):
    """Dynamic operational status of a trading strategy."""
    ACTIVE = "ACTIVE"
    BOOSTED = "BOOSTED"          # High-performing; allocation increased
    DAMPENED = "DAMPENED"        # Marginal performance; allocation reduced
    QUARANTINED = "QUARANTINED"  # Drawdown/loss breach; temporarily halted
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass
class StrategyPerformanceMetrics:
    """Institutional-grade per-strategy statistical scorecard."""
    strategy_type: str
    name: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate_pct: float = 0.0
    profit_factor: float = 1.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    mathematical_expectancy_usd: float = 0.0
    total_realized_pnl_usd: float = 0.0
    average_win_usd: float = 0.0
    average_loss_usd: float = 0.0
    average_hold_time_seconds: float = 0.0
    status: StrategyStatus = StrategyStatus.INSUFFICIENT_DATA
    dynamic_weight: float = 1.0
    loss_reasons: dict[str, int] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class ClosedTradeOutcome:
    """Standardized event packet emitted upon closing any paper or live trade."""
    trade_id: str
    token_address: str
    chain: str
    strategy_type: str
    entry_price: float = 0.0
    exit_price: float = 0.0
    realized_pnl_usd: float = 0.0
    realized_pnl_native: float = 0.0
    roi_pct: float = 0.0
    holding_time_seconds: float = 0.0
    exit_reason: str = "UNKNOWN"
    actual_slippage_bps: int = 0
    expected_slippage_bps: int = 150
    fill_latency_ms: float = 50.0
    council_verdicts: dict[str, Any] = field(default_factory=dict)
    council_confidence: float = 0.80
    feature_vector: list[float] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    holding_duration_s: float = 0.0
    is_win: Optional[bool] = None
    timestamp_ns: int = field(default_factory=time.time_ns)

    def __post_init__(self) -> None:
        if self.holding_duration_s == 0.0 and self.holding_time_seconds != 0.0:
            self.holding_duration_s = self.holding_time_seconds
        elif self.holding_time_seconds == 0.0 and self.holding_duration_s != 0.0:
            self.holding_time_seconds = self.holding_duration_s

        if self.is_win is None:
            self.is_win = (self.realized_pnl_usd > 0.0 or self.roi_pct > 0.0)

        if self.timestamp_ns == 0:
            self.timestamp_ns = int(self.timestamp * 1e9)

