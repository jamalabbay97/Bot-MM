"""
alpha_engine.observability — Institutional Observability, Recorder, Replay & Validation
========================================================================================
Exports:
- MarketTickRecorder: Real-time tick recorder writing to SQLite in WAL mode
- ReplayEngine: Deterministic historical replay using production execution logic
- WalkForwardValidator: Rolling time-split walk-forward validation
- ModelVsRealityReporter: Fill execution bias analysis against observed market
- SystemDoctor: Startup diagnostics self-check suite
"""

from __future__ import annotations

from alpha_engine.observability.doctor import CheckStatus, DoctorResult, SystemDoctor
from alpha_engine.observability.model_vs_reality import (
    FillRealityComparison,
    ModelRealityReport,
    ModelVsRealityReporter,
)
from alpha_engine.observability.recorder import MarketTickRecorder
from alpha_engine.observability.replay import ReplayEngine, ReplayResult
from alpha_engine.observability.walk_forward import (
    WalkForwardResult,
    WalkForwardValidator,
    WalkForwardWindow,
    WindowPerformance,
)

__all__ = [
    "MarketTickRecorder",
    "ReplayEngine",
    "ReplayResult",
    "WalkForwardValidator",
    "WalkForwardWindow",
    "WalkForwardResult",
    "WindowPerformance",
    "ModelVsRealityReporter",
    "FillRealityComparison",
    "ModelRealityReport",
    "SystemDoctor",
    "DoctorResult",
    "CheckStatus",
]

