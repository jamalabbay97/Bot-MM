"""
alpha_engine.dashboard — Real-Time Institutional Observability Dashboard
========================================================================
FastAPI + WebSocket Live Monitoring & Strategy Performance Dashboard
"""

from __future__ import annotations

from alpha_engine.dashboard.app import create_app, get_telemetry_aggregator, set_telemetry_aggregator
from alpha_engine.dashboard.server import DashboardServer
from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator

__all__ = [
    "create_app",
    "DashboardServer",
    "DashboardTelemetryAggregator",
    "get_telemetry_aggregator",
    "set_telemetry_aggregator",
]
