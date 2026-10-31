"""
alpha_engine.dashboard.app — FastAPI & WebSocket High-Precision Observability Hub
================================================================================
Institutional Multi-Agent Paper Trading Engine Dashboard
Python 3.11+ | FastAPI | WebSockets | Real-Time Telemetry Streaming & Control
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator

logger = logging.getLogger(__name__)

# Global singleton aggregator
_GLOBAL_AGGREGATOR: Optional[DashboardTelemetryAggregator] = None


def get_telemetry_aggregator() -> DashboardTelemetryAggregator:
    global _GLOBAL_AGGREGATOR
    if _GLOBAL_AGGREGATOR is None:
        _GLOBAL_AGGREGATOR = DashboardTelemetryAggregator()
    return _GLOBAL_AGGREGATOR


def set_telemetry_aggregator(aggregator: DashboardTelemetryAggregator) -> None:
    global _GLOBAL_AGGREGATOR
    _GLOBAL_AGGREGATOR = aggregator


def create_app(aggregator: Optional[DashboardTelemetryAggregator] = None) -> FastAPI:
    """Create and configure the FastAPI dashboard application."""
    if aggregator is not None:
        set_telemetry_aggregator(aggregator)

    app = FastAPI(
        title="AlphaBot Institutional Observability & Control Center",
        description="Comprehensive real-time multi-agent quantitative trading engine monitoring & telemetry system",
        version="2.5.0",
    )

    # Enable CORS for local and web UI access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Static assets directory
    static_dir_env = os.environ.get("DASHBOARD_STATIC_DIR")
    static_dir = Path(static_dir_env) if static_dir_env else (Path(__file__).parent / "static")
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def serve_dashboard() -> Any:
        index_file = static_dir / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return HTMLResponse("<h2>AlphaBot Observability Dashboard Initializing...</h2>")

    @app.get("/api/status")
    async def get_status() -> dict[str, Any]:
        agg = get_telemetry_aggregator()
        return await agg.get_full_telemetry_snapshot()

    @app.get("/api/pipeline")
    async def get_pipeline() -> dict[str, Any]:
        agg = get_telemetry_aggregator()
        return await agg.get_pipeline_flow()

    @app.get("/api/strategies")
    async def get_strategies() -> list[dict[str, Any]]:
        agg = get_telemetry_aggregator()
        return await agg.get_strategy_performance()

    @app.get("/api/portfolio")
    async def get_portfolio() -> dict[str, Any]:
        agg = get_telemetry_aggregator()
        return await agg.get_portfolio_snapshot()

    @app.get("/api/trades")
    async def get_trades(limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
        """Trading Operations: open lots, closed trades with PnL, win/loss stats, execution logs."""
        agg = get_telemetry_aggregator()
        return await agg.get_trading_operations(limit=limit)

    @app.get("/api/agents")
    async def get_agents(limit: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
        """Agent Activity: real-time council discussions, agent profiles, consensus verdicts."""
        agg = get_telemetry_aggregator()
        return await agg.get_agent_activity(limit=limit)

    @app.get("/api/debates")
    async def get_debates(limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
        agg = get_telemetry_aggregator()
        data = await agg.get_agent_activity(limit=limit)
        return data.get("debates", [])

    @app.get("/api/decisions")
    async def get_decisions(
        filter_type: Optional[str] = Query(None, alias="type"),
        limit: int = Query(50, ge=1, le=200),
    ) -> list[dict[str, Any]]:
        """Decision-Making Process: evaluated tokens, approvals, vetoes, and reasons."""
        agg = get_telemetry_aggregator()
        return await agg.get_decision_records(limit=limit, filter_type=filter_type)

    @app.get("/api/risk")
    async def get_risk() -> dict[str, Any]:
        """Risk Management: circuit breakers, drawdown, loss streaks, and sizing rules."""
        agg = get_telemetry_aggregator()
        return await agg.get_risk_overview()

    @app.get("/api/activity")
    async def get_activity(
        category: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        search: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        """System Activity & Logs: filterable by category, severity level, or search keyword."""
        agg = get_telemetry_aggregator()
        return await agg.get_activity_logs(category=category, level=level, search=search, limit=limit)

    @app.get("/api/token/{token_address}")
    async def get_token_deep_dive(token_address: str) -> dict[str, Any]:
        """Deep Investigation: full end-to-end trace journey for any token."""
        agg = get_telemetry_aggregator()
        return await agg.get_token_deep_dive(token_address)

    @app.get("/api/doctor")
    async def get_doctor_diagnostics() -> dict[str, Any]:
        """Live RPC and service latency ping."""
        agg = get_telemetry_aggregator()
        return await agg.run_doctor_diagnostics()

    @app.get("/api/health")
    async def get_health() -> dict[str, Any]:
        agg = get_telemetry_aggregator()
        return await agg.get_connection_health()

    @app.get("/api/alerts")
    async def get_alerts() -> list[dict[str, Any]]:
        agg = get_telemetry_aggregator()
        return await agg.detect_inconsistencies_and_alerts()

    @app.post("/api/action/reset_ledger")
    async def action_reset_ledger() -> JSONResponse:
        """Action: Reset paper trading ledger history and portfolio equity."""
        agg = get_telemetry_aggregator()
        if agg.runner and hasattr(agg.runner, "reset_paper_trading_ledger"):
            await agg.runner.reset_paper_trading_ledger()
            agg.record_activity(
                category="TRADING",
                title="Paper Ledger Reset",
                details="Paper trading ledger and position book reset to pristine initial state ($8,300 USD).",
                level="WARNING",
                component="DashboardControl",
            )
            return JSONResponse(status_code=200, content={"status": "SUCCESS", "message": "Paper ledger successfully reset."})
        return JSONResponse(status_code=503, content={"status": "ERROR", "message": "Engine runner not bound."})

    @app.post("/api/action/close_all")
    async def action_close_all() -> JSONResponse:
        """Action: Force-close all open positions at market prices."""
        agg = get_telemetry_aggregator()
        if agg.runner and hasattr(agg.runner, "close_all_active_positions"):
            closed = await agg.runner.close_all_active_positions()
            agg.record_activity(
                category="TRADING",
                title="Emergency Close All",
                details=f"Force-closed all {closed} active lots at market prices.",
                level="WARNING",
                component="DashboardControl",
            )
            return JSONResponse(status_code=200, content={"status": "SUCCESS", "closed_count": closed})
        return JSONResponse(status_code=503, content={"status": "ERROR", "message": "Engine runner not bound."})

    @app.websocket("/ws/telemetry")
    async def websocket_telemetry_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        agg = get_telemetry_aggregator()
        logger.info("[Dashboard] WebSocket client connected to telemetry stream.")
        try:
            while True:
                snapshot = await agg.get_full_telemetry_snapshot()
                await websocket.send_text(json.dumps(snapshot, default=str))
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            logger.info("[Dashboard] WebSocket client disconnected.")
        except Exception as exc:
            logger.debug("[Dashboard] WebSocket streaming error: %s", exc)

    return app
