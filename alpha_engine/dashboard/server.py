"""
alpha_engine.dashboard.server — Asynchronous Uvicorn Dashboard Server Manager
=============================================================================
Manages background lifecycle of the FastAPI / WebSocket Observability Hub.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import uvicorn

from alpha_engine.dashboard.app import create_app
from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator

logger = logging.getLogger(__name__)


class DashboardServer:
    """Runs FastAPI/Uvicorn as an asynchronous background service."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        aggregator: Optional[DashboardTelemetryAggregator] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.aggregator = aggregator or DashboardTelemetryAggregator()
        self.app = create_app(self.aggregator)
        self._server: Optional[uvicorn.Server] = None
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Start Uvicorn in the current asyncio loop without blocking."""
        if self._task and not self._task.done():
            return

        import socket
        for offset in range(10):
            candidate_port = self.port + offset
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((self.host, candidate_port))
                s.close()
                self.port = candidate_port
                break
            except OSError:
                continue

        config = uvicorn.Config(
            app=self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve(), name="dashboard_uvicorn_server")
        url = f"http://localhost:{self.port}/"
        logger.info("[Dashboard] Observability Hub live at %s", url)
        print(f"\n{'=' * 78}")
        print(f"📊 REAL-TIME INSTITUTIONAL DASHBOARD LIVE: {url}")
        print(f"{'=' * 78}\n", flush=True)

    async def stop(self) -> None:
        """Gracefully stop Uvicorn server."""
        if self._server:
            self._server.should_exit = True
        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        logger.info("[Dashboard] Observability Hub stopped.")
