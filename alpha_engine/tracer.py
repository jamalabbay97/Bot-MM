"""
alpha_engine.tracer — Token Lifecycle Observability & Async Event Tracer
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio + aiosqlite
"""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

from alpha_engine.models.observability import LifecycleEvent, TraceStage, TraceStatus
from alpha_engine.execution.ledger import SQLiteLedger

logger = logging.getLogger(__name__)


class LifecycleTracer:
    """Centralized observability tracer for token lifecycles."""

    def __init__(self, ledger: Optional[SQLiteLedger] = None):
        self.ledger = ledger
        self._queue: asyncio.Queue[LifecycleEvent] = asyncio.Queue(maxsize=5000)
        self._worker_task: Optional[asyncio.Task] = None

    def start_worker(self) -> None:
        """Start the background task that flushes traces to SQLite."""
        if (self._worker_task is None or self._worker_task.done()) and self.ledger is not None:
            try:
                loop = asyncio.get_running_loop()
                self._worker_task = loop.create_task(self._flush_worker(), name="trace_flusher")
            except RuntimeError:
                pass

    async def _flush_worker(self) -> None:
        while True:
            try:
                event = await self._queue.get()
                events = [event]
                while not self._queue.empty() and len(events) < 50:
                    try:
                        events.append(self._queue.get_nowait())
                    except Exception:
                        break
                if self.ledger:
                    try:
                        if hasattr(self.ledger, "record_trace_events_batch"):
                            await self.ledger.record_trace_events_batch(events)
                        else:
                            for ev in events:
                                await self.ledger.record_trace_event(ev)
                    except Exception as err:
                        logger.debug("Tracer flush batch error: %s", err)
                for _ in events:
                    self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Tracer flush error: %s", e)

    async def flush(self) -> None:
        """Drain currently queued traces directly into the ledger."""
        events: list[LifecycleEvent] = []
        while not self._queue.empty():
            try:
                events.append(self._queue.get_nowait())
                self._queue.task_done()
            except Exception:
                break
        if events and self.ledger:
            try:
                if hasattr(self.ledger, "record_trace_events_batch"):
                    await self.ledger.record_trace_events_batch(events)
                else:
                    for ev in events:
                        await self.ledger.record_trace_event(ev)
            except Exception as err:
                logger.debug("Tracer direct flush error: %s", err)

    async def stop_worker(self) -> None:
        """Gracefully drain remaining traces and cancel the background worker task."""
        await self.flush()
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        self._worker_task = None

    @asynccontextmanager
    async def span(
        self,
        token_address: str,
        chain: str,
        stage: TraceStage,
        component: str,
        function_name: str,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[LifecycleEvent, None]:
        """
        Async context manager to trace a block of code.
        Example:
            async with tracer.span(token, chain, TraceStage.SECURITY_SCREENING, "Gatekeeper", "screen_token") as event:
                result = do_screening()
                event.output_data = result
                if not result.passed:
                    event.status = TraceStatus.REJECTED
                    event.reason = "Honeypot detected"
        """
        start_time = time.perf_counter()

        event = LifecycleEvent(
            token_address=token_address,
            chain=chain,
            stage=stage,
            component=component,
            function_name=function_name,
            input_data=input_data or {},
        )

        try:
            yield event
            if event.status == TraceStatus.PENDING:
                event.status = TraceStatus.PASSED
        except Exception as e:
            event.status = TraceStatus.ERROR
            event.reason = str(e)
            event.output_data["traceback"] = traceback.format_exc()
            raise
        finally:
            event.duration_ms = (time.perf_counter() - start_time) * 1000
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop trace if system is completely overwhelmed


# Global singleton instance for easy imports across isolated modules
tracer = LifecycleTracer()
