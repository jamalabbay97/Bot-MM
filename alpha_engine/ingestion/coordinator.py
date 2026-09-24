"""
alpha_engine.ingestion.coordinator — Ingestion Coordinator & Resilient Stream
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Sequence

from alpha_engine.ingestion.decoders import SvmPoolMeta
from alpha_engine.ingestion.evm import (
    EVMIngester,
    _RECONNECT_INITIAL_DELAY_S,
    _RECONNECT_JITTER_FRACTION,
    _RECONNECT_MAX_DELAY_S,
    _RECONNECT_MULTIPLIER,
)
from alpha_engine.ingestion.svm import SVMIngester
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    ShutdownSentinel,
    SwapEvent,
)
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """
    Stateful exponential backoff with ±50% uniform jitter.
    """

    def __init__(
        self,
        initial: float = _RECONNECT_INITIAL_DELAY_S,
        multiplier: float = _RECONNECT_MULTIPLIER,
        max_delay: float = _RECONNECT_MAX_DELAY_S,
        jitter: float = _RECONNECT_JITTER_FRACTION,
    ) -> None:
        self._initial = initial
        self._multiplier = multiplier
        self._max_delay = max_delay
        self._jitter = jitter
        self._attempt = 0

    def next_delay(self) -> float:
        base = min(self._initial * (self._multiplier ** self._attempt), self._max_delay)
        jitter_delta = base * self._jitter * (2 * random.random() - 1)
        delay = max(0.0, base + jitter_delta)
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0


class IngestionCoordinator:
    """
    Manages EVM and SVM ingesters under a single asyncio task group,
    exposing a unified async-iterable event stream.
    """

    def __init__(
        self,
        evm_ws_url: str,
        svm_ws_url: str,
        pool_watchlist: Sequence[tuple[str, str, str, int, int, str]],
        pool_registry: dict[str, SvmPoolMeta],
        limiter: RateLimiterRegistry,
        queue_maxsize: int = 256,
    ) -> None:
        self._queue: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=queue_maxsize)

        self._evm = EVMIngester(
            ws_url=evm_ws_url,
            pool_watchlist=pool_watchlist,
            event_queue=self._queue,
            limiter=limiter,
        )
        self._svm = SVMIngester(
            ws_url=svm_ws_url,
            pool_registry=pool_registry,
            event_queue=self._queue,
            limiter=limiter,
        )
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def event_queue(
        self,
    ) -> asyncio.Queue[SwapEvent | PoolStateUpdateEvent | ShutdownSentinel]:
        return self._queue

    async def __aenter__(self) -> "IngestionCoordinator":
        self._tasks = [
            asyncio.create_task(self._evm.run(), name="evm_ingester"),
            asyncio.create_task(self._svm.run(), name="svm_ingester"),
        ]
        logger.info("IngestionCoordinator started (EVM + SVM ingesters running).")
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._evm.stop()
        await self._svm.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._queue.put(ShutdownSentinel())
        logger.info("IngestionCoordinator stopped.")

    def __aiter__(self) -> "IngestionCoordinator":
        return self

    async def __anext__(self) -> SwapEvent | PoolStateUpdateEvent:
        item = await self._queue.get()
        if isinstance(item, ShutdownSentinel):
            raise StopAsyncIteration
        return item
