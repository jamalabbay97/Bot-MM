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
from typing import Any, Optional, Sequence

from alpha_engine.ingestion.decoders import SvmPoolMeta
from alpha_engine.ingestion.evm import (
    EVMIngester,
    _RECONNECT_INITIAL_DELAY_S,
    _RECONNECT_JITTER_FRACTION,
    _RECONNECT_MAX_DELAY_S,
    _RECONNECT_MULTIPLIER,
)
from alpha_engine.ingestion.svm import SVMIngester
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
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
    Manages EVM, SVM, and Telegram ingesters under a single lifecycle coordinator,
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
        telegram_api_id: Optional[int] = None,
        telegram_api_hash: Optional[str] = None,
        telegram_session_name: str = "bot_mm_session",
        telegram_bot_token: Optional[str] = None,
        telegram_channels: Optional[Sequence[str | int]] = None,
        telegram_ingester: Optional[TelegramIngester] = None,
    ) -> None:
        self._queue: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel
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

        if telegram_ingester is not None:
            self._telegram = telegram_ingester
        else:
            self._telegram = TelegramIngester(
                event_queue=self._queue,
                api_id=telegram_api_id,
                api_hash=telegram_api_hash,
                session_name=telegram_session_name,
                bot_token=telegram_bot_token,
                target_channels=telegram_channels,
                limiter=limiter,
            )

        self._tasks: list[asyncio.Task[None]] = []

    @property
    def event_queue(
        self,
    ) -> asyncio.Queue[SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel]:
        return self._queue

    @property
    def evm_ingester(self) -> EVMIngester:
        return self._evm

    @property
    def svm_ingester(self) -> SVMIngester:
        return self._svm

    @property
    def telegram_ingester(self) -> TelegramIngester:
        return self._telegram

    async def start(self) -> None:
        """
        Spawns all three ingestion loops (EVM, SVM, Telegram) concurrently.
        """
        if self._tasks:
            return

        self._tasks = [
            asyncio.create_task(self._evm.run(), name="evm_ingester"),
            asyncio.create_task(self._svm.run(), name="svm_ingester"),
            asyncio.create_task(self._telegram.run(), name="telegram_ingester"),
        ]
        logger.info("IngestionCoordinator started (EVM + SVM + Telegram ingesters running).")

    async def stop(self) -> None:
        """
        Gracefully stop all three ingesters and tear down tasks.
        """
        await self._evm.stop()
        await self._svm.stop()
        await self._telegram.stop()

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

        await self._queue.put(ShutdownSentinel())
        logger.info("IngestionCoordinator stopped.")

    async def __aenter__(self) -> "IngestionCoordinator":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    def __aiter__(self) -> "IngestionCoordinator":
        return self

    async def __anext__(self) -> SwapEvent | PoolStateUpdateEvent | RawSignalEvent:
        item = await self._queue.get()
        if isinstance(item, ShutdownSentinel):
            raise StopAsyncIteration
        return item
