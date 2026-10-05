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
import time
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
from alpha_engine.ingestion.x_stream import XStreamIngester
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SwapEvent,
)
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import is_blacklisted_token

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


class TokenTTLCache:
    """
    In-memory sliding-window TTL cache for token deduplication.
    Thread-safe / async-lock protected. Drops duplicate token signals processed
    within the configured TTL sliding window (default 60 seconds).
    """

    def __init__(self, ttl_seconds: float = 60.0, maxsize: int = 10_000) -> None:
        self._ttl = ttl_seconds
        self._maxsize = maxsize
        self._cache: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def is_duplicate_or_add(self, token_address: str) -> bool:
        """
        Check if token_address was registered within the sliding window.
        Returns True if duplicate (and drops/ignores), otherwise records timestamp and returns False.
        """
        if not token_address:
            return False
        now = time.monotonic()
        async with self._lock:
            # Housekeeping: prune expired entries when approaching capacity
            if len(self._cache) > self._maxsize:
                expired = [k for k, exp in self._cache.items() if now >= exp]
                for k in expired:
                    del self._cache[k]
                if len(self._cache) > self._maxsize:
                    sorted_items = sorted(self._cache.items(), key=lambda kv: kv[1])
                    for k, _ in sorted_items[: len(self._cache) // 5]:
                        self._cache.pop(k, None)

            exp = self._cache.get(token_address)
            if exp is not None and now < exp:
                return True
            self._cache[token_address] = now + self._ttl
            return False

    def is_duplicate_sync(self, token_address: str) -> bool:
        """Synchronous check for fast-path non-async call sites."""
        if not token_address:
            return False
        now = time.monotonic()
        exp = self._cache.get(token_address)
        if exp is not None and now < exp:
            return True
        self._cache[token_address] = now + self._ttl
        return False

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


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
        signal_queue: Optional[asyncio.Queue[Any]] = None,
        telegram_api_id: Optional[int] = None,
        telegram_api_hash: Optional[str] = None,
        telegram_session_name: str = "bot_mm_session",
        telegram_session_string: Optional[str] = None,
        telegram_bot_token: Optional[str] = None,
        telegram_channels: Optional[Sequence[str | int]] = None,
        telegram_admin_ids: Optional[Sequence[int]] = None,
        db_path: str = "paper_trading.db",
        status_provider: Optional[Any] = None,
        telegram_ingester: Optional[TelegramIngester] = None,
        x_stream_ingester: Optional[XStreamIngester] = None,
        x_bearer_token: Optional[str] = None,
        enable_x_stream: bool = False,
        gatekeeper: Optional[Any] = None,
        ai_supervisor: Optional[Any] = None,
        svm_failover_urls: Optional[Sequence[str]] = None,
    ) -> None:
        self._queue: asyncio.Queue[
            SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel
        ] = asyncio.Queue(maxsize=queue_maxsize)
        self._signal_queue: asyncio.Queue[Any] = (
            signal_queue if signal_queue is not None else asyncio.Queue(maxsize=queue_maxsize)
        )
        self._gatekeeper = gatekeeper
        self._dedup_cache = TokenTTLCache(ttl_seconds=60.0)

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
            failover_urls=list(svm_failover_urls) if svm_failover_urls else None,
        )

        if telegram_ingester is not None:
            self._telegram = telegram_ingester
            if signal_queue is not None and getattr(self._telegram, "_signal_queue", None) is None:
                self._telegram._signal_queue = self._signal_queue
        else:
            self._telegram = TelegramIngester(
                event_queue=self._queue,
                signal_queue=self._signal_queue,
                api_id=telegram_api_id,
                api_hash=telegram_api_hash,
                session_name=telegram_session_name,
                session_string=telegram_session_string,
                bot_token=telegram_bot_token,
                target_channels=telegram_channels,
                admin_ids=telegram_admin_ids,
                db_path=db_path,
                status_provider=status_provider,
                limiter=limiter,
                ai_supervisor=ai_supervisor,
            )

        if x_stream_ingester is not None:
            self._x_stream: Optional[XStreamIngester] = x_stream_ingester
        elif enable_x_stream:
            self._x_stream = XStreamIngester(
                event_queue=self._queue,
                bearer_token=x_bearer_token,
                limiter=limiter,
            )
        else:
            self._x_stream = None

        self._tasks: list[asyncio.Task[None]] = []
        self._screening_semaphore = asyncio.Semaphore(10)
        self._background_tasks: set[asyncio.Task[Any]] = set()

    @property
    def screening_semaphore(self) -> asyncio.Semaphore:
        return self._screening_semaphore

    @property
    def background_tasks(self) -> set[asyncio.Task[Any]]:
        return self._background_tasks

    @property
    def event_queue(
        self,
    ) -> asyncio.Queue[SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel]:
        return self._queue

    @property
    def signal_queue(self) -> asyncio.Queue[Any]:
        return self._signal_queue

    @property
    def dedup_cache(self) -> TokenTTLCache:
        """In-memory sliding-window TTL cache for signal deduplication."""
        return self._dedup_cache

    async def is_duplicate_token(self, token_address: str) -> bool:
        """Check and record token address in coordinator deduplication cache."""
        return await self._dedup_cache.is_duplicate_or_add(token_address)

    @property
    def evm_ingester(self) -> EVMIngester:
        return self._evm

    @property
    def svm_ingester(self) -> SVMIngester:
        return self._svm

    @property
    def telegram_ingester(self) -> TelegramIngester:
        return self._telegram

    @property
    def x_stream_ingester(self) -> Optional[XStreamIngester]:
        return self._x_stream

    async def start(self) -> None:
        """
        Spawns all ingestion loops (EVM, SVM, Telegram, X-Stream) concurrently.
        """
        if self._tasks:
            return

        self._tasks = [
            asyncio.create_task(self._evm.run(), name="evm_ingester"),
            asyncio.create_task(self._svm.run(), name="svm_ingester"),
            asyncio.create_task(self._telegram.run(), name="telegram_ingester"),
        ]
        if self._x_stream is not None:
            self._tasks.append(
                asyncio.create_task(self._x_stream.run(), name="x_stream_ingester")
            )
        logger.info("IngestionCoordinator started (EVM + SVM + Telegram + X-Stream running).")

    async def stop(self) -> None:
        """
        Gracefully stop all ingesters and tear down tasks.
        """
        await self._evm.stop()
        await self._svm.stop()
        await self._telegram.stop()
        if self._x_stream is not None:
            await self._x_stream.stop()

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

        # Cancel and drain background screening tasks
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        await self._queue.put(ShutdownSentinel())
        logger.info("IngestionCoordinator stopped.")

    async def _process_ingestion_queue(self) -> None:
        """
        Processes incoming ingestion queue events with bounded parallel asynchronous
        security screening (Semaphore(10)), fast-path blacklisting, and TTL deduplication.
        """
        while True:
            item = await self._queue.get()

            if isinstance(item, ShutdownSentinel):
                logger.info("IngestionCoordinator: ShutdownSentinel received, draining background screening tasks...")
                if self._signal_queue is not None:
                    await self._signal_queue.put(item)
                if self._background_tasks:
                    await asyncio.gather(*self._background_tasks, return_exceptions=True)
                break

            # Fast-path blacklist and deduplication filters for RawSignalEvent
            if isinstance(item, RawSignalEvent):
                # 1. Fast-path blacklist filter
                if is_blacklisted_token(item.token_address, item.chain):
                    logger.debug(
                        "IngestionCoordinator._process_ingestion_queue: Dropping blacklisted %s (%s)",
                        item.token_address[:10],
                        item.chain.value,
                    )
                    continue

                # 2. Gatekeeper negative rejection cache check
                if self._gatekeeper is not None and getattr(self._gatekeeper, "is_rejected", None):
                    if self._gatekeeper.is_rejected(item.token_address, item.chain) is True:
                        logger.debug(
                            "IngestionCoordinator._process_ingestion_queue: Dropping cached rejected token %s",
                            item.token_address[:10],
                        )
                        continue

                # 3. In-memory sliding-window TTL cache deduplication (60s window)
                if await self._dedup_cache.is_duplicate_or_add(item.token_address):
                    logger.debug(
                        "IngestionCoordinator._process_ingestion_queue: Dropping duplicate token %s within TTL",
                        item.token_address[:10],
                    )
                    continue

                # Spawn parallel screening worker via bounded concurrency gate
                task = asyncio.create_task(
                    self._screen_candidate_worker(item),
                    name=f"screen_candidate_{item.token_address[:8]}",
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                continue

            # Forward other events directly
            if self._signal_queue is not None:
                await self._signal_queue.put(item)

    async def _screen_candidate_worker(self, raw_signal: RawSignalEvent) -> None:
        """
        Worker routine executed under bounded worker concurrency gate (Semaphore(10))
        for asynchronous token screening via GoPlus, RugCheck, and Preflight.
        """
        async with self._screening_semaphore:
            try:
                gk = self._gatekeeper
                if gk is None:
                    if self._signal_queue is not None:
                        await self._signal_queue.put(raw_signal)
                    return

                # Screen candidate through external APIs (GoPlus, RugCheck, preflight)
                if hasattr(gk, "evaluate_token"):
                    report = await gk.evaluate_token(
                        token_address=raw_signal.token_address,
                        chain=raw_signal.chain,
                        pool_address=raw_signal.pool_address or "",
                    )
                elif hasattr(gk, "screen_token"):
                    report = await gk.screen_token(
                        token_address=raw_signal.token_address,
                        chain=raw_signal.chain,
                        pool_address=raw_signal.pool_address or "",
                    )
                else:
                    report = None

                if report is not None:
                    if not getattr(report, "passes_hard_gates", True):
                        logger.info(
                            "Candidate token %s rejected during parallel security screening.",
                            raw_signal.token_address[:10],
                        )
                        return

                if self._signal_queue is not None:
                    await self._signal_queue.put(raw_signal)
            except Exception as exc:
                logger.error(
                    "Parallel security screening error for token %s: %s",
                    raw_signal.token_address[:10],
                    exc,
                    exc_info=True,
                )

    async def __aenter__(self) -> "IngestionCoordinator":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    def __aiter__(self) -> "IngestionCoordinator":
        return self

    async def __anext__(self) -> SwapEvent | PoolStateUpdateEvent | RawSignalEvent:
        while True:
            item = await self._queue.get()
            if isinstance(item, ShutdownSentinel):
                raise StopAsyncIteration

            if isinstance(item, RawSignalEvent):
                # 1. Fast-path blacklist filter: drop WSOL, native base/quote and system tokens
                if is_blacklisted_token(item.token_address, item.chain):
                    logger.debug(
                        "IngestionCoordinator: Dropping RawSignalEvent for blacklisted/quote token %s (%s)",
                        item.token_address,
                        item.chain.value,
                    )
                    continue

                # Fast-path check: drop if already in negative rejection cache
                if self._gatekeeper is not None and getattr(self._gatekeeper, "is_rejected", None):
                    if self._gatekeeper.is_rejected(item.token_address, item.chain) is True:
                        logger.debug(
                            "IngestionCoordinator: Dropping RawSignalEvent for cached rejected token %s",
                            item.token_address[:10],
                        )
                        continue

                # 2. In-memory sliding-window TTL cache deduplication (60s window)
                if await self._dedup_cache.is_duplicate_or_add(item.token_address):
                    logger.debug(
                        "IngestionCoordinator: Dropping duplicate RawSignalEvent for token %s within 60s TTL window",
                        item.token_address,
                    )
                    continue

            elif isinstance(item, SwapEvent):
                # Fast-path check: drop incoming swaps for tokens already rejected in negative cache
                target_token = item.token_in if is_blacklisted_token(item.token_out, item.chain) else item.token_out
                if self._gatekeeper is not None and getattr(self._gatekeeper, "is_rejected", None):
                    if self._gatekeeper.is_rejected(target_token, item.chain) is True:
                        logger.debug(
                            "IngestionCoordinator: Dropping SwapEvent for cached rejected token %s with zero latency",
                            target_token[:10],
                        )
                        continue

            return item


__all__ = [
    "IngestionCoordinator",
    "TokenTTLCache",
    "ExponentialBackoff",
]
