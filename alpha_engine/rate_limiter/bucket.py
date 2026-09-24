"""
alpha_engine.rate_limiter.bucket — Async Token-Bucket Rate Limiter
==================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio
"""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class RateLimitError(RuntimeError):
    """
    Raised when the requested token cost exceeds the bucket's max capacity.
    This is a hard configuration error — not a transient wait condition.
    """


class AsyncTokenBucket:
    """
    Asynchronous token-bucket rate limiter with continuous-leak refill.

    Parameters
    ----------
    capacity        : Maximum tokens the bucket can hold (also burst ceiling).
    refill_rate     : Tokens added per second (continuous model).
    name            : Human-readable label used in log messages.

    Usage
    -----
        limiter = AsyncTokenBucket(capacity=25.0, refill_rate=25.0, name="alchemy")
        await limiter.acquire(cost=1)
    """

    _MAX_WAIT_S: float = 60.0

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        name: str = "bucket",
    ) -> None:
        if capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {capacity}")
        if refill_rate <= 0:
            raise ValueError(f"refill_rate must be > 0, got {refill_rate}")

        self.capacity: float = capacity
        self.refill_rate: float = refill_rate
        self.name: str = name

        self._tokens: float = capacity
        self._last_refill: float = time.monotonic()
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        accrued = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + accrued)
        self._last_refill = now

    def _time_until_available(self, cost: float) -> float:
        deficit = cost - self._tokens
        if deficit <= 0.0:
            return 0.0
        return deficit / self.refill_rate

    async def acquire(self, cost: float = 1.0) -> None:
        if cost <= 0:
            raise ValueError(f"cost must be > 0, got {cost}")
        if cost > self.capacity:
            raise RateLimitError(
                f"[{self.name}] Requested cost ({cost}) exceeds bucket "
                f"capacity ({self.capacity}). Increase capacity or reduce cost."
            )

        lock = self._get_lock()

        while True:
            async with lock:
                self._refill()
                wait_s = self._time_until_available(cost)

                if wait_s <= 0.0:
                    self._tokens -= cost
                    logger.debug(
                        "[%s] Consumed %.2f tokens | remaining=%.2f",
                        self.name,
                        cost,
                        self._tokens,
                    )
                    return

            sleep_duration = min(wait_s + 1e-4, self._MAX_WAIT_S)
            logger.debug(
                "[%s] Throttled — sleeping %.4f s for %.2f tokens",
                self.name,
                sleep_duration,
                cost,
            )
            await asyncio.sleep(sleep_duration)

    def acquire_nowait(self, cost: float = 1.0) -> bool:
        if cost <= 0 or cost > self.capacity:
            return False
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False

    @property
    def available_tokens(self) -> float:
        return self._tokens

    def __repr__(self) -> str:
        return (
            f"AsyncTokenBucket(name={self.name!r}, "
            f"capacity={self.capacity}, "
            f"refill_rate={self.refill_rate}/s, "
            f"tokens={self._tokens:.2f})"
        )
