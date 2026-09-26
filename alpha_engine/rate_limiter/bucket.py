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
from contextlib import asynccontextmanager
from typing import AsyncIterator

logger = logging.getLogger(__name__)


class RateLimitError(RuntimeError):
    """
    Raised when the requested token cost exceeds the bucket's max capacity.
    This is a hard configuration error — not a transient wait condition.
    """


class AsyncTokenBucket:
    """
    Asynchronous token-bucket rate limiter with continuous-leak refill,
    transient penalty backoff, and async context management.

    Strictly complies with free-tier developer RPC quotas:
      - Alchemy (Base EVM) : 20 Compute Units / second
      - Helius (Solana SVM): 8 requests / second

    Parameters
    ----------
    capacity        : Maximum tokens the bucket can hold (burst ceiling).
    refill_rate     : Tokens accrued per second (continuous fractional model).
    name            : Human-readable identifier for logging and metrics.

    Usage
    -----
        limiter = AsyncTokenBucket(capacity=20.0, refill_rate=20.0, name="alchemy")
        await limiter.acquire(cost=1.0)

        # Or using the context manager:
        async with limiter:
            ...
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

        self.capacity: float = float(capacity)
        self.refill_rate: float = float(refill_rate)
        self.name: str = name

        self._tokens: float = float(capacity)
        self._last_refill: float = time.monotonic()
        self._penalty_until: float = 0.0
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            accrued = elapsed * self.refill_rate
            self._tokens = min(self.capacity, self._tokens + accrued)
            self._last_refill = now

    def _time_until_available(self, cost: float) -> float:
        now = time.monotonic()
        penalty_wait = max(0.0, self._penalty_until - now)
        deficit = cost - self._tokens
        if deficit <= 0.0:
            return penalty_wait
        token_wait = deficit / self.refill_rate
        return max(penalty_wait, token_wait)

    async def acquire(self, cost: float = 1.0) -> None:
        """
        Asynchronously consume tokens from the bucket.
        Suspends the coroutine until sufficient tokens have accrued.
        """
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
        """
        Non-blocking attempt to consume tokens.
        Returns True if tokens were deducted, False otherwise.
        """
        if cost <= 0 or cost > self.capacity:
            return False

        now = time.monotonic()
        if now < self._penalty_until:
            return False

        self._refill()
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False

    def penalize(self, penalty_seconds: float) -> None:
        """
        Apply a dynamic penalty delay when an upstream RPC or API returns HTTP 429
        or FloodWaitError. All subsequent acquisitions wait until the penalty expires.
        """
        if penalty_seconds <= 0:
            return
        now = time.monotonic()
        self._penalty_until = max(self._penalty_until, now + penalty_seconds)
        self._tokens = 0.0  # Drain tokens to force strict backoff
        logger.warning(
            "[%s] Rate-limit penalty applied: pausing for %.2f s",
            self.name,
            penalty_seconds,
        )

    @asynccontextmanager
    async def throttle(self, cost: float = 1.0) -> AsyncIterator[None]:
        """
        Context manager to acquire tokens before executing a protected block.
        """
        await self.acquire(cost=cost)
        try:
            yield
        finally:
            pass

    async def __aenter__(self) -> "AsyncTokenBucket":
        await self.acquire(cost=1.0)
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens

    @property
    def is_penalized(self) -> bool:
        return time.monotonic() < self._penalty_until

    def __repr__(self) -> str:
        return (
            f"AsyncTokenBucket(name={self.name!r}, "
            f"capacity={self.capacity:.1f}, "
            f"refill_rate={self.refill_rate:.1f}/s, "
            f"tokens={self.available_tokens:.2f})"
        )
