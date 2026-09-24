"""
rate_limiter.py — Async Token-Bucket Rate Limiter
==================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio

Guarantees outbound request volumes stay strictly under free-tier quotas:
  - Alchemy (Base EVM)  : 25 Compute Units / second
  - Helius  (Solana)    : 10 requests / second

Design: Token-Bucket with continuous leak.
  - Tokens refill at a constant rate (capacity / period_seconds).
  - Each acquire() call atomically deducts `cost` tokens.
  - If insufficient tokens are available the coroutine sleeps until
    enough tokens have accumulated, using adaptive sleep to avoid spinning.
  - Thread-safety: asyncio.Lock serialises all mutations within a single
    event-loop thread; no cross-thread sharing is assumed or supported.

No mock functions.  No pass statements.  Fully async.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class RateLimitError(RuntimeError):
    """
    Raised when the requested token cost exceeds the bucket's max capacity.
    This is a hard configuration error — not a transient wait condition.
    """


# ---------------------------------------------------------------------------
# AsyncTokenBucket
# ---------------------------------------------------------------------------


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
        response = await session.get(url)

    Implementation notes
    --------------------
    - _tokens       : Current token count (float, continuous).
    - _last_refill  : Monotonic time of last refill calculation.
    - _lock         : asyncio.Lock protecting _tokens and _last_refill.
    - Refill is computed lazily on each acquire() call — no background task.
    """

    #: Maximum single-call backoff (seconds) to prevent infinite sleep.
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

        # Mutable internal state — protected by _lock in async context
        self._tokens: float = capacity          # Start full
        self._last_refill: float = time.monotonic()
        # asyncio.Lock created lazily in the running event loop
        self._lock: asyncio.Lock | None = None

    # ------------------------------------------------------------------
    # Lock lifecycle: created on first use to avoid event-loop binding issues
    # ------------------------------------------------------------------

    def _get_lock(self) -> asyncio.Lock:
        """Return the asyncio.Lock, creating it on first access."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ------------------------------------------------------------------
    # Internal helpers (must be called while holding the lock)
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """
        Apply tokens accrued since the last refill call.
        Caller must hold `_lock`.
        """
        now = time.monotonic()
        elapsed = now - self._last_refill
        accrued = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + accrued)
        self._last_refill = now

    def _time_until_available(self, cost: float) -> float:
        """
        Seconds until `cost` tokens will be available without sleeping.
        Returns 0.0 if tokens are already sufficient.
        Caller must hold `_lock`.
        """
        deficit = cost - self._tokens
        if deficit <= 0.0:
            return 0.0
        return deficit / self.refill_rate

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def acquire(self, cost: float = 1.0) -> None:
        """
        Block until `cost` tokens are available, then atomically deduct them.

        Parameters
        ----------
        cost : Number of tokens to consume. Must satisfy 0 < cost <= capacity.

        Raises
        ------
        RateLimitError : If cost > capacity (request can never be satisfied).
        ValueError     : If cost <= 0.
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

            # Release lock before sleeping so other coroutines can run.
            # Small epsilon avoids re-entering loop 1 ns short due to FP.
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
        Non-blocking attempt to consume `cost` tokens synchronously.

        Returns True and deducts tokens if available; False otherwise.
        NOT protected by the async lock — use only in single-threaded
        synchronous contexts (e.g., setup / unit tests).
        """
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
        """
        Current token level (approximate — no refill, no lock).
        Use for monitoring / logging only, never for control flow.
        """
        return self._tokens

    def __repr__(self) -> str:
        return (
            f"AsyncTokenBucket(name={self.name!r}, "
            f"capacity={self.capacity}, "
            f"refill_rate={self.refill_rate}/s, "
            f"tokens={self._tokens:.2f})"
        )


# ---------------------------------------------------------------------------
# Pre-configured factory functions for each RPC provider
# ---------------------------------------------------------------------------


def build_alchemy_limiter() -> AsyncTokenBucket:
    """
    Token-bucket for Alchemy's Base RPC free tier.

    Quota  : 25 Compute Units / second (CU/s)
    Burst  : 25 CU (no headroom above steady-state)
    Policy : eth_subscribe, eth_call, eth_getLogs each cost 1–2 CU.
             Pass the correct `cost` argument per Alchemy's CU pricing table.
    """
    return AsyncTokenBucket(
        capacity=25.0,
        refill_rate=25.0,
        name="alchemy_base",
    )


def build_helius_limiter() -> AsyncTokenBucket:
    """
    Token-bucket for Helius's Solana RPC free tier.

    Quota  : 10 requests / second
    Burst  : 10 requests (single-second burst)
    Policy : Each JSON-RPC method call costs 1 request unit.
             WebSocket subscription messages are NOT counted —
             only the initial logsSubscribe setup and REST calls.
    """
    return AsyncTokenBucket(
        capacity=10.0,
        refill_rate=10.0,
        name="helius_solana",
    )


# ---------------------------------------------------------------------------
# Composite registry for engine-wide dependency injection
# ---------------------------------------------------------------------------


@dataclass
class RateLimiterRegistry:
    """
    Central registry holding one limiter per RPC provider.
    Passed as a constructor dependency to all ingestion subsystems.

    Usage
    -----
        registry = RateLimiterRegistry.default()
        await registry.alchemy.acquire(cost=1)   # before eth_call
        await registry.helius.acquire(cost=1)    # before getAccountInfo
    """

    alchemy: AsyncTokenBucket
    helius: AsyncTokenBucket

    @classmethod
    def default(cls) -> "RateLimiterRegistry":
        """
        Build the registry with default free-tier limiters.
        Call once at application startup; share across all modules.
        """
        return cls(
            alchemy=build_alchemy_limiter(),
            helius=build_helius_limiter(),
        )

    def __repr__(self) -> str:
        return (
            f"RateLimiterRegistry("
            f"alchemy={self.alchemy!r}, "
            f"helius={self.helius!r})"
        )
