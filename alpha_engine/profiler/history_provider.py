"""
alpha_engine.profiler.history_provider — Abstract Wallet History Provider Interface
===================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio + token bucket rate limiting
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Sequence

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.profiler import NormalizedSwap
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)


class ProviderUnavailableError(Exception):
    """Raised when external history provider is degraded, rate-limited, or unreachable."""
    pass


class ProviderQuotaExceededError(ProviderUnavailableError):
    """Raised when the free-tier daily request quota for the provider has been exhausted."""
    pass


@dataclass
class WalletHistoryResult:
    """Encapsulates history fetch outcome with graceful degradation status."""
    status: str  # "ok" or "unavailable"
    swaps: Sequence[NormalizedSwap]
    reason: Optional[str] = None

    @property
    def is_available(self) -> bool:
        return self.status == "ok"


class WalletHistoryProvider(ABC):
    """
    Abstract interface for fetching, normalizing, and caching historical wallet swaps.
    Integrates SQLite caching, free-tier quota accounting, and token-bucket rate limiting.
    """

    def __init__(
        self,
        provider_name: str = "generic_provider",
        history_db: Optional[WalletHistoryDatabase] = None,
        rate_limiters: Optional[RateLimiterRegistry] = None,
        cache_ttl_s: int = 3600,
        daily_request_quota: int = 1000,
        backfill_window_days: int = 30,
        max_retries: int = 3,
        base_retry_delay_s: float = 0.5,
    ) -> None:
        self.provider_name = provider_name
        self.history_db = history_db
        self.rate_limiters = rate_limiters
        self.cache_ttl_s = cache_ttl_s
        self.daily_request_quota = daily_request_quota
        self.backfill_window_days = backfill_window_days
        self.max_retries = max_retries
        self.base_retry_delay_s = base_retry_delay_s

    async def fetch_swaps(
        self,
        wallet: str,
        chain: ChainIdentifier,
        since: Optional[int] = None,
        until: Optional[int] = None,
        limit: int = 100,
    ) -> list[NormalizedSwap]:
        """
        Fetch normalized swap events for a wallet.
        Checks SQLite cache first. On miss, enforces quota and rate limit,
        fetches with retries/backoff, updates cache, and returns normalized swaps.
        Raises ProviderUnavailableError on network/quota failure (NEVER returns empty-as-success).
        """
        clean_wallet = wallet.strip()
        now = int(time.time())
        if until is None:
            until = now
        if since is None:
            since = now - (self.backfill_window_days * 86400)

        # 1. Check SQLite Cache
        if self.history_db is not None:
            sync_info = await self.history_db.get_sync_status(clean_wallet, chain)
            if sync_info is not None:
                is_fresh = (now - sync_info["last_synced_at"]) < self.cache_ttl_s
                covers_window = (
                    sync_info["earliest_synced_time"] <= since
                    and sync_info["latest_synced_time"] >= until
                )
                if is_fresh and covers_window:
                    cached = await self.history_db.get_cached_swaps(
                        clean_wallet, chain, since=since, until=until
                    )
                    if cached:
                        logger.debug("Cache hit for wallet %s on %s (%d swaps)", clean_wallet[:10], chain.value, len(cached))
                        return cached

        # 2. Check Daily Quota Accounting
        if self.history_db is not None:
            allowed = await self.history_db.check_and_increment_quota(
                self.provider_name, self.daily_request_quota, cost=1
            )
            if not allowed:
                msg = f"Free-tier daily quota exhausted for provider {self.provider_name} ({self.daily_request_quota} reqs)"
                logger.warning(msg)
                raise ProviderQuotaExceededError(msg)

        # 3. Rate Limit Token Bucket Acquisition
        if self.rate_limiters is not None:
            try:
                await self.rate_limiters.acquire_for(chain, cost=1.0)
            except Exception as exc:
                logger.warning("Rate limiter acquisition error on %s: %s", chain.value, exc)

        # 4. Fetch from Network with Retries and Exponential Backoff
        swaps = await self._fetch_with_retries(clean_wallet, chain, since, until, limit)

        # 5. Persist to SQLite Cache
        if self.history_db is not None:
            if swaps:
                await self.history_db.upsert_swaps(clean_wallet, chain, swaps)
            earliest = min(min((s.block_time for s in swaps), default=since), since)
            latest = max(max((s.block_time for s in swaps), default=until), until)
            await self.history_db.update_sync_status(clean_wallet, chain, earliest, latest)

        return swaps

    async def _fetch_with_retries(
        self,
        wallet: str,
        chain: ChainIdentifier,
        since: int,
        until: int,
        limit: int,
    ) -> list[NormalizedSwap]:
        """Execute network fetch with exponential backoff on transient errors."""
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                return await self._fetch_swaps_network(wallet, chain, since, until, limit)
            except ProviderQuotaExceededError:
                raise
            except Exception as exc:
                last_exc = exc
                is_rate_limited = "429" in str(exc) or "rate limit" in str(exc).lower()
                if is_rate_limited and self.rate_limiters is not None:
                    self.rate_limiters.penalize(chain, penalty_seconds=5.0)

                delay = self.base_retry_delay_s * (2 ** attempt)
                logger.warning(
                    "[%s] Attempt %d/%d failed for wallet %s: %s. Backoff %.2fs",
                    self.provider_name, attempt + 1, self.max_retries, wallet[:10], exc, delay
                )
                await asyncio.sleep(delay)

        # Fail closed: Do NOT return empty-as-success (Rule 4)
        raise ProviderUnavailableError(
            f"Provider {self.provider_name} unavailable after {self.max_retries} attempts: {last_exc}"
        )

    @abstractmethod
    async def _fetch_swaps_network(
        self,
        wallet: str,
        chain: ChainIdentifier,
        since: int,
        until: int,
        limit: int,
    ) -> list[NormalizedSwap]:
        """Concrete network-fetching logic implemented by chain-specific subclasses."""
        ...
