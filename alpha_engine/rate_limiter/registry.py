"""
alpha_engine.rate_limiter.registry — Preconfigured Limiters & Composite Registry
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio
"""

from __future__ import annotations

from dataclasses import dataclass

from alpha_engine.rate_limiter.bucket import AsyncTokenBucket


def build_alchemy_limiter() -> AsyncTokenBucket:
    """
    Token-bucket for Alchemy's Base RPC free tier.
    Quota  : 25 Compute Units / second (CU/s)
    Burst  : 25 CU (no headroom above steady-state)
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
    """
    return AsyncTokenBucket(
        capacity=10.0,
        refill_rate=10.0,
        name="helius_solana",
    )


@dataclass
class RateLimiterRegistry:
    """
    Central registry holding one limiter per RPC provider.
    Passed as a constructor dependency to all ingestion subsystems.
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
