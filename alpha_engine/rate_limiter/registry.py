"""
alpha_engine.rate_limiter.registry — Preconfigured Limiters & Composite Registry
================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.rate_limiter.bucket import AsyncTokenBucket


def build_alchemy_limiter() -> AsyncTokenBucket:
    """
    Token-bucket for Alchemy's Base RPC free tier.
    Strictly capped at 20 Compute Units / second (CU/s).
    """
    return AsyncTokenBucket(
        capacity=20.0,
        refill_rate=20.0,
        name="alchemy_base",
    )


def build_helius_limiter() -> AsyncTokenBucket:
    """
    Token-bucket for Helius's Solana RPC free tier.
    Strictly capped at 8 requests / second (req/s).
    """
    return AsyncTokenBucket(
        capacity=8.0,
        refill_rate=8.0,
        name="helius_solana",
    )


@dataclass
class RateLimiterRegistry:
    """
    Central registry holding rate limiters for all external RPC and network clients.
    Passed as a dependency to ingestion, security, and profiler subsystems.
    """

    alchemy: AsyncTokenBucket
    helius: AsyncTokenBucket

    @classmethod
    def default(cls) -> "RateLimiterRegistry":
        """
        Build the registry strictly configured for free-tier limits:
          - Alchemy: 20 CU/s
          - Helius : 8 req/s
        """
        return cls(
            alchemy=build_alchemy_limiter(),
            helius=build_helius_limiter(),
        )

    def for_chain(self, chain: Union[ChainIdentifier, str]) -> AsyncTokenBucket:
        """
        Retrieve the token bucket associated with a specific chain target.
        """
        chain_str = chain.value if isinstance(chain, ChainIdentifier) else str(chain).lower()
        if "base" in chain_str or "evm" in chain_str or "8453" in chain_str:
            return self.alchemy
        elif "solana" in chain_str or "svm" in chain_str:
            return self.helius
        raise ValueError(f"Unknown or unsupported chain for rate limiting: {chain}")

    async def acquire_for(
        self,
        chain_or_provider: Union[ChainIdentifier, str],
        cost: float = 1.0,
    ) -> None:
        """
        Acquire tokens for the appropriate limiter based on chain or provider name.
        """
        limiter = self.for_chain(chain_or_provider)
        await limiter.acquire(cost=cost)

    def penalize(
        self,
        chain_or_provider: Union[ChainIdentifier, str],
        penalty_seconds: float,
    ) -> None:
        """
        Apply a temporary backoff penalty when an RPC endpoint returns 429.
        """
        limiter = self.for_chain(chain_or_provider)
        limiter.penalize(penalty_seconds)

    def __repr__(self) -> str:
        return (
            f"RateLimiterRegistry("
            f"alchemy={self.alchemy!r}, "
            f"helius={self.helius!r})"
        )
