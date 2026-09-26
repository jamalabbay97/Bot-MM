"""
rate_limiter.py — Async Token-Bucket Rate Limiter (Compatibility Facade)
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio
Re-exports all rate limiter definitions from alpha_engine.rate_limiter.
"""

from alpha_engine.rate_limiter import (
    AsyncTokenBucket,
    RateLimiterRegistry,
    RateLimitError,
    build_alchemy_limiter,
    build_helius_limiter,
)

__all__ = [
    "RateLimitError",
    "AsyncTokenBucket",
    "build_alchemy_limiter",
    "build_helius_limiter",
    "RateLimiterRegistry",
]
