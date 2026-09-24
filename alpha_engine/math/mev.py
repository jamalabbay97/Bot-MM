"""
alpha_engine.math.mev — Stochastic Latency & Burst-Driven MEV Front-Run Model
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass
from decimal import Decimal

from alpha_engine.math.cpmm import cpmm_buy_quote
from alpha_engine.models.state import PoolState

logger = logging.getLogger(__name__)

# Fill-latency model parameters (milliseconds)
LATENCY_MIN_MS: int = 1_200
LATENCY_MAX_MS: int = 3_500

# MEV burst model: front-run fraction drawn from LogNormal
_MEV_LOGNORMAL_MU: float = math.log(0.005)   # median 0.5 % of depth
_MEV_LOGNORMAL_SIGMA: float = 0.7            # gives fat right tail
_MEV_MIN_FRACTION: float = 0.001             # 0.1 % floor
_MEV_MAX_FRACTION: float = 0.025             # 2.5 % ceiling

_DECIMAL_ZERO = Decimal("0")


@dataclass(frozen=True)
class LatencyResult:
    """
    Output of the stochastic latency + MEV front-run model.
    """

    latency_ms: int
    fill_timestamp_ns: int
    frontrun_fraction: float
    frontrun_volume: Decimal
    adjusted_pool_state: PoolState


def sample_fill_latency() -> int:
    """Sample a fill latency from Uniform(LATENCY_MIN_MS, LATENCY_MAX_MS)."""
    return random.randint(LATENCY_MIN_MS, LATENCY_MAX_MS)


def simulate_frontrun_volume(pool: PoolState) -> tuple[float, Decimal]:
    """
    Estimate MEV front-run volume using a burst-driven Log-Normal model.
    Depth-relative fraction in [0.1%, 2.5%] of pool.native_reserve.
    """
    raw_fraction = random.lognormvariate(_MEV_LOGNORMAL_MU, _MEV_LOGNORMAL_SIGMA)
    fraction = max(_MEV_MIN_FRACTION, min(raw_fraction, _MEV_MAX_FRACTION))
    frontrun_volume = Decimal(str(fraction)) * pool.native_reserve

    logger.debug(
        "MEV burst: fraction=%.4f%%, volume=%s native (pool depth=%s)",
        fraction * 100,
        frontrun_volume,
        pool.native_reserve,
    )
    return fraction, frontrun_volume


def apply_frontrun_to_pool(
    pool: PoolState,
    frontrun_volume_native: Decimal,
) -> PoolState:
    """
    Apply simulated adversarial BUY pressure to pool reserves.
    """
    max_frontrun = pool.native_reserve * Decimal("0.5")
    safe_volume = min(frontrun_volume_native, max_frontrun)

    if safe_volume <= _DECIMAL_ZERO:
        return pool

    try:
        quote = cpmm_buy_quote(pool, safe_volume)
    except ValueError as exc:
        logger.warning("Front-run quote failed: %s — using original pool.", exc)
        return pool

    updated = pool.model_copy(
        update={
            "native_reserve": quote.new_reserve_in,
            "token_reserve": quote.new_reserve_out,
        }
    )
    logger.debug(
        "Post-MEV pool: native %s→%s, token %s→%s",
        pool.native_reserve, updated.native_reserve,
        pool.token_reserve, updated.token_reserve,
    )
    return updated


def simulate_latency(
    pool: PoolState,
    signal_timestamp_ns: int,
) -> LatencyResult:
    """
    Full stochastic latency + burst MEV pipeline.
    """
    latency_ms = sample_fill_latency()
    fill_ts_ns = signal_timestamp_ns + latency_ms * 1_000_000

    fraction, frontrun_volume = simulate_frontrun_volume(pool)
    adjusted_pool = apply_frontrun_to_pool(pool, frontrun_volume)

    return LatencyResult(
        latency_ms=latency_ms,
        fill_timestamp_ns=fill_ts_ns,
        frontrun_fraction=fraction,
        frontrun_volume=frontrun_volume,
        adjusted_pool_state=adjusted_pool,
    )
