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


def calculate_sandwich_risk(
    pool: PoolState,
    trade_size_native: Decimal,
    max_slippage_bps: int = 150,
    mempool_is_public: bool = True,
    risk_threshold: float = 0.50,
) -> tuple[bool, float, str]:
    """
    Calculate sandwich-attack risk score in [0.0, 1.0] and evaluate against risk threshold.

    Parameters
    ----------
    pool : PoolState
        Current on-chain pool reserve state.
    trade_size_native : Decimal
        Planned trade size in native asset.
    max_slippage_bps : int
        User-configured maximum slippage in basis points.
    mempool_is_public : bool
        True if routed to public mempool (unprotected), False if routed via private bundle/relay.
    risk_threshold : float
        Threshold above which is_high_risk returns True.

    Returns
    -------
    tuple[bool, float, str]
        (is_high_risk, risk_score, reason)
    """
    if not mempool_is_public:
        return False, 0.05, "PROTECTED_PRIVATE_RELAY: Zero public mempool exposure"

    if pool.native_reserve <= Decimal("0") or trade_size_native <= Decimal("0"):
        return True, 1.0, "INVALID_POOL_OR_TRADE_SIZE: Near-zero liquidity depth"

    # 1. Depth fraction: trade_size / pool_native_reserve
    depth_fraction = float(trade_size_native / pool.native_reserve)

    # 2. Slippage vulnerability fraction (e.g. 100 bps = 1.0%, 300 bps = 3.0%)
    slippage_fraction = max_slippage_bps / 10_000.0

    # 3. Base composite risk calculation
    # High depth fraction (>1%) or wide slippage (>150 bps) in public mempool attracts searchers
    depth_risk = min(1.0, depth_fraction / 0.02)  # Max risk at 2% pool depth
    slippage_risk = min(1.0, slippage_fraction / 0.03)  # Max risk at 3% slippage

    raw_score = 0.60 * depth_risk + 0.40 * slippage_risk
    score = round(max(0.0, min(1.0, raw_score)), 3)

    is_high_risk = score >= risk_threshold

    reasons: list[str] = []
    if depth_fraction >= 0.01:
        reasons.append(f"Trade size is {depth_fraction:.2%} of pool depth")
    if max_slippage_bps > 200:
        reasons.append(f"Wide slippage tolerance ({max_slippage_bps} bps)")
    if is_high_risk and not reasons:
        reasons.append("Composite MEV exploitability score exceeds safe threshold")

    reason_str = " | ".join(reasons) if reasons else "Acceptable MEV risk"
    return is_high_risk, score, reason_str
