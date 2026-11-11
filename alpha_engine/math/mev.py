"""
alpha_engine.math.mev — Stochastic Latency & Conditional MEV Front-Run Model
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+

Execution Assumptions & Model Justification:
--------------------------------------------
1. Fill Latency Modeling:
   - Solana: 45ms to 320ms LogNormal distribution.
     Justification: Solana 400ms target slot time (Solana Labs Consensus Docs:
     https://docs.solanalabs.com/consensus/fork-generation#slot-time). Direct TPU/QUIC
     broadcast and Jito bundles achieve sub-slot confirmation between 45ms and 320ms.
   - Base: 150ms to 2000ms LogNormal distribution.
     Justification: Base (OP Stack) block time is 2.0s (2,000ms) (Base Docs:
     https://docs.base.org/chain/block-time/). Sequencer propagation to block inclusion
     ranges from 150ms (sequencer receipt / Flashblocks) to 2000ms (next block boundary).

2. Price Movement During Latency:
   - Fill price is determined from the market pool state AT target fill time
     (signal_time + latency). If the market moves up or down during the latency window,
     the fill price reflects that move.
   - If no pool state fresher than a configurable bound (fill_max_stale_ms, default 4000ms)
     exists at fill time, the fill is rejected fail-closed with telemetry reason
     'stale_pool_at_fill_rejected'.

3. Conditional MEV & Sandwich Attack Protection:
   - Private Route Protection: If routed via private relays / Jito bundles
     (is_private_route=True), there is ZERO public mempool exposure, resulting in
     0 frontrun volume and 0 adverse price drag.
   - Public Mempool Vulnerability: Searchers only attempt sandwich attacks if:
     * Trade size exceeds depth threshold (default >= 0.5% pool depth), and
     * Slippage tolerance leaves sufficient extractable room (default >= 50 bps).
     Trades below these thresholds experience 0 frontrun volume.
   - Adversarial Probability & Sizing: For public vulnerable trades, sandwich probability
     increases with depth fraction and slippage. If triggered, frontrun volume extracts
     a portion of allowable slippage, capped at mev_max_sandwich_impact_bps.
   - Deterministic Seeding: All stochastic latency and MEV draws use a seeded RNG
     (config.execution_seed).
"""

from __future__ import annotations

import inspect
import logging
import math
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union

from alpha_engine.math.cpmm import cpmm_buy_quote
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.state import PoolState

if TYPE_CHECKING:
    from alpha_engine.config import EngineConfig

logger = logging.getLogger(__name__)

# Fallback default latency parameters (milliseconds)
DEFAULT_SOLANA_LATENCY_MIN_MS: int = 45
DEFAULT_SOLANA_LATENCY_MAX_MS: int = 320
DEFAULT_BASE_LATENCY_MIN_MS: int = 150
DEFAULT_BASE_LATENCY_MAX_MS: int = 2000

# Legacy fallback bounds for backward compatibility
LATENCY_MIN_MS: int = 45
LATENCY_MAX_MS: int = 320

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
    adjusted_pool_state: Optional[PoolState]
    pre_mev_pool_state: Optional[PoolState] = None
    rejection_reason: Optional[str] = None


def sample_fill_latency(
    chain: Optional[ChainIdentifier] = None,
    config: Optional[EngineConfig] = None,
    rng: Optional[random.Random] = None,
) -> int:
    """
    Sample fill latency based on chain-specific parameters from config.

    Defaults:
    - Solana: 45ms - 320ms (LogNormal skewed toward lower median).
    - Base: 150ms - 2000ms (LogNormal skewed toward block boundary).
    """
    if chain == ChainIdentifier.BASE_MAINNET:
        min_ms = config.latency_min_ms_base if config else DEFAULT_BASE_LATENCY_MIN_MS
        max_ms = config.latency_max_ms_base if config else DEFAULT_BASE_LATENCY_MAX_MS
    else:
        min_ms = config.latency_min_ms_solana if config else DEFAULT_SOLANA_LATENCY_MIN_MS
        max_ms = config.latency_max_ms_solana if config else DEFAULT_SOLANA_LATENCY_MAX_MS

    if min_ms > max_ms:
        min_ms, max_ms = max_ms, min_ms

    r = rng
    if r is None:
        if config and config.execution_seed is not None:
            r = random.Random(config.execution_seed)
        else:
            r = random

    dist = (getattr(config, "latency_distribution", "lognormal") or "lognormal").lower()
    if dist == "uniform":
        return r.randint(min_ms, max_ms)

    # Log-Normal sampling bounded by [min_ms, max_ms]
    ln_min = math.log(max(1, min_ms))
    ln_max = math.log(max(1, max_ms))
    # Median placed at ~35% into the logarithmic range with moderate right skew
    mu = ln_min + 0.35 * (ln_max - ln_min)
    sigma = max(0.1, 0.30 * (ln_max - ln_min))

    sampled = math.exp(r.gauss(mu, sigma))
    clamped = max(min_ms, min(max_ms, int(round(sampled))))
    return clamped


def simulate_frontrun_volume(
    pool: PoolState,
    trade_size_native: Optional[Decimal] = None,
    max_slippage_bps: int = 50,
    is_private_route: bool = False,
    config: Optional[EngineConfig] = None,
    rng: Optional[random.Random] = None,
) -> tuple[float, Decimal]:
    """
    Estimate MEV front-run volume conditional on route, depth, slippage, and protection.

    Rules:
    1. If is_private_route is True: 0 volume, 0 fraction (MEV immune).
    2. If trade is too small relative to depth (< depth_threshold) and has tight slippage
       (< slippage_threshold): 0 volume (unprofitable for searchers).
    3. If vulnerable: probability scales with depth impact and slippage. If triggered,
       volume scales with depth and consumes allowable slippage up to config ceiling.
    4. Deterministic with seeded RNG.
    """
    if is_private_route:
        logger.debug("Trade routed via private bundle/relay: zero MEV front-run exposure.")
        return 0.0, Decimal("0")

    if pool.native_reserve <= _DECIMAL_ZERO:
        return 0.0, Decimal("0")

    if trade_size_native is None or trade_size_native <= _DECIMAL_ZERO:
        return 0.0, Decimal("0")

    depth_fraction = float(trade_size_native / pool.native_reserve)
    depth_thresh = getattr(config, "mev_sandwich_depth_threshold", 0.005) if config else 0.005
    slip_thresh = getattr(config, "mev_sandwich_slippage_threshold_bps", 50) if config else 50
    max_impact_bps = getattr(config, "mev_max_sandwich_impact_bps", 500) if config else 500

    # Below exploitation thresholds: searchers cannot profit after L1/gas costs
    if depth_fraction < depth_thresh and max_slippage_bps < slip_thresh:
        logger.debug(
            "Trade below MEV exploitability thresholds (depth=%.3f%% < %.3f%%, slip=%dbps < %dbps): 0 MEV.",
            depth_fraction * 100,
            depth_thresh * 100,
            max_slippage_bps,
            slip_thresh,
        )
        return 0.0, Decimal("0")

    r = rng
    if r is None:
        if config and config.execution_seed is not None:
            r = random.Random(config.execution_seed)
        else:
            r = random

    # Probability of sandwich attack increases with depth impact and allowable slippage
    depth_risk = min(1.0, depth_fraction / 0.02)
    slippage_risk = min(1.0, max_slippage_bps / 300.0)
    sandwich_prob = min(0.95, max(0.05, 0.60 * depth_risk + 0.40 * slippage_risk))

    if r.random() > sandwich_prob:
        logger.debug("MEV sandwich probability draw missed (prob=%.2f): 0 MEV.", sandwich_prob)
        return 0.0, Decimal("0")

    # Frontrun extraction: searchers push price to consume allowable slippage room
    slippage_room_fraction = max_slippage_bps / 10000.0
    impact_fraction = min(
        max_impact_bps / 10000.0,
        max(0.001, slippage_room_fraction * 0.5 * (1.0 + depth_fraction)),
    )

    frontrun_volume = pool.native_reserve * Decimal(str(round(impact_fraction * 0.5, 6)))
    # Bound frontrun volume: at most 5% of native reserve and at most 2x trade size
    max_cap = min(pool.native_reserve * Decimal("0.05"), trade_size_native * Decimal("2.0"))
    frontrun_volume = max(_DECIMAL_ZERO, min(frontrun_volume, max_cap))
    frontrun_fraction = float(frontrun_volume / pool.native_reserve) if pool.native_reserve > 0 else 0.0

    logger.debug(
        "MEV sandwich triggered: fraction=%.4f%%, volume=%s native (pool depth=%s)",
        frontrun_fraction * 100,
        frontrun_volume,
        pool.native_reserve,
    )
    return frontrun_fraction, frontrun_volume


def apply_frontrun_to_pool(
    pool: PoolState,
    frontrun_volume_native: Decimal,
) -> PoolState:
    """
    Apply simulated adversarial BUY pressure to pool reserves.
    """
    if frontrun_volume_native <= _DECIMAL_ZERO:
        return pool

    max_frontrun = pool.native_reserve * Decimal("0.5")
    safe_volume = min(frontrun_volume_native, max_frontrun)

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
        pool.native_reserve,
        updated.native_reserve,
        pool.token_reserve,
        updated.token_reserve,
    )
    return updated


def determine_market_state_at_fill(
    initial_pool: PoolState,
    target_fill_timestamp_ns: int,
    market_state_provider: Optional[Any] = None,
    max_stale_bound_ms: int = 4000,
) -> tuple[Optional[PoolState], Optional[str]]:
    """
    Determine the pool state AT the target fill time (signal_time + latency).

    If a market_state_provider is supplied (e.g. tick feed, callable, or recorded states),
    queries it for the market state at target_fill_timestamp_ns.
    Verifies freshness against max_stale_bound_ms: if no state fresher than the bound
    exists at that time, rejects the fill fail-closed with 'stale_pool_at_fill_rejected'.
    """
    resolved_pool: Optional[PoolState] = None

    if market_state_provider is not None:
        if callable(market_state_provider):
            try:
                sig = inspect.signature(market_state_provider)
                num_params = len(sig.parameters)
                if num_params == 1:
                    resolved_pool = market_state_provider(target_fill_timestamp_ns)
                elif num_params == 3:
                    resolved_pool = market_state_provider(
                        initial_pool.chain,
                        getattr(initial_pool, "token_address", ""),
                        target_fill_timestamp_ns,
                    )
                elif num_params == 4:
                    resolved_pool = market_state_provider(
                        initial_pool.chain,
                        getattr(initial_pool, "token_address", ""),
                        getattr(initial_pool, "pool_address", ""),
                        target_fill_timestamp_ns,
                    )
                else:
                    resolved_pool = market_state_provider(
                        chain=initial_pool.chain,
                        token=getattr(initial_pool, "token_address", ""),
                        at_timestamp_ns=target_fill_timestamp_ns,
                    )
            except Exception as exc:
                logger.warning("market_state_provider raised during fill resolution: %s", exc)
                return None, "stale_pool_at_fill_rejected"
        elif isinstance(market_state_provider, Sequence):
            # Sequence of PoolState or (timestamp_ns, PoolState)
            candidates = []
            for item in market_state_provider:
                if isinstance(item, tuple) and len(item) == 2:
                    candidates.append((int(item[0]), item[1]))
                elif isinstance(item, PoolState):
                    ts = getattr(item, "timestamp_ns", None) or int(getattr(item, "timestamp_s", 0) * 1e9)
                    candidates.append((ts, item))
            # Sort by timestamp
            candidates.sort(key=lambda x: x[0])
            # Find the freshest state at or immediately before/after fill time
            valid = [c for c in candidates if c[0] <= target_fill_timestamp_ns]
            if valid:
                resolved_pool = valid[-1][1]
            elif candidates:
                resolved_pool = candidates[0][1]
        elif isinstance(market_state_provider, PoolState):
            resolved_pool = market_state_provider

    if resolved_pool is None:
        resolved_pool = initial_pool

    # Freshness verification: state timestamp must be within max_stale_bound_ms of fill time
    pool_ts_ns = getattr(resolved_pool, "timestamp_ns", None)
    if pool_ts_ns is None:
        pool_ts_s = getattr(resolved_pool, "timestamp_s", 0.0)
        pool_ts_ns = int(pool_ts_s * 1_000_000_000) if pool_ts_s > 0 else 0

    if pool_ts_ns > 0:
        age_ms = (target_fill_timestamp_ns - pool_ts_ns) / 1_000_000
        if age_ms > max_stale_bound_ms:
            logger.warning(
                "Pool state at fill time is stale (age=%.1fms > max=%.1fms); rejecting fill.",
                age_ms,
                max_stale_bound_ms,
            )
            return None, "stale_pool_at_fill_rejected"

    return resolved_pool, None


def simulate_latency(
    pool: PoolState,
    signal_timestamp_ns: int,
    chain: Optional[ChainIdentifier] = None,
    trade_size_native: Optional[Decimal] = None,
    max_slippage_bps: int = 50,
    is_private_route: bool = False,
    market_state_provider: Optional[Any] = None,
    config: Optional[EngineConfig] = None,
    rng: Optional[random.Random] = None,
) -> LatencyResult:
    """
    Full stochastic latency + conditional MEV front-run pipeline.

    Steps:
    1. Sample fill latency for chain from configured distribution.
    2. Determine fill timestamp = signal_timestamp_ns + latency_ms * 1_000_000.
    3. Resolve freshest real pool state at fill time (fail-closed if stale).
    4. Compute conditional MEV frontrun on the fill-time pool state.
    5. Return LatencyResult.
    """
    target_chain = chain or getattr(pool, "chain", None) or ChainIdentifier.SOLANA_MAINNET
    latency_ms = sample_fill_latency(chain=target_chain, config=config, rng=rng)
    fill_ts_ns = signal_timestamp_ns + latency_ms * 1_000_000

    max_stale_ms = getattr(config, "fill_max_stale_ms", 4000) if config else 4000
    fill_pool, rej_reason = determine_market_state_at_fill(
        initial_pool=pool,
        target_fill_timestamp_ns=fill_ts_ns,
        market_state_provider=market_state_provider,
        max_stale_bound_ms=max_stale_ms,
    )

    if fill_pool is None:
        return LatencyResult(
            latency_ms=latency_ms,
            fill_timestamp_ns=fill_ts_ns,
            frontrun_fraction=0.0,
            frontrun_volume=_DECIMAL_ZERO,
            adjusted_pool_state=None,
            pre_mev_pool_state=None,
            rejection_reason=rej_reason or "stale_pool_at_fill_rejected",
        )

    fraction, frontrun_volume = simulate_frontrun_volume(
        pool=fill_pool,
        trade_size_native=trade_size_native,
        max_slippage_bps=max_slippage_bps,
        is_private_route=is_private_route,
        config=config,
        rng=rng,
    )
    adjusted_pool = apply_frontrun_to_pool(fill_pool, frontrun_volume)

    return LatencyResult(
        latency_ms=latency_ms,
        fill_timestamp_ns=fill_ts_ns,
        frontrun_fraction=fraction,
        frontrun_volume=frontrun_volume,
        adjusted_pool_state=adjusted_pool,
        pre_mev_pool_state=fill_pool,
        rejection_reason=None,
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
    """
    if not mempool_is_public:
        return False, 0.05, "PROTECTED_PRIVATE_RELAY: Zero public mempool exposure"

    if pool.native_reserve <= Decimal("0") or trade_size_native <= Decimal("0"):
        return True, 1.0, "INVALID_POOL_OR_TRADE_SIZE: Near-zero liquidity depth"

    depth_fraction = float(trade_size_native / pool.native_reserve)
    slippage_fraction = max_slippage_bps / 10_000.0

    depth_risk = min(1.0, depth_fraction / 0.02)
    slippage_risk = min(1.0, slippage_fraction / 0.03)

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
