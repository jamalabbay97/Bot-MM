"""
quant_math.py — Quantitative Math & Dynamic Slippage Engine (Compatibility Facade)
==================================================================================
Re-exports all mathematical components from alpha_engine.math.
"""

from alpha_engine.math import (
    GAS_COST_BASE_USD,
    GAS_COST_SOL_USD,
    CpmmQuote,
    KellySizing,
    LatencyResult,
    apply_frontrun_to_pool,
    classify_alpha_score,
    compute_position_size,
    cpmm_buy_quote,
    cpmm_out,
    cpmm_sell_quote,
    execution_price_buy,
    execution_price_sell,
    gas_cost_usd,
    half_kelly_fraction,
    price_impact_bps,
    sample_fill_latency,
    simulate_frontrun_volume,
    simulate_latency,
    spot_price,
)
from alpha_engine.math.cpmm import (
    BPS_DENOMINATOR,
    _MAX_IMPACT_BPS,
    _compute_price_impact_bps,
)
from alpha_engine.math.mev import (
    LATENCY_MAX_MS,
    LATENCY_MIN_MS,
)
from alpha_engine.math.sizing import (
    GAS_MULTIPLE_GATE,
    MAX_POSITION_FRACTION,
)

__all__ = [
    "CpmmQuote",
    "cpmm_out",
    "cpmm_buy_quote",
    "cpmm_sell_quote",
    "spot_price",
    "execution_price_buy",
    "execution_price_sell",
    "price_impact_bps",
    "LatencyResult",
    "sample_fill_latency",
    "simulate_frontrun_volume",
    "apply_frontrun_to_pool",
    "simulate_latency",
    "KellySizing",
    "half_kelly_fraction",
    "compute_position_size",
    "gas_cost_usd",
    "classify_alpha_score",
    "GAS_COST_SOL_USD",
    "GAS_COST_BASE_USD",
    "MAX_POSITION_FRACTION",
    "BPS_DENOMINATOR",
    "LATENCY_MIN_MS",
    "LATENCY_MAX_MS",
    "GAS_MULTIPLE_GATE",
    "_compute_price_impact_bps",
    "_MAX_IMPACT_BPS",
]
