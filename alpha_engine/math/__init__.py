"""
alpha_engine.math — Quantitative Math, AMM Dynamics & Risk Modeling
===================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from alpha_engine.math.cpmm import (
    CpmmQuote,
    cpmm_buy_quote,
    cpmm_out,
    cpmm_sell_quote,
    execution_price_buy,
    execution_price_sell,
    price_impact_bps,
    spot_price,
)
from alpha_engine.math.mev import (
    LatencyResult,
    apply_frontrun_to_pool,
    sample_fill_latency,
    simulate_frontrun_volume,
    simulate_latency,
)
from alpha_engine.math.sizing import (
    GAS_COST_BASE_USD,
    GAS_COST_SOL_USD,
    KellySizing,
    classify_alpha_score,
    compute_position_size,
    gas_cost_usd,
    half_kelly_fraction,
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
]
