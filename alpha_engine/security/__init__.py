"""
alpha_engine.security — Two-Tier Gatekeeper & Security Verifier
==============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from alpha_engine.security.constants import (
    _API_TIMEOUT_S,
    _BURN_ADDRESSES,
    _ERC20_BALANCE_ABI,
    _GOPLUS_CHAIN_IDS,
    _GOPLUS_URL,
    _MAX_SELL_TAX_BPS,
    _MAX_TOP10_CONCENTRATION,
    _MIN_LP_BURNED_RATIO,
    _MIN_SELL_RETURN_RATIO,
    _ROUTER_ABI_SWAP_EXACT_ETH,
    _RUGCHECK_URL,
)
from alpha_engine.security.gatekeeper import (
    SecurityGatekeeper,
    _build_fallback_report,
)
from alpha_engine.security.goplus import (
    _calculate_lp_burned_ratio,
    _calculate_top10_concentration,
    _fetch_goplus_report,
    _parse_goplus_report,
)
from alpha_engine.security.preflight import _tier2_evm_preflight
from alpha_engine.security.rugcheck import (
    _fetch_rugcheck_report,
    _parse_rugcheck_report,
)

__all__ = [
    "SecurityGatekeeper",
    "_build_fallback_report",
    "_tier2_evm_preflight",
    "_fetch_goplus_report",
    "_parse_goplus_report",
    "_fetch_rugcheck_report",
    "_parse_rugcheck_report",
    "_calculate_lp_burned_ratio",
    "_calculate_top10_concentration",
    "_GOPLUS_URL",
    "_RUGCHECK_URL",
    "_GOPLUS_CHAIN_IDS",
    "_MAX_SELL_TAX_BPS",
    "_MIN_LP_BURNED_RATIO",
    "_MAX_TOP10_CONCENTRATION",
    "_API_TIMEOUT_S",
    "_MIN_SELL_RETURN_RATIO",
    "_BURN_ADDRESSES",
    "_ROUTER_ABI_SWAP_EXACT_ETH",
    "_ERC20_BALANCE_ABI",
]
