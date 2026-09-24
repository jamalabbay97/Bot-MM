"""
alpha_engine.security.constants — Constants, Thresholds & ABIs
==============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from decimal import Decimal
from typing import Any

from alpha_engine.models.enums import ChainIdentifier

_GOPLUS_URL = "https://api.gopluslabs.io/api/v1/token_security/{chain_id}"
_RUGCHECK_URL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

_GOPLUS_CHAIN_IDS: dict[ChainIdentifier, str] = {
    ChainIdentifier.BASE_MAINNET: "8453",
}

# Hard-reject thresholds
_MAX_SELL_TAX_BPS = 500          # 5%
_MIN_LP_BURNED_RATIO = 0.90      # 90%
_MAX_TOP10_CONCENTRATION = 0.20  # 20%
_API_TIMEOUT_S = 1.5
_MIN_SELL_RETURN_RATIO = Decimal("0.90")   # Tier 2 threshold

# Dead / burn addresses for LP burned detection
_BURN_ADDRESSES: frozenset[str] = frozenset({
    "0x000000000000000000000000000000000000dead",
    "0x0000000000000000000000000000000000000000",
    "1nc1nerator11111111111111111111111111111111",   # Solana burn
})

# Uniswap v2 / Aerodrome router ABI fragments needed for eth_call simulation
_ROUTER_ABI_SWAP_EXACT_ETH: list[dict[str, Any]] = [
    {
        "name": "swapExactETHForTokensSupportingFeeOnTransferTokens",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [],
    },
    {
        "name": "swapExactTokensForETHSupportingFeeOnTransferTokens",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [],
    },
]

_ERC20_BALANCE_ABI: list[dict[str, Any]] = [
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "approve",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]
