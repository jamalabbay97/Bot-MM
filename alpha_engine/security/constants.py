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
_MAX_BUY_TAX_BPS = 500           # 5%
_MIN_LP_BURNED_RATIO = 0.90      # 90%
_MAX_TOP10_CONCENTRATION = 0.20  # 20%
_API_TIMEOUT_S = 1.5
_MIN_SELL_RETURN_RATIO = Decimal("0.90")   # Tier 2 threshold
MIN_LOCK_DURATION_SECONDS = 180 * 86400    # 6 months

# Dead / burn addresses for LP burned detection
_BURN_ADDRESSES: frozenset[str] = frozenset({
    "0x000000000000000000000000000000000000dead",
    "0x0000000000000000000000000000000000000000",
    "1nc1nerator11111111111111111111111111111111",   # Solana burn
})

# Verified LP Lockers on EVM
VERIFIED_LP_LOCKERS: frozenset[str] = frozenset({
    "0x663a02cdd4a5a1a2c53051412383c2718e24479f",  # Unicrypt Base
    "0xe2fe530c047f2d85298b07d9333c05737f1435fb",  # Team.Finance Lock
    "0x71b5759d73262fbbf247952223f86e03919c3d09",  # PinkLock Base
})

# Bytecode 4-byte function selectors and signatures for fee/tax modification or trading traps
TAX_MUTATION_MAP: dict[str, str] = {
    "9c3d4f19": "setTax(uint256,uint256)",
    "c39f8f60": "setTax(uint256)",
    "d1f90c37": "updateFees(uint256,uint256)",
    "8a8c523c": "enableTrading()",
    "694e9f78": "setFee(uint256)",
    "421c4327": "setMaxTxPercent(uint256)",
    "8f9a55c0": "setMaxTransferAmount(uint256)",
    "f5a2fc72": "setMaxWallet(uint256)",
}

TAX_MUTATION_SELECTORS: frozenset[str] = frozenset(TAX_MUTATION_MAP.keys())

# Known Privacy / Mixer Addresses (Tornado Cash, Railgun) & known rug funding
MIXER_AND_RUG_FUNDING_ADDRESSES: frozenset[str] = frozenset({
    "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",  # Tornado Cash Router
    "0x722122df12d4e14e13ac3b6895a86e84145b6967",  # Tornado Cash Proxy
    "0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936",  # Tornado 0.1 ETH
    "0x910cbd523d972eb0a6f4cae4618ad62622b39dbf",  # Tornado 1 ETH
    "0xa160cdab225685da1d56aa342ad8841c3b53f291",  # Tornado 10 ETH
    "0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3",  # Tornado 100 ETH
    "0xfa7093cdd9ee6932b4eb2c9e1cde7ce00b1fa4b9",  # Railgun Contract
    "0x50de13ad81423ca5538e1a1005bc407817eb578a",  # Railgun Relayer
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
