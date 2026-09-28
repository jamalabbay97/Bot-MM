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

class CaseInsensitiveFrozenSet(frozenset):
    """frozenset subclass supporting case-insensitive address membership checks."""
    def __contains__(self, item: Any) -> bool:
        if isinstance(item, str):
            return super().__contains__(item) or super().__contains__(item.lower())
        return super().__contains__(item)


# Verified LP Lockers on EVM
VERIFIED_LP_LOCKERS: CaseInsensitiveFrozenSet = CaseInsensitiveFrozenSet({
    "0x663a02cdd4a5a1a2c53051412383c2718e24479f",  # Unicrypt Base
    "0x663A5C229c09b049E36dCc11a9B0d4a8Eb9db214",  # Unicrypt V2
    "0xe2fe530c047f2d85298b07d9333c05737f1435fb",  # Team.Finance Lock
    "0xE2FE530C047f2d85298b07D91337b0262C99D255",  # Team.Finance V2
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
MIXER_AND_RUG_FUNDING_ADDRESSES: CaseInsensitiveFrozenSet = CaseInsensitiveFrozenSet({
    "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",  # Tornado Cash Router
    "0x722122df12d4e14e13ac3b6895a86e84145b6967",  # Tornado Cash Proxy
    "0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936",  # Tornado 0.1 ETH
    "0x910cbd523d972eb0a6f4cae4618ad62622b39dbf",  # Tornado 1 ETH
    "0xa160cdab225685da1d56aa342ad8841c3b53f291",  # Tornado 10 ETH
    "0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3",  # Tornado 100 ETH
    "0xfa7093cdd9ee6932b4eb2c9e1cde7ce00b1fa4b9",  # Railgun Contract
    "0x50de13ad81423ca5538e1a1005bc407817eb578a",  # Railgun Relayer
})

# Solana well-known system / DEX / router / program IDs to exclude from token screening
SOLANA_SYSTEM_PROGRAM_IDS: frozenset[str] = frozenset({
    "11111111111111111111111111111111",                     # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",          # Token Program
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",          # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",          # Associated Token Account
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",          # Raydium Liquidity Pool V4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",          # Raydium CLMM
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",          # Raydium CPMM
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",          # Raydium Authority
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",          # Pump.fun Program
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",          # Pump.fun Fee Account
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1",          # Pump.fun Global
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",          # Serum v3
    "opnb2TXrmDdHGauUQavWet8xTzJpdodAx7LvnR38sNY",          # OpenBook
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",          # Meteora DLMM
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",          # Meteora Pools
    "ComputeBudget111111111111111111111111111111",          # Compute Budget
    "SysvarRent111111111111111111111111111111111",          # Sysvar Rent
    "SysvarC1ock11111111111111111111111111111111",          # Sysvar Clock
    "SysvarRecentB1ockHashes11111111111111111111",          # Sysvar Blockhashes
    "So11111111111111111111111111111111111111112",          # Wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",          # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",          # USDT
    "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9",          # Flash Trade
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",          # Jupiter V6
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",          # Jupiter V4
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",          # Orca Whirlpool
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",          # Metaplex Token Metadata
})

# EVM well-known system / router / zero addresses to exclude
EVM_SYSTEM_ADDRESSES: CaseInsensitiveFrozenSet = CaseInsensitiveFrozenSet({
    "0x0000000000000000000000000000000000000000",
    "0x4200000000000000000000000000000000000006",          # Base WETH
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",          # Base USDC
    "0xcf77a3ba9a5ca399b7c97c749566343833341fdc",          # Aerodrome Router
})

# Static fast-path blacklist tokens (native, wrapped, quote, and routing assets)
SOLANA_BLACKLIST_TOKENS: frozenset[str] = SOLANA_SYSTEM_PROGRAM_IDS | frozenset({
    "So11111111111111111111111111111111111111112",          # Wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",          # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",          # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",          # mSOL
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",          # bSOL
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",          # JitoSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",          # stSOL
})

EVM_BLACKLIST_TOKENS: CaseInsensitiveFrozenSet = CaseInsensitiveFrozenSet(
    EVM_SYSTEM_ADDRESSES | {
        "0x0000000000000000000000000000000000000000",
        "0x4200000000000000000000000000000000000006",      # Base WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",      # Base USDC
        "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",      # Base USDbC
        "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",      # Base DAI / USDT
        "0xcf77a3ba9a5ca399b7c97c749566343833341fdc",      # Aerodrome Router
    }
)


def is_blacklisted_token(token_address: str, chain: ChainIdentifier) -> bool:
    """
    Fast-path static lookup to identify native, wrapped, system, or quote tokens to ignore.
    Prevents firing unnecessary external security API calls for known infrastructure tokens.
    """
    if not token_address:
        return True
    if chain == ChainIdentifier.SOLANA_MAINNET:
        return (
            token_address in SOLANA_BLACKLIST_TOKENS
            or token_address.startswith("11111111")
        )
    if chain == ChainIdentifier.BASE_MAINNET:
        return (
            token_address in EVM_BLACKLIST_TOKENS
            or token_address.lower() in EVM_BLACKLIST_TOKENS
        )
    return False



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
