"""
wallet_profiler.py — Smart Money Behavioral & Cluster Profiler
==============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | asyncio | SQLite WAL Mode

Module A:
- Survivorship & Outlier Bias detection (> 60% PnL from a single lucky trade).
- Wash-Trading & Self-Funding provenance (first 5 txs & <= 3-hop BFS trace to deployer/multisig).
- Holding Time Filter (median < 45 seconds MEV bot rejection).
- Minimum Criteria (>= 15 closed trades, win rate >= 55%, >= 21 days active trading).
- Dead-Wallet Revivals (> 45 days dormant followed by sudden trading).
- Local Whitelist DB with SQLite WAL concurrency.
"""

from alpha_engine.models import (
    ChainIdentifier,
    FundingHop,
    InitialTxRecord,
    WalletClassification,
    WalletProfile,
    WalletTradeRecord,
    WhitelistRecord,
    WhitelistStatus,
)
from alpha_engine.profiler import (
    SmartMoneyProfiler,
    WalletEvaluator,
    WhitelistDatabase,
)

__all__ = [
    "SmartMoneyProfiler",
    "WhitelistDatabase",
    "WalletEvaluator",
    "WalletProfile",
    "WalletTradeRecord",
    "FundingHop",
    "InitialTxRecord",
    "WhitelistRecord",
    "WalletClassification",
    "WhitelistStatus",
    "ChainIdentifier",
]
