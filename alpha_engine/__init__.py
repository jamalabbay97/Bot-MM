"""
alpha_engine — Multi-Chain Paper Trading & Alpha Analytics Engine
=================================================================
Deterministic Realism | Zero-Capital Simulation | Pure Python 3.11+
"""

import sys
from pathlib import Path

# Auto-bootstrap local .venv site-packages if running outside virtual environment
_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT / ".venv" / "lib").glob("python*/site-packages"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from alpha_engine.dns_resolver import patch_dns_resolvers

patch_dns_resolvers()

from alpha_engine.config import EngineConfig
from alpha_engine.engine import (
    PaperTradingEngine,
    PoolRegistry,
    SignalGenerator,
)
from alpha_engine.execution import (
    OpenLot,
    PaperExecutor,
    PositionBook,
    RunningMetrics,
    SQLiteLedger,
)
from alpha_engine.tracer import LifecycleTracer, tracer
from alpha_engine.ingestion import (
    EVMIngester,
    IngestionCoordinator,
    SVMIngester,
    TelegramIngester,
)
from alpha_engine.math import (
    CpmmQuote,
    LatencyResult,
    classify_alpha_score,
    compute_position_size,
    cpmm_buy_quote,
    cpmm_out,
    cpmm_sell_quote,
    gas_cost_usd,
    half_kelly_fraction,
    price_impact_bps,
    sample_fill_latency,
    simulate_latency,
    spot_price,
)
from alpha_engine.models import (
    ChainIdentifier,
    ExitOrder,
    ExitStage,
    FundingHop,
    InitialTxRecord,
    NewsSignalEvent,
    NewsSignalStatus,
    OpenPositionLot,
    OrderSide,
    PaperFill,
    PoolState,
    PoolStateUpdateEvent,
    PortfolioSnapshot,
    RawSignalEvent,
    SecurityReport,
    SecurityTier,
    ShutdownSentinel,
    SignalEvent,
    SignalSource,
    SignalStrength,
    SwapEvent,
    TelegramMessage,
    TradeExitReason,
    TradeRecord,
    TradingPlatform,
    PlatformSource,
    WalletClassification,
    WalletProfile,
    WalletTradeRecord,
    WhitelistRecord,
    WhitelistStatus,
    resolve_trade_platform,
)
from alpha_engine.profiler import (
    SmartMoneyProfiler,
    WalletEvaluator,
    WhitelistDatabase,
)
from alpha_engine.rate_limiter import (
    AsyncTokenBucket,
    RateLimiterRegistry,
    RateLimitError,
    build_alchemy_limiter,
    build_helius_limiter,
)
from alpha_engine.security import SecurityGatekeeper

__version__ = "0.1.0"

__all__ = [
    # Top-level Orchestrator & Config
    "PaperTradingEngine",
    "EngineConfig",
    "SignalGenerator",
    "PoolRegistry",
    # Models & Enums
    "ChainIdentifier",
    "OrderSide",
    "SecurityTier",
    "SignalStrength",
    "WalletClassification",
    "WhitelistStatus",
    "NewsSignalStatus",
    "SignalSource",
    "ExitStage",
    "TradeExitReason",
    "TradingPlatform",
    "PlatformSource",
    "resolve_trade_platform",
    "SwapEvent",
    "SignalEvent",
    "RawSignalEvent",
    "PoolStateUpdateEvent",
    "ShutdownSentinel",
    "TelegramMessage",
    "NewsSignalEvent",
    "WalletTradeRecord",
    "FundingHop",
    "InitialTxRecord",
    "WalletProfile",
    "WhitelistRecord",
    "PoolState",
    "SecurityReport",
    "PaperFill",
    "TradeRecord",
    "OpenPositionLot",
    "ExitOrder",
    "PortfolioSnapshot",
    # Smart Money Profiler
    "SmartMoneyProfiler",
    "WalletEvaluator",
    "WhitelistDatabase",
    # Quant Math & Sizing
    "CpmmQuote",
    "cpmm_out",
    "cpmm_buy_quote",
    "cpmm_sell_quote",
    "spot_price",
    "price_impact_bps",
    "LatencyResult",
    "simulate_latency",
    "sample_fill_latency",
    "half_kelly_fraction",
    "compute_position_size",
    "gas_cost_usd",
    "classify_alpha_score",
    # Rate Limiting
    "AsyncTokenBucket",
    "RateLimiterRegistry",
    "RateLimitError",
    "build_alchemy_limiter",
    "build_helius_limiter",
    # Security
    "SecurityGatekeeper",
    # Ingestion
    "IngestionCoordinator",
    "EVMIngester",
    "SVMIngester",
    "TelegramIngester",
    # Execution & Persistence
    "OpenLot",
    "PositionBook",
    "RunningMetrics",
    "PaperExecutor",
    "SQLiteLedger",
    "tracer",
    "LifecycleTracer",
]
