"""
alpha_engine — Multi-Chain Paper Trading & Alpha Analytics Engine
=================================================================
Deterministic Realism | Zero-Capital Simulation | Pure Python 3.11+
"""

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
from alpha_engine.ingestion import (
    EVMIngester,
    IngestionCoordinator,
    SVMIngester,
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
    OrderSide,
    PaperFill,
    PoolState,
    PoolStateUpdateEvent,
    PortfolioSnapshot,
    SecurityReport,
    SecurityTier,
    ShutdownSentinel,
    SignalEvent,
    SignalStrength,
    SwapEvent,
    TradeRecord,
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
    "SwapEvent",
    "SignalEvent",
    "PoolStateUpdateEvent",
    "ShutdownSentinel",
    "PoolState",
    "SecurityReport",
    "PaperFill",
    "TradeRecord",
    "PortfolioSnapshot",
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
    # Execution & Persistence
    "OpenLot",
    "PositionBook",
    "RunningMetrics",
    "PaperExecutor",
    "SQLiteLedger",
]
