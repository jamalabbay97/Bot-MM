"""
alpha_engine.models — Domain Models, Types, and Event Schemas
=============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitStage,
    LotStatus,
    NewsSignalStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    StrategyHorizon,
    TradeExitReason,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    PumpMintEvent,
    PumpSwapEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.news import (
    NewsEvent,
    NewsSignalEvent,
    TelegramMessage,
)
from alpha_engine.models.profiler import (
    FundingHop,
    InitialTxRecord,
    WalletProfile,
    WalletTradeRecord,
    WhitelistRecord,
)
from alpha_engine.models.ai import (
    ActionParameters,
    AISupervisorDecisionEnum,
    AISupervisorResponse,
    AuditedTokenOutcome,
    FeedbackTuning,
    GlobalRiskMode,
    HoneypotRisk,
    LiquidityHealth,
    LossAttribution,
    OutcomeTrackerMetrics,
    SecurityAssessment,
    TakeProfitStage,
    WalletAudit,
    WalletRiskClassification,
)
from alpha_engine.models.state import (
    ExitOrder,
    OpenPositionLot,
    PaperFill,
    PoolState,
    PortfolioSnapshot,
    SecurityReport,
    TradeRecord,
)
from alpha_engine.models.decisions import (
    DecisionRecord,
    DecisionSignal,
    DecisionType,
    PatternFeatureVector,
    RevivalPatternFeatureVector,
)

__all__ = [
    # Base Config
    "_STRICT_MODEL_CFG",
    # Enums
    "ChainIdentifier",
    "ExecutionVenue",
    "OrderSide",
    "SecurityTier",
    "SignalStrength",
    "StrategyHorizon",
    "WalletClassification",
    "WhitelistStatus",
    "NewsSignalStatus",
    "SignalSource",
    "ExitStage",
    "LotStatus",
    "TradeExitReason",
    # Ingestion & Pipeline Events
    "SwapEvent",
    "SignalEvent",
    "RawSignalEvent",
    "PoolStateUpdateEvent",
    "ShutdownSentinel",
    "PumpMintEvent",
    "PumpSwapEvent",
    # News & Social Models
    "TelegramMessage",
    "NewsSignalEvent",
    "NewsEvent",
    # Profiler Models
    "WalletTradeRecord",
    "FundingHop",
    "InitialTxRecord",
    "WalletProfile",
    "WhitelistRecord",
    # State & Execution Models
    "PoolState",
    "SecurityReport",
    "PaperFill",
    "TradeRecord",
    "OpenPositionLot",
    "ExitOrder",
    "PortfolioSnapshot",
    # AI Supervisor Models
    "AISupervisorDecisionEnum",
    "WalletRiskClassification",
    "HoneypotRisk",
    "LiquidityHealth",
    "GlobalRiskMode",
    "LossAttribution",
    "TakeProfitStage",
    "ActionParameters",
    "WalletAudit",
    "SecurityAssessment",
    "FeedbackTuning",
    "AISupervisorResponse",
    "AuditedTokenOutcome",
    "OutcomeTrackerMetrics",
    # Structured Decision Audit & Pattern Models
    "DecisionType",
    "DecisionRecord",
    "DecisionSignal",
    "PatternFeatureVector",
    "RevivalPatternFeatureVector",
]

