"""
alpha_engine.models — Domain Models, Types, and Event Schemas
=============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    LotStatus,
    NewsSignalStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.news import (
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

__all__ = [
    # Base Config
    "_STRICT_MODEL_CFG",
    # Enums
    "ChainIdentifier",
    "OrderSide",
    "SecurityTier",
    "SignalStrength",
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
    # News & Social Models
    "TelegramMessage",
    "NewsSignalEvent",
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
]

