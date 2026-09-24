"""
alpha_engine.models — Domain Models, Types, and Event Schemas
=============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
"""

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalStrength,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    ShutdownSentinel,
    SignalEvent,
    SwapEvent,
)
from alpha_engine.models.state import (
    PaperFill,
    PoolState,
    PortfolioSnapshot,
    SecurityReport,
    TradeRecord,
)

__all__ = [
    "_STRICT_MODEL_CFG",
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
]
