"""
models.py — Domain Models, Types, and Event Schemas (Compatibility Facade)
==========================================================================
Re-exports all domain models from alpha_engine.models for full backwards compatibility.
"""

from alpha_engine.models import (
    _STRICT_MODEL_CFG,
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
