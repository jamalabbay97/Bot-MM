"""
alpha_engine.models.enums — Domain Enumerations
===============================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from enum import Enum


class ChainIdentifier(str, Enum):
    """Supported chain targets. Inherits str so values serialize cleanly."""

    BASE_MAINNET = "base_mainnet"
    SOLANA_MAINNET = "solana_mainnet"


class OrderSide(str, Enum):
    """Direction of a simulated paper trade."""

    BUY = "buy"
    SELL = "sell"


class SecurityTier(str, Enum):
    """Which tier(s) of the gatekeeper flagged the token."""

    CLEAN = "clean"
    TIER1_REJECTED = "tier1_rejected"       # External API (GoPlus / RugCheck)
    TIER2_HONEYPOT = "tier2_honeypot"       # Local eth_call pre-flight
    TIER1_WARN = "tier1_warn"               # Soft warnings, not hard reject


class SignalStrength(str, Enum):
    """Qualitative label for signal confidence, derived from alpha scoring."""

    STRONG = "strong"       # >80th percentile score
    MODERATE = "moderate"   # 40-80th percentile
    WEAK = "weak"           # <40th percentile; reduces Kelly fraction
