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


class WalletClassification(str, Enum):
    """Classification assigned by the Smart Money Behavioral Profiler."""

    APPROVED = "approved"                     # Passes all criteria, whitelisted
    INSIDER = "insider"                       # Gas/funds trace to deployer/multisig <= 3 hops
    WASH_TRADER = "wash_trader"               # Self-funding or circular volume
    MEV_BOT = "mev_bot"                       # Median holding time < 45 seconds
    LUCKY_OUTLIER = "lucky_outlier"           # >60% PnL from a single lucky trade
    DEAD_REVIVAL = "dead_revival"             # Inactive >45 days, compromised/sold keys
    INSUFFICIENT_HISTORY = "insufficient_history"  # <15 trades or <21 days active
    LOW_WIN_RATE = "low_win_rate"             # Win rate < 55%


class WhitelistStatus(str, Enum):
    """Status in the local SQLite whitelist database."""

    ACTIVE = "active"
    BANNED = "banned"
    SUSPENDED = "suspended"


class NewsSignalStatus(str, Enum):
    """Validation status for news / social scraper signals."""

    VALID = "valid"
    COORDINATED_DUMP = "coordinated_dump"     # >=3 channels in <=15s
    BAIT_AND_SWITCH = "bait_and_switch"       # MessageEdited event injected CA
    DUPLICATE = "duplicate"
    INVALID_CA = "invalid_ca"


class SignalSource(str, Enum):
    """Origin of a generated trading signal."""

    WHALE_WALLET = "whale_wallet"
    TELEGRAM_SCRAPER = "telegram_scraper"
    DEX_SWAP = "dex_swap"


class ExitStage(str, Enum):
    """Lifecycle stages for staged exit execution."""

    NONE = "none"
    TP1 = "tp1"             # 50% sold at +100% gain
    TP2 = "tp2"             # 25% sold at +200% gain
    MOONBAG = "moonbag"     # 25% running position
    SL = "sl"               # Stop loss triggered
    RUGPULL = "rugpull"     # Liquidity drain rug (<$500 pool)


class TradeExitReason(str, Enum):
    """Specific exit condition triggering a sell order."""

    TP_50 = "tp_50"                   # Staged Take-Profit 1 (+100% gain)
    TP_25 = "tp_25"                   # Staged Take-Profit 2 (+200% gain)
    SL_INITIAL = "sl_initial"         # Initial hard stop-loss (-15%)
    SL_TRAILING = "sl_trailing"       # Trailing break-even stop (+5% after +50%)
    RUG_LIQUIDITY_DRAIN = "rug_liquidity_drain"  # Pool reserves drop below $500
    MANUAL_CLOSE = "manual_close"

