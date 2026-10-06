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
    SMART_MONEY = "approved"                  # Passes all criteria, smart money
    INSIDER = "insider"                       # Gas/funds trace to deployer/multisig <= 3 hops
    WASH_TRADER = "wash_trader"               # Self-funding or circular volume
    MEV_BOT = "mev_bot"                       # Median holding time < 45 seconds
    LUCKY_OUTLIER = "lucky_outlier"           # >60% PnL from a single lucky trade
    DEAD_REVIVAL = "dead_revival"             # Inactive >45 days, compromised/sold keys
    INSUFFICIENT_HISTORY = "insufficient_history"  # <15 trades or <21 days active
    LOW_WIN_RATE = "low_win_rate"             # Win rate < 55%
    CIRCULAR_WASH = "circular_wash"           # Circular transaction loop / wash trading


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
    BOT_FARM_FLAGGED = "bot_farm_flagged"     # Flagged by engagement velocity / anti-sybil


class StrategyType(str, Enum):
    """Categorical classification of trading strategy."""

    FAST_SNIPE = "fast_snipe"
    WAVE2_BREAKOUT = "wave2_breakout"
    REVIVAL_SWING = "revival_swing"


class ExitProfile(str, Enum):
    """
    Position exit profile and risk management regime.
    FAST_SNIPE: Short-lived scalps (120s velocity decay, -25% SL, +25%/+50% TP).
    REVIVAL_SWING: Multi-hour/day hold, 35% trailing ATH drawdown, +100%/+200%/+400%/+800% ladder.
    """

    FAST_SNIPE = "FAST_SNIPE"            # Existing 120s velocity decay, -25% SL, +25%/+50% TP
    REVIVAL_SWING = "REVIVAL_SWING"      # Multi-hour/day hold, 35% trailing drawdown, 100%/200% TP ladder


class SignalSource(str, Enum):
    """Origin of a generated trading signal."""

    WHALE_WALLET = "whale_wallet"
    TELEGRAM_SCRAPER = "telegram_scraper"
    DEX_SWAP = "dex_swap"
    X_SENTIMENT = "x_sentiment"
    PUMP_FUN_MINT = "pump_fun_mint"
    PAIR_CREATED = "pair_created"
    DYNAMIC_EXIT = "dynamic_exit"
    WAVE2_BREAKOUT = "wave2_breakout"
    REVIVAL_BREAKOUT = "revival_breakout"


class LotStatus(str, Enum):
    """Monotonic position lifecycle state transitions."""

    PENDING_BUY = "pending_buy"
    OPEN = "open"
    PENDING_SELL = "pending_sell"
    CLOSED = "closed"
    SETTLED = "settled"


class ExitStage(str, Enum):
    """Lifecycle stages for staged exit execution."""

    NONE = "none"
    TP1 = "tp1"             # Scale out 50% (+25% gain or 2x)
    TP2 = "tp2"             # Sell remaining 50% (+50% gain or 3x)
    TP3 = "tp3"             # Incremental sell at 5x
    TP4 = "tp4"             # Incremental sell at 10x
    MOONBAG = "moonbag"     # Running moonbag position
    SL = "sl"               # Stop loss triggered (-8% or -15%)
    TRAILING_SL = "trailing_sl"  # Trailing stop locked
    RUGPULL = "rugpull"     # Liquidity drain rug (<$500 pool or >30% drop)
    TIMEOUT = "timeout"     # Velocity decay timeout


class TradeExitReason(str, Enum):
    """Specific exit condition triggering a sell order."""

    TP_25 = "tp_25"                                # Take-Profit: +25% (Scale out 50%)
    TP_50 = "tp_50"                                # Take-Profit: +50% (Sell remaining 50%)
    TP_2X = "tp_2x"                                # Take Profit Ladder: 50% sold at 2x (+100%)
    TP_3X = "tp_3x"                                # Take Profit Ladder: Incremental sold at 3x
    TP_5X = "tp_5x"                                # Take Profit Ladder: Incremental sold at 5x
    TP_10X = "tp_10x"                              # Take Profit Ladder: Incremental sold at 10x
    SL_INITIAL = "sl_initial"                      # Hard stop-loss (-8% / -15%)
    SL_HARD = "sl_hard"                            # Alias for hard stop-loss
    SL_TRAILING = "sl_trailing"                    # Trailing stop
    TRAILING_PEAK_DRAWDOWN_35PCT = "trailing_peak_drawdown_35pct"  # Multi-day swing: 35% drawdown from ATH peak
    TIMEOUT_VELOCITY_DECAY = "timeout_velocity_decay"  # Duration > 120s and PnL < +3%
    TIMEOUT = "timeout"                            # Alias for timeout decay
    SLIPPAGE_INSOLVENCY = "slippage_insolvency"    # Curve cannot absorb without > 15% price collapse
    RUG_LIQUIDITY_DRAIN = "rug_liquidity_drain"    # Pool reserves drop below $500
    EMERGENCY_DRAIN = "emergency_drain"            # Pool reserves dropped >30% or insolvency collapse
    EMERGENCY_HONEYPOT_MUTATION = "emergency_honeypot_mutation"  # Contract variables mutated post-entry
    MANUAL_CLOSE = "manual_close"


class StrategyHorizon(str, Enum):
    """
    Execution horizon and strategy regime:
    - SHORT_TERM_SCALP: Execution horizon < 15 minutes, sub-second execution,
      dynamic trailing stop-loss (4-7%), stepped take-profit (e.g. 20%, 50%, 100%),
      MEV / private bundle enforcement.
    - LONG_TERM_SWING: Execution horizon > 24 hours to 7 days, DCA entry/exit,
      deep security filtering, token consolidation breakout, social momentum tracking.
    """

    SHORT_TERM_SCALP = "short_term_scalp"
    LONG_TERM_SWING = "long_term_swing"


class ExecutionVenue(str, Enum):
    """
    Supported decentralized exchange execution venues across EVM and SVM.
    """

    UNISWAP_V2 = "uniswap_v2"
    UNISWAP_V3 = "uniswap_v3"
    RAYDIUM_AMM = "raydium_amm"
    RAYDIUM_CPMM = "raydium_cpmm"
    RAYDIUM_CLMM = "raydium_clmm"
    ORCA_WHIRLPOOL = "orca_whirlpool"
    PUMP_FUN = "pump_fun"


