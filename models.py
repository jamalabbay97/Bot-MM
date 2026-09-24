"""
models.py — Domain Models, Types, and Event Schemas
=====================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2

All monetary/reserve values use `Decimal` for exact arithmetic.
Timestamps are nanoseconds-since-epoch (int) for sub-millisecond precision.
No floats are used for financial quantities.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from enum import Enum
from typing import Annotated, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ChainIdentifier(str, Enum):
    """Supported chain targets.  Inherits `str` so values serialize cleanly."""

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


# ---------------------------------------------------------------------------
# Shared configuration for all Pydantic models
# ---------------------------------------------------------------------------

_STRICT_MODEL_CFG = ConfigDict(
    frozen=True,                    # Immutable after construction
    arbitrary_types_allowed=True,   # Allows Decimal without annotation games
    str_strip_whitespace=True,
    validate_default=True,
    populate_by_name=True,
)


# ---------------------------------------------------------------------------
# Core Event: Raw on-chain swap detected by the ingestion layer
# ---------------------------------------------------------------------------


class SwapEvent(BaseModel):
    """
    Represents a single DEX swap captured from a chain's event stream.

    Fields
    ------
    timestamp_ns    : Wall-clock nanoseconds at ingestion time (not block time).
    block_number    : Confirmed block / slot number.
    chain           : Which network this swap originated from.
    pool_address    : The liquidity pool contract / account address.
    token_in        : Address (EVM hex or SVM base58) of the sold token.
    token_out       : Address of the bought token.
    amount_in       : Raw token units sent into the pool (pre-fee).
    amount_out      : Raw token units received from the pool (post-fee).
    sender          : Initiating wallet address.
    tx_hash         : Transaction hash (hex string for EVM; base58 sig for SVM).
    log_index       : EVM log index within the block (None for Solana).
    """

    model_config = _STRICT_MODEL_CFG

    timestamp_ns: Annotated[int, Field(gt=0, description="Wall-clock ns at ingestion")]
    block_number: Annotated[int, Field(ge=0)]
    chain: ChainIdentifier
    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    token_in: Annotated[str, Field(min_length=32, max_length=66)]
    token_out: Annotated[str, Field(min_length=32, max_length=66)]
    amount_in: Decimal
    amount_out: Decimal
    sender: Annotated[str, Field(min_length=32, max_length=66)]
    tx_hash: Annotated[str, Field(min_length=32, max_length=90)]
    log_index: Optional[int] = None  # EVM only

    @field_validator("amount_in", "amount_out", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        """Accept int / float / str and coerce; reject negatives."""
        d = Decimal(str(v))
        if d < 0:
            raise ValueError("Swap amounts must be non-negative.")
        return d

    @model_validator(mode="after")
    def token_addresses_differ(self) -> "SwapEvent":
        if self.token_in == self.token_out:
            raise ValueError("token_in and token_out must be distinct addresses.")
        return self


# ---------------------------------------------------------------------------
# Pool State: Live reserve snapshot used by the CPMM math engine
# ---------------------------------------------------------------------------


class PoolState(BaseModel):
    """
    Current on-chain reserve state for a CPMM liquidity pool.

    The invariant  token_reserve * native_reserve = k  must hold
    (modulo fee collection).  All values are in raw token units —
    callers must handle decimals from the token metadata layer.

    Fields
    ------
    pool_address        : Canonical pool identifier.
    chain               : Chain this pool lives on.
    token_reserve       : Reserve of the non-native (quote) token.
    native_reserve      : Reserve of the native / base asset (ETH, SOL).
    fee_numerator       : Numerator of the LP fee fraction (e.g. 3 for 0.3%).
    fee_denominator     : Denominator (e.g. 1000).
    last_updated_block  : Block / slot at which reserves were last refreshed.
    token_decimals      : On-chain decimal places for the non-native token.
    native_decimals     : On-chain decimal places for the native asset (18=ETH, 9=SOL).
    """

    model_config = _STRICT_MODEL_CFG

    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    chain: ChainIdentifier
    token_reserve: Annotated[Decimal, Field(gt=Decimal(0))]
    native_reserve: Annotated[Decimal, Field(gt=Decimal(0))]
    fee_numerator: Annotated[int, Field(ge=0, le=10_000)]
    fee_denominator: Annotated[int, Field(ge=1)]
    last_updated_block: Annotated[int, Field(ge=0)]
    token_decimals: Annotated[int, Field(ge=0, le=18)] = 18
    native_decimals: Annotated[int, Field(ge=0, le=18)] = 18

    @model_validator(mode="after")
    def fee_fraction_sane(self) -> "PoolState":
        fee = Decimal(self.fee_numerator) / Decimal(self.fee_denominator)
        if fee >= Decimal("0.5"):
            raise ValueError(
                f"Fee fraction {fee} >= 0.5; this is almost certainly wrong."
            )
        return self

    @property
    def fee_gamma(self) -> Decimal:
        """gamma = fee expressed as a multiplier (e.g. 0.003 for 0.3% fee)."""
        return Decimal(self.fee_numerator) / Decimal(self.fee_denominator)

    @property
    def spot_price_native_per_token(self) -> Decimal:
        """Marginal spot price: native / token (how much native per 1 token)."""
        return self.native_reserve / self.token_reserve

    @property
    def invariant_k(self) -> Decimal:
        """Constant product k = x * y."""
        return self.token_reserve * self.native_reserve


# ---------------------------------------------------------------------------
# Security Report: Output from the two-tier gatekeeper
# ---------------------------------------------------------------------------


class SecurityReport(BaseModel):
    """
    Aggregated output of the dual-tier security gatekeeper for a token.

    Tier 1 is populated from GoPlus (EVM) or RugCheck (Solana) API calls.
    Tier 2 is populated from local eth_call pre-flight simulation (EVM only).
    Solana Tier 2 defaults to None / inapplicable.

    Fields
    ------
    token_address           : Token being evaluated.
    chain                   : Chain context.
    tier                    : Which tier(s) evaluated and what they concluded.
    is_honeypot             : True if Tier 2 sandbox detected sell revert.
    buy_tax_bps             : Observed buy-side transfer tax in basis points.
    sell_tax_bps            : Observed sell-side transfer tax in basis points.
    lp_burned_ratio         : Fraction of LP tokens sent to dead address [0,1].
    top10_concentration     : Fraction of supply held by top-10 wallets excl. LP [0,1].
    mint_authority_disabled : True only if mint authority is renounced/disabled.
    verified_source_code    : Contract source code verified on-chain explorer.
    external_api_raw        : Raw JSON blob from external API (stored verbatim).
    """

    model_config = _STRICT_MODEL_CFG

    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    chain: ChainIdentifier
    tier: SecurityTier
    is_honeypot: bool
    buy_tax_bps: Annotated[int, Field(ge=0, le=10_000)]
    sell_tax_bps: Annotated[int, Field(ge=0, le=10_000)]
    lp_burned_ratio: Annotated[float, Field(ge=0.0, le=1.0)]
    top10_concentration: Annotated[float, Field(ge=0.0, le=1.0)]
    mint_authority_disabled: bool
    verified_source_code: bool = False
    external_api_raw: Optional[str] = None  # JSON string, not parsed dict

    @property
    def passes_hard_gates(self) -> bool:
        """
        Returns True only if ALL hard-reject criteria are satisfied:
          - Sell tax <= 5% (500 bps)
          - LP burned >= 90%
          - Mint authority is disabled
          - Top-10 holder concentration (excl. LP) <= 20%
          - Not flagged as honeypot
        """
        return (
            self.sell_tax_bps <= 500
            and self.lp_burned_ratio >= 0.90
            and self.mint_authority_disabled
            and self.top10_concentration <= 0.20
            and not self.is_honeypot
        )


# ---------------------------------------------------------------------------
# Paper Fill: Result of the simulated execution engine
# ---------------------------------------------------------------------------


class PaperFill(BaseModel):
    """
    Immutable record of a simulated trade fill produced by the execution engine.

    All values are post-CPMM-math and post-latency-penalty.
    `simulated_gas_cost_usd` is a fixed parametric cost per chain:
      - Solana : $0.03
      - Base   : $0.05

    Fields
    ------
    order_id                : UUID v4 string, unique per fill attempt.
    token_address           : Token contract / mint address.
    pool_address            : Pool used for the simulated fill.
    chain                   : Chain context.
    side                    : BUY or SELL.
    simulated_native_spent  : Native asset spent (BUY) or received (SELL), Decimal.
    tokens_acquired         : Token quantity acquired (BUY) or sold (SELL).
    effective_price         : native_spent / tokens_acquired in native/token units.
    price_impact_bps        : |1 - (execution_price / spot_price)| * 10_000.
    simulated_gas_cost_usd  : Fixed chain-dependent priority fee in USD.
    fill_latency_ms         : Stochastic delay between signal and fill (ms).
    signal_timestamp_ns     : Nanosecond timestamp of the originating signal.
    fill_timestamp_ns       : signal_timestamp_ns + fill_latency_ms * 1_000_000.
    kelly_fraction          : Kelly fraction used for sizing this trade [0, 1].
    portfolio_equity_usd    : Portfolio USD equity at time of sizing decision.
    """

    model_config = _STRICT_MODEL_CFG

    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    chain: ChainIdentifier
    side: OrderSide
    simulated_native_spent: Decimal
    tokens_acquired: Decimal
    effective_price: Decimal
    price_impact_bps: Annotated[int, Field(ge=0)]
    simulated_gas_cost_usd: Annotated[Decimal, Field(ge=Decimal(0))]
    fill_latency_ms: Annotated[int, Field(ge=0)]
    signal_timestamp_ns: Annotated[int, Field(gt=0)]
    fill_timestamp_ns: Annotated[int, Field(gt=0)]
    kelly_fraction: Annotated[float, Field(ge=0.0, le=1.0)]
    portfolio_equity_usd: Annotated[Decimal, Field(gt=Decimal(0))]

    @model_validator(mode="after")
    def fill_after_signal(self) -> "PaperFill":
        if self.fill_timestamp_ns <= self.signal_timestamp_ns:
            raise ValueError("fill_timestamp_ns must be strictly after signal_timestamp_ns.")
        return self

    @model_validator(mode="after")
    def effective_price_consistent(self) -> "PaperFill":
        if self.tokens_acquired > 0:
            recomputed = self.simulated_native_spent / self.tokens_acquired
            if abs(recomputed - self.effective_price) > Decimal("1e-12"):
                raise ValueError(
                    f"effective_price {self.effective_price} inconsistent with "
                    f"simulated_native_spent / tokens_acquired = {recomputed}."
                )
        return self


# ---------------------------------------------------------------------------
# Signal Event: Bridges ingestion layer -> execution layer
# ---------------------------------------------------------------------------


class SignalEvent(BaseModel):
    """
    A trading signal generated after ingestion filtering and security gating.
    Passed via asyncio.Queue to the execution engine.

    Fields
    ------
    signal_id           : UUID v4 string.
    timestamp_ns        : Nanosecond timestamp of signal generation.
    chain               : Origin chain.
    pool_address        : Pool where the opportunity was detected.
    token_address       : Non-native token address.
    suggested_side      : Recommended trade direction.
    trigger_swap        : The SwapEvent that triggered this signal.
    pool_state          : Pool reserves at the moment of signal generation.
    security_report     : Passed security gatekeeper output.
    strength            : Qualitative signal confidence label.
    alpha_score         : Quantitative score [0.0, 1.0] used for Kelly sizing.
    """

    model_config = _STRICT_MODEL_CFG

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: Annotated[int, Field(gt=0)]
    chain: ChainIdentifier
    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    suggested_side: OrderSide
    trigger_swap: SwapEvent
    pool_state: PoolState
    security_report: SecurityReport
    strength: SignalStrength
    alpha_score: Annotated[float, Field(ge=0.0, le=1.0)]


# ---------------------------------------------------------------------------
# Trade Record: Immutable SQLite ledger entry (one per fill)
# ---------------------------------------------------------------------------


class TradeRecord(BaseModel):
    """
    Persisted ledger entry written to the `trades` table in SQLite.
    This is the source-of-truth for all metrics calculations.

    All Decimal fields are stored as TEXT in SQLite to preserve precision.
    """

    model_config = _STRICT_MODEL_CFG

    trade_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str                   # FK -> PaperFill.order_id
    signal_id: str                  # FK -> SignalEvent.signal_id
    chain: ChainIdentifier
    token_address: str
    pool_address: str
    side: OrderSide
    native_spent: Decimal           # positive for BUY, negative for SELL
    tokens_delta: Decimal           # positive for BUY, negative for SELL
    effective_price: Decimal
    price_impact_bps: int
    gas_cost_usd: Decimal
    fill_latency_ms: int
    kelly_fraction: float
    portfolio_equity_usd: Decimal
    realized_pnl_usd: Optional[Decimal] = None   # Set on SELL only
    signal_timestamp_ns: int
    fill_timestamp_ns: int

    @classmethod
    def from_fill(
        cls,
        fill: "PaperFill",
        signal_id: str,
        realized_pnl_usd: Optional[Decimal] = None,
    ) -> "TradeRecord":
        """
        Construct a TradeRecord from a completed PaperFill and its signal ID.
        Signs native_spent and tokens_delta according to trade direction:
          BUY  : native_spent > 0 (outflow), tokens_delta > 0 (inflow)
          SELL : native_spent < 0 (inflow),  tokens_delta < 0 (outflow)
        """
        if fill.side == OrderSide.BUY:
            native_spent = fill.simulated_native_spent
            tokens_delta = fill.tokens_acquired
        else:
            native_spent = -fill.simulated_native_spent
            tokens_delta = -fill.tokens_acquired

        return cls(
            order_id=fill.order_id,
            signal_id=signal_id,
            chain=fill.chain,
            token_address=fill.token_address,
            pool_address=fill.pool_address,
            side=fill.side,
            native_spent=native_spent,
            tokens_delta=tokens_delta,
            effective_price=fill.effective_price,
            price_impact_bps=fill.price_impact_bps,
            gas_cost_usd=fill.simulated_gas_cost_usd,
            fill_latency_ms=fill.fill_latency_ms,
            kelly_fraction=fill.kelly_fraction,
            portfolio_equity_usd=fill.portfolio_equity_usd,
            realized_pnl_usd=realized_pnl_usd,
            signal_timestamp_ns=fill.signal_timestamp_ns,
            fill_timestamp_ns=fill.fill_timestamp_ns,
        )


# ---------------------------------------------------------------------------
# Portfolio Snapshot: Periodic valuation checkpoint written to SQLite
# ---------------------------------------------------------------------------


class PortfolioSnapshot(BaseModel):
    """
    Point-in-time snapshot of portfolio state.
    Written periodically (e.g., every 60 s) and after every trade.

    Fields
    ------
    snapshot_id         : UUID v4 string.
    timestamp_ns        : Wall-clock nanosecond timestamp.
    total_equity_usd    : Total mark-to-market portfolio value in USD.
    cash_native_sol     : Undeployed SOL balance.
    cash_native_eth     : Undeployed ETH balance (on Base).
    open_positions      : Number of currently open token positions.
    realized_pnl_usd    : Cumulative realized P&L since inception.
    unrealized_pnl_usd  : Current mark-to-market unrealized P&L.
    max_drawdown_pct    : Maximum drawdown % since inception (0-100).
    win_rate_pct        : Win rate % across all closed trades since inception.
    profit_factor       : Gross profit / Gross loss (inf if no losses).
    total_gas_spent_usd : Cumulative simulated gas/fees paid since inception.
    """

    model_config = _STRICT_MODEL_CFG

    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: Annotated[int, Field(gt=0)]
    total_equity_usd: Annotated[Decimal, Field(ge=Decimal(0))]
    cash_native_sol: Annotated[Decimal, Field(ge=Decimal(0))]
    cash_native_eth: Annotated[Decimal, Field(ge=Decimal(0))]
    open_positions: Annotated[int, Field(ge=0)]
    realized_pnl_usd: Decimal
    unrealized_pnl_usd: Decimal
    max_drawdown_pct: Annotated[float, Field(ge=0.0, le=100.0)]
    win_rate_pct: Annotated[float, Field(ge=0.0, le=100.0)]
    profit_factor: Annotated[float, Field(ge=0.0)]
    total_gas_spent_usd: Annotated[Decimal, Field(ge=Decimal(0))]


# ---------------------------------------------------------------------------
# Sentinel: Typed poison-pill for graceful asyncio.Queue shutdown
# ---------------------------------------------------------------------------


class ShutdownSentinel(BaseModel):
    """
    Enqueued into every asyncio.Queue to signal consumers to drain and exit.
    Identified by its type; no fields required.
    """

    model_config = ConfigDict(frozen=True)
    reason: str = "graceful_shutdown"
