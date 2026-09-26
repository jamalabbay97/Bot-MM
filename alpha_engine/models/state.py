"""
alpha_engine.models.state — Core Domain Entities & Ledger State
===============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Annotated, Optional

from pydantic import (
    BaseModel,
    Field,
    model_validator,
)

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    OrderSide,
    SecurityTier,
    TradeExitReason,
)


class PoolState(BaseModel):
    """
    Current on-chain reserve state for a CPMM liquidity pool.

    The invariant  token_reserve * native_reserve = k  must hold
    (modulo fee collection). All values are in raw token units —
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


class PaperFill(BaseModel):
    """
    Immutable record of a simulated trade fill produced by the execution engine.

    All values are post-CPMM-math and post-latency-penalty.
    `simulated_gas_cost_usd` is a fixed parametric cost per chain:
      - Solana : $0.03
      - Base   : $0.05
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
    exit_reason: Optional[TradeExitReason] = None
    exit_stage: Optional[ExitStage] = None
    signal_timestamp_ns: int
    fill_timestamp_ns: int

    @classmethod
    def from_fill(
        cls,
        fill: "PaperFill",
        signal_id: str,
        realized_pnl_usd: Optional[Decimal] = None,
        exit_reason: Optional[TradeExitReason] = None,
        exit_stage: Optional[ExitStage] = None,
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
            exit_reason=exit_reason,
            exit_stage=exit_stage,
            signal_timestamp_ns=fill.signal_timestamp_ns,
            fill_timestamp_ns=fill.fill_timestamp_ns,
        )


class OpenPositionLot(BaseModel):
    """
    Tracks an active open position lot undergoing staged exit monitoring.
    """

    model_config = _STRICT_MODEL_CFG

    lot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    token_address: str
    pool_address: str
    chain: ChainIdentifier
    initial_tokens: Decimal
    remaining_tokens: Decimal
    entry_native_spent: Decimal
    entry_price: Decimal
    entry_timestamp_ns: int
    highest_price_observed: Decimal
    current_stage: ExitStage = ExitStage.NONE
    tp1_sold: bool = False
    tp2_sold: bool = False
    trailing_sl_price: Decimal


class ExitOrder(BaseModel):
    """
    Simulated sell order triggered by the autonomous exit engine.
    """

    model_config = _STRICT_MODEL_CFG

    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    lot_id: str
    token_address: str
    pool_address: str
    chain: ChainIdentifier
    tokens_to_sell: Decimal
    reason: TradeExitReason
    stage: ExitStage
    timestamp_ns: int


class PortfolioSnapshot(BaseModel):
    """
    Point-in-time snapshot of portfolio state.
    Written periodically (e.g., every 60 s) and after every trade.
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
    sharpe_ratio: float = 0.0
    win_loss_ratio: float = 0.0
    total_gas_spent_usd: Annotated[Decimal, Field(ge=Decimal(0))]

