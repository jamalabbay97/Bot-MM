"""
alpha_engine.models.profiler — Behavioral & Cluster Profiler Schemas
===================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Annotated, Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import ChainIdentifier, SwapVenue, WalletClassification, WhitelistStatus


class NormalizedSwap(BaseModel):
    """
    Standardized, venue-agnostic representation of a wallet swap or transfer event.
    Extracted from parsed transactions (Helius SVM, Alchemy EVM, DEX logs).
    """

    model_config = _STRICT_MODEL_CFG

    tx_hash: Annotated[str, Field(min_length=1, max_length=128, description="Transaction hash or signature")]
    block_time: Annotated[int, Field(gt=0, description="Unix timestamp of transaction in seconds")]
    chain: ChainIdentifier = Field(description="Chain identifier")
    wallet_address: Annotated[str, Field(min_length=1, max_length=66, description="Wallet address initiating swap")]
    venue: str = Field(default=SwapVenue.UNKNOWN.value, description="DEX or protocol venue name")
    token_in_address: str = Field(description="Token mint/contract sent by the wallet")
    token_in_symbol: Optional[str] = Field(default=None, description="Symbol of token sent")
    token_in_amount: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Amount of token sent")
    token_out_address: str = Field(description="Token mint/contract received by the wallet")
    token_out_symbol: Optional[str] = Field(default=None, description="Symbol of token received")
    token_out_amount: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Amount of token received")
    fee_native: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Total tx fee in native units (SOL/ETH)")
    is_buy: bool = Field(description="True if buying non-native token with native asset")
    token_address: str = Field(description="Address of target traded non-native token")
    token_amount: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Net token volume swapped")
    native_amount: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Net native volume swapped (SOL/ETH)")
    is_transfer: bool = Field(default=False, description="True if pure token transfer rather than DEX swap")
    custom_metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary custom metadata")


class OpenPositionRecord(BaseModel):
    """
    Represents an open, unrealized position resulting from an unmatched buy lot.
    Strictly excluded from realized PnL and historical win rate calculations.
    """

    model_config = _STRICT_MODEL_CFG

    token_address: str = Field(description="Token contract address")
    amount_remaining: Decimal = Field(gt=Decimal(0), description="Remaining unsold token quantity")
    cost_basis_native: Decimal = Field(ge=Decimal(0), description="Native cost basis including buy fees")
    cost_basis_usd: Decimal = Field(ge=Decimal(0), description="USD cost basis at historical entry price")
    buy_timestamp: int = Field(gt=0, description="Unix timestamp of initial buy lot")
    buy_tx_hash: str = Field(description="Transaction hash of buy lot")
    entry_price_native: Decimal = Field(ge=Decimal(0), description="Entry price in native currency")
    entry_price_usd: Optional[Decimal] = Field(default=None, description="Historical entry price in USD")
    custom_metadata: dict[str, Any] = Field(default_factory=dict, description="Custom metadata")


class TradeReconstructionResult(BaseModel):
    """
    Output of the TradeReconstructor for a target wallet.
    Separates closed round-trip trades from open unrealized positions.
    """

    model_config = _STRICT_MODEL_CFG

    wallet_address: str = Field(description="Target wallet address")
    chain: ChainIdentifier = Field(description="Chain identifier")
    closed_trades: list[WalletTradeRecord] = Field(default_factory=list, description="Closed round-trip trades")
    open_positions: list[OpenPositionRecord] = Field(default_factory=list, description="Currently open positions")
    total_realized_pnl_usd: Decimal = Field(default=Decimal(0), description="Cumulative realized USD PnL")
    total_realized_native_pnl: Decimal = Field(default=Decimal(0), description="Cumulative realized native PnL")
    win_rate_pct: float = Field(default=0.0, description="Realized win rate percentage")
    unmatched_sells_count: int = Field(default=0, ge=0, description="Count of sells with no prior matched buy")
    reconstructed_at_s: int = Field(default_factory=lambda: int(time.time()), description="Reconstruction timestamp")


class WalletTradeRecord(BaseModel):
    """
    Represents an evaluated historical closed trade by a target wallet.
    Used to compute holding time, win rate, and outlier PnL metrics.
    """

    model_config = _STRICT_MODEL_CFG

    token_address: Annotated[str, Field(min_length=32, max_length=66, description="Token contract address")]
    buy_tx_hash: Annotated[str, Field(min_length=32, max_length=90, description="Buy transaction hash")]
    sell_tx_hash: Annotated[str, Field(min_length=32, max_length=90, description="Sell transaction hash")]
    buy_timestamp: Annotated[int, Field(gt=0, description="Unix timestamp in seconds")]
    sell_timestamp: Annotated[int, Field(gt=0, description="Unix timestamp in seconds")]
    holding_time_seconds: Annotated[float, Field(ge=0.0, description="Holding time in seconds")]
    invested_native: Annotated[Decimal, Field(gt=Decimal(0), description="Total invested native currency")]
    realized_native_pnl: Decimal = Field(default=Decimal(0), description="Realized PnL in native units")
    realized_pnl_usd: Decimal = Field(default=Decimal(0), description="Realized PnL in USD")
    roi_pct: float = Field(default=0.0, description="Return on investment percentage")
    entry_price_usd: Optional[Decimal] = Field(default=None, description="Optional entry price in USD")
    exit_price_usd: Optional[Decimal] = Field(default=None, description="Optional exit price in USD")
    custom_metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary custom trade metadata")

    @field_validator("token_address", "buy_tx_hash", "sell_tx_hash", mode="before")
    @classmethod
    def sanitize_hex_string(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip()
        return str(v)

    @model_validator(mode="after")
    def sell_after_buy(self) -> "WalletTradeRecord":
        if self.sell_timestamp < self.buy_timestamp:
            raise ValueError("sell_timestamp must be >= buy_timestamp")
        return self

    @property
    def is_win(self) -> bool:
        return self.realized_pnl_usd > Decimal(0)


class FundingHop(BaseModel):
    """
    Audit record for a step in the initial gas/funds trace of a wallet.
    Traced up to 3 hops backward to identify deployer or shared multi-sig roots.
    """

    model_config = _STRICT_MODEL_CFG

    source_address: Annotated[str, Field(min_length=32, max_length=66, description="Source address")]
    destination_address: Annotated[str, Field(min_length=32, max_length=66, description="Destination address")]
    tx_hash: Annotated[str, Field(min_length=32, max_length=90, description="Transaction hash")]
    hop_depth: Annotated[int, Field(ge=1, le=3, description="Depth of hop (1-3)")]
    amount_native: Decimal = Field(default=Decimal(0), ge=Decimal(0), description="Native currency amount transferred")
    is_deployer: bool = Field(default=False, description="Whether source is contract deployer")
    is_multisig: bool = Field(default=False, description="Whether source is known multi-sig")
    entity_label: Optional[str] = Field(default=None, description="Optional entity label or CEX tag")
    custom_metadata: Optional[dict[str, Any]] = Field(default=None, description="Optional arbitrary metadata")


class InitialTxRecord(BaseModel):
    """
    One of the first 5 transactions of a target wallet.
    Used for wash-trading and self-funding provenance checks.
    """

    model_config = _STRICT_MODEL_CFG

    tx_hash: Annotated[str, Field(min_length=32, max_length=90, description="Transaction hash")]
    block_number: int = Field(ge=0, description="Block number")
    timestamp: int = Field(gt=0, description="Block timestamp in seconds")
    from_address: Annotated[str, Field(min_length=32, max_length=66, description="From address")]
    to_address: Annotated[str, Field(min_length=32, max_length=66, description="To address")]
    value_native: Decimal = Field(ge=Decimal(0), description="Native value transferred")
    gas_used: int = Field(default=0, ge=0, description="Gas units used")
    is_contract_creation: bool = Field(default=False, description="Whether tx created a contract")
    custom_metadata: Optional[dict[str, Any]] = Field(default=None, description="Optional arbitrary metadata")


class WalletProfile(BaseModel):
    """
    Comprehensive quantitative and behavioral profile of a target wallet.
    Determines whether the wallet enters the curated alpha whitelist.
    """

    model_config = _STRICT_MODEL_CFG

    wallet_address: Annotated[str, Field(min_length=32, max_length=66, description="Target wallet address")]
    chain: ChainIdentifier = Field(description="Target chain identifier")
    classification: WalletClassification = Field(description="Behavioral classification")
    is_whitelisted: bool = Field(description="Whether wallet passes all whitelist criteria")
    total_trades: Annotated[int, Field(ge=0, description="Total historical trades analyzed")]
    winning_trades: Annotated[int, Field(ge=0, description="Total winning trades")]
    losing_trades: Annotated[int, Field(ge=0, description="Total losing trades")]
    win_rate_pct: Annotated[float, Field(ge=0.0, le=100.0, description="Win rate percentage")]
    total_pnl_usd: Decimal = Field(description="Total cumulative realized PnL in USD")
    max_single_trade_pnl_usd: Decimal = Field(description="Maximum single trade PnL in USD")
    outlier_pnl_ratio: Annotated[float, Field(ge=0.0, description="Ratio of best trade PnL to total positive PnL")]
    median_holding_time_seconds: Annotated[float, Field(ge=0.0, description="Median holding duration in seconds")]
    active_days: Annotated[float, Field(ge=0.0, description="Total active days")]
    days_since_last_active: Annotated[float, Field(ge=0.0, description="Days elapsed since most recent activity")]
    first_tx_timestamp: int = Field(description="Timestamp of earliest observed transaction")
    last_tx_timestamp: int = Field(description="Timestamp of most recent observed transaction")
    cluster_tag: Optional[str] = Field(default=None, description="Optional cluster or syndicate tag")
    funding_hops: list[FundingHop] = Field(default_factory=list, description="Trace of initial funding hops")
    rejection_reasons: list[str] = Field(default_factory=list, description="Reasons for whitelist rejection if any")
    smart_money_score: float = Field(default=0.0, description="Smart Money Score [0.0, 1.0]")
    sharpe_ratio: float = Field(default=0.0, description="Sharpe ratio metric [0.0, 1.0]")
    holding_discipline: float = Field(default=0.0, description="Holding discipline metric [0.0, 1.0]")
    longevity: float = Field(default=0.0, description="Longevity metric [0.0, 1.0]")
    analyzed_at_ns: int = Field(default_factory=lambda: time.time_ns(), description="Analysis timestamp in nanoseconds")
    custom_metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary custom metadata dict")


class WhitelistRecord(BaseModel):
    """
    Record persisted in the local SQLite whitelist database.
    """

    model_config = _STRICT_MODEL_CFG

    wallet_address: Annotated[str, Field(min_length=32, max_length=66, description="Whitelisted wallet address")]
    chain: ChainIdentifier = Field(description="Chain target")
    classification: WalletClassification = Field(description="Wallet classification")
    status: WhitelistStatus = Field(description="Current whitelist status")
    win_rate_pct: Annotated[float, Field(ge=0.0, le=100.0, description="Historical win rate percentage")]
    total_trades: Annotated[int, Field(ge=0, description="Total trades executed")]
    total_pnl_usd: Decimal = Field(description="Cumulative realized PnL in USD")
    median_holding_time_s: Annotated[float, Field(ge=0.0, description="Median holding duration in seconds")]
    active_days: Annotated[float, Field(ge=0.0, description="Active days on-chain")]
    cluster_tag: Optional[str] = Field(default=None, description="Cluster or syndicate identifier")
    rejection_reasons: str = Field(default="", description="Rejection rationale string")
    smart_money_score: float = Field(default=0.0, description="Smart Money Score [0.0, 1.0]")
    created_at_ns: int = Field(description="Record creation timestamp in nanoseconds")
    updated_at_ns: int = Field(description="Record update timestamp in nanoseconds")
    custom_metadata: Optional[dict[str, Any]] = Field(default=None, description="Custom metadata dict")

    def __getitem__(self, item: str) -> Any:
        val = getattr(self, item)
        if hasattr(val, "value"):
            return val.value
        return val
