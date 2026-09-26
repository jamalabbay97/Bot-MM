"""
alpha_engine.models.profiler — Behavioral & Cluster Profiler Schemas
===================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import ChainIdentifier, WalletClassification, WhitelistStatus


class WalletTradeRecord(BaseModel):
    """
    Represents an evaluated historical closed trade by a target wallet.
    Used to compute holding time, win rate, and outlier PnL metrics.
    """

    model_config = _STRICT_MODEL_CFG

    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    buy_tx_hash: Annotated[str, Field(min_length=32, max_length=90)]
    sell_tx_hash: Annotated[str, Field(min_length=32, max_length=90)]
    buy_timestamp: Annotated[int, Field(gt=0, description="Unix timestamp in seconds")]
    sell_timestamp: Annotated[int, Field(gt=0, description="Unix timestamp in seconds")]
    holding_time_seconds: Annotated[float, Field(ge=0.0)]
    invested_native: Annotated[Decimal, Field(gt=Decimal(0))]
    realized_native_pnl: Decimal
    realized_pnl_usd: Decimal
    roi_pct: float

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

    source_address: Annotated[str, Field(min_length=32, max_length=66)]
    destination_address: Annotated[str, Field(min_length=32, max_length=66)]
    tx_hash: Annotated[str, Field(min_length=32, max_length=90)]
    hop_depth: Annotated[int, Field(ge=1, le=3)]
    amount_native: Decimal = Decimal(0)
    is_deployer: bool = False
    is_multisig: bool = False
    entity_label: Optional[str] = None


class InitialTxRecord(BaseModel):
    """
    One of the first 5 transactions of a target wallet.
    Used for wash-trading and self-funding provenance checks.
    """

    model_config = _STRICT_MODEL_CFG

    tx_hash: Annotated[str, Field(min_length=32, max_length=90)]
    block_number: int
    timestamp: int
    from_address: Annotated[str, Field(min_length=32, max_length=66)]
    to_address: Annotated[str, Field(min_length=32, max_length=66)]
    value_native: Decimal
    gas_used: int = 0
    is_contract_creation: bool = False


class WalletProfile(BaseModel):
    """
    Comprehensive quantitative and behavioral profile of a target wallet.
    Determines whether the wallet enters the curated alpha whitelist.
    """

    model_config = _STRICT_MODEL_CFG

    wallet_address: Annotated[str, Field(min_length=32, max_length=66)]
    chain: ChainIdentifier
    classification: WalletClassification
    is_whitelisted: bool
    total_trades: Annotated[int, Field(ge=0)]
    winning_trades: Annotated[int, Field(ge=0)]
    losing_trades: Annotated[int, Field(ge=0)]
    win_rate_pct: Annotated[float, Field(ge=0.0, le=100.0)]
    total_pnl_usd: Decimal
    max_single_trade_pnl_usd: Decimal
    outlier_pnl_ratio: Annotated[float, Field(ge=0.0)]
    median_holding_time_seconds: Annotated[float, Field(ge=0.0)]
    active_days: Annotated[float, Field(ge=0.0)]
    days_since_last_active: Annotated[float, Field(ge=0.0)]
    first_tx_timestamp: int
    last_tx_timestamp: int
    cluster_tag: Optional[str] = None
    funding_hops: list[FundingHop] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    analyzed_at_ns: int = Field(default_factory=lambda: time.time_ns())


class WhitelistRecord(BaseModel):
    """
    Record persisted in the local SQLite whitelist database.
    """

    model_config = _STRICT_MODEL_CFG

    wallet_address: str
    chain: ChainIdentifier
    classification: WalletClassification
    status: WhitelistStatus
    win_rate_pct: float
    total_trades: int
    total_pnl_usd: Decimal
    median_holding_time_s: float
    active_days: float
    cluster_tag: Optional[str] = None
    rejection_reasons: str = ""
    created_at_ns: int
    updated_at_ns: int
