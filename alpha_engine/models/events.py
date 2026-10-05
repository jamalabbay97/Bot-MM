"""
alpha_engine.models.events — Pipeline Event Schemas
===================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from typing import Any,Annotated, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    NewsSignalStatus,
    OrderSide,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.state import PoolState, SecurityReport


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
    amount_in       : Decimal-normalised token units sent into the pool (pre-fee).
    amount_out      : Decimal-normalised token units received from the pool (post-fee).
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


class SignalEvent(BaseModel):
    """
    A trading signal generated after ingestion filtering and security gating.
    Passed via asyncio.Queue to the execution engine.
    """

    model_config = _STRICT_MODEL_CFG

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: Annotated[int, Field(gt=0)]
    chain: ChainIdentifier
    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    suggested_side: OrderSide
    trigger_swap: Optional[SwapEvent] = None
    pool_state: PoolState
    security_report: SecurityReport
    strength: SignalStrength
    alpha_score: Annotated[float, Field(ge=0.0, le=1.0)]
    source: SignalSource = SignalSource.DEX_SWAP
    narrative_cluster: Optional[str] = Field(default=None, description="Theme cluster: ai, dog, cat, political, utility, etc.")
    exit_profile: ExitProfile = Field(default=ExitProfile.FAST_SNIPE, description="Associated exit profile (FAST_SNIPE vs REVIVAL_SWING)")
    base_market_cap: Optional[Decimal] = Field(default=None, description="Market cap at the consolidation base")
    strategy_pattern: Optional[str] = Field(default=None, description="Strategy pattern: fast_snipe, wave2_breakout, revival_breakout")
    token_age_hours: Optional[float] = Field(default=None, description="Token age in hours at signal trigger")
    consolidation_length_hours: Optional[float] = Field(default=None, description="Consolidation duration in hours")
    volume_surge_multiplier: Optional[float] = Field(default=None, description="Volume surge over SMA")
    net_buy_delta: Optional[float] = Field(default=None, description="Net buy volume ratio")

    @property
    def signal_source(self) -> SignalSource:
        return self.source


class PoolStateUpdateEvent(BaseModel):
    """
    Emitted by the EVM ingester when a Uniswap v2 / Aerodrome `Sync` log
    is decoded. Carries a freshly normalised PoolState so the engine's
    PoolRegistry never operates on stale liquidity.
    """

    model_config = _STRICT_MODEL_CFG

    timestamp_ns: Annotated[int, Field(gt=0)]
    chain: ChainIdentifier
    pool_address: Annotated[str, Field(min_length=32, max_length=66)]
    new_pool_state: PoolState


class ShutdownSentinel(BaseModel):
    """
    Enqueued into every asyncio.Queue to signal consumers to drain and exit.
    Identified by its type; no fields required.
    """

    model_config = ConfigDict(frozen=True)
    reason: str = "graceful_shutdown"


class RawSignalEvent(BaseModel):
    """
    Raw signal emitted by external scrapers (e.g., Telegram / social sentiment scraper)
    prior to pool-lookup and two-tier security gating.
    """

    model_config = _STRICT_MODEL_CFG

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: Annotated[int, Field(gt=0)] = Field(default_factory=lambda: time.time_ns())
    chain: ChainIdentifier
    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    pool_address: Optional[str] = None
    source: SignalSource = SignalSource.TELEGRAM_SCRAPER
    originating_channel: str = ""
    channel_id: int = 0
    message_id: int = 0
    raw_text: str = ""
    status: NewsSignalStatus = NewsSignalStatus.VALID
    sybil_channel_count: Annotated[int, Field(ge=1)] = 1
    is_edit: bool = False
    narrative_cluster: Optional[str] = Field(default=None, description="Theme cluster: ai, dog, cat, political, utility, etc.")


class PumpMintEvent(BaseModel):
    """
    Event emitted when a Pump.fun mint / bonding curve creation is detected.
    """

    model_config = _STRICT_MODEL_CFG

    timestamp_ns: Annotated[int, Field(gt=0)] = Field(default_factory=lambda: time.time_ns())
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET
    mint: Annotated[str, Field(min_length=32, max_length=66)]
    bonding_curve: Annotated[str, Field(min_length=32, max_length=66)]
    virtual_sol_reserves: Decimal = Decimal("30.0")
    virtual_token_reserves: Decimal = Decimal("1073000000.0")
    token_decimals: int = 6
    native_decimals: int = 9
    tx_hash: str = ""
    slot: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if args and isinstance(args[0], dict):
            d = dict(args[0])
            d.update(kwargs)
            super().__init__(**d)
        else:
            super().__init__(**kwargs)

    @property
    def token_address(self) -> str:
        return self.mint

    @property
    def pool_address(self) -> str:
        return self.bonding_curve

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def __iter__(self):
        yield self.mint
        yield self.virtual_sol_reserves


class PumpSwapEvent(SwapEvent):
    """
    Event emitted when a Pump.fun swap / trade transaction is detected.
    Subclasses SwapEvent for backwards compatibility while providing
    dedicated Pump.fun attributes and helpers.
    """

    mint: str = ""
    sol_amount: Decimal = Decimal("0")
    token_amount: Decimal = Decimal("0")
    buyer: str = ""
    is_buy: bool = True
    virtual_sol_reserves: Decimal = Decimal("30.0")
    virtual_token_reserves: Decimal = Decimal("1073000000.0")
    has_authoritative_reserves: bool = False
    slot: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if args and isinstance(args[0], dict):
            d = dict(args[0])
            d.update(kwargs)
            if "tx_hash" in d and len(str(d["tx_hash"])) < 32:
                d["tx_hash"] = str(d["tx_hash"]).ljust(32, "0")
            super().__init__(**d)
        else:
            if "tx_hash" in kwargs and len(str(kwargs["tx_hash"])) < 32:
                kwargs["tx_hash"] = str(kwargs["tx_hash"]).ljust(32, "0")
            super().__init__(**kwargs)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def __iter__(self):
        yield self.mint
        yield self.sol_amount
        yield self.token_amount
        yield self.buyer

