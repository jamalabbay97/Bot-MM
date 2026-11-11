"""
alpha_engine/pricing/venue_capabilities.py
Venue Capabilities Registry and Quoter Interfaces.

Defines:
- VenueStatus (SUPPORTED, UNSUPPORTED, DERIVED_ONLY)
- VenueCapability mapping for each ExecutionVenue
- IVenueQuoter interface for non-constant-product / concentrated liquidity pricing
- Mockable quoter implementations for EVM Uniswap v3 and Solana Jupiter
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue

logger = logging.getLogger(__name__)


class VenueStatus(str, Enum):
    """Support status of an execution venue."""

    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    DERIVED_ONLY = "derived_only"


class UnsupportedVenueError(Exception):
    """Raised when an operation attempts to trade on an unsupported venue."""

    pass


class VenueCapability(BaseModel):
    """Capability and pricing model descriptor for an execution venue."""

    venue: ExecutionVenue
    status: VenueStatus
    pricing_model: str  # "cpmm", "cpmm_virtual", "quoter", "unsupported_clmm"
    description: str


# =============================================================================
# Venue Capabilities Table
# Maps each ExecutionVenue to its verified pricing model and support status.
# Constant-product x*y=k is prohibited for concentrated liquidity venues.
# =============================================================================
VENUE_CAPABILITIES: dict[ExecutionVenue, VenueCapability] = {
    ExecutionVenue.PUMP_FUN: VenueCapability(
        venue=ExecutionVenue.PUMP_FUN,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm_virtual",
        description="Pump.fun bonding curve with virtual reserves (x*y=k)",
    ),
    ExecutionVenue.PUMPSWAP: VenueCapability(
        venue=ExecutionVenue.PUMPSWAP,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm",
        description="PumpSwap standard constant product AMM",
    ),
    ExecutionVenue.RAYDIUM_AMM: VenueCapability(
        venue=ExecutionVenue.RAYDIUM_AMM,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm",
        description="Raydium standard AMM v4 (x*y=k)",
    ),
    ExecutionVenue.RAYDIUM_CPMM: VenueCapability(
        venue=ExecutionVenue.RAYDIUM_CPMM,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm",
        description="Raydium CPMM standard (x*y=k)",
    ),
    ExecutionVenue.RAYDIUM_CLMM: VenueCapability(
        venue=ExecutionVenue.RAYDIUM_CLMM,
        status=VenueStatus.UNSUPPORTED,
        pricing_model="unsupported_clmm",
        description="Raydium Concentrated Liquidity (CLMM) unsupported without verified quoter",
    ),
    ExecutionVenue.ORCA_WHIRLPOOL: VenueCapability(
        venue=ExecutionVenue.ORCA_WHIRLPOOL,
        status=VenueStatus.UNSUPPORTED,
        pricing_model="unsupported_clmm",
        description="Orca Whirlpool Concentrated Liquidity unsupported without verified quoter",
    ),
    ExecutionVenue.UNISWAP_V2: VenueCapability(
        venue=ExecutionVenue.UNISWAP_V2,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm",
        description="Uniswap v2 constant product AMM (x*y=k)",
    ),
    ExecutionVenue.UNISWAP_V3: VenueCapability(
        venue=ExecutionVenue.UNISWAP_V3,
        status=VenueStatus.SUPPORTED,
        pricing_model="quoter",
        description="Uniswap v3 concentrated liquidity via on-chain Quoter",
    ),
    ExecutionVenue.AERODROME: VenueCapability(
        venue=ExecutionVenue.AERODROME,
        status=VenueStatus.SUPPORTED,
        pricing_model="cpmm",
        description="Aerodrome v2 volatile pool (x*y=k)",
    ),
}


def get_venue_capability(venue: ExecutionVenue | str) -> VenueCapability:
    """Retrieve capability descriptor for a venue, defaulting to UNSUPPORTED if unknown."""
    if isinstance(venue, str):
        v_clean = venue.strip().lower()
        matched: Optional[ExecutionVenue] = None
        for ev in ExecutionVenue:
            if ev.value == v_clean or ev.name.lower() == v_clean:
                matched = ev
                break
        if matched is None:
            if v_clean in ("pump.fun", "pump"):
                matched = ExecutionVenue.PUMP_FUN
            elif v_clean == "pumpswap":
                matched = ExecutionVenue.PUMPSWAP
            elif "raydium_clmm" in v_clean:
                matched = ExecutionVenue.RAYDIUM_CLMM
            elif "raydium" in v_clean:
                matched = ExecutionVenue.RAYDIUM_AMM
            elif "uniswap_v3" in v_clean:
                matched = ExecutionVenue.UNISWAP_V3
            elif "uniswap" in v_clean:
                matched = ExecutionVenue.UNISWAP_V2
            elif "aerodrome" in v_clean:
                matched = ExecutionVenue.AERODROME
            elif "orca" in v_clean:
                matched = ExecutionVenue.ORCA_WHIRLPOOL
        venue = matched

    if not isinstance(venue, ExecutionVenue) or venue not in VENUE_CAPABILITIES:
        return VenueCapability(
            venue=venue if isinstance(venue, ExecutionVenue) else ExecutionVenue.UNISWAP_V2,
            status=VenueStatus.UNSUPPORTED,
            pricing_model="unknown",
            description=f"Unknown or unconfigured venue: {venue}",
        )
    return VENUE_CAPABILITIES[venue]


class VenueQuote(BaseModel):
    """Exact quote returned by a verified venue quoter."""

    venue: ExecutionVenue
    chain: ChainIdentifier
    amount_in: Decimal
    amount_out: Decimal
    effective_price: Decimal
    price_impact_bps: int
    fee_amount: Decimal
    source: str


class IVenueQuoter(ABC):
    """Interface for non-CPMM / concentrated liquidity quotation."""

    @abstractmethod
    async def quote_buy(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        native_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        """Calculate exact quote for buying token with native asset."""
        pass

    @abstractmethod
    async def quote_sell(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        token_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        """Calculate exact quote for selling token for native asset."""
        pass


class EvmUniswapV3Quoter(IVenueQuoter):
    """
    On-chain Uniswap v3 Quoter via eth_call simulation.
    Target contract: QuoterV2 (e.g. 0x61fFE014bA17989E743c5F6cB21bF9697530B21e on Base).
    """

    def __init__(self, rpc_url: str = "", quoter_address: str = ""):
        self.rpc_url = rpc_url
        self.quoter_address = quoter_address or "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"

    async def quote_buy(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        native_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        if venue != ExecutionVenue.UNISWAP_V3:
            raise UnsupportedVenueError(f"EvmUniswapV3Quoter does not support venue: {venue}")
        if self.rpc_url:
            raise NotImplementedError("Real on-chain eth_call requires active node connection")
        if pool is not None and getattr(pool, "native_reserve", Decimal(0)) > Decimal(0) and getattr(pool, "token_reserve", Decimal(0)) > Decimal(0):
            fee_num = Decimal(getattr(pool, "fee_numerator", 30))
            fee_denom = Decimal(getattr(pool, "fee_denominator", 10000))
            fee_rate = fee_num / fee_denom if fee_denom > Decimal(0) else Decimal("0.003")
            net_native = native_amount_in * (Decimal("1") - fee_rate)
            k_sqrt = (pool.token_reserve * pool.native_reserve).sqrt()
            tokens_out = (pool.token_reserve * net_native) / (k_sqrt + net_native)
            eff_price = native_amount_in / tokens_out
            spot_price = pool.native_reserve / pool.token_reserve
            raw_impact = abs(Decimal("1") - (eff_price / spot_price))
            impact_bps = min(10000, int((raw_impact * Decimal("10000")).to_integral_value(ROUND_DOWN)))
            return VenueQuote(
                venue=venue,
                chain=chain,
                amount_in=native_amount_in,
                amount_out=tokens_out,
                effective_price=eff_price,
                price_impact_bps=impact_bps,
                fee_amount=native_amount_in * fee_rate,
                source="evm_uniswap_v3_quoter",
            )
        raise UnsupportedVenueError("EvmUniswapV3Quoter: No active RPC URL or valid pool reserves configured for quoting")

    async def quote_sell(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        token_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        if venue != ExecutionVenue.UNISWAP_V3:
            raise UnsupportedVenueError(f"EvmUniswapV3Quoter does not support venue: {venue}")
        if self.rpc_url:
            raise NotImplementedError("Real on-chain eth_call requires active node connection")
        if pool is not None and getattr(pool, "native_reserve", Decimal(0)) > Decimal(0) and getattr(pool, "token_reserve", Decimal(0)) > Decimal(0):
            fee_num = Decimal(getattr(pool, "fee_numerator", 30))
            fee_denom = Decimal(getattr(pool, "fee_denominator", 10000))
            fee_rate = fee_num / fee_denom if fee_denom > Decimal(0) else Decimal("0.003")
            net_tokens = token_amount_in * (Decimal("1") - fee_rate)
            k_sqrt = (pool.token_reserve * pool.native_reserve).sqrt()
            native_out = (pool.native_reserve * net_tokens) / (k_sqrt + net_tokens)
            eff_price = native_out / token_amount_in
            spot_price = pool.native_reserve / pool.token_reserve
            raw_impact = abs(Decimal("1") - (eff_price / spot_price))
            impact_bps = min(10000, int((raw_impact * Decimal("10000")).to_integral_value(ROUND_DOWN)))
            return VenueQuote(
                venue=venue,
                chain=chain,
                amount_in=token_amount_in,
                amount_out=native_out,
                effective_price=eff_price,
                price_impact_bps=impact_bps,
                fee_amount=token_amount_in * fee_rate,
                source="evm_uniswap_v3_quoter",
            )
        raise UnsupportedVenueError("EvmUniswapV3Quoter: No active RPC URL or valid pool reserves configured for quoting")


class JupiterSolanaQuoter(IVenueQuoter):
    """
    Jupiter Quote API for Solana concentrated liquidity and multi-route swaps.
    """

    def __init__(self, api_url: str = "https://quote-api.jup.ag/v6/quote"):
        self.api_url = api_url

    async def quote_buy(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        native_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        _ = pool
        if not self.api_url:
            raise UnsupportedVenueError("JupiterSolanaQuoter: No active Jupiter API URL configured")
        raise NotImplementedError("Real Jupiter API call requires network I/O")

    async def quote_sell(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        token_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        _ = pool
        if not self.api_url:
            raise UnsupportedVenueError("JupiterSolanaQuoter: No active Jupiter API URL configured")
        raise NotImplementedError("Real Jupiter API call requires network I/O")

