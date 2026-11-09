"""
alpha_engine.pricing.native_price_service — Unified Live Native Price Service
=============================================================================
Single source of truth for native asset prices (SOL, ETH) in USD.
Provides:
- Primary public source (DefiLlama / MarketOracle)
- Secondary independent public source (Coinbase / Binance)
- TTL caching and configurable max_age_s staleness checks
- Cross-source divergence check with configurable tolerance percentage
- Asynchronous background polling task managed in runner lifecycle
- Fail-closed gating policy (reject new entries on STALE, DIVERGENT, UNAVAILABLE)
- Full support for explicit ALLOW_STATIC_NATIVE_PRICES test/bootstrap mode
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Any, Optional, Union

import aiohttp

from alpha_engine.models.enums import ChainIdentifier

logger = logging.getLogger(__name__)

SOLANA_MINT_WRAPPED_SOL = "So11111111111111111111111111111111111111112"
BASE_WETH_ADDRESS = "0x4200000000000000000000000000000000000006"


class NativePriceStatus(str, Enum):
    OK = "OK"
    STALE = "STALE"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class PriceResult(tuple):
    """
    3-tuple (price, age_s, status) with additional metadata attributes.
    Supports unpacking directly as:
        price, age_s, status = service.get_price(chain)
    """

    def __new__(
        cls,
        price: Decimal,
        age_s: float,
        status: NativePriceStatus,
        source: str = "unknown",
        secondary_price: Optional[Decimal] = None,
        divergence_pct: Optional[float] = None,
    ):
        instance = super().__new__(cls, (price, age_s, status))
        instance.price = price
        instance.age_s = age_s
        instance.status = status
        instance.source = source
        instance.secondary_price = secondary_price
        instance.divergence_pct = divergence_pct
        return instance

    @property
    def is_valid(self) -> bool:
        return self.status == NativePriceStatus.OK


class BasePriceSource(ABC):
    """Abstract interface for native asset price providers."""

    name: str = "base"

    @abstractmethod
    async def fetch_prices(self, session: Optional[aiohttp.ClientSession] = None) -> dict[str, Decimal]:
        """
        Fetch spot prices in USD.
        Must return mapping: {'solana': Decimal, 'base_eth': Decimal} (or subset).
        """
        raise NotImplementedError


class DefiLlamaPriceSource(BasePriceSource):
    """Primary price source querying DefiLlama coins public API."""

    name: str = "DefiLlama"
    ENDPOINT: str = "https://coins.llama.fi/prices/current"

    async def fetch_prices(self, session: Optional[aiohttp.ClientSession] = None) -> dict[str, Decimal]:
        coins_query = f"solana:{SOLANA_MINT_WRAPPED_SOL},coingecko:ethereum"
        url = f"{self.ENDPOINT}/{coins_query}"
        headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}

        owns_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0))
            owns_session = True

        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"DefiLlama API returned HTTP {resp.status}")
                data = await resp.json()
                coins = data.get("coins", {})
                out: dict[str, Decimal] = {}

                sol_key = f"solana:{SOLANA_MINT_WRAPPED_SOL}"
                if sol_key in coins and "price" in coins[sol_key]:
                    out["solana"] = Decimal(str(coins[sol_key]["price"]))

                eth_key = "coingecko:ethereum"
                if eth_key in coins and "price" in coins[eth_key]:
                    out["base_eth"] = Decimal(str(coins[eth_key]["price"]))

                if not out:
                    raise RuntimeError("DefiLlama returned empty coins dictionary")
                return out
        finally:
            if owns_session and not session.closed:
                await session.close()


class CoinbasePriceSource(BasePriceSource):
    """Secondary price source querying Coinbase spot price public REST API."""

    name: str = "Coinbase"
    SOL_URL: str = "https://api.coinbase.com/v2/prices/SOL-USD/spot"
    ETH_URL: str = "https://api.coinbase.com/v2/prices/ETH-USD/spot"

    async def fetch_prices(self, session: Optional[aiohttp.ClientSession] = None) -> dict[str, Decimal]:
        headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
        owns_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0))
            owns_session = True

        out: dict[str, Decimal] = {}
        try:
            # Query SOL
            async with session.get(self.SOL_URL, headers=headers) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    amt = payload.get("data", {}).get("amount")
                    if amt:
                        out["solana"] = Decimal(str(amt))

            # Query ETH
            async with session.get(self.ETH_URL, headers=headers) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    amt = payload.get("data", {}).get("amount")
                    if amt:
                        out["base_eth"] = Decimal(str(amt))

            if not out:
                raise RuntimeError("Coinbase spot queries returned no prices")
            return out
        finally:
            if owns_session and not session.closed:
                await session.close()


class BinancePriceSource(BasePriceSource):
    """Alternative secondary price source querying Binance public ticker REST API."""

    name: str = "Binance"
    URL: str = "https://api.binance.com/api/v3/ticker/price"

    async def fetch_prices(self, session: Optional[aiohttp.ClientSession] = None) -> dict[str, Decimal]:
        headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
        owns_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0))
            owns_session = True

        out: dict[str, Decimal] = {}
        try:
            async with session.get(f"{self.URL}?symbol=SOLUSDT", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    p = data.get("price")
                    if p:
                        out["solana"] = Decimal(str(p))

            async with session.get(f"{self.URL}?symbol=ETHUSDT", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    p = data.get("price")
                    if p:
                        out["base_eth"] = Decimal(str(p))

            if not out:
                raise RuntimeError("Binance price ticker returned no prices")
            return out
        finally:
            if owns_session and not session.closed:
                await session.close()


class NativePriceService:
    """
    Unified, single source of truth for native asset prices.
    Manages dual-source fetching, caching, divergence validation, staleness checks,
    and automatic background refresh task during runner lifecycle.
    """

    def __init__(
        self,
        primary_source: Optional[BasePriceSource] = None,
        secondary_source: Optional[BasePriceSource] = None,
        refresh_interval_s: float = 15.0,
        cache_ttl_s: float = 15.0,
        max_age_s: float = 60.0,
        divergence_tolerance_pct: float = 5.0,
        allow_static_prices: bool = False,
        static_sol_price: Optional[Decimal] = None,
        static_eth_price: Optional[Decimal] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.primary_source: BasePriceSource = primary_source or DefiLlamaPriceSource()
        self.secondary_source: BasePriceSource = secondary_source or CoinbasePriceSource()
        self.refresh_interval_s: float = refresh_interval_s
        self.cache_ttl_s: float = cache_ttl_s
        self.max_age_s: float = max_age_s
        self.divergence_tolerance_pct: float = divergence_tolerance_pct
        self.allow_static_prices: bool = allow_static_prices
        self.static_sol_price: Optional[Decimal] = static_sol_price
        self.static_eth_price: Optional[Decimal] = static_eth_price

        self._session = session
        self._owns_session = False
        self._lock = asyncio.Lock()
        self._running = False
        self._refresh_task: Optional[asyncio.Task[None]] = None

        # State storage: key -> (price: Decimal, updated_at: float, status: NativePriceStatus, source: str)
        self._prices: dict[str, tuple[Decimal, float, NativePriceStatus, str, Optional[Decimal], Optional[float]]] = {}
        self._cache_timestamp: dict[str, float] = {}
        self._active_source: dict[str, str] = {}
        self._last_logged_status: dict[str, NativePriceStatus] = {}

    def _normalize_chain(self, chain: Union[ChainIdentifier, str]) -> str:
        chain_str = chain.value if hasattr(chain, "value") else str(chain).lower()
        if "sol" in chain_str:
            return "solana"
        if "base" in chain_str or "eth" in chain_str or "evm" in chain_str:
            return "base_eth"
        return "solana"

    def set_price(
        self,
        chain: Union[ChainIdentifier, str],
        price: Decimal,
        status: NativePriceStatus = NativePriceStatus.OK,
        source: str = "manual",
    ) -> None:
        """Manual injection for tests and overrides."""
        key = self._normalize_chain(chain)
        now = time.time()
        self._prices[key] = (price, now, status, source, None, None)
        self._cache_timestamp[key] = now
        self._active_source[key] = source

    def get_active_source(self, chain: Union[ChainIdentifier, str]) -> str:
        key = self._normalize_chain(chain)
        return self._active_source.get(key, "none")

    def get_price(self, chain: Union[ChainIdentifier, str]) -> PriceResult:
        """
        Expose: get_price(chain) -> (price, age_s, status)
        """
        key = self._normalize_chain(chain)

        # 1. Check if static price override mode is explicitly enabled
        if self.allow_static_prices:
            static_p = self.static_sol_price if key == "solana" else self.static_eth_price
            if static_p is not None and static_p > Decimal(0):
                return PriceResult(
                    price=static_p,
                    age_s=0.0,
                    status=NativePriceStatus.OK,
                    source="static",
                )

        now = time.time()
        record = self._prices.get(key)
        if record is None:
            return PriceResult(
                price=Decimal(0),
                age_s=float("inf"),
                status=NativePriceStatus.UNAVAILABLE,
                source="none",
            )

        price, updated_at, stored_status, source, sec_price, div_pct = record
        age_s = max(0.0, now - self._cache_timestamp.get(key, updated_at))

        # Check staleness: if older than max_age_s, transition to STALE
        if age_s > self.max_age_s:
            eff_status = NativePriceStatus.STALE
        else:
            eff_status = stored_status

        # Transition logging (WARNING on every transition, not on every tick)
        self._log_transition(key, eff_status, price, age_s, source)

        return PriceResult(
            price=price,
            age_s=age_s,
            status=eff_status,
            source=source,
            secondary_price=sec_price,
            divergence_pct=div_pct,
        )

    def _log_transition(
        self,
        key: str,
        status: NativePriceStatus,
        price: Decimal,
        age_s: float,
        source: str,
    ) -> None:
        last = self._last_logged_status.get(key)
        if last != status:
            self._last_logged_status[key] = status
            if status == NativePriceStatus.OK:
                logger.info(
                    "[NativePriceService] Status for %s transitioned to OK | Price=$%s (source=%s, age=%.1fs)",
                    key,
                    price,
                    source,
                    age_s,
                )
            elif status == NativePriceStatus.STALE:
                logger.warning(
                    "[NativePriceService] Status for %s transitioned to STALE | Age=%.1fs > Max=%.1fs (last price=$%s)",
                    key,
                    age_s,
                    self.max_age_s,
                    price,
                )
            elif status == NativePriceStatus.DIVERGENT:
                logger.warning(
                    "[NativePriceService] Status for %s transitioned to DIVERGENT | Sources diverge beyond tolerance %.2f%%",
                    key,
                    self.divergence_tolerance_pct,
                )
            elif status == NativePriceStatus.UNAVAILABLE:
                logger.warning(
                    "[NativePriceService] Status for %s transitioned to UNAVAILABLE | All price sources failed",
                    key,
                )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0))
            self._owns_session = True
        return self._session

    async def refresh(self) -> None:
        """Fetch prices from primary and secondary sources, evaluate divergence, and update cache."""
        if self.allow_static_prices:
            now = time.time()
            if self.static_sol_price is not None:
                self.set_price("solana", self.static_sol_price, NativePriceStatus.OK, "static")
            if self.static_eth_price is not None:
                self.set_price("base_eth", self.static_eth_price, NativePriceStatus.OK, "static")
            return

        async with self._lock:
            session = await self._get_session()
            prim_task = self.primary_source.fetch_prices(session)
            sec_task = self.secondary_source.fetch_prices(session)

            res_prim, res_sec = await asyncio.gather(prim_task, sec_task, return_exceptions=True)

            prim_dict: dict[str, Decimal] = res_prim if isinstance(res_prim, dict) else {}
            sec_dict: dict[str, Decimal] = res_sec if isinstance(res_sec, dict) else {}

            now = time.time()
            for asset in ("solana", "base_eth"):
                p_prim = prim_dict.get(asset)
                p_sec = sec_dict.get(asset)

                if p_prim is not None and p_sec is not None:
                    # Both sources succeeded: check divergence
                    min_p = min(p_prim, p_sec)
                    div_pct = float(abs(p_prim - p_sec) / min_p * 100) if min_p > Decimal(0) else 0.0
                    if div_pct > self.divergence_tolerance_pct:
                        self._prices[asset] = (p_prim, now, NativePriceStatus.DIVERGENT, "divergent", p_sec, div_pct)
                        self._cache_timestamp[asset] = now
                        self._active_source[asset] = "divergent"
                    else:
                        self._prices[asset] = (p_prim, now, NativePriceStatus.OK, "primary", p_sec, div_pct)
                        self._cache_timestamp[asset] = now
                        self._active_source[asset] = "primary"

                elif p_prim is not None and p_sec is None:
                    # Primary succeeded, secondary failed
                    self._prices[asset] = (p_prim, now, NativePriceStatus.OK, "primary", None, None)
                    self._cache_timestamp[asset] = now
                    self._active_source[asset] = "primary"

                elif p_prim is None and p_sec is not None:
                    # Primary failed, secondary succeeded -> secondary takes over
                    self._prices[asset] = (p_sec, now, NativePriceStatus.OK, "secondary", p_sec, None)
                    self._cache_timestamp[asset] = now
                    self._active_source[asset] = "secondary"

                else:
                    # Both sources failed
                    existing = self._prices.get(asset)
                    if existing is not None:
                        old_p, old_t, _, old_src, _, _ = existing
                        age_s = now - self._cache_timestamp.get(asset, old_t)
                        stat = NativePriceStatus.STALE if age_s > self.max_age_s else NativePriceStatus.OK
                        self._prices[asset] = (old_p, old_t, stat, old_src, None, None)
                    else:
                        self._prices[asset] = (Decimal(0), now, NativePriceStatus.UNAVAILABLE, "none", None, None)
                        self._cache_timestamp[asset] = now
                        self._active_source[asset] = "none"

                # Check and log transition
                price_res = self.get_price(asset)
                self._log_transition(asset, price_res.status, price_res.price, price_res.age_s, price_res.source)

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.refresh_interval_s)
                if not self._running:
                    break
                await self.refresh()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[NativePriceService] Unexpected error in refresh loop: %s", exc)

    async def start(self) -> None:
        """Start the background refresh task in the runner lifecycle."""
        if self.allow_static_prices:
            logger.warning("[NativePriceService] ALLOW_STATIC_NATIVE_PRICES is True - using static native price overrides.")
        if self._refresh_task is None or self._refresh_task.done():
            self._running = True
            try:
                await self.refresh()
            except Exception as exc:
                logger.warning("[NativePriceService] Initial price refresh failed: %s", exc)
            self._refresh_task = asyncio.create_task(self._refresh_loop(), name="native_price_refresh_task")
            logger.info("[NativePriceService] Background refresh task started (interval=%.1fs)", self.refresh_interval_s)

    async def stop(self) -> None:
        """Stop the background refresh task in the runner teardown."""
        self._running = False
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.info("[NativePriceService] Background refresh task stopped.")

