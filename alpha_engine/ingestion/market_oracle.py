"""
alpha_engine.ingestion.market_oracle — Macro Market Sentiment & Multi-Source Public Oracles
===========================================================================================
Integrates public APIs discovered via public-apis/public-apis:
1. Alternative.me Crypto Fear & Greed Index (Macro Market Sentiment)
2. DefiLlama Cross-Chain Coin Price & TVL Oracle (api.llama.fi & coins.llama.fi)
3. GeckoTerminal Public On-Chain DEX Analytics (api.geckoterminal.com/api/v2)

Provides resilient, asynchronous, rate-limited, and cached market intelligence for:
- Macro sentiment awareness for AlphaSupervisor-AI risk posture
- Independent price verification fallback when local DEX pool ticks lag
- Chain-level TVL and liquidity trends on Solana and Base
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Optional, Union

import aiohttp

from alpha_engine.models.enums import ChainIdentifier

logger = logging.getLogger(__name__)

# TTL Cache Durations (Seconds) to respect public API free limits
FEAR_GREED_TTL_S: float = 300.0
DEFILLAMA_PRICE_TTL_S: float = 20.0
DEFILLAMA_TVL_TTL_S: float = 600.0
GECKOTERMINAL_TTL_S: float = 30.0

SOLANA_MINT_WRAPPED_SOL = "So11111111111111111111111111111111111111112"
BASE_WETH_ADDRESS = "0x4200000000000000000000000000000000000006"


class MarketOracle:
    """
    Public API Oracle client aggregating free, open-source crypto market data:
    - Fear & Greed sentiment index (Alternative.me)
    - Multi-chain token prices and chain TVL (DefiLlama)
    - On-chain DEX buy/sell momentum and reserves (GeckoTerminal)
    """

    FEAR_AND_GREED_URL = "https://api.alternative.me/fng/?limit=1"
    DEFILLAMA_COINS_URL = "https://coins.llama.fi/prices/current"
    DEFILLAMA_CHAINS_URL = "https://api.llama.fi/v2/chains"
    GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_s: float = 6.0,
    ) -> None:
        self._session = session
        self._owns_session = False
        self._timeout_s = timeout_s

        # In-memory caches to respect free tier rate limits
        self._fng_cached: Optional[dict[str, Any]] = None
        self._fng_expiry: float = 0.0

        self._prices_cached: dict[str, float] = {}
        self._prices_expiry: float = 0.0

        self._tvl_cached: dict[str, float] = {}
        self._tvl_expiry: float = 0.0

        self._pool_stats_cached: dict[str, tuple[dict[str, Any], float]] = {}
        self._token_stats_cached: dict[str, tuple[dict[str, Any], float]] = {}

        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout_s)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close underlying HTTP session if owned by this instance."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # -------------------------------------------------------------------------
    # 1. Macro Sentiment: Crypto Fear & Greed Index (Alternative.me)
    # -------------------------------------------------------------------------

    async def get_fear_and_greed_index(self, force_refresh: bool = False) -> dict[str, Any]:
        """
        Fetch current crypto market Fear & Greed index from Alternative.me.
        Cached for 300s (5 minutes) to avoid redundant requests.

        Returns:
            dict containing:
            - score / value (int): 0-100 (0=Extreme Fear, 100=Extreme Greed)
            - classification (str): e.g. "Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"
            - risk_modifier (float): 0.75 to 1.20 scaling factor for engine risk appetite
            - timestamp (int): update epoch timestamp
        """
        now = time.time()
        if not force_refresh and self._fng_cached and now < self._fng_expiry:
            return self._fng_cached

        async with self._lock:
            if not force_refresh and self._fng_cached and now < self._fng_expiry:
                return self._fng_cached

            session = await self._get_session()
            headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
            try:
                async with session.get(self.FEAR_AND_GREED_URL, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        records = data.get("data", [])
                        if records:
                            item = records[0]
                            score = int(item.get("value", 50))
                            classification = str(item.get("value_classification", "Neutral"))
                            ts = int(item.get("timestamp", int(now)))

                            # Derive dynamic Kelly/position risk modifier
                            if score <= 25:
                                risk_modifier = 0.75   # Extreme Fear: defensively constrain risk
                            elif score <= 45:
                                risk_modifier = 0.85   # Fear: moderately cautious
                            elif score <= 55:
                                risk_modifier = 1.00   # Neutral: standard sizing
                            elif score <= 75:
                                risk_modifier = 1.10   # Greed: healthy trend participation
                            else:
                                risk_modifier = 1.20   # Extreme Greed: aggressive breakout mode

                            res = {
                                "score": score,
                                "value": score,
                                "classification": classification,
                                "risk_modifier": risk_modifier,
                                "timestamp": ts,
                                "source": "Alternative.me",
                            }
                            self._fng_cached = res
                            self._fng_expiry = now + FEAR_GREED_TTL_S
                            return res
                    logger.debug("Alternative.me returned status %s", resp.status)
            except Exception as exc:
                logger.debug("Failed to fetch Fear & Greed index: %s", exc)

            # Fallback to default neutral sentiment if external API fails
            fallback = {
                "score": 50,
                "value": 50,
                "classification": "Neutral",
                "risk_modifier": 1.00,
                "timestamp": int(now),
                "source": "Fallback",
            }
            if self._fng_cached is None:
                self._fng_cached = fallback
                self._fng_expiry = now + 60.0
            return self._fng_cached

    get_fear_and_greed = get_fear_and_greed_index

    # -------------------------------------------------------------------------
    # 2. DefiLlama Cross-Chain Coin Price & Chain TVL
    # -------------------------------------------------------------------------

    async def get_defillama_coin_prices(self, force_refresh: bool = False) -> dict[str, float]:
        """
        Fetch reference coin prices for SOL and Base ETH from DefiLlama coins oracle.
        Cached for 20s.
        """
        now = time.time()
        if not force_refresh and self._prices_cached and now < self._prices_expiry:
            return dict(self._prices_cached)

        async with self._lock:
            if not force_refresh and self._prices_cached and now < self._prices_expiry:
                return dict(self._prices_cached)

            coins_query = f"solana:{SOLANA_MINT_WRAPPED_SOL},coingecko:ethereum"
            url = f"{self.DEFILLAMA_COINS_URL}/{coins_query}"
            session = await self._get_session()
            headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        coins = data.get("coins", {})
                        out: dict[str, float] = {}

                        sol_key = f"solana:{SOLANA_MINT_WRAPPED_SOL}"
                        if sol_key in coins and "price" in coins[sol_key]:
                            out["solana"] = float(coins[sol_key]["price"])

                        eth_key = "coingecko:ethereum"
                        if eth_key in coins and "price" in coins[eth_key]:
                            out["base_eth"] = float(coins[eth_key]["price"])

                        if out:
                            self._prices_cached = out
                            self._prices_expiry = now + DEFILLAMA_PRICE_TTL_S
                            return dict(out)
            except Exception as exc:
                logger.debug("DefiLlama price lookup failed: %s", exc)

            return dict(self._prices_cached) if self._prices_cached else {}

    async def get_token_price_defillama(
        self,
        chain: Union[ChainIdentifier, str],
        token_address: str,
    ) -> Optional[Decimal]:
        """
        Query DefiLlama coins API for single token spot price in USD.
        Supported chains: Solana, Base, Ethereum.
        """
        token_clean = token_address.strip()
        chain_name = chain.value if isinstance(chain, ChainIdentifier) else str(chain)
        chain_prefix = "solana" if "solana" in chain_name.lower() else "base"
        coin_key = f"{chain_prefix}:{token_clean}"

        url = f"{self.DEFILLAMA_COINS_URL}/{coin_key}"
        session = await self._get_session()
        headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    coins = data.get("coins", {})
                    coin_data = coins.get(coin_key)
                    if coin_data and "price" in coin_data:
                        return Decimal(str(coin_data["price"]))
        except Exception as exc:
            logger.debug("DefiLlama single token price lookup failed for %s: %s", coin_key, exc)

        return None

    async def get_defillama_chain_tvl(self, force_refresh: bool = False) -> dict[str, float]:
        """
        Query DefiLlama for current chain-level Total Value Locked (TVL) in USD for Solana and Base.
        Cached for 600s (10 minutes).
        """
        now = time.time()
        if not force_refresh and self._tvl_cached and now < self._tvl_expiry:
            return dict(self._tvl_cached)

        async with self._lock:
            if not force_refresh and self._tvl_cached and now < self._tvl_expiry:
                return dict(self._tvl_cached)

            session = await self._get_session()
            headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}
            try:
                async with session.get(self.DEFILLAMA_CHAINS_URL, headers=headers) as resp:
                    if resp.status == 200:
                        chains = await resp.json()
                        out: dict[str, float] = {}
                        for ch in chains:
                            name = str(ch.get("name", "")).lower()
                            if name == "solana":
                                out["solana"] = float(ch.get("tvl", 0.0))
                            elif name == "base":
                                out["base"] = float(ch.get("tvl", 0.0))

                        if out:
                            self._tvl_cached = out
                            self._tvl_expiry = now + DEFILLAMA_TVL_TTL_S
                            return dict(out)
            except Exception as exc:
                logger.debug("DefiLlama chain TVL lookup failed: %s", exc)

            return dict(self._tvl_cached) if self._tvl_cached else {}

    async def get_chain_tvl(self, chain_name: str = "solana") -> Optional[float]:
        """Single chain TVL helper returning float USD value."""
        tvls = await self.get_defillama_chain_tvl()
        c = chain_name.strip().lower()
        return tvls.get(c)

    # -------------------------------------------------------------------------
    # 3. GeckoTerminal On-Chain DEX Analytics (CoinGecko DEX layer)
    # -------------------------------------------------------------------------

    async def get_geckoterminal_pool_stats(
        self,
        network: str,
        token_address: str,
        force_refresh: bool = False,
    ) -> Optional[dict[str, Any]]:
        """
        Query GeckoTerminal open API for top pool liquidity, 24h volume, FDV, and price.
        Networks: 'solana' or 'base'.
        Cached for 30s.
        """
        net = network.strip().lower()
        tok = token_address.strip()
        cache_key = f"{net}:{tok}"

        now = time.time()
        if not force_refresh and cache_key in self._pool_stats_cached:
            res, exp = self._pool_stats_cached[cache_key]
            if now < exp:
                return res

        url = f"{self.GECKOTERMINAL_BASE_URL}/networks/{net}/tokens/{tok}/pools"
        session = await self._get_session()
        headers = {
            "User-Agent": "Bot-MM-Engine/1.0",
            "Accept": "application/json;version=20230302",
        }
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pools = data.get("data", [])
                    if pools:
                        top_pool = pools[0]
                        attrs = top_pool.get("attributes", {})
                        relationships = top_pool.get("relationships", {})
                        dex_id = relationships.get("dex", {}).get("data", {}).get("id", "dex")

                        price_usd = float(attrs.get("base_token_price_usd") or 0.0)
                        reserve_usd = float(attrs.get("reserve_in_usd") or 0.0)
                        fdv = float(attrs.get("fdv_usd") or 0.0)
                        vol_24h = float(attrs.get("volume_usd", {}).get("h24") or 0.0)
                        name = attrs.get("name", "")

                        stats = {
                            "pool_name": name,
                            "dex": dex_id,
                            "base_token_price_usd": price_usd,
                            "reserve_in_usd": reserve_usd,
                            "fdv_usd": fdv,
                            "volume_usd_24h": vol_24h,
                            "network": net,
                            "token_address": tok,
                        }
                        self._pool_stats_cached[cache_key] = (stats, now + GECKOTERMINAL_TTL_S)
                        return stats
        except Exception as exc:
            logger.debug("GeckoTerminal pool lookup failed for %s: %s", cache_key, exc)

        return None

    async def get_geckoterminal_token_stats(
        self,
        chain: Union[ChainIdentifier, str],
        token_address: str,
    ) -> Optional[dict[str, Any]]:
        """Query GeckoTerminal token endpoint directly."""
        net = "solana" if (isinstance(chain, ChainIdentifier) and chain == ChainIdentifier.SOLANA_MAINNET) or str(chain).lower() == "solana" else "base"
        pool_stats = await self.get_geckoterminal_pool_stats(net, token_address)
        if pool_stats:
            return {
                "name": pool_stats.get("pool_name", ""),
                "symbol": "",
                "price_usd": pool_stats.get("base_token_price_usd", 0.0),
                "fdv_usd": pool_stats.get("fdv_usd", 0.0),
                "volume_24h_usd": pool_stats.get("volume_usd_24h", 0.0),
                "network": net,
                "token_address": token_address,
            }
        return None

    # -------------------------------------------------------------------------
    # 4. Aggregated Macro Telemetry
    # -------------------------------------------------------------------------

    async def get_macro_market_summary(self) -> dict[str, Any]:
        """
        Produce a consolidated macro market report combining:
        - Alternative.me Fear & Greed sentiment + risk modifier
        - DefiLlama reference asset prices (SOL & ETH)
        - DefiLlama Solana & Base TVL
        - Oracle operational status
        """
        fng_task = self.get_fear_and_greed()
        prices_task = self.get_defillama_coin_prices()
        tvl_task = self.get_defillama_chain_tvl()

        fng, prices, tvl = await asyncio.gather(
            fng_task, prices_task, tvl_task, return_exceptions=True
        )

        fng_dict = fng if isinstance(fng, dict) else {"score": 50, "value": 50, "classification": "Neutral", "risk_modifier": 1.0}
        prices_dict = prices if isinstance(prices, dict) else {}
        tvl_dict = tvl if isinstance(tvl, dict) else {}

        return {
            "fear_and_greed": fng_dict,
            "prices": prices_dict,
            "tvl": tvl_dict,
            "solana_tvl_usd": tvl_dict.get("solana", 0.0),
            "base_tvl_usd": tvl_dict.get("base", 0.0),
            "oracles_active": ["Alternative.me", "DefiLlama", "GeckoTerminal"],
            "timestamp": int(time.time()),
        }


__all__ = [
    "MarketOracle",
    "FEAR_GREED_TTL_S",
    "DEFILLAMA_PRICE_TTL_S",
    "DEFILLAMA_TVL_TTL_S",
    "GECKOTERMINAL_TTL_S",
]
