"""
alpha_engine.profiler.price_oracle — Historical Native Asset Price Oracle
=========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp + DefiLlama Historical API
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

import aiohttp

from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.profiler.history_db import WalletHistoryDatabase

logger = logging.getLogger(__name__)

SOLANA_WRAPPED_SOL = "So11111111111111111111111111111111111111112"
BASE_WETH = "0x4200000000000000000000000000000000000006"


class HistoricalPriceOracle:
    """
    Fetches historical native asset prices (SOL / ETH) in USD at specific timestamps
    using DefiLlama's historical coins oracle, with SQLite-backed hourly caching.
    """

    DEFILLAMA_HISTORICAL_URL = "https://coins.llama.fi/prices/historical"

    def __init__(
        self,
        history_db: Optional[WalletHistoryDatabase] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.history_db = history_db
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5.0)
            )
        return self._session

    async def get_historical_native_price(
        self,
        chain: ChainIdentifier,
        timestamp: int,
    ) -> Decimal:
        """
        Retrieve historical native token price in USD for given chain and timestamp.
        Checks SQLite cache first. Fails closed if real data cannot be retrieved.
        """
        # 1. Check local cache
        if self.history_db is not None:
            cached = await self.history_db.get_cached_price(chain, timestamp)
            if cached is not None and cached > Decimal(0):
                return cached

        # 2. Query DefiLlama Historical Endpoint
        coin_key = (
            f"solana:{SOLANA_WRAPPED_SOL}"
            if chain == ChainIdentifier.SOLANA_MAINNET
            else "coingecko:ethereum"
        )
        url = f"{self.DEFILLAMA_HISTORICAL_URL}/{int(timestamp)}/{coin_key}"
        headers = {"User-Agent": "Bot-MM-Engine/1.0", "Accept": "application/json"}

        try:
            session = await self._get_session()
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    coins = data.get("coins", {})
                    if coin_key in coins and "price" in coins[coin_key]:
                        price_val = Decimal(str(coins[coin_key]["price"]))
                        if price_val > Decimal(0):
                            if self.history_db is not None:
                                await self.history_db.cache_price(chain, timestamp, price_val)
                            return price_val
                logger.warning(
                    "DefiLlama historical price returned status %d for %s at %d",
                    resp.status, coin_key, timestamp
                )
        except Exception as exc:
            logger.warning("Failed to fetch historical native price for %s at %d: %s", chain.value, timestamp, exc)

        # Fail-closed: No invented numbers (FIXING_RULES Rule 4 & 5)
        raise ValueError(
            f"Historical native price unavailable for chain {chain.value} at timestamp {timestamp}"
        )

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

