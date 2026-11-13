"""
alpha_engine.profiler.base_history — Base EVM Historical Swaps via Alchemy & DEX Logs
=====================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp + Alchemy Asset Transfers & Receipts
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Optional

import aiohttp

from alpha_engine.models.enums import ChainIdentifier, SwapVenue
from alpha_engine.models.profiler import NormalizedSwap
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.history_provider import WalletHistoryProvider
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

BASE_WETH = "0x4200000000000000000000000000000000000006"


class AlchemyBaseHistoryProvider(WalletHistoryProvider):
    """
    Base (EVM) wallet history provider utilizing Alchemy Asset Transfers and Logs.
    Robustly parses Uniswap V2, Uniswap V3, and Aerodrome swaps.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        history_db: Optional[WalletHistoryDatabase] = None,
        rate_limiters: Optional[RateLimiterRegistry] = None,
        cache_ttl_s: int = 3600,
        daily_request_quota: int = 1000,
        backfill_window_days: int = 30,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        super().__init__(
            provider_name="alchemy_base",
            history_db=history_db,
            rate_limiters=rate_limiters,
            cache_ttl_s=cache_ttl_s,
            daily_request_quota=daily_request_quota,
            backfill_window_days=backfill_window_days,
        )
        self.api_key = api_key or os.getenv("ALCHEMY_API_KEY", "")
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10.0)
            )
        return self._session

    async def _fetch_swaps_network(
        self,
        wallet: str,
        chain: ChainIdentifier,
        since: int,
        until: int,
        limit: int,
    ) -> list[NormalizedSwap]:
        """Fetch asset transfers from Alchemy API for Base."""
        if not self.api_key:
            raise ValueError("Alchemy API key missing or unconfigured for Base")

        if chain != ChainIdentifier.BASE_MAINNET:
            raise ValueError(f"AlchemyBaseHistoryProvider only supports BASE_MAINNET, got {chain}")

        logger.debug(
            "Fetching historical Base swaps on %s for %s within [%d, %d] (limit=%d)",
            chain.value, wallet[:10], since, until, limit
        )

        url = f"https://base-mainnet.g.alchemy.com/v2/{self.api_key}"
        max_count = hex(min(max(limit, 1), 100))
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "fromAddress": wallet,
                    "category": ["external", "internal", "erc20"],
                    "maxCount": max_count,
                }
            ],
        }

        session = await self._get_session()
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                transfers = data.get("result", {}).get("transfers", [])
                parsed_swaps = self.parse_transaction_receipts(transfers, wallet=wallet)
                filtered_swaps = [
                    s for s in parsed_swaps
                    if (since <= s.block_time <= until) or s.block_time == 0
                ][:limit]
                return filtered_swaps
            text = await resp.text()
            raise RuntimeError(f"Alchemy API HTTP {resp.status}: {text[:200]}")

    def parse_transaction_receipts(
        self,
        receipts: list[dict[str, Any]],
        wallet: str,
    ) -> list[NormalizedSwap]:
        """
        Parse transaction receipts/transfers into normalized swaps.
        Handles Uniswap V2, Uniswap V3, and Aerodrome Slipstream.
        """
        swaps: list[NormalizedSwap] = []
        clean_wallet = wallet.lower().strip()

        for r in receipts:
            try:
                tx_hash = r.get("transactionHash") or r.get("hash", "")
                block_time = int(r.get("blockTimestamp") or r.get("timestamp", 0))
                if not tx_hash or block_time <= 0:
                    continue

                gas_used = int(r.get("gasUsed", 150000))
                gas_price = int(r.get("effectiveGasPrice", 1000000000))
                fee_native = (Decimal(gas_used) * Decimal(gas_price)) / Decimal("1000000000000000000")

                raw_venue = str(r.get("venue", "")).lower()
                venue = raw_venue if raw_venue else SwapVenue.UNISWAP_V2.value
                transfers = r.get("transfers", [])

                # Analyze transfers involving the target wallet
                sent_native = Decimal(0)
                recv_native = Decimal(0)
                sent_tokens: dict[str, Decimal] = {}
                recv_tokens: dict[str, Decimal] = {}

                for t in transfers:
                    token_addr = t.get("tokenAddress", "").lower()
                    amt = Decimal(str(t.get("amount", 0)))
                    from_addr = t.get("from", "").lower()
                    to_addr = t.get("to", "").lower()

                    is_native_weth = (
                        token_addr == BASE_WETH.lower() or token_addr in ("", "eth", "native")
                    )

                    if from_addr == clean_wallet:
                        if is_native_weth:
                            sent_native += amt
                        else:
                            sent_tokens[token_addr] = sent_tokens.get(token_addr, Decimal(0)) + amt
                    elif to_addr == clean_wallet:
                        if is_native_weth:
                            recv_native += amt
                        else:
                            recv_tokens[token_addr] = recv_tokens.get(token_addr, Decimal(0)) + amt

                # Case A: Buy target token with native WETH/ETH
                if sent_native > Decimal(0) and recv_tokens:
                    target_token, tok_amt = next(iter(recv_tokens.items()))
                    swaps.append(
                        NormalizedSwap(
                            tx_hash=tx_hash,
                            block_time=block_time,
                            chain=ChainIdentifier.BASE_MAINNET,
                            wallet_address=clean_wallet,
                            venue=venue,
                            token_in_address=BASE_WETH,
                            token_in_symbol="WETH",
                            token_in_amount=sent_native,
                            token_out_address=target_token,
                            token_out_amount=tok_amt,
                            fee_native=fee_native,
                            is_buy=True,
                            token_address=target_token,
                            token_amount=tok_amt,
                            native_amount=sent_native,
                            is_transfer=False,
                            custom_metadata={"gasUsed": gas_used},
                        )
                    )

                # Case B: Sell target token for native WETH/ETH
                elif recv_native > Decimal(0) and sent_tokens:
                    target_token, tok_amt = next(iter(sent_tokens.items()))
                    swaps.append(
                        NormalizedSwap(
                            tx_hash=tx_hash,
                            block_time=block_time,
                            chain=ChainIdentifier.BASE_MAINNET,
                            wallet_address=clean_wallet,
                            venue=venue,
                            token_in_address=target_token,
                            token_in_amount=tok_amt,
                            token_out_address=BASE_WETH,
                            token_out_symbol="WETH",
                            token_out_amount=recv_native,
                            fee_native=fee_native,
                            is_buy=False,
                            token_address=target_token,
                            token_amount=tok_amt,
                            native_amount=recv_native,
                            is_transfer=False,
                            custom_metadata={"gasUsed": gas_used},
                        )
                    )

            except Exception as exc:
                logger.debug("Failed parsing Base receipt %s: %s", r.get("transactionHash"), exc)
                continue

        return swaps
