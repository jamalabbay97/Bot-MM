"""
alpha_engine.profiler.solana_history — Solana Historical Swaps via Helius Enhanced Transactions
==============================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | aiohttp + Helius Parsed API
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

SOLANA_WRAPPED_SOL = "So11111111111111111111111111111111111111112"


class HeliusSolanaHistoryProvider(WalletHistoryProvider):
    """
    Solana wallet history provider using Helius Enhanced Transactions API.
    Robustly parses swap transactions from Pump.fun, PumpSwap, Raydium, and Jupiter.
    """

    HELIUS_BASE_URL = "https://api.helius.xyz/v0"

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
            provider_name="helius_solana",
            history_db=history_db,
            rate_limiters=rate_limiters,
            cache_ttl_s=cache_ttl_s,
            daily_request_quota=daily_request_quota,
            backfill_window_days=backfill_window_days,
        )
        self.api_key = api_key or os.getenv("HELIUS_API_KEY", "")
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10.0)
            )
        return self._session

    async def _fetch_enhanced_transactions_raw(
        self,
        wallet: str,
        before: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch a page of parsed transactions from Helius API."""
        if not self.api_key:
            raise ValueError("Helius API key missing or unconfigured")

        url = f"{self.HELIUS_BASE_URL}/addresses/{wallet}/transactions?api-key={self.api_key}&limit={limit}"
        if before:
            url += f"&before={before}"

        session = await self._get_session()
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
                return []
            text = await resp.text()
            raise RuntimeError(f"Helius API HTTP {resp.status}: {text[:200]}")

    async def _fetch_swaps_network(
        self,
        wallet: str,
        chain: ChainIdentifier,
        since: int,
        until: int,
        limit: int,
    ) -> list[NormalizedSwap]:
        """Paginates through Helius enhanced transactions and parses swaps."""
        all_swaps: list[NormalizedSwap] = []
        last_sig: Optional[str] = None
        reached_since = False

        while len(all_swaps) < limit and not reached_since:
            raw_txs = await self._fetch_enhanced_transactions_raw(
                wallet=wallet, before=last_sig, limit=min(100, limit - len(all_swaps))
            )
            if not raw_txs:
                break

            parsed_batch = self.parse_enhanced_transactions(raw_txs, wallet=wallet)
            for s in parsed_batch:
                if s.block_time < since:
                    reached_since = True
                    break
                if s.block_time <= until:
                    all_swaps.append(s)

            last_sig = raw_txs[-1].get("signature")
            if not last_sig or len(raw_txs) < 10:
                break

        return all_swaps

    def parse_enhanced_transactions(
        self,
        raw_txs: list[dict[str, Any]],
        wallet: str,
    ) -> list[NormalizedSwap]:
        """
        Parse raw Helius transaction dictionaries into standardized NormalizedSwap events.
        Handles Pump.fun, Raydium (v4/CPMM/CLMM), Jupiter multi-route, and pure transfers.
        """
        swaps: list[NormalizedSwap] = []
        clean_wallet = wallet.strip()

        for tx in raw_txs:
            try:
                sig = tx.get("signature") or tx.get("tx_hash", "")
                block_time = int(tx.get("timestamp") or tx.get("blockTime") or 0)
                if not sig or block_time <= 0:
                    continue

                fee_lamports = tx.get("fee", 5000)
                fee_native = Decimal(str(fee_lamports)) / Decimal("1000000000")
                source = str(tx.get("source", "")).upper()
                tx_type = str(tx.get("type", "")).upper()

                # Determine venue
                venue = SwapVenue.UNKNOWN.value
                if "PUMP" in source:
                    venue = SwapVenue.PUMP_FUN.value
                elif "RAYDIUM" in source:
                    venue = SwapVenue.RAYDIUM.value
                elif "JUPITER" in source:
                    venue = SwapVenue.JUPITER.value
                elif source:
                    venue = source.lower()

                events = tx.get("events", {})
                swap_ev = events.get("swap") if isinstance(events, dict) else None

                # 1. Parse structured swap event if available
                if swap_ev and isinstance(swap_ev, dict):
                    parsed_swap = self._parse_swap_event(
                        swap_ev=swap_ev,
                        sig=sig,
                        block_time=block_time,
                        fee_native=fee_native,
                        venue=venue,
                        clean_wallet=clean_wallet,
                        custom_meta={"source": source, "type": tx_type},
                    )
                    if parsed_swap is not None:
                        swaps.append(parsed_swap)
                        continue

                # 2. Check token and native transfer arrays (fallback & pure transfers)
                token_transfers = tx.get("tokenTransfers", [])
                native_transfers = tx.get("nativeTransfers", [])

                if tx_type == "TRANSFER" and not swap_ev:
                    # Pure external token transfer
                    for tt in token_transfers:
                        mint = tt.get("mint", "")
                        amt = Decimal(str(tt.get("tokenAmount", 0)))
                        from_user = tt.get("fromUserAccount", "")
                        to_user = tt.get("toUserAccount", "")

                        if to_user == clean_wallet and amt > Decimal(0):
                            swaps.append(
                                NormalizedSwap(
                                    tx_hash=sig,
                                    block_time=block_time,
                                    chain=ChainIdentifier.SOLANA_MAINNET,
                                    wallet_address=clean_wallet,
                                    venue="transfer",
                                    token_in_address=mint,
                                    token_in_amount=Decimal(0),
                                    token_out_address=mint,
                                    token_out_amount=amt,
                                    fee_native=fee_native,
                                    is_buy=True,  # Incoming transfer
                                    token_address=mint,
                                    token_amount=amt,
                                    native_amount=Decimal(0),
                                    is_transfer=True,
                                    custom_metadata={"from": from_user},
                                )
                            )
                        elif from_user == clean_wallet and amt > Decimal(0):
                            swaps.append(
                                NormalizedSwap(
                                    tx_hash=sig,
                                    block_time=block_time,
                                    chain=ChainIdentifier.SOLANA_MAINNET,
                                    wallet_address=clean_wallet,
                                    venue="transfer",
                                    token_in_address=mint,
                                    token_in_amount=amt,
                                    token_out_address=mint,
                                    token_out_amount=Decimal(0),
                                    fee_native=fee_native,
                                    is_buy=False,  # Outgoing transfer
                                    token_address=mint,
                                    token_amount=amt,
                                    native_amount=Decimal(0),
                                    is_transfer=True,
                                    custom_metadata={"to": to_user},
                                )
                            )
                    continue

                # 3. Fallback: Reconstruct swap from tokenTransfers + nativeTransfers
                fallback_swap = self._parse_from_transfers(
                    token_transfers=token_transfers,
                    native_transfers=native_transfers,
                    sig=sig,
                    block_time=block_time,
                    fee_native=fee_native,
                    venue=venue,
                    clean_wallet=clean_wallet,
                    custom_meta={"source": source, "type": tx_type},
                )
                if fallback_swap is not None:
                    swaps.append(fallback_swap)

            except Exception as exc:
                logger.debug("Failed parsing Solana tx %s: %s", tx.get("signature"), exc)
                continue

        return swaps

    def _parse_swap_event(
        self,
        swap_ev: dict[str, Any],
        sig: str,
        block_time: int,
        fee_native: Decimal,
        venue: str,
        clean_wallet: str,
        custom_meta: dict[str, Any],
    ) -> Optional[NormalizedSwap]:
        """Extract swap details from Helius events.swap object."""
        native_input = swap_ev.get("nativeInput")
        native_output = swap_ev.get("nativeOutput")
        token_inputs = swap_ev.get("tokenInputs", [])
        token_outputs = swap_ev.get("tokenOutputs", [])

        # Case A: Buy token with native SOL
        if native_input and token_outputs:
            sol_amount = Decimal(str(native_input.get("amount", 0))) / Decimal("1000000000")
            target_out = token_outputs[0]
            mint = target_out.get("mint", "")
            raw = target_out.get("rawTokenAmount", {})
            decimals = int(raw.get("decimals", 6))
            token_amount = Decimal(str(raw.get("tokenAmount", 0))) / Decimal(10 ** decimals)

            if sol_amount > Decimal(0) and token_amount > Decimal(0):
                return NormalizedSwap(
                    tx_hash=sig,
                    block_time=block_time,
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    wallet_address=clean_wallet,
                    venue=venue,
                    token_in_address=SOLANA_WRAPPED_SOL,
                    token_in_symbol="SOL",
                    token_in_amount=sol_amount,
                    token_out_address=mint,
                    token_out_amount=token_amount,
                    fee_native=fee_native,
                    is_buy=True,
                    token_address=mint,
                    token_amount=token_amount,
                    native_amount=sol_amount,
                    is_transfer=False,
                    custom_metadata=custom_meta,
                )

        # Case B: Sell token for native SOL
        if native_output and token_inputs:
            sol_amount = Decimal(str(native_output.get("amount", 0))) / Decimal("1000000000")
            target_in = token_inputs[0]
            mint = target_in.get("mint", "")
            raw = target_in.get("rawTokenAmount", {})
            decimals = int(raw.get("decimals", 6))
            token_amount = Decimal(str(raw.get("tokenAmount", 0))) / Decimal(10 ** decimals)

            if sol_amount > Decimal(0) and token_amount > Decimal(0):
                return NormalizedSwap(
                    tx_hash=sig,
                    block_time=block_time,
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    wallet_address=clean_wallet,
                    venue=venue,
                    token_in_address=mint,
                    token_in_amount=token_amount,
                    token_out_address=SOLANA_WRAPPED_SOL,
                    token_out_symbol="SOL",
                    token_out_amount=sol_amount,
                    fee_native=fee_native,
                    is_buy=False,
                    token_address=mint,
                    token_amount=token_amount,
                    native_amount=sol_amount,
                    is_transfer=False,
                    custom_metadata=custom_meta,
                )

        return None

    def _parse_from_transfers(
        self,
        token_transfers: list[dict[str, Any]],
        native_transfers: list[dict[str, Any]],
        sig: str,
        block_time: int,
        fee_native: Decimal,
        venue: str,
        clean_wallet: str,
        custom_meta: dict[str, Any],
    ) -> Optional[NormalizedSwap]:
        """Deduce swap direction from net balance changes in tokenTransfers and nativeTransfers."""
        net_native = Decimal(0)
        for nt in native_transfers:
            amt = Decimal(str(nt.get("amount", 0))) / Decimal("1000000000")
            if nt.get("toUserAccount") == clean_wallet:
                net_native += amt
            elif nt.get("fromUserAccount") == clean_wallet:
                net_native -= amt

        token_map: dict[str, Decimal] = {}
        for tt in token_transfers:
            mint = tt.get("mint", "")
            if not mint or mint == SOLANA_WRAPPED_SOL:
                continue
            amt = Decimal(str(tt.get("tokenAmount", 0)))
            if tt.get("toUserAccount") == clean_wallet:
                token_map[mint] = token_map.get(mint, Decimal(0)) + amt
            elif tt.get("fromUserAccount") == clean_wallet:
                token_map[mint] = token_map.get(mint, Decimal(0)) - amt

        if not token_map:
            return None

        for mint, net_tok in token_map.items():
            if net_tok > Decimal(0) and net_native < Decimal(0):
                # Bought token with SOL
                return NormalizedSwap(
                    tx_hash=sig,
                    block_time=block_time,
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    wallet_address=clean_wallet,
                    venue=venue,
                    token_in_address=SOLANA_WRAPPED_SOL,
                    token_in_amount=abs(net_native),
                    token_out_address=mint,
                    token_out_amount=net_tok,
                    fee_native=fee_native,
                    is_buy=True,
                    token_address=mint,
                    token_amount=net_tok,
                    native_amount=abs(net_native),
                    is_transfer=False,
                    custom_metadata=custom_meta,
                )
            elif net_tok < Decimal(0) and net_native > Decimal(0):
                # Sold token for SOL
                return NormalizedSwap(
                    tx_hash=sig,
                    block_time=block_time,
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    wallet_address=clean_wallet,
                    venue=venue,
                    token_in_address=mint,
                    token_in_amount=abs(net_tok),
                    token_out_address=SOLANA_WRAPPED_SOL,
                    token_out_amount=net_native,
                    fee_native=fee_native,
                    is_buy=False,
                    token_address=mint,
                    token_amount=abs(net_tok),
                    native_amount=net_native,
                    is_transfer=False,
                    custom_metadata=custom_meta,
                )

        return None

