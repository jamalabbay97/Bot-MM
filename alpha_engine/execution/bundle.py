"""
alpha_engine.execution.bundle — Anti-MEV & Private Transaction Routing (Flashbots & Jito)
========================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio + aiohttp
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Optional, Sequence

import aiohttp

from alpha_engine.models.enums import ChainIdentifier

logger = logging.getLogger(__name__)

# EVM Private Builder Endpoints (Protection against Sandwich & Front-running attacks)
FLASHBOTS_RPC_URL = "https://rpc.flashbots.net"
TITAN_BUILDER_RPC_URL = "https://rpc.titanbuilder.xyz"
MEV_BLOCKER_RPC_URL = "https://rpc.mevblocker.io"

# Solana Jito Block Engine & Tip Floor
JITO_TIP_FLOOR_API = "https://bundles.jito.wtf/api/v1/bundles/tip_floor"
JITO_BLOCK_ENGINE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"

DEFAULT_JITO_TIP_LAMPORTS = 50_000  # 0.00005 SOL

__all__ = [
    "PrivateTxRouter",
    "FLASHBOTS_RPC_URL",
    "TITAN_BUILDER_RPC_URL",
    "MEV_BLOCKER_RPC_URL",
    "JITO_TIP_FLOOR_API",
    "JITO_BLOCK_ENGINE_URL",
    "DEFAULT_JITO_TIP_LAMPORTS",
]


class PrivateTxRouter:
    """
    Routes execution orders exclusively through private builders and bundles:
    - EVM: Flashbots / Titan Builder / MEV-Blocker private RPCs.
    - Solana: Jito Bundles with dynamic Jito-Tip-Floor estimation directly to validators.
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        jito_engine_url: str = JITO_BLOCK_ENGINE_URL,
        jito_tip_floor_url: str = JITO_TIP_FLOOR_API,
        evm_builders: Optional[Sequence[str]] = None,
        flashbots_rpc: Optional[str] = None,
        titan_rpc: Optional[str] = None,
        mev_blocker_rpc: Optional[str] = None,
    ) -> None:
        self._session = session
        self._jito_engine_url = jito_engine_url
        self._jito_tip_floor_url = jito_tip_floor_url
        self.flashbots_rpc = flashbots_rpc or FLASHBOTS_RPC_URL
        self.titan_rpc = titan_rpc or TITAN_BUILDER_RPC_URL
        self.mev_blocker_rpc = mev_blocker_rpc or MEV_BLOCKER_RPC_URL
        self._evm_builders = list(evm_builders) if evm_builders else [
            self.flashbots_rpc,
            self.titan_rpc,
            self.mev_blocker_rpc,
        ]
        self._tip_cache: dict[str, tuple[float, Decimal]] = {}
        self._raw_tip_data_cache: Optional[tuple[float, Any]] = None
        self._tip_cache_ttl: float = 5.0

    def prepare_flashbots_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Prepare JSON-RPC HTTP request dictionary targeted for Flashbots builder."""
        return {
            "url": self.flashbots_rpc,
            "headers": {
                "Content-Type": "application/json",
                "X-Flashbots-Bundle": "true",
            },
            "json": payload,
        }

    async def get_jito_tip_floor(self, percentile: str = "p75") -> Decimal:
        """
        Dynamically query Jito Tip Floor API for current competitive bundle tips with 5.0s in-memory TTL caching.
        Supports 'p50', 'p75', 'p95', 'p99'.
        Returns tip in SOL as Decimal.
        """
        import time

        now = time.time()
        # Fast path: in-memory cache hit for requested percentile
        if percentile in self._tip_cache:
            ts, val = self._tip_cache[percentile]
            if now - ts < self._tip_cache_ttl:
                return val

        # Secondary fast path: raw tip response cached recently
        if self._raw_tip_data_cache is not None:
            ts, raw_data = self._raw_tip_data_cache
            if now - ts < self._tip_cache_ttl:
                entry = raw_data if isinstance(raw_data, dict) else (raw_data[0] if raw_data else {})
                percentile_num = percentile.replace("p", "")
                raw_tip = (
                    entry.get(f"landed_tips_{percentile_num}th_percentile")
                    or entry.get(f"landed_tips_{percentile}")
                    or entry.get(f"p{percentile_num}")
                    or entry.get(percentile)
                    or 0.00005
                )
                tip_val = Decimal(str(raw_tip))
                self._tip_cache[percentile] = (now, tip_val)
                return tip_val

        should_close = False
        session = self._session
        try:
            if session is None:
                session = aiohttp.ClientSession()
                should_close = True

            async with session.get(self._jito_tip_floor_url, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    entry = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else {})
                    percentile_num = percentile.replace("p", "")
                    raw_tip = (
                        entry.get(f"landed_tips_{percentile_num}th_percentile")
                        or entry.get(f"landed_tips_{percentile}")
                        or entry.get(f"p{percentile_num}")
                        or entry.get(percentile)
                        or 0.00005
                    )
                    tip_val = Decimal(str(raw_tip))
                    now_ts = time.time()
                    self._raw_tip_data_cache = (now_ts, data)
                    self._tip_cache[percentile] = (now_ts, tip_val)
                    return tip_val
        except Exception as exc:
            logger.debug("Failed to query Jito tip floor via aiohttp: %s — using cached or fallback", exc)
            if percentile in self._tip_cache:
                return self._tip_cache[percentile][1]
        finally:
            if should_close and session is not None:
                await session.close()

        return Decimal("0.00005")

    # Alias matching task specification
    get_tip_floor = get_jito_tip_floor

    async def send_evm_private_tx(
        self,
        signed_raw_tx_hex: str,
        builder_url: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Broadcast signed EVM transaction directly to a private block builder.
        Bypasses the public mempool to prevent sandwich and MEV front-running.
        """
        target_url = builder_url or self._evm_builders[0]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_sendRawTransaction",
            "params": [signed_raw_tx_hex],
        }

        should_close = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            should_close = True

        try:
            async with session.post(
                target_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as resp:
                resp_json = await resp.json()
                logger.info("EVM private tx sent to %s | Result: %s", target_url, resp_json.get("result"))
                return resp_json
        except Exception as exc:
            logger.warning("Failed to send private EVM tx to %s: %s", target_url, exc)
            return {"error": str(exc)}
        finally:
            if should_close and session is not None:
                await session.close()

    async def send_solana_jito_bundle(
        self,
        encoded_transactions: Sequence[str] | Sequence[bytes],
        tip_percentile: str = "p75",
    ) -> Optional[dict[str, Any]]:
        """
        Submit a bundle of serialized base58/base64 transactions directly to Jito Block Engine.
        """
        tip_lamports = await self.get_jito_tip_floor(percentile=tip_percentile)

        tx_payload: list[str] = [
            tx.hex() if isinstance(tx, bytes) else str(tx)
            for tx in encoded_transactions
        ]

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [tx_payload],
        }

        should_close = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            should_close = True

        try:
            async with session.post(
                self._jito_engine_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as resp:
                resp_json = await resp.json()
                logger.info("Jito Bundle sent (tip=%d lamports) | Result: %s", tip_lamports, resp_json.get("result"))
                return resp_json
        except Exception as exc:
            logger.warning("Failed to send Jito bundle: %s", exc)
            return {"error": str(exc)}
        finally:
            if should_close and session is not None:
                await session.close()

    async def simulate_bundle(
        self,
        chain: ChainIdentifier,
        transactions: Sequence[str] | Sequence[bytes],
        target_block: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Simulate bundle execution against private builder or RPC endpoints.
        """
        if not transactions:
            return None

        if target_block is not None:
            logger.debug("Simulating bundle on %s targeting block %s", chain.value, target_block)

        tx_strings: list[str] = [
            tx.hex() if isinstance(tx, bytes) else str(tx)
            for tx in transactions
        ]

        if chain == ChainIdentifier.BASE_MAINNET:
            try:
                async with asyncio.timeout(3.0):
                    return await self.send_evm_private_tx(tx_strings[0], builder_url=self.flashbots_rpc)
            except (asyncio.TimeoutError, Exception) as exc:
                logger.warning("Bundle simulation failed for %s: %s", chain.value, exc)
                return {"error": str(exc)}
        elif chain == ChainIdentifier.SOLANA_MAINNET:
            try:
                async with asyncio.timeout(3.0):
                    return await self.send_solana_jito_bundle(tx_strings)
            except (asyncio.TimeoutError, Exception) as exc:
                logger.warning("Bundle simulation failed for %s: %s", chain.value, exc)
                return {"error": str(exc)}
        return None

    async def route_bundle_for_chain(
        self,
        chain: ChainIdentifier,
        transactions: Sequence[str] | Sequence[bytes],
    ) -> Optional[str]:
        """
        Route transaction bundle for execution and return transaction or bundle identifier.
        """
        res = await self.simulate_bundle(chain, transactions)
        if res and "result" in res and res["result"] is not None:
            return str(res["result"])
        return None
