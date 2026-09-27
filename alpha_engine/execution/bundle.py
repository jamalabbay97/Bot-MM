"""
alpha_engine.execution.bundle — Anti-MEV & Private Transaction Routing (Flashbots & Jito)
========================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | asyncio + aiohttp
"""

from __future__ import annotations

import asyncio
import logging
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
    ) -> None:
        self._session = session
        self._jito_engine_url = jito_engine_url
        self._jito_tip_floor_url = jito_tip_floor_url
        self._evm_builders = list(evm_builders) if evm_builders else [
            FLASHBOTS_RPC_URL,
            TITAN_BUILDER_RPC_URL,
            MEV_BLOCKER_RPC_URL,
        ]

    async def get_jito_tip_floor(self, percentile: str = "p75") -> int:
        """
        Dynamically query Jito Tip Floor API for current competitive bundle tips.
        Supports 'p50', 'p75', 'p95', 'p99'.
        """
        try:
            should_close = False
            session = self._session
            if session is None:
                session = aiohttp.ClientSession()
                should_close = True

            async with session.get(self._jito_tip_floor_url, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        entry = data[0]
                        # Tip in SOL converted to lamports
                        sol_tip = float(entry.get(f"landed_tips_{percentile}", 0.00005) or 0.00005)
                        tip_lamports = int(sol_tip * 1_000_000_000)
                        logger.debug("Jito dynamic tip (%s): %d lamports (%.6f SOL)", percentile, tip_lamports, sol_tip)
                        return max(10_000, tip_lamports)
        except Exception as exc:
            logger.debug("Failed to query Jito tip floor: %s — using default %d lamports", exc, DEFAULT_JITO_TIP_LAMPORTS)
        finally:
            if should_close and session is not None:
                await session.close()

        return DEFAULT_JITO_TIP_LAMPORTS

    async def send_evm_private_tx(
        self,
        signed_raw_tx_hex: str,
        builder_url: Optional[str] = None,
    ) -> dict[str, Any]:
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
        encoded_transactions: list[str],
        tip_percentile: str = "p75",
    ) -> dict[str, Any]:
        """
        Submit a bundle of serialized base58/base64 transactions directly to Jito Block Engine.
        """
        tip_lamports = await self.get_jito_tip_floor(percentile=tip_percentile)

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_transactions],
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
