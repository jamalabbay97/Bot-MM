"""
alpha_engine.ingestion.svm — SVM (Solana) WebSocket Log Ingester
================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | websockets
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from alpha_engine.ingestion.decoders import (
    PUMP_FUN_PROGRAM_ID,
    RAYDIUM_AMM_PROGRAM_ID,
    SvmPoolMeta,
    _normalise,
    _parse_pump_fun_logs,
    _parse_raydium_initialize2_logs,
    _parse_raydium_log_line,
)
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
    ShutdownSentinel,
    SwapEvent,
)
from alpha_engine.models.state import PoolState
from alpha_engine.rate_limiter.registry import RateLimiterRegistry
from alpha_engine.security.constants import SOLANA_SYSTEM_PROGRAM_IDS


logger = logging.getLogger(__name__)

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_MAP = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _b58decode(s: str) -> bytes:
    if not s or not isinstance(s, str):
        raise ValueError("Invalid Base58 string")
    n = 0
    for c in s:
        if c not in _B58_MAP:
            raise ValueError(f"Invalid character in Base58: {c}")
        n = n * 58 + _B58_MAP[c]
    b = n.to_bytes((n.bit_length() + 7) // 8, "big") if n > 0 else b""
    pad = 0
    for c in s:
        if c == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + b


def is_valid_solana_pubkey(addr: str) -> bool:
    """Validate that an address is a genuine 32-byte Base58 Solana public key."""
    if not isinstance(addr, str) or len(addr) < 32 or len(addr) > 44:
        return False
    try:
        decoded = _b58decode(addr)
        return len(decoded) == 32
    except Exception:
        return False


class SVMIngester:
    """
    Ingests Raydium V4 swap events from Solana via Helius WebSocket.
    """

    def __init__(
        self,
        ws_url: str,
        pool_registry: dict[str, SvmPoolMeta],
        event_queue: asyncio.Queue[SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel],
        limiter: RateLimiterRegistry,
    ) -> None:
        self._ws_url = ws_url
        self._pool_registry: dict[str, SvmPoolMeta] = dict(pool_registry)
        self._queue = event_queue
        self._limiter = limiter
        self._running = False

        from alpha_engine.dns_resolver import patch_dns_resolvers
        patch_dns_resolvers()

        from alpha_engine.ingestion.coordinator import ExponentialBackoff
        self._backoff = ExponentialBackoff()
        self._sub_to_pool: dict[int, str] = {}
        self._ssl_fallback = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
                if self._running:
                    logger.info("SVM WebSocket closed cleanly — reconnecting.")
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                if not self._running:
                    break
                err_msg = str(exc)
                if ("CERTIFICATE_VERIFY_FAILED" in err_msg or "certificate verify failed" in err_msg) and not self._ssl_fallback:
                    logger.warning(
                        "SVM WebSocket SSL certificate verification failed (%s). Activating resilient unverified SSL fallback.",
                        exc,
                    )
                    self._ssl_fallback = True
                    await asyncio.sleep(0.5)
                    continue

                delay = self._backoff.next_delay()
                logger.warning("SVM WebSocket error: %s — retrying in %.1fs", exc, delay)
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                logger.info("SVMIngester task cancelled.")
                break

    async def stop(self) -> None:
        self._running = False

    async def _connect_and_stream(self) -> None:
        await self._limiter.helius.acquire(cost=1.0)

        import ssl
        import certifi

        ssl_ctx = None
        if self._ws_url.startswith("wss://"):
            try:
                ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            except Exception:
                ssl_ctx = ssl.create_default_context()
            if self._ssl_fallback:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        async with websockets.connect(
            self._ws_url,
            ssl=ssl_ctx,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        ) as ws:
            logger.info("SVM WebSocket connected to Helius")
            self._backoff.reset()
            self._sub_to_pool.clear()

            req_id = 1
            pending_ids: dict[int, str] = {}

            raw_targets = list(self._pool_registry.keys())
            if PUMP_FUN_PROGRAM_ID not in raw_targets:
                raw_targets.append(PUMP_FUN_PROGRAM_ID)
            if RAYDIUM_AMM_PROGRAM_ID not in raw_targets:
                raw_targets.append(RAYDIUM_AMM_PROGRAM_ID)

            valid_targets: list[str] = []
            for target in raw_targets:
                if not target or not isinstance(target, str):
                    continue
                clean = target.strip()
                if is_valid_solana_pubkey(clean):
                    if clean not in valid_targets:
                        valid_targets.append(clean)
                else:
                    logger.warning(
                        "Rejecting malformed or non-32-byte Solana address for mentions filter: '%s'",
                        target,
                    )

            if not valid_targets:
                valid_targets = [PUMP_FUN_PROGRAM_ID, RAYDIUM_AMM_PROGRAM_ID]

            for target_pubkey in valid_targets:
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [target_pubkey]},
                        {"commitment": "confirmed"},
                    ],
                })
                await self._limiter.helius.acquire(cost=1.0)
                await ws.send(msg)
                pending_ids[req_id] = target_pubkey
                req_id += 1

            confirmed = 0
            expected = len(pending_ids)
            while confirmed < expected:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "SVM subscription confirmation timeout after %d/%d pools",
                        confirmed, expected,
                    )
                    break
                try:
                    resp: dict[str, Any] = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                resp_id = resp.get("id")
                sub_id = resp.get("result")
                if resp_id in pending_ids and isinstance(sub_id, int):
                    pool_key = pending_ids[resp_id]
                    self._sub_to_pool[sub_id] = pool_key
                    confirmed += 1
                    logger.info(
                        "SVM pool %s…%s subscribed (sub_id=%d)",
                        pool_key[:6], pool_key[-4:], sub_id,
                    )
                elif "error" in resp:
                    logger.error(
                        "SVM logsSubscribe error for req_id=%s: %s",
                        resp_id, resp["error"],
                    )
                    confirmed += 1

            logger.info(
                "SVM subscribed to %d/%d pool(s).",
                len(self._sub_to_pool), expected,
            )

            async for raw_msg in ws:
                if not self._running:
                    return
                try:
                    msg: dict[str, Any] = json.loads(raw_msg)
                except json.JSONDecodeError:
                    continue

                if msg.get("method") != "logsNotification":
                    continue

                params = msg.get("params", {})
                sub_id = params.get("subscription")
                pool_key = self._sub_to_pool.get(sub_id)
                if pool_key is None:
                    continue

                value = params.get("result", {}).get("value", {})
                if not value or value.get("err") is not None:
                    continue

                tx_sig: str = value.get("signature", "")
                logs: list[str] = value.get("logs", [])

                # 1. Check for Raydium Swap
                event = self._parse_raydium_transaction(tx_sig, logs, pool_key)
                if event is not None:
                    await self._queue.put(event)
                    logger.debug(
                        "SVM Swap queued: pool=%s sig=%s",
                        pool_key[:10], tx_sig[:12],
                    )

                # 2. Check for Pump.fun Mint / Bonding Curve Creation
                pump_info = _parse_pump_fun_logs(logs, tx_sig)
                if pump_info is not None:
                    mint_addr = pump_info["mint"]
                    curve_addr = pump_info["bonding_curve"]
                    if (
                        mint_addr
                        and mint_addr not in SOLANA_SYSTEM_PROGRAM_IDS
                        and not mint_addr.startswith("11111111")
                    ):
                        self._pool_registry[curve_addr] = (
                            mint_addr,
                            "So11111111111111111111111111111111111111112",
                            pump_info["token_decimals"],
                            pump_info["native_decimals"],
                        )
                        pool_state = PoolState(
                            pool_address=curve_addr,
                            chain=ChainIdentifier.SOLANA_MAINNET,
                            token_address=mint_addr,
                            native_reserve=pump_info["virtual_sol_reserves"],
                            token_reserve=pump_info["virtual_token_reserves"],
                            fee_numerator=10,
                            fee_denominator=1000,
                            last_updated_block=0,
                            token_decimals=pump_info["token_decimals"],
                            native_decimals=pump_info["native_decimals"],
                        )
                        await self._queue.put(
                            PoolStateUpdateEvent(
                                timestamp_ns=time.time_ns(),
                                chain=ChainIdentifier.SOLANA_MAINNET,
                                pool_address=curve_addr,
                                new_pool_state=pool_state,
                            )
                        )
                        await self._queue.put(
                            RawSignalEvent(
                                chain=ChainIdentifier.SOLANA_MAINNET,
                                token_address=mint_addr,
                                pool_address=curve_addr,
                                source=SignalSource.PUMP_FUN_MINT,
                                originating_channel="pump_fun_stream",
                                raw_text=f"Pump.fun New Mint: {mint_addr} curve={curve_addr}",
                            )
                        )
                        logger.info("SVM Pump.fun mint detected: %s (curve=%s)", mint_addr[:10], curve_addr[:10])

                # 3. Check for Raydium AMM Pool Creation
                ray_init = _parse_raydium_initialize2_logs(logs, tx_sig)
                if ray_init is not None:
                    pool_addr = ray_init.get("pool_address", "")
                    if (
                        pool_addr
                        and pool_addr not in SOLANA_SYSTEM_PROGRAM_IDS
                        and not pool_addr.startswith("11111111")
                    ):
                        await self._queue.put(
                            RawSignalEvent(
                                chain=ChainIdentifier.SOLANA_MAINNET,
                                token_address=pool_addr,
                                pool_address=pool_addr,
                                source=SignalSource.PAIR_CREATED,
                                originating_channel="raydium_stream",
                                raw_text=f"Raydium AMM CreatePool: {pool_addr}",
                            )
                        )
                        logger.info("SVM Raydium CreatePool detected: %s", pool_addr[:10])


    def _parse_raydium_transaction(
        self,
        tx_signature: str,
        logs: list[str],
        pool_pubkey: str,
    ) -> SwapEvent | None:
        meta = self._pool_registry.get(pool_pubkey)
        if meta is None:
            return None

        coin_mint, pc_mint, coin_dec, pc_dec = meta

        for line in logs:
            parsed = _parse_raydium_log_line(line)
            if parsed is None:
                continue

            direction: int = parsed["direction"]

            if direction == 0:
                token_in = pc_mint
                token_out = coin_mint
                amount_in = _normalise(parsed["amount_in"], pc_dec)
                amount_out = _normalise(parsed["amount_out"], coin_dec)
            else:
                token_in = coin_mint
                token_out = pc_mint
                amount_in = _normalise(parsed["amount_in"], coin_dec)
                amount_out = _normalise(parsed["amount_out"], pc_dec)

            if amount_in <= Decimal(0) or amount_out <= Decimal(0):
                continue

            return SwapEvent(
                timestamp_ns=time.time_ns(),
                block_number=0,
                chain=ChainIdentifier.SOLANA_MAINNET,
                pool_address=pool_pubkey,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_out,
                sender=tx_signature[:44],
                tx_hash=tx_signature,
                log_index=None,
            )

        return None
