"""
alpha_engine.ingestion.evm — EVM (Base) WebSocket Log Ingester
==============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | websockets
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Sequence

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from alpha_engine.ingestion.decoders import (
    _SWAP_TOPIC,
    _SYNC_TOPIC,
    EvmPoolMeta,
    _decode_evm_swap_log,
    _decode_evm_sync_log,
)
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    ShutdownSentinel,
    SwapEvent,
)
from alpha_engine.models.state import PoolState
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

_RECONNECT_INITIAL_DELAY_S: float = 1.0
_RECONNECT_MULTIPLIER: float = 2.0
_RECONNECT_MAX_DELAY_S: float = 60.0
_RECONNECT_JITTER_FRACTION: float = 0.5


class EVMIngester:
    """
    Ingests Uniswap v2 / Aerodrome Swap and Sync events from Base via Alchemy.
    """

    def __init__(
        self,
        ws_url: str,
        pool_watchlist: Sequence[tuple[str, str, str, int, int, str]],
        event_queue: asyncio.Queue[SwapEvent | PoolStateUpdateEvent | ShutdownSentinel],
        limiter: RateLimiterRegistry,
    ) -> None:
        if len(pool_watchlist) > 5:
            raise ValueError(
                f"EVMIngester watchlist capped at 5 addresses (free tier). "
                f"Got {len(pool_watchlist)}."
            )
        self._ws_url = ws_url
        self._queue = event_queue
        self._limiter = limiter
        self._running = False

        from alpha_engine.ingestion.coordinator import ExponentialBackoff
        self._backoff = ExponentialBackoff()

        self._pool_meta: dict[str, EvmPoolMeta] = {
            addr.lower(): (t0.lower(), t1.lower(), d0, d1, nat.lower())
            for addr, t0, t1, d0, d1, nat in pool_watchlist
        }
        self._live_pools: dict[str, PoolState] = {}

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
                if self._running:
                    logger.info("EVM WebSocket closed cleanly — reconnecting.")
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                if not self._running:
                    break
                delay = self._backoff.next_delay()
                logger.warning("EVM WebSocket error: %s — retrying in %.1fs", exc, delay)
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                logger.info("EVMIngester task cancelled.")
                break

    async def stop(self) -> None:
        self._running = False

    async def _connect_and_stream(self) -> None:
        await self._limiter.alchemy.acquire(cost=1.0)

        async with websockets.connect(
            self._ws_url,
            ping_interval=20,
            ping_timeout=30,
        ) as ws:
            logger.info("EVM WebSocket connected: %s", self._ws_url[:60])
            self._backoff.reset()

            pool_addresses = list(self._pool_meta.keys())

            subscribe_msg = json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": pool_addresses,
                        "topics": [[_SWAP_TOPIC, _SYNC_TOPIC]],
                    },
                ],
            })
            await ws.send(subscribe_msg)

            confirmation = json.loads(await ws.recv())
            sub_id = confirmation.get("result")
            if not sub_id:
                raise WebSocketException(
                    f"eth_subscribe failed: {confirmation.get('error')}"
                )
            logger.info(
                "EVM subscribed (sub_id=%s) for %d pools [Swap + Sync]",
                sub_id, len(pool_addresses),
            )

            async for raw_msg in ws:
                if not self._running:
                    return
                try:
                    msg: dict[str, Any] = json.loads(raw_msg)
                except json.JSONDecodeError as exc:
                    logger.debug("Non-JSON WS message: %s", exc)
                    continue

                params = msg.get("params", {})
                result = params.get("result", {})
                log_entry: dict[str, Any] = result if isinstance(result, dict) else {}

                pool_addr = log_entry.get("address", "").lower()
                meta = self._pool_meta.get(pool_addr)
                if meta is None:
                    continue

                token0, token1, dec0, dec1, native = meta
                topics: list[str] = log_entry.get("topics", [])
                if not topics:
                    continue

                topic0 = topics[0].lower()

                if topic0 == _SWAP_TOPIC:
                    event = _decode_evm_swap_log(
                        log_entry, token0, token1, dec0, dec1, native
                    )
                    if event is not None:
                        await self._queue.put(event)
                        logger.debug(
                            "EVM Swap queued: pool=%s tx=%s",
                            event.pool_address[:10], event.tx_hash[:12],
                        )

                elif topic0 == _SYNC_TOPIC:
                    existing = self._live_pools.get(pool_addr)
                    update = _decode_evm_sync_log(
                        log_entry, pool_addr, token0, token1,
                        dec0, dec1, native, existing,
                    )
                    if update is not None:
                        self._live_pools[pool_addr] = update.new_pool_state
                        await self._queue.put(update)
                        logger.debug(
                            "EVM Sync queued: pool=%s native_reserve=%s",
                            pool_addr[:10],
                            update.new_pool_state.native_reserve,
                        )
