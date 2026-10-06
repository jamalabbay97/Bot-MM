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
from typing import Any, Sequence, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from alpha_engine.ingestion.decoders import (
    _PAIR_CREATED_TOPIC,
    _POOL_CREATED_TOPIC,
    _SWAP_TOPIC,
    _SYNC_TOPIC,
    _UNISWAP_V3_SWAP_TOPIC,
    EvmPoolMeta,
    _decode_evm_pair_created_log,
    _decode_evm_pool_created_log,
    _decode_evm_swap_log,
    _decode_evm_sync_log,
    _decode_evm_v3_swap_log,
)
from alpha_engine.ingestion.dex_metrics import DEXMetricsAggregator
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.events import (
    PoolStateUpdateEvent,
    RawSignalEvent,
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
        event_queue: asyncio.Queue[SwapEvent | PoolStateUpdateEvent | RawSignalEvent | ShutdownSentinel],
        limiter: RateLimiterRegistry,
        metrics_aggregator: Optional[DEXMetricsAggregator] = None,
    ) -> None:
        if len(pool_watchlist) > 5:
            raise ValueError(
                f"EVMIngester watchlist capped at 5 addresses (free tier). "
                f"Got {len(pool_watchlist)}."
            )
        self._ws_url = ws_url
        self._queue = event_queue
        self._limiter = limiter
        self._metrics_aggregator = metrics_aggregator
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

            # 1. Subscribe to watched pools for Swap, Sync, and V3 Swap events
            if pool_addresses:
                subscribe_pools_msg = json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": pool_addresses,
                            "topics": [[_SWAP_TOPIC, _SYNC_TOPIC, _UNISWAP_V3_SWAP_TOPIC]],
                        },
                    ],
                })
                await ws.send(subscribe_pools_msg)
                conf1 = json.loads(await ws.recv())
                logger.info("EVM watched pools subscribed (sub_id=%s)", conf1.get("result"))

            # 2. Subscribe to factory PairCreated and PoolCreated events
            subscribe_factories_msg = json.dumps({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "topics": [[_PAIR_CREATED_TOPIC, _POOL_CREATED_TOPIC]],
                    },
                ],
            })
            await ws.send(subscribe_factories_msg)
            conf2 = json.loads(await ws.recv())
            logger.info("EVM factory pair creation subscribed (sub_id=%s)", conf2.get("result"))

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

                topics: list[str] = log_entry.get("topics", [])
                if not topics:
                    continue

                topic0 = topics[0].lower()

                # Handle PairCreated (Aerodrome / Uniswap V2)
                if topic0 == _PAIR_CREATED_TOPIC:
                    pair_data = _decode_evm_pair_created_log(log_entry)
                    if pair_data is not None:
                        new_pair = pair_data["pair"]
                        t0 = pair_data["token0"]
                        t1 = pair_data["token1"]
                        weth = "0x4200000000000000000000000000000000000006"
                        self._pool_meta[new_pair] = (t0, t1, 18, 18, weth)
                        target_token = t1 if t0.lower() == weth.lower() else t0
                        await self._queue.put(
                            RawSignalEvent(
                                chain=ChainIdentifier.BASE_MAINNET,
                                token_address=target_token,
                                pool_address=new_pair,
                                source=SignalSource.PAIR_CREATED,
                                originating_channel="evm_factory_stream",
                                raw_text=f"PairCreated: pair={new_pair} token0={t0} token1={t1} factory={pair_data['factory']}",
                            )
                        )
                        logger.info("EVM PairCreated detected: pair=%s token=%s", new_pair[:10], target_token[:10])
                    continue

                # Handle PoolCreated (Uniswap V3 / Aerodrome SlipStream)
                elif topic0 == _POOL_CREATED_TOPIC:
                    pool_data = _decode_evm_pool_created_log(log_entry)
                    if pool_data is not None:
                        new_pool = pool_data["pool"]
                        t0 = pool_data["token0"]
                        t1 = pool_data["token1"]
                        weth = "0x4200000000000000000000000000000000000006"
                        self._pool_meta[new_pool] = (t0, t1, 18, 18, weth)
                        target_token = t1 if t0.lower() == weth.lower() else t0
                        await self._queue.put(
                            RawSignalEvent(
                                chain=ChainIdentifier.BASE_MAINNET,
                                token_address=target_token,
                                pool_address=new_pool,
                                source=SignalSource.PAIR_CREATED,
                                originating_channel="evm_v3_factory_stream",
                                raw_text=f"PoolCreated: pool={new_pool} token0={t0} token1={t1} fee={pool_data['fee']}",
                            )
                        )
                        logger.info("EVM PoolCreated detected: pool=%s token=%s", new_pool[:10], target_token[:10])
                    continue

                pool_addr = log_entry.get("address", "").lower()
                meta = self._pool_meta.get(pool_addr)
                if meta is None:
                    continue

                token0, token1, dec0, dec1, native = meta

                if topic0 == _SWAP_TOPIC:
                    event = _decode_evm_swap_log(
                        log_entry, token0, token1, dec0, dec1, native
                    )
                    if event is not None:
                        if self._metrics_aggregator is not None:
                            self._metrics_aggregator.record_swap(
                                event, pool_state=self._live_pools.get(pool_addr)
                            )
                        await self._queue.put(event)
                        logger.debug(
                            "EVM Swap queued: pool=%s tx=%s",
                            event.pool_address[:10], event.tx_hash[:12],
                        )

                elif topic0 == _UNISWAP_V3_SWAP_TOPIC:
                    v3_result = _decode_evm_v3_swap_log(
                        log_entry, token0, token1, dec0, dec1, native
                    )
                    if v3_result is not None:
                        event, tick, liquidity = v3_result
                        if self._metrics_aggregator is not None:
                            self._metrics_aggregator.record_concentrated_tick(
                                pool_addr, tick, liquidity
                            )
                            self._metrics_aggregator.record_swap(
                                event, pool_state=self._live_pools.get(pool_addr)
                            )
                        await self._queue.put(event)
                        logger.debug(
                            "EVM V3 Swap queued: pool=%s tick=%d tx=%s",
                            event.pool_address[:10], tick, event.tx_hash[:12],
                        )

                elif topic0 == _SYNC_TOPIC:
                    existing = self._live_pools.get(pool_addr)
                    update = _decode_evm_sync_log(
                        log_entry, pool_addr, token0, token1,
                        dec0, dec1, native, existing,
                    )
                    if update is not None:
                        self._live_pools[pool_addr] = update.new_pool_state
                        if self._metrics_aggregator is not None:
                            self._metrics_aggregator.record_pool_update_event(update)
                        await self._queue.put(update)
                        logger.debug(
                            "EVM Sync queued: pool=%s native_reserve=%s",
                            pool_addr[:10],
                            update.new_pool_state.native_reserve,
                        )

