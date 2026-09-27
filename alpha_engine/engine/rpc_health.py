"""
alpha_engine.engine.rpc_health — Multi-RPC Health Benchmarking & Failover Supervisor
====================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from alpha_engine.models.enums import ChainIdentifier

logger = logging.getLogger(__name__)


@dataclass
class RPCEndpoint:
    """State tracking for a single RPC node endpoint."""

    url: str
    chain: ChainIdentifier
    latency_ms: float = 0.0
    latest_block: int = 0
    is_healthy: bool = True
    consecutive_errors: int = 0
    last_checked_ns: int = field(default=0)
    metadata: dict[str, Any] = field(default_factory=dict)


class RPCHealthMonitor:
    """
    Maintains multi-RPC failover with automatic latency benchmarking and block lag detection:
    - Benchmarks round-trip latency (HTTP JSON-RPC eth_blockNumber / getSlot).
    - If node desync > 400ms or block lag > 2, triggers immediate seamless failover.
    - Seamlessly retains all active trade state across failover events.
    """

    def __init__(
        self,
        endpoints: dict[ChainIdentifier, list[str]],
        latency_threshold_ms: float = 400.0,
        max_block_lag: int = 2,
    ) -> None:
        self._latency_threshold_ms = latency_threshold_ms
        self._max_block_lag = max_block_lag

        self._endpoints: dict[ChainIdentifier, list[RPCEndpoint]] = {}
        self._active_indices: dict[ChainIdentifier, int] = {}

        for chain, urls in endpoints.items():
            if not urls:
                continue
            self._endpoints[chain] = [
                RPCEndpoint(url=u, chain=chain) for u in urls
            ]
            self._active_indices[chain] = 0

    def get_active_rpc(self, chain: ChainIdentifier) -> str:
        """Return the current active RPC endpoint URL for a given chain."""
        eps = self._endpoints.get(chain, [])
        if not eps:
            return ""
        idx = self._active_indices.get(chain, 0)
        return eps[idx].url

    async def benchmark_endpoint(
        self,
        endpoint: RPCEndpoint,
        session: Optional[aiohttp.ClientSession] = None,
        timeout_seconds: float = 2.0,
    ) -> RPCEndpoint:
        """Measure round-trip response latency and query latest block/slot."""
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        if endpoint.chain == ChainIdentifier.BASE_MAINNET:
            payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        else:
            payload = {"jsonrpc": "2.0", "method": "getSlot", "params": [], "id": 1}

        t_start = time.perf_counter()
        try:
            async with session.post(
                endpoint.url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as resp:
                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000.0
                endpoint.latency_ms = latency_ms

                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result")
                    if res is not None:
                        if isinstance(res, str) and res.startswith("0x"):
                            endpoint.latest_block = int(res, 16)
                        else:
                            endpoint.latest_block = int(res)
                        endpoint.is_healthy = True
                        endpoint.consecutive_errors = 0
                    else:
                        endpoint.consecutive_errors += 1
                        endpoint.is_healthy = False
                else:
                    endpoint.consecutive_errors += 1
                    endpoint.is_healthy = False

        except Exception as exc:  # noqa: BLE001
            t_end = time.perf_counter()
            endpoint.latency_ms = (t_end - t_start) * 1000.0
            endpoint.consecutive_errors += 1
            endpoint.is_healthy = False
            logger.debug("Benchmark error for %s (%s): %s", endpoint.url[:30], endpoint.chain.value, exc)
        finally:
            endpoint.last_checked_ns = time.time_ns()
            if close_session:
                await session.close()

        return endpoint

    async def check_and_failover(
        self,
        chain: ChainIdentifier,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> str:
        """
        Benchmark all endpoints for a chain, evaluate latency and block lag,
        and trigger failover if threshold conditions are breached.
        """
        eps = self._endpoints.get(chain, [])
        if not eps or len(eps) <= 1:
            return self.get_active_rpc(chain)

        # Benchmark in parallel
        await asyncio.gather(*(self.benchmark_endpoint(ep, session=session) for ep in eps))

        max_block = max((ep.latest_block for ep in eps if ep.is_healthy), default=0)
        curr_idx = self._active_indices.get(chain, 0)
        curr_ep = eps[curr_idx]

        block_lag = max_block - curr_ep.latest_block if curr_ep.is_healthy else 999
        needs_failover = (
            not curr_ep.is_healthy
            or curr_ep.latency_ms > self._latency_threshold_ms
            or block_lag > self._max_block_lag
        )

        if needs_failover:
            # Find candidate with is_healthy, block_lag <= max_lag, lowest latency
            healthy_candidates = [
                (i, ep)
                for i, ep in enumerate(eps)
                if ep.is_healthy and (max_block - ep.latest_block) <= self._max_block_lag
            ]
            if healthy_candidates:
                best_idx, best_ep = min(healthy_candidates, key=lambda x: x[1].latency_ms)
                if best_idx != curr_idx:
                    logger.warning(
                        "RPC FAILOVER TRIGGERED [%s]: Switching from %s (latency=%.1fms, lag=%d) "
                        "-> %s (latency=%.1fms, lag=%d). Active trade state preserved.",
                        chain.value,
                        curr_ep.url[:35],
                        curr_ep.latency_ms,
                        block_lag,
                        best_ep.url[:35],
                        best_ep.latency_ms,
                        max_block - best_ep.latest_block,
                    )
                    self._active_indices[chain] = best_idx
            else:
                logger.error(
                    "All RPC endpoints degraded for %s! Retaining %s (latency=%.1fms, lag=%d)",
                    chain.value,
                    curr_ep.url[:35],
                    curr_ep.latency_ms,
                    block_lag,
                )

        return self.get_active_rpc(chain)

    def get_status(self) -> dict[str, Any]:
        """Return diagnostic health status across all registered endpoints."""
        report = {}
        for chain, eps in self._endpoints.items():
            active_idx = self._active_indices.get(chain, 0)
            report[chain.value] = {
                "active_url": eps[active_idx].url if eps else None,
                "endpoints": [
                    {
                        "url": ep.url,
                        "latency_ms": ep.latency_ms,
                        "latest_block": ep.latest_block,
                        "is_healthy": ep.is_healthy,
                        "is_active": i == active_idx,
                    }
                    for i, ep in enumerate(eps)
                ],
            }
        return report
