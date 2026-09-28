"""
alpha_engine.engine.registry — Dynamic In-Memory Liquidity Pool Registry
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

from alpha_engine.models.events import SwapEvent
from alpha_engine.models.state import PoolState


class PoolRegistry:
    """
    In-memory registry of live PoolState objects, keyed by pool address.
    """

    def __init__(self) -> None:
        self._pools: dict[str, PoolState] = {}

    def update_from_swap(self, swap: SwapEvent, pool: PoolState) -> PoolState:
        """Apply a swap event to a pool's reserves and store the new state."""
        if swap.token_in == pool.pool_address:
            return pool

        new_pool = pool.model_copy(
            update={
                "native_reserve": pool.native_reserve + swap.amount_in
                if swap.token_in != pool.pool_address
                else pool.native_reserve - swap.amount_out,
                "token_reserve": pool.token_reserve - swap.amount_out
                if swap.token_out != pool.pool_address
                else pool.token_reserve + swap.amount_in,
                "last_updated_block": swap.block_number,
            }
        )
        self._pools[swap.pool_address] = new_pool
        return new_pool

    def get(self, pool_or_token_address: str) -> PoolState | None:
        if pool_or_token_address in self._pools:
            return self._pools[pool_or_token_address]
        target_lower = pool_or_token_address.lower()
        for p in self._pools.values():
            tok = getattr(p, "token_address", None)
            if p.pool_address.lower() == target_lower or (tok and tok.lower() == target_lower):
                return p
        return None

    def set(self, pool_address: str, state: PoolState) -> None:
        self._pools[pool_address] = state
