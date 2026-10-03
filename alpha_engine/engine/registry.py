"""
alpha_engine.engine.registry — Dynamic In-Memory Liquidity Pool Registry
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

from decimal import Decimal

from alpha_engine.models.events import PumpSwapEvent, SwapEvent
from alpha_engine.models.state import PoolState
from alpha_engine.security.constants import is_blacklisted_token


class PoolRegistry:
    """
    In-memory registry of live PoolState objects, keyed by pool address and token address.
    """

    def __init__(self) -> None:
        self._pools: dict[str, PoolState] = {}

    def update_from_swap(self, swap: SwapEvent, pool: PoolState) -> PoolState:
        """Apply a swap event to a pool's reserves and store the new state."""
        # 1. Authoritative on-chain reserves if provided by binary log decoding
        if (
            isinstance(swap, PumpSwapEvent)
            and getattr(swap, "has_authoritative_reserves", False)
            and getattr(swap, "virtual_sol_reserves", Decimal(0)) > Decimal(0)
            and getattr(swap, "virtual_token_reserves", Decimal(0)) > Decimal(0)
        ):
            new_pool = pool.model_copy(
                update={
                    "native_reserve": swap.virtual_sol_reserves,
                    "token_reserve": swap.virtual_token_reserves,
                    "last_updated_block": swap.block_number,
                }
            )
            self._pools[swap.pool_address] = new_pool
            if pool.token_address:
                self._pools[pool.token_address] = new_pool
            return new_pool

        # 2. Determine swap direction: BUY vs SELL of pool.token_address
        # SELL: Trader sends token IN, receives native currency OUT.
        # BUY:  Trader sends native currency IN, receives token OUT.
        is_sell = False
        if hasattr(swap, "is_buy") and isinstance(swap.is_buy, bool):
            is_sell = not swap.is_buy
        elif pool.token_address and swap.token_in.lower() == pool.token_address.lower():
            is_sell = True
        elif is_blacklisted_token(swap.token_out, pool.chain):
            is_sell = True

        if is_sell:
            # Trader sold tokens to pool: token reserve grows, native reserve shrinks
            new_native = max(Decimal("0.000000001"), pool.native_reserve - swap.amount_out)
            new_token = max(Decimal("1"), pool.token_reserve + swap.amount_in)
        else:
            # Trader bought tokens from pool: native reserve grows, token reserve shrinks
            new_native = max(Decimal("0.000000001"), pool.native_reserve + swap.amount_in)
            new_token = max(Decimal("1"), pool.token_reserve - swap.amount_out)

        new_pool = pool.model_copy(
            update={
                "native_reserve": new_native,
                "token_reserve": new_token,
                "last_updated_block": swap.block_number,
            }
        )
        self._pools[swap.pool_address] = new_pool
        if pool.token_address:
            self._pools[pool.token_address] = new_pool
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
        if getattr(state, "token_address", None):
            self._pools[state.token_address] = state
