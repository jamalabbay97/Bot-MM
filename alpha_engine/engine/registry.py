"""
alpha_engine.engine.registry — Dynamic In-Memory Liquidity Pool Registry
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import time
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
        target_token = getattr(swap, "mint", None)
        if not target_token:
            if is_blacklisted_token(swap.token_out, pool.chain):
                target_token = swap.token_in
            elif is_blacklisted_token(swap.token_in, pool.chain):
                target_token = swap.token_out
            else:
                target_token = pool.token_address

        target_pool_address = swap.pool_address or pool.pool_address

        # 1. Authoritative on-chain reserves if provided by binary log decoding
        if (
            isinstance(swap, PumpSwapEvent)
            and getattr(swap, "has_authoritative_reserves", False)
            and getattr(swap, "virtual_sol_reserves", Decimal(0)) > Decimal(0)
            and getattr(swap, "virtual_token_reserves", Decimal(0)) > Decimal(0)
        ):
            new_pool = pool.model_copy(
                update={
                    "pool_address": target_pool_address,
                    "token_address": target_token,
                    "native_reserve": swap.virtual_sol_reserves,
                    "token_reserve": swap.virtual_token_reserves,
                    "last_updated_block": swap.block_number,
                    "timestamp_s": time.time(),
                }
            )
            self._pools[target_pool_address] = new_pool
            return new_pool

        # 2. Determine swap direction: BUY vs SELL of target_token
        # SELL: Trader sends token IN, receives native currency OUT.
        # BUY:  Trader sends native currency IN, receives token OUT.
        is_sell = False
        if hasattr(swap, "is_buy") and isinstance(swap.is_buy, bool):
            is_sell = not swap.is_buy
        elif target_token and swap.token_in.lower() == target_token.lower():
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
                "pool_address": target_pool_address,
                "token_address": target_token,
                "native_reserve": new_native,
                "token_reserve": new_token,
                "last_updated_block": swap.block_number,
                "timestamp_s": time.time(),
            }
        )
        self._pools[target_pool_address] = new_pool
        return new_pool

    def get(self, pool_or_token_address: str, expected_token: str | None = None) -> PoolState | None:
        if not pool_or_token_address:
            return None

        # 1. Exact pool address lookup
        pool = self._pools.get(pool_or_token_address)
        if pool is not None:
            if expected_token:
                tok = getattr(pool, "token_address", None)
                if tok and tok.lower() != expected_token.lower():
                    return None
            return pool

        # 2. Case-insensitive pool address lookup
        target_lower = pool_or_token_address.lower()
        for p_addr, p in self._pools.items():
            if p_addr.lower() == target_lower:
                if expected_token:
                    tok = getattr(p, "token_address", None)
                    if tok and tok.lower() != expected_token.lower():
                        return None
                return p

        # 3. Fallback: Lookup by token address if no pool address matched
        matching_pools = [
            p for p in self._pools.values()
            if getattr(p, "token_address", None) and p.token_address.lower() == target_lower
        ]
        if expected_token:
            matching_pools = [
                p for p in matching_pools
                if getattr(p, "token_address", None) and p.token_address.lower() == expected_token.lower()
            ]

        if len(matching_pools) == 1:
            return matching_pools[0]
        elif len(matching_pools) > 1:
            # Ambiguous token lookup: multiple pools exist for this token!
            # Do NOT treat an arbitrary pool as the authoritative state.
            return None

        return None

    def get_pool(self, pool_address: str, expected_token: str | None = None) -> PoolState | None:
        """Lookup pool strictly by pool address."""
        if not pool_address:
            return None
        pool = self._pools.get(pool_address)
        if pool is None:
            target_lower = pool_address.lower()
            for p_addr, p in self._pools.items():
                if p_addr.lower() == target_lower:
                    pool = p
                    break
        if pool is not None and expected_token:
            tok = getattr(pool, "token_address", None)
            if tok and tok.lower() != expected_token.lower():
                return None
        return pool

    def get_pools_for_token(self, token_address: str) -> list[PoolState]:
        """Return all distinct pool states associated with a token."""
        if not token_address:
            return []
        target_lower = token_address.lower()
        return [
            p for p in self._pools.values()
            if getattr(p, "token_address", None) and p.token_address.lower() == target_lower
        ]

    def set(self, pool_address: str, state: PoolState) -> None:
        """Store pool state keyed by pool address."""
        target_addr = pool_address or state.pool_address
        self._pools[target_addr] = state
        if state.pool_address and state.pool_address != target_addr:
            self._pools[state.pool_address] = state


import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from alpha_engine.ruflo.protocol import AgentRole, ToolDefinition

logger = logging.getLogger(__name__)


@dataclass
class RufloToolRegistration:
    """Container for a registered tool with its handler, metadata, and concurrency lock."""

    tool_def: ToolDefinition
    handler: Callable[..., Coroutine[Any, Any, Any]]
    agent_name: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    execution_count: int = 0
    error_count: int = 0


class RufloToolRegistry:
    """
    Dynamic structured tool registry for the Ruflo multi-agent swarm.
    Manages tool schemas, role permissions, concurrency locks, and execution telemetry.
    """

    def __init__(self) -> None:
        self._tools: dict[str, RufloToolRegistration] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        tool_def: ToolDefinition,
        handler: Callable[..., Coroutine[Any, Any, Any]],
        agent_name: str = "system",
    ) -> None:
        self._tools[tool_def.name] = RufloToolRegistration(
            tool_def=tool_def,
            handler=handler,
            agent_name=agent_name,
        )
        logger.debug("RufloToolRegistry: Registered '%s' from '%s'", tool_def.name, agent_name)

    def get(self, name: str) -> Optional[RufloToolRegistration]:
        return self._tools.get(name)

    def list_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": reg.tool_def.name,
                "description": reg.tool_def.description,
                "category": reg.tool_def.category,
                "agent_name": reg.agent_name,
                "requires_lock": reg.tool_def.requires_lock,
                "execution_count": reg.execution_count,
                "error_count": reg.error_count,
            }
            for reg in self._tools.values()
        ]

    async def execute(self, tool_name: str, **kwargs: Any) -> Any:
        reg = self.get(tool_name)
        if not reg:
            raise KeyError(f"Tool '{tool_name}' not found in RufloToolRegistry.")

        try:
            if reg.tool_def.requires_lock:
                async with reg.lock:
                    result = await reg.handler(**kwargs)
            else:
                result = await reg.handler(**kwargs)
            reg.execution_count += 1
            return result
        except Exception as exc:
            reg.error_count += 1
            logger.error("RufloToolRegistry error executing '%s': %s", tool_name, exc)
            raise


    def has_tool(self, name: str) -> bool:
        return name in self._tools

    register_tool = register
    invoke_tool = execute


class RufloAgentRegistry:
    """
    Shared agent registry providing agent lookup, state discovery,
    and concurrency synchronization across the trading swarm.
    """

    def __init__(self) -> None:
        self._agents: dict[str, Any] = {}
        self._role_map: dict[AgentRole, Any] = {}
        self._shared_state: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    def register_agent(self, agent: Any) -> None:
        name = getattr(agent, "name", str(agent))
        role = getattr(agent, "role", None)
        self._agents[name] = agent
        if role is not None and isinstance(role, AgentRole):
            self._role_map[role] = agent
        logger.debug("RufloAgentRegistry: Registered agent '%s' (%s)", name, role)

    def has_role(self, role: AgentRole) -> bool:
        return role in self._role_map

    def get_agent(self, role_or_name: Any) -> Optional[Any]:
        if isinstance(role_or_name, AgentRole):
            return self._role_map.get(role_or_name)
        return self._agents.get(role_or_name)

    def get_agent_by_name(self, name: str) -> Optional[Any]:
        return self._agents.get(name)

    def get_agent_by_role(self, role: AgentRole) -> Optional[Any]:
        return self._role_map.get(role)

    def get_active_agents(self) -> list[Any]:
        return list(self._agents.values())

    def set_shared_state(self, key: str, value: Any) -> None:
        self._shared_state[key] = value

    def get_shared_state(self, key: str, default: Any = None) -> Any:
        return self._shared_state.get(key, default)

    def get_swarm_summary(self) -> dict[str, Any]:
        return {
            "registered_agents_count": len(self._agents),
            "agents": list(self._agents.keys()),
            "roles": [r.value for r in self._role_map.keys()],
            "shared_state_keys": list(self._shared_state.keys()),
        }
