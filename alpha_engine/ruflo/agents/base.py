"""
alpha_engine.ruflo.agents.base — Base Abstract Agent for Ruflo Multi-Agent Swarm
===============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | ruflo asynchronous agent harness | Concurrency & message bus
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Optional

from alpha_engine.ruflo.protocol import AgentRole, AgentStatus, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class BaseRufloAgent(ABC):
    """
    Abstract base class for specialized agents within the Ruflo swarm.
    Provides lifecycle controls, asynchronous tool dispatching, structured
    message exchange, concurrency locks, and execution telemetry.
    """

    def __init__(
        self,
        name: str,
        role: AgentRole,
        description: str,
    ) -> None:
        self.name = name
        self.role = role
        self.description = description
        self.status: AgentStatus = AgentStatus.INITIALIZING

        self._inbox: asyncio.Queue[SwarmMessage] = asyncio.Queue(maxsize=500)
        self._tools: dict[str, ToolDefinition] = {}
        self._tool_handlers: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}
        self._lock = asyncio.Lock()
        self._running_task: Optional[asyncio.Task[Any]] = None
        self._shutdown_event = asyncio.Event()

        # Telemetry metrics
        self.tasks_executed: int = 0
        self.errors_count: int = 0
        self.last_active_time: float = time.time()

    def register_tool(
        self,
        tool_def: ToolDefinition,
        handler: Callable[..., Coroutine[Any, Any, Any]],
    ) -> None:
        """Register a callable tool with typed schema under this agent."""
        self._tools[tool_def.name] = tool_def
        self._tool_handlers[tool_def.name] = handler
        logger.debug("[%s] Registered tool '%s'", self.name, tool_def.name)

    def get_tools(self) -> list[ToolDefinition]:
        """Return list of all tool definitions exposed by this agent."""
        return list(self._tools.values())

    async def execute_tool(self, tool_name: str, **kwargs: Any) -> Any:
        """Execute a registered tool by name with parameter validation and concurrency lock."""
        if tool_name not in self._tool_handlers:
            raise KeyError(f"Tool '{tool_name}' not registered on agent '{self.name}'.")

        tool_def = self._tools[tool_name]
        handler = self._tool_handlers[tool_name]

        self.last_active_time = time.time()
        try:
            if tool_def.requires_lock:
                async with self._lock:
                    result = await handler(**kwargs)
            else:
                result = await handler(**kwargs)

            self.tasks_executed += 1
            return result
        except Exception as exc:
            self.errors_count += 1
            logger.error("[%s] Error executing tool '%s': %s", self.name, tool_name, exc)
            raise

    async def post_message(self, message: SwarmMessage) -> None:
        """Deliver a message to this agent's inbox."""
        try:
            self._inbox.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("[%s] Inbox saturated; dropping message %s", self.name, message.message_id)

    async def start(self) -> None:
        """Start the agent's internal event loop task."""
        if self.status == AgentStatus.ACTIVE:
            return
        self.status = AgentStatus.ACTIVE
        self._shutdown_event.clear()
        self._running_task = asyncio.create_task(self._run_loop(), name=f"ruflo_agent_{self.name}")
        logger.info("[%s] Ruflo Agent started with role %s", self.name, self.role.value)

    async def stop(self) -> None:
        """Stop the agent and wait for shutdown."""
        self.status = AgentStatus.STOPPED
        self._shutdown_event.set()
        if self._running_task and not self._running_task.done():
            self._running_task.cancel()
            try:
                await self._running_task
            except asyncio.CancelledError:
                pass
        logger.info("[%s] Ruflo Agent stopped", self.name)

    async def _run_loop(self) -> None:
        """Default agent message processing loop."""
        while not self._shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=1.0)
                await self.handle_message(msg)
                self._inbox.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[%s] Error processing message: %s", self.name, exc)

    @abstractmethod
    async def handle_message(self, message: SwarmMessage) -> None:
        """Handle incoming swarm message (implemented by specialized subclasses)."""
        pass

    def get_status_summary(self) -> dict[str, Any]:
        """Return operational telemetry summary of the agent."""
        return {
            "name": self.name,
            "role": self.role.value,
            "status": self.status.value,
            "tools_count": len(self._tools),
            "tools": list(self._tools.keys()),
            "tasks_executed": self.tasks_executed,
            "errors_count": self.errors_count,
            "inbox_size": self._inbox.qsize(),
            "last_active_time": self.last_active_time,
        }
