"""
alpha_engine.ruflo.agents.scouting_agent — Ingestion & Scouting Swarm Agent
===========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Ruflo Ingestion & Scouting Agent | Multi-Chain Streamer
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.models.enums import ChainIdentifier, SignalSource
from alpha_engine.models.events import RawSignalEvent
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class IngestionScoutingAgent(BaseRufloAgent):
    """
    Ingestion & Scouting Swarm Agent:
    Wraps IngestionCoordinator, SVMIngester, EVMIngester, decoders, Telegram, and X streams.
    Asynchronously streams real-time on-chain swaps, Pump.fun launches, and social momentum
    into the staging and signal generation pipelines.
    """

    def __init__(
        self,
        coordinator: Optional[IngestionCoordinator] = None,
        name: str = "IngestionScout",
        config: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=AgentRole.SCOUT,
            description="Autonomous multi-chain market scouting and ingestion agent",
        )
        self.coordinator = coordinator
        self.config = config
        self._register_agent_tools()

    def _register_agent_tools(self) -> None:
        self.register_tool(
            ToolDefinition(
                name="scout_stream_status",
                description="Check active status of EVM, SVM, and social ingestion channels",
                category="ingestion",
                parameters_schema={},
            ),
            self.tool_stream_status,
        )
        self.register_tool(
            ToolDefinition(
                name="scout_subscribe_token",
                description="Subscribe to live binary on-chain order flow for a token mint",
                category="ingestion",
                parameters_schema={"token_address": {"type": "string"}, "chain": {"type": "string"}},
            ),
            self.tool_subscribe_token,
        )
        self.register_tool(
            ToolDefinition(
                name="scout_inject_signal",
                description="Inject an on-demand contract discovery or Telegram alpha call",
                category="ingestion",
                parameters_schema={
                    "token_address": {"type": "string"},
                    "chain": {"type": "string"},
                    "source": {"type": "string"},
                },
            ),
            self.tool_inject_signal,
        )

    async def tool_stream_status(self) -> dict[str, Any]:
        if not self.coordinator:
            return {"status": "NO_COORDINATOR", "active_tasks": 0}
        return {
            "status": "RUNNING" if self.coordinator._tasks else "IDLE",
            "active_tasks_count": len(self.coordinator._tasks),
            "evm_ws_url": getattr(self.coordinator, "evm_ws_url", ""),
            "svm_ws_url": getattr(self.coordinator, "svm_ws_url", ""),
            "has_telegram": getattr(self.coordinator, "telegram_ingester", None) is not None,
            "has_x_stream": getattr(self.coordinator, "x_ingester", None) is not None,
        }

    async def tool_subscribe_token(self, token_address: str, chain: str = "solana_mainnet") -> bool:
        if self.coordinator and self.coordinator.svm_ingester:
            try:
                await self.coordinator.svm_ingester.subscribe_pump_fun_mint(token_address)
                logger.info("[%s] Subscribed SVM ingester to token: %s", self.name, token_address[:10])
                return True
            except Exception as exc:
                logger.warning("[%s] Failed to subscribe to token %s: %s", self.name, token_address[:10], exc)
        return False

    async def tool_inject_signal(
        self,
        token_address: str,
        chain: str = "solana_mainnet",
        source: str = "TELEGRAM_SCRAPER",
    ) -> dict[str, Any]:
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        source_enum = getattr(SignalSource, source.upper(), SignalSource.TELEGRAM_SCRAPER)
        evt = RawSignalEvent(
            token_address=token_address,
            chain=chain_enum,
            source=source_enum,
            metadata={"injected_by": self.name},
        )
        if self.coordinator:
            await self.coordinator.event_queue.put(evt)
            logger.info("[%s] Injected RawSignalEvent for %s into coordinator queue", self.name, token_address[:10])

        return {
            "status": "SIGNAL_INJECTED",
            "token": token_address,
            "chain": chain,
            "source": source,
            "queued": self.coordinator is not None,
        }

    async def handle_message(self, message: SwarmMessage) -> None:
        if message.message_type == "SUBSCRIBE_TOKEN":
            addr = message.payload.get("token_address")
            chain = message.payload.get("chain", "solana_mainnet")
            if addr:
                await self.tool_subscribe_token(addr, chain)
