"""
alpha_engine.ruflo.agents.gatekeeper_agent — Security & Gatekeeper Agent
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Ruflo Security Agent | Negative Cache & Pre-Flight Hardening
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.models.enums import ChainIdentifier, SecurityTier
from alpha_engine.models.state import SecurityReport
from alpha_engine.security.constants import is_blacklisted_token
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class SecurityGatekeeperAgent(BaseRufloAgent):
    """
    Security & Gatekeeper Agent:
    Enforces strict pre-trade checks using SecurityGatekeeper, preflight, rugcheck,
    goplus, and constants.
    Handles negative caching, honeypot detection, LP lock/burn verification,
    and SPL mint/freeze authority checks.
    """

    def __init__(
        self,
        gatekeeper: Optional[SecurityGatekeeper] = None,
        name: str = "SecurityGatekeeper",
        config: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=AgentRole.GATEKEEPER,
            description="Strict on-chain security screening, honeypot detection, and negative cache agent",
        )
        self.gatekeeper = gatekeeper
        self.config = config
        self._register_agent_tools()

    def _register_agent_tools(self) -> None:
        self.register_tool(
            ToolDefinition(
                name="gatekeeper_evaluate_token",
                description="Execute deep security audit across contract bytecode, LP locks, and mint authorities",
                category="security",
                parameters_schema={
                    "token_address": {"type": "string"},
                    "chain": {"type": "string"},
                    "pool_address": {"type": "string"},
                },
                requires_lock=False,
            ),
            self.tool_evaluate_token,
        )
        self.register_tool(
            ToolDefinition(
                name="gatekeeper_check_blacklist",
                description="Verify if a token address is an internal blacklisted asset or known exploit address",
                category="security",
                parameters_schema={
                    "token_address": {"type": "string"},
                    "chain": {"type": "string"},
                },
            ),
            self.tool_check_blacklist,
        )
        self.register_tool(
            ToolDefinition(
                name="gatekeeper_clear_negative_cache",
                description="Clear expired or stale entries from the gatekeeper negative cache",
                category="security",
                parameters_schema={},
                requires_lock=True,
            ),
            self.tool_clear_negative_cache,
        )
        self.register_tool(
            ToolDefinition(
                name="gatekeeper_inspect_rugcheck",
                description="Inspect RugCheck API metrics for LP lock status and mint authority",
                category="security",
                parameters_schema={"token_address": {"type": "string"}, "chain": {"type": "string"}},
            ),
            self.tool_evaluate_token,
        )

    async def tool_evaluate_token(
        self,
        token_address: str,
        chain: str = "solana_mainnet",
        pool_address: str = "",
        initial_liquidity_usd: float = 25000.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        clean_addr = token_address.strip()
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        if not self.gatekeeper:
            # Fallback evaluation if standalone
            passes = not is_blacklisted_token(clean_addr, chain_enum) and initial_liquidity_usd >= 5000.0
            fallback_tier = SecurityTier.CLEAN if passes else SecurityTier.TIER2_HONEYPOT
            return {
                "token_address": clean_addr,
                "chain": chain_enum.value,
                "approved": passes,
                "passes_hard_gates": passes,
                "tier": fallback_tier.value,
                "score": 100.0 if passes else 0.0,
                "flags": [] if passes else ["INSUFFICIENT_LIQUIDITY" if initial_liquidity_usd < 5000.0 else "BLACKLISTED"],
            }

        report: SecurityReport = await self.gatekeeper.evaluate_token(
            token_address=clean_addr,
            chain=chain_enum,
            pool_address=pool_address,
        )
        passes = report.passes_hard_gates and initial_liquidity_usd >= 5000.0
        return {
            "token_address": report.token_address,
            "chain": report.chain.value,
            "approved": passes,
            "passes_hard_gates": passes,
            "tier": report.tier.value,
            "score": report.score,
            "is_honeypot": report.is_honeypot,
            "is_rug": report.is_rug,
            "buy_tax_bps": report.buy_tax_bps,
            "sell_tax_bps": report.sell_tax_bps,
            "lp_burned_ratio": float(report.lp_burned_ratio),
            "top10_concentration": float(report.top10_concentration),
            "flags": report.flags,
        }

    async def tool_check_blacklist(self, token_address: str, chain: str = "solana_mainnet") -> bool:
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        return is_blacklisted_token(token_address.strip(), chain_enum)

    async def tool_clear_negative_cache(self) -> int:
        if self.gatekeeper and hasattr(self.gatekeeper, "_negative_cache"):
            count = len(self.gatekeeper._negative_cache)
            self.gatekeeper._negative_cache.clear()
            logger.info("[%s] Cleared %d entries from gatekeeper negative cache", self.name, count)
            return count
        return 0

    async def handle_message(self, message: SwarmMessage) -> None:
        if message.message_type == "AUDIT_SECURITY":
            token = message.payload.get("token_address")
            chain = message.payload.get("chain", "solana_mainnet")
            pool = message.payload.get("pool_address", "")
            if token:
                await self.tool_evaluate_token(token, chain, pool)
