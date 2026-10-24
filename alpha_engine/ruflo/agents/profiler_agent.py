"""
alpha_engine.ruflo.agents.profiler_agent — Smart Money & Whale Profiler Agent
=============================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Ruflo Profiler Agent | Sybil & Wash-Trading Noise Detection
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.models.enums import ChainIdentifier, WalletClassification, WhitelistStatus
from alpha_engine.models.profiler import WalletProfile, WhitelistRecord
from alpha_engine.profiler.evaluator import WalletEvaluator
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDatabase
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.protocol import AgentRole, SwarmMessage, ToolDefinition

logger = logging.getLogger(__name__)


class SmartMoneyProfilerAgent(BaseRufloAgent):
    """
    Smart Money & Whale Profiler Agent:
    Wraps SmartMoneyProfiler, WalletEvaluator, and WhitelistDatabase.
    Evaluates wallet PnL, Sortino/Sharpe ratios, win rates, and detects Sybil/wash-trading
    clusters and MEV sandwich bots before copy-trade signal approval.
    """

    def __init__(
        self,
        profiler: Optional[SmartMoneyProfiler] = None,
        whitelist_db: Optional[WhitelistDatabase] = None,
        name: str = "WhaleProfiler",
        config: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            role=AgentRole.PROFILER,
            description="Smart money tracking, wallet profiling, and Sybil detection agent",
        )
        self.config = config
        self.profiler = profiler or SmartMoneyProfiler(whitelist_db=whitelist_db)
        self.whitelist_db = whitelist_db or self.profiler.whitelist_db
        self.evaluator = self.profiler.evaluator or WalletEvaluator()
        self._register_agent_tools()

    def _register_agent_tools(self) -> None:
        self.register_tool(
            ToolDefinition(
                name="profiler_evaluate_wallet",
                description="Evaluate on-chain wallet metrics and classify as smart money or insider",
                category="profiler",
                parameters_schema={
                    "wallet_address": {"type": "string"},
                    "chain": {"type": "string"},
                },
            ),
            self.tool_evaluate_wallet,
        )
        self.register_tool(
            ToolDefinition(
                name="profiler_check_whitelist",
                description="Query the persistent whitelist status of a target trader wallet",
                category="profiler",
                parameters_schema={"wallet_address": {"type": "string"}},
            ),
            self.tool_check_whitelist,
        )
        self.register_tool(
            ToolDefinition(
                name="profiler_demote_wallet",
                description="Demote or ban a degrading wallet from the copy-trade whitelist",
                category="profiler",
                parameters_schema={
                    "wallet_address": {"type": "string"},
                    "reason": {"type": "string"},
                    "new_status": {"type": "string"},
                },
                requires_lock=True,
            ),
            self.tool_demote_wallet,
        )
        self.register_tool(
            ToolDefinition(
                name="profiler_get_top_wallets",
                description="Retrieve curated top-performing smart money wallets from database",
                category="profiler",
                parameters_schema={"chain": {"type": "string"}, "limit": {"type": "integer"}},
            ),
            self.tool_get_top_wallets,
        )

    async def tool_evaluate_wallet(
        self,
        wallet_address: str,
        chain: str = "solana_mainnet",
    ) -> dict[str, Any]:
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        profile: WalletProfile = await self.profiler.profile_wallet(
            wallet_address=wallet_address,
            chain=chain_enum,
            trades=[],
        )
        is_eligible = (
            profile.is_whitelisted
            or profile.classification == WalletClassification.SMART_MONEY
            or profile.classification.value == "approved"
        )
        return {
            "wallet_address": profile.wallet_address,
            "chain": profile.chain.value,
            "classification": profile.classification.value,
            "is_whitelisted": profile.is_whitelisted,
            "eligible_for_copytrade": is_eligible,
            "smart_money_score": profile.smart_money_score,
            "win_rate_pct": profile.win_rate_pct,
            "total_trades": profile.total_trades,
            "total_pnl_usd": str(profile.total_pnl_usd),
            "rejection_reasons": profile.rejection_reasons,
        }

    async def tool_check_whitelist(self, wallet_address: str) -> dict[str, Any]:
        record: Optional[WhitelistRecord] = await self.whitelist_db.get_wallet(wallet_address)
        if not record:
            return {"wallet_address": wallet_address, "status": "NOT_FOUND", "is_active": False}
        return {
            "wallet_address": record.wallet_address,
            "chain": record.chain.value,
            "status": record.status.value,
            "is_active": record.status == WhitelistStatus.ACTIVE,
            "smart_money_score": record.smart_money_score,
            "win_rate_pct": record.win_rate_pct,
        }

    async def tool_demote_wallet(
        self,
        wallet_address: str,
        reason: str,
        new_status: str = "SUSPENDED",
    ) -> bool:
        status_enum = getattr(WhitelistStatus, new_status.upper(), WhitelistStatus.SUSPENDED)
        await self.profiler.demote_or_ban_wallet(
            wallet_address=wallet_address,
            new_status=status_enum,
            reason=reason,
        )
        logger.info("[%s] Demoted wallet %s to %s. Reason: %s", self.name, wallet_address[:10], status_enum.value, reason)
        return True

    async def tool_get_top_wallets(
        self,
        chain: str = "solana_mainnet",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        chain_enum = (
            ChainIdentifier.BASE_MAINNET if "base" in chain.lower() or "evm" in chain.lower()
            else ChainIdentifier.SOLANA_MAINNET
        )
        records: list[WhitelistRecord] = await self.whitelist_db.get_active_whitelist(chain=chain_enum, limit=limit)
        return [
            {
                "wallet_address": r.wallet_address,
                "chain": r.chain.value,
                "status": r.status.value,
                "smart_money_score": r.smart_money_score,
                "win_rate_pct": r.win_rate_pct,
                "total_trades": r.total_trades,
            }
            for r in records
        ]

    async def handle_message(self, message: SwarmMessage) -> None:
        if message.message_type == "AUDIT_WALLET":
            wallet = message.payload.get("wallet_address")
            chain = message.payload.get("chain", "solana_mainnet")
            if wallet:
                await self.tool_evaluate_wallet(wallet, chain)
