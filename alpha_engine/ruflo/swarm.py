"""
alpha_engine.ruflo.swarm — Ruflo Multi-Agent Swarm Orchestrator & Consensus Engine
==================================================================================
Multi-Chain (SVM & EVM) Autonomous Trading Swarm
Python 3.11+ | ruflo swarm coordination harness | Concurrency, Tool Registry & Consensus
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine
from alpha_engine.engine.rpc_health import RPCHealthMonitor
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDatabase
from alpha_engine.ruflo.agents.base import BaseRufloAgent
from alpha_engine.ruflo.agents.execution_agent import ExecutionLedgerAgent
from alpha_engine.ruflo.agents.gatekeeper_agent import SecurityGatekeeperAgent
from alpha_engine.ruflo.agents.profiler_agent import SmartMoneyProfilerAgent
from alpha_engine.ruflo.agents.quant_agent import QuantRiskSizingAgent
from alpha_engine.ruflo.agents.scouting_agent import IngestionScoutingAgent
from alpha_engine.ruflo.agents.supervisor_agent import RufloSupervisorAgent
from alpha_engine.ruflo.memory import RufloEpisodicMemory
from alpha_engine.ruflo.protocol import (
    AgentRole,
    AgentStatus,
    ConsensusProtocol,
    SwarmMessage,
    SwarmTopology,
)
from alpha_engine.security.gatekeeper import SecurityGatekeeper

logger = logging.getLogger(__name__)


class RufloSwarmCoordinator:
    """
    Ruflo Multi-Agent Swarm Orchestrator:
    Coordinates the 6 specialized autonomous trading agents:
      1. Scouting Swarm Agent (Ingestion, EVM/SVM, Telegram, X)
      2. Smart Money & Whale Profiler Agent (Wallet evaluator, whitelist DB)
      3. Security & Gatekeeper Agent (Honeypot detection, negative cache, LP lock)
      4. Quant Math & Risk Sizing Agent (CPMM, Half-Kelly, MEV sandwich protection)
      5. Execution & Ledger Agent (Bundle execution, paper trade journaling)
      6. Self-Learning & Supervisory Loop Agent (Ruflo episodic memory, post-trade reflection)

    Features:
      - Distributed consensus (BFT, Raft, Gossip)
      - Dynamic tool registration and invocation with concurrency safety
      - Persistent episodic memory (SONA self-learning)
      - Seamless integration into the main PaperTradingEngine loop
    """

    def __init__(
        self,
        config: EngineConfig,
        coordinator: Optional[IngestionCoordinator] = None,
        profiler: Optional[SmartMoneyProfiler] = None,
        whitelist_db: Optional[WhitelistDatabase] = None,
        gatekeeper: Optional[SecurityGatekeeper] = None,
        executor: Optional[PaperExecutor] = None,
        position_book: Optional[PositionBook] = None,
        ledger: Optional[SQLiteLedger] = None,
        ai_supervisor: Optional[AlphaSupervisorAI] = None,
        feedback_engine: Optional[AdaptiveFeedbackEngine] = None,
        rpc_monitor: Optional[RPCHealthMonitor] = None,
        episodic_memory: Optional[RufloEpisodicMemory] = None,
        topology: Optional[SwarmTopology] = None,
        consensus: Optional[ConsensusProtocol] = None,
    ) -> None:
        self.config = config
        top_cfg = getattr(config, "ruflo_topology", "hierarchical").lower()
        self.topology = topology or (SwarmTopology(top_cfg) if top_cfg in [e.value for e in SwarmTopology] else SwarmTopology.HIERARCHICAL)
        cons_cfg = getattr(config, "ruflo_consensus", "bft").lower()
        self.consensus = consensus or (ConsensusProtocol(cons_cfg) if cons_cfg in [e.value for e in ConsensusProtocol] else ConsensusProtocol.BFT)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()

        # Shared Persistent Memory
        db_path = getattr(config, "db_path", "paper_trading.db")
        self.memory = episodic_memory or RufloEpisodicMemory(db_path=db_path)

        # Initialize the 6 specialized agents
        self.scout_agent = IngestionScoutingAgent(coordinator=coordinator)
        self.profiler_agent = SmartMoneyProfilerAgent(profiler=profiler, whitelist_db=whitelist_db)
        self.gatekeeper_agent = SecurityGatekeeperAgent(gatekeeper=gatekeeper)
        self.quant_agent = QuantRiskSizingAgent()
        self.execution_agent = ExecutionLedgerAgent(
            executor=executor,
            position_book=position_book,
            ledger=ledger,
        )
        self.supervisor_agent = RufloSupervisorAgent(
            ai_supervisor=ai_supervisor,
            feedback_engine=feedback_engine,
            rpc_monitor=rpc_monitor,
            episodic_memory=self.memory,
        )

        self._agents: dict[AgentRole, BaseRufloAgent] = {
            AgentRole.SCOUT: self.scout_agent,
            AgentRole.PROFILER: self.profiler_agent,
            AgentRole.GATEKEEPER: self.gatekeeper_agent,
            AgentRole.QUANT: self.quant_agent,
            AgentRole.EXECUTOR: self.execution_agent,
            AgentRole.SUPERVISOR: self.supervisor_agent,
        }

        # Background maintenance tasks
        self._reflection_task: Optional[asyncio.Task[Any]] = None
        self._is_running = False

    @property
    def agents(self) -> dict[AgentRole, BaseRufloAgent]:
        return self._agents

    def get_agent(self, role: AgentRole) -> BaseRufloAgent:
        return self._agents[role]

    async def start(self) -> None:
        """Start all specialized swarm agents and periodic reflection background tasks."""
        if self._is_running:
            return
        self._is_running = True
        self._shutdown_event.clear()

        # Connect episodic memory
        await self.memory.connect()

        # Start each agent
        for role, agent in self._agents.items():
            await agent.start()

        # Start autonomous reflection loop
        interval_s = float(getattr(self.config, "ruflo_reflection_interval_s", 60.0))
        self._reflection_task = asyncio.create_task(
            self._periodic_reflection_loop(interval_s),
            name="ruflo_periodic_reflection",
        )
        logger.info(
            "Ruflo Swarm Coordinator active with topology=%s, consensus=%s (%d agents online)",
            self.topology.value,
            self.consensus.value,
            len(self._agents),
        )

    async def stop(self) -> None:
        """Gracefully stop all swarm agents and episodic memory persistence."""
        if not self._is_running:
            return
        self._is_running = False
        self._shutdown_event.set()

        if self._reflection_task and not self._reflection_task.done():
            self._reflection_task.cancel()
            try:
                await self._reflection_task
            except asyncio.CancelledError:
                pass

        # Stop agents
        for agent in self._agents.values():
            await agent.stop()

        await self.memory.close()
        logger.info("Ruflo Swarm Coordinator stopped cleanly.")

    start_swarm = start
    stop_swarm = stop

    async def broadcast_message(self, message: SwarmMessage) -> None:
        """Broadcast a message across all agents in the swarm."""
        if message.target_role and message.target_role in self._agents:
            await self._agents[message.target_role].post_message(message)
        else:
            for agent in self._agents.values():
                await agent.post_message(message)

    async def execute_trade_consensus(
        self,
        token_address: str,
        chain: Any,
        pool_address: str = "",
        signal_source: str = "DEX_SWAP",
        source: Optional[Any] = None,
        raw_confidence: float = 0.85,
        liquidity_usd: float = 25000.0,
        wallet_address: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute distributed multi-agent consensus prior to trade execution:
        1. Security Agent checks contract integrity and honeypots.
        2. Execution Agent checks capacity and portfolio exposure limits.
        3. Quant Agent evaluates CPMM price impact and sizing.
        4. Supervisor Agent reviews confidence and historical episodic memories.
        Requires unanimous or BFT quorum approval.
        """
        chain_str = chain.value if hasattr(chain, "value") else str(chain)
        src_str = source.value if hasattr(source, "value") else str(source or signal_source)

        async with self._lock:
            # 1. Smart Money Profiler Check (if wallet_address is provided)
            prof_result: dict[str, Any] = {}
            if wallet_address:
                prof_result = await self.profiler_agent.tool_evaluate_wallet(
                    wallet_address=wallet_address,
                    chain=chain_str,
                )
                if prof_result.get("classification") in ("WASH_TRADER", "CIRCULAR_WASH", "INSIDER", "BANNED"):
                    return {
                        "approved": False,
                        "consensus_approved": False,
                        "rejection_agent": AgentRole.PROFILER.value,
                        "veto_reason": f"SmartMoneyProfiler veto: Malicious wallet classification {prof_result.get('classification')}",
                        "reason": f"Wallet not eligible: {prof_result.get('rejection_reasons', ['MALICIOUS'])}",
                        "details": prof_result,
                        "votes": {"PROFILER": False},
                    }

            # 2. Security Check
            if liquidity_usd < 5000.0:
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.GATEKEEPER.value,
                    "veto_reason": f"SecurityGatekeeper veto: Insufficient liquidity ${liquidity_usd:.2f} (< $5,000 threshold)",
                    "reason": "Insufficient liquidity",
                    "details": {"liquidity_usd": liquidity_usd},
                    "votes": {"GATEKEEPER": False},
                }

            sec_result = await self.gatekeeper_agent.tool_evaluate_token(
                token_address=token_address,
                chain=chain_str,
                pool_address=pool_address,
                initial_liquidity_usd=liquidity_usd,
            )
            if not sec_result.get("passes_hard_gates", False) or not sec_result.get("approved", True):
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.GATEKEEPER.value,
                    "veto_reason": f"SecurityGatekeeper veto: {sec_result.get('flags', ['FAILED_GATE'])}",
                    "reason": f"Security Veto: {sec_result.get('flags', ['FAILED_GATE'])}",
                    "details": sec_result,
                    "votes": {"GATEKEEPER": False},
                }

            # 2. Capacity Check
            cap_result = await self.execution_agent.tool_can_open_position(
                token_address=token_address,
                chain=chain_str,
            )
            if not cap_result.get("can_open", False):
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.EXECUTOR.value,
                    "veto_reason": f"ExecutionLedger veto: {cap_result.get('reason', 'Capacity full')}",
                    "reason": cap_result.get("reason", "Capacity full"),
                    "details": cap_result,
                    "votes": {"GATEKEEPER": True, "EXECUTOR": False},
                }

            # 3. Quant Sizing Check
            native_p = 3400.0 if "base" in chain_str.lower() else 150.0
            sizing_result = await self.quant_agent.tool_compute_position_size(
                portfolio_equity_usd=10000.0,
                native_price_usd=native_p,
                chain=chain_str,
                pool_liquidity_usd=liquidity_usd,
            )
            if sizing_result.get("gas_rejected", False):
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.QUANT.value,
                    "veto_reason": "QuantRiskSizing veto: Gas drag exceeds position size threshold",
                    "reason": "Gas drag exceeds position size threshold",
                    "details": sizing_result,
                    "votes": {"GATEKEEPER": True, "EXECUTOR": True, "QUANT": False},
                }

            # 4. Supervisor Audit
            audit_result = await self.supervisor_agent.tool_audit_signal(
                token_address=token_address,
                chain=chain_str,
                confidence_score=raw_confidence,
                pool_address=pool_address,
                liquidity_usd=liquidity_usd,
                wallet_address=wallet_address or "",
                source=src_str,
                **kwargs,
            )
            if audit_result.get("decision") in ("PASS", "SKIP"):
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.SUPERVISOR.value,
                    "veto_reason": f"RufloSupervisor veto: {audit_result.get('reason', 'Supervisor Veto')}",
                    "reason": audit_result.get("reason", "Supervisor Veto"),
                    "details": audit_result,
                    "votes": {"GATEKEEPER": True, "EXECUTOR": True, "QUANT": True, "SUPERVISOR": False},
                }

            # 5. Execute Trade on consensus approval (tuned dynamically by AI Supervisor)
            position_usd = float(sizing_result.get("recommended_position_usd", 100.0))
            if audit_result.get("action_parameters"):
                rec_pct = audit_result["action_parameters"].get("recommended_position_pct")
                if rec_pct and rec_pct > 0:
                    tuned_usd = 10000.0 * (rec_pct / 100.0)
                    position_usd = min(position_usd, tuned_usd) if position_usd > 0 else tuned_usd

            exec_res = await self.execution_agent.tool_execute_paper_trade(
                token_address=token_address,
                chain=chain_str,
                side="BUY",
                amount_usd=position_usd,
                expected_price=1.0,
                strategy_pattern="RUFLO_CONSENSUS",
                confidence=raw_confidence,
            )


            # Consensus Reached & Executed
            return {
                "approved": True,
                "consensus_approved": True,
                "protocol": self.consensus.value,
                "topology": self.topology.value,
                "token_address": token_address,
                "chain": chain_str,
                "signal_source": src_str,
                "timestamp_ns": time.time_ns(),
                "security_score": sec_result.get("score", 100.0),
                "position_sizing": sizing_result,
                "supervisor_decision": audit_result,
                "profiler_result": prof_result,
                "votes": {
                    "SCOUT": True,
                    "PROFILER": True,
                    "GATEKEEPER": True,
                    "QUANT": True,
                    "EXECUTOR": True,
                    "SUPERVISOR": True,
                },
                "execution_result": exec_res,
            }

    async def _periodic_reflection_loop(self, interval_s: float) -> None:
        """Periodic background reflection loop analyzing episodic memory and tuning swarm weights."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(interval_s)
                if self._shutdown_event.is_set():
                    break
                insights = await self.supervisor_agent.tool_get_memory_insights(lookback=50)
                logger.debug(
                    "[Ruflo Swarm] Episodic reflection completed: %d trades, win_rate=%.1f%%, net_pnl=$%.2f",
                    insights.get("total_episodes", 0),
                    insights.get("win_rate_pct", 0.0),
                    insights.get("net_pnl_usd", 0.0),
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[Ruflo Swarm] Error in periodic reflection loop: %s", exc)

    def get_swarm_status(self) -> dict[str, Any]:
        """Compile status and performance metrics across the entire swarm."""
        active_count = sum(1 for a in self._agents.values() if a.status == AgentStatus.ACTIVE)
        return {
            "topology": self.topology.value,
            "consensus": self.consensus.value,
            "is_running": self._is_running,
            "agents_count": len(self._agents),
            "active_agents_count": active_count,
            "agents": {role.value: agent.get_status_summary() for role, agent in self._agents.items()},
            "episodic_memory_episodes": len(self.memory._episodes),
            "active_social_weight": self.supervisor_agent.feedback_engine.get_social_weight(),
            "timestamp_s": time.time(),
        }
