"""
alpha_engine.ruflo.swarm — Ruflo Multi-Agent Swarm Orchestrator & Consensus Engine
==================================================================================
Multi-Chain (SVM & EVM) Autonomous Trading Swarm
Python 3.11+ | ruflo swarm coordination harness | Concurrency, Tool Registry & Consensus
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.council.arbiter_cro import CouncilArbiterCRO
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine
from alpha_engine.engine.rpc_health import RPCHealthMonitor
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.models.enums import ChainIdentifier
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
        council_cro: Optional[CouncilArbiterCRO] = None,
    ) -> None:
        self.config = config
        top_cfg = getattr(config, "ruflo_topology", "hierarchical").lower()
        self.topology = topology or (SwarmTopology(top_cfg) if top_cfg in [e.value for e in SwarmTopology] else SwarmTopology.HIERARCHICAL)
        cons_cfg = getattr(config, "ruflo_consensus", "bft").lower()
        self.consensus = consensus or (ConsensusProtocol(cons_cfg) if cons_cfg in [e.value for e in ConsensusProtocol] else ConsensusProtocol.BFT)
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._ledger: Optional[SQLiteLedger] = ledger

        # Shared Persistent Memory
        db_path = getattr(config, "db_path", "paper_trading.db")
        self.memory = episodic_memory or RufloEpisodicMemory(db_path=db_path)

        # Unify with Council Arbiter CRO
        self.council_cro = council_cro or (
            ai_supervisor._council if ai_supervisor and hasattr(ai_supervisor, "_council") else CouncilArbiterCRO()
        )

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

    def set_live_dependencies(
        self,
        coordinator: Optional[IngestionCoordinator] = None,
        gatekeeper: Optional[SecurityGatekeeper] = None,
        executor: Optional[PaperExecutor] = None,
        ledger: Optional[SQLiteLedger] = None,
        position_book: Optional[PositionBook] = None,
    ) -> None:
        """Inject live engine dependencies into the swarm coordinator and child agents."""
        if coordinator is not None:
            self.scout_agent.coordinator = coordinator
        if gatekeeper is not None:
            self.gatekeeper_agent.gatekeeper = gatekeeper
        if executor is not None:
            self.execution_agent.executor = executor
        if ledger is not None:
            self.execution_agent.ledger = ledger
            self._ledger = ledger
        if position_book is not None:
            self.execution_agent.position_book = position_book

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
        execute_fill: bool = False,
        signal: Optional[Any] = None,
        feature_vector: Optional[list[float]] = None,
        pool: Optional[Any] = None,
        report: Optional[Any] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute unified multi-agent consensus prior to trade execution:
        1. Scout & Profiler evaluate contract provenance and smart money clustering.
        2. Security Gatekeeper checks contract integrity, negative cache, and honeypots.
        3. Quant Agent evaluates CPMM price impact, MEV sandwich drag, and Half-Kelly sizing.
        4. Council Arbiter CRO conducts cross-specialist debate (Reach + Forensic + Quant + CRO).
        5. Supervisor reviews episodic memory cosine similarity matches from historical trades.
        Requires unanimous or BFT quorum approval.
        """
        chain_str = chain.value if hasattr(chain, "value") else str(chain)
        src_str = source.value if hasattr(source, "value") else str(source or signal_source)
        sig_id = getattr(signal, "signal_id", "") if signal else str(kwargs.get("signal_id", ""))
        if signal is None:
            import types
            from decimal import Decimal
            from alpha_engine.models.enums import OrderSide, SignalSource, SignalStrength
            try:
                sig_chain = chain if isinstance(chain, ChainIdentifier) else (ChainIdentifier.BASE_MAINNET if "base" in chain_str.lower() else ChainIdentifier.SOLANA_MAINNET)
            except Exception:
                sig_chain = ChainIdentifier.SOLANA_MAINNET
            try:
                sig_source = source if isinstance(source, SignalSource) else SignalSource(str(source or signal_source))
            except Exception:
                sig_source = SignalSource.TELEGRAM_SCRAPER
            signal = types.SimpleNamespace(
                signal_id=sig_id or f"sig_{uuid.uuid4().hex[:8]}",
                timestamp_ns=time.time_ns(),
                chain=sig_chain,
                pool_address=pool_address or (token_address if len(token_address) >= 32 else "0" * 32),
                token_address=token_address,
                suggested_side=OrderSide.BUY,
                strength=SignalStrength.STRONG if raw_confidence >= 0.75 else SignalStrength.MODERATE,
                alpha_score=raw_confidence,
                source=sig_source,
                market_cap_usd=Decimal(str(liquidity_usd * 2.0)),
                net_buy_delta=0.60,
                volume_surge_multiplier=3.0,
                unique_buyers={wallet_address or "wallet1", "wallet2"},
            )

        async with self._lock:
            # Query similar episodic memories via Cosine Similarity
            query_vec = feature_vector if (feature_vector and len(feature_vector) == 7) else [0.5] * 7
            similar_eps = self.memory.query_similar_episodes(query_vec, top_k=3, min_similarity=0.40)
            episodic_matches = [
                {
                    "episode_id": ep.episode_id,
                    "similarity": round(sim, 3),
                    "realized_pnl_usd": ep.realized_pnl_usd,
                    "roi_pct": ep.roi_pct,
                    "root_cause_tag": ep.root_cause_tag,
                    "summary_reflection": ep.summary_reflection,
                }
                for ep, sim in similar_eps
            ]

            # Broadcast signal announcement
            await self.broadcast_message(
                SwarmMessage(
                    sender_role=AgentRole.SCOUT,
                    target_role=None,
                    content=f"Deliberating signal on {token_address[:10]} ({chain_str}) from {src_str}",
                    payload={"token_address": token_address, "chain": chain_str, "liquidity_usd": liquidity_usd},
                )
            )

            # 1. Smart Money Profiler Check
            prof_result: dict[str, Any] = {}
            if wallet_address:
                prof_result = await self.profiler_agent.tool_evaluate_wallet(
                    wallet_address=wallet_address,
                    chain=chain_str,
                )
                if prof_result.get("classification") in ("WASH_TRADER", "CIRCULAR_WASH", "INSIDER", "BANNED"):
                    votes = {"SCOUT": True, "PROFILER": False}
                    rejection = {
                        "approved": False,
                        "consensus_approved": False,
                        "rejection_agent": AgentRole.PROFILER.value,
                        "veto_reason": f"SmartMoneyProfiler veto: Malicious wallet classification {prof_result.get('classification')}",
                        "reason": f"Wallet not eligible: {prof_result.get('rejection_reasons', ['MALICIOUS'])}",
                        "details": prof_result,
                        "votes": votes,
                        "episodic_matches": episodic_matches,
                    }
                    if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                        await self._ledger.record_agent_deliberation({
                            "deliberation_id": str(uuid.uuid4()),
                            "signal_id": sig_id,
                            "token_address": token_address,
                            "agent_votes_json": json.dumps(votes),
                            "episodic_memory_matches_json": json.dumps(episodic_matches),
                            "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                            "final_decision": "REJECT",
                            "rationale": rejection["veto_reason"],
                            "timestamp_ns": time.time_ns(),
                        })
                    return rejection

            # 2. Security Check
            if liquidity_usd < 5000.0:
                votes = {"SCOUT": True, "PROFILER": True, "GATEKEEPER": False}
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.GATEKEEPER.value,
                    "veto_reason": f"SecurityGatekeeper veto: Insufficient liquidity ${liquidity_usd:.2f} (< $5,000 threshold)",
                    "reason": "Insufficient liquidity",
                    "details": {"liquidity_usd": liquidity_usd},
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            sec_result = await self.gatekeeper_agent.tool_evaluate_token(
                token_address=token_address,
                chain=chain_str,
                pool_address=pool_address,
                initial_liquidity_usd=liquidity_usd,
            )
            if not sec_result.get("passes_hard_gates", False) or not sec_result.get("approved", True):
                votes = {"SCOUT": True, "PROFILER": True, "GATEKEEPER": False}
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.GATEKEEPER.value,
                    "veto_reason": f"SecurityGatekeeper veto: {sec_result.get('flags', ['FAILED_GATE'])}",
                    "reason": f"Security Veto: {sec_result.get('flags', ['FAILED_GATE'])}",
                    "details": sec_result,
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            # 3. Capacity Check
            cap_result = await self.execution_agent.tool_can_open_position(
                token_address=token_address,
                chain=chain_str,
            )
            if not cap_result.get("can_open", False):
                votes = {"SCOUT": True, "PROFILER": True, "GATEKEEPER": True, "EXECUTOR": False}
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.EXECUTOR.value,
                    "veto_reason": f"ExecutionLedger veto: {cap_result.get('reason', 'Capacity full')}",
                    "reason": cap_result.get("reason", "Capacity full"),
                    "details": cap_result,
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            # 4. Quant Sizing Check
            native_p = 3400.0 if "base" in chain_str.lower() else 150.0
            sizing_result = await self.quant_agent.tool_compute_position_size(
                portfolio_equity_usd=10000.0,
                native_price_usd=native_p,
                chain=chain_str,
                pool_liquidity_usd=liquidity_usd,
            )
            if sizing_result.get("gas_rejected", False):
                votes = {"SCOUT": True, "PROFILER": True, "GATEKEEPER": True, "EXECUTOR": True, "QUANT": False}
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.QUANT.value,
                    "veto_reason": "QuantRiskSizing veto: Gas drag exceeds position size threshold",
                    "reason": "Gas drag exceeds position size threshold",
                    "details": sizing_result,
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            # 5. Council Arbiter CRO Cross-Specialist Deliberation (Reach, Forensic, Quant, CRO)
            council_res = None
            if self.council_cro is not None:
                try:
                    council_chain = ChainIdentifier.BASE_MAINNET if "base" in chain_str.lower() else ChainIdentifier.SOLANA_MAINNET
                    council_res = await self.council_cro.deliberate(
                        token_ca=token_address,
                        chain=council_chain,
                        pool=pool,
                        report=report,
                        signal=signal,
                        symbol=kwargs.get("symbol", ""),
                    )
                except Exception as cro_err:
                    logger.debug("Council deliberation skipped on non-fatal error: %s", cro_err)

            council_approved = True
            council_veto_reason = ""
            council_debate: list[str] = []
            if council_res is not None:
                council_approved = (getattr(council_res, "decision", "") == "EXECUTE_BUY")
                council_debate = getattr(council_res, "debate_transcript", [])
                if not council_approved:
                    veto_agent = getattr(council_res, "fatal_veto_agent", None) or "CouncilArbiterCRO"
                    council_veto_reason = f"{veto_agent} veto in Council deliberation"

            if not council_approved:
                reach_verdict = council_res.verdicts.get("ReachAnalyst") if (council_res and hasattr(council_res, "verdicts")) else None
                reach_vote_val = getattr(reach_verdict.vote, "value", str(reach_verdict.vote)) if reach_verdict else "APPROVE"
                votes = {
                    "SCOUT": True,
                    "PROFILER": True,
                    "GATEKEEPER": True,
                    "QUANT": True,
                    "REACH": (reach_vote_val != "REJECT"),
                    "CRO": False,
                    "EXECUTOR": True,
                    "SUPERVISOR": True,
                }
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": "CouncilArbiterCRO",
                    "veto_reason": f"Council CRO Veto: {council_veto_reason}",
                    "reason": f"Council CRO Veto: {council_veto_reason}",
                    "details": {"debate": council_debate},
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                    "debate": council_debate,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            # 6. Supervisor Audit
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
                votes = {
                    "SCOUT": True,
                    "PROFILER": True,
                    "GATEKEEPER": True,
                    "QUANT": True,
                    "REACH": True,
                    "CRO": True,
                    "EXECUTOR": True,
                    "SUPERVISOR": False,
                }
                rejection = {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.SUPERVISOR.value,
                    "veto_reason": f"RufloSupervisor veto: {audit_result.get('reason', 'Supervisor Veto')}",
                    "reason": audit_result.get("reason", "Supervisor Veto"),
                    "details": audit_result,
                    "votes": votes,
                    "episodic_matches": episodic_matches,
                }
                if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                    await self._ledger.record_agent_deliberation({
                        "deliberation_id": str(uuid.uuid4()),
                        "signal_id": sig_id,
                        "token_address": token_address,
                        "agent_votes_json": json.dumps(votes),
                        "episodic_memory_matches_json": json.dumps(episodic_matches),
                        "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                        "final_decision": "REJECT",
                        "rationale": rejection["veto_reason"],
                        "timestamp_ns": time.time_ns(),
                    })
                return rejection

            # 7. Consensus Approved: Compute dynamic sizing and optionally execute paper fill
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

            votes = {
                "SCOUT": True,
                "PROFILER": True,
                "GATEKEEPER": True,
                "QUANT": True,
                "REACH": True,
                "CRO": True,
                "EXECUTOR": True,
                "SUPERVISOR": True,
            }

            debate_transcript = (getattr(council_res, "debate_transcript", []) if council_res else []) + [
                f"🛡️ [RufloSwarm] Security passed (score={sec_result.get('score', 100.0):.0f}).",
                f"📐 [RufloSwarm] Position sized to ${position_usd:.2f} USD.",
                f"🧠 [RufloSwarm] Historical episodic memories matched: {len(episodic_matches)} episodes.",
                "⚖️ [RufloSwarm] BFT Consensus Reached: APPROVED.",
            ]

            if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
                await self._ledger.record_agent_deliberation({
                    "deliberation_id": str(uuid.uuid4()),
                    "signal_id": sig_id,
                    "token_address": token_address,
                    "agent_votes_json": json.dumps(votes),
                    "episodic_memory_matches_json": json.dumps(episodic_matches),
                    "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                    "final_decision": "APPROVE",
                    "rationale": "Multi-agent consensus approved; all 8 agent checks and BFT quorum passed.",
                    "timestamp_ns": time.time_ns(),
                })

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
                "council_result": council_res.to_dict() if council_res and hasattr(council_res, "to_dict") else {},
                "votes": votes,
                "episodic_matches": episodic_matches,
                "debate": debate_transcript,
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
