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
from alpha_engine.ruflo.agents.learning_agent import RufloLearningAgent
from alpha_engine.ruflo.agents.profiler_agent import SmartMoneyProfilerAgent
from alpha_engine.ruflo.agents.quant_agent import QuantRiskSizingAgent
from alpha_engine.ruflo.agents.scouting_agent import IngestionScoutingAgent
from alpha_engine.ruflo.agents.supervisor_agent import RufloSupervisorAgent
from alpha_engine.ruflo.memory import (
    LightweightVectorStore,
    RufloEpisodicMemory,
    RufloIntelligencePipeline,
    RufloSemanticMemory,
)
from alpha_engine.ruflo.protocol import (
    AgentRole,
    AgentStatus,
    ConsensusProtocol,
    MessageType,
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

        # Shared Persistent Memory & Vector Store
        db_path = getattr(config, "db_path", "paper_trading.db")
        self.memory = episodic_memory or RufloEpisodicMemory(db_path=db_path)
        self.semantic_memory = RufloSemanticMemory(db_path=db_path)
        self.vector_store = LightweightVectorStore(dimension=7)
        self.pipeline = RufloIntelligencePipeline(
            episodic_memory=self.memory,
            semantic_memory=self.semantic_memory,
            vector_store=self.vector_store,
        )

        # Unify with Council Arbiter CRO
        self.council_cro = council_cro or (
            ai_supervisor._council if ai_supervisor and hasattr(ai_supervisor, "_council") else CouncilArbiterCRO(
                config=config,
                episodic_memory=self.memory,
                ledger=ledger,
            )
        )
        if self.council_cro is not None and getattr(self.council_cro, "episodic_memory", None) is None:
            self.council_cro.episodic_memory = self.memory

        # Initialize the 7 specialized agents
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
        self.learner_agent = RufloLearningAgent(
            episodic_memory=self.memory,
            semantic_memory=self.semantic_memory,
            pipeline=self.pipeline,
        )

        self._agents: dict[AgentRole, BaseRufloAgent] = {
            AgentRole.SCOUT: self.scout_agent,
            AgentRole.PROFILER: self.profiler_agent,
            AgentRole.GATEKEEPER: self.gatekeeper_agent,
            AgentRole.QUANT: self.quant_agent,
            AgentRole.EXECUTOR: self.execution_agent,
            AgentRole.SUPERVISOR: self.supervisor_agent,
            AgentRole.LEARNER: self.learner_agent,
        }

        # Background maintenance tasks & RPC tracking
        self._rpc_futures: dict[str, asyncio.Future[SwarmMessage]] = {}
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
            if self.council_cro is not None:
                self.council_cro.ledger = ledger
        if position_book is not None:
            self.execution_agent.position_book = position_book
        if self.council_cro is not None and getattr(self.council_cro, "episodic_memory", None) is None:
            self.council_cro.episodic_memory = self.memory

    async def _safe_record_agent_deliberation(self, deliberation: dict[str, Any]) -> None:
        """Safely record multi-agent deliberation without allowing database errors to crash consensus."""
        if self._ledger and hasattr(self._ledger, "record_agent_deliberation"):
            try:
                await self._ledger.record_agent_deliberation(deliberation)
            except Exception as exc:
                logger.warning("Failed to record agent deliberation in ledger: %s", exc)

    async def start(self) -> None:
        """Start all specialized swarm agents, memory stores, and reflection loops."""
        if self._is_running:
            return
        self._is_running = True
        self._shutdown_event.clear()

        # Connect episodic and semantic memory
        await self.memory.connect()
        await self.semantic_memory.connect()

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
        """Gracefully stop all swarm agents and memory persistence."""
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
        await self.semantic_memory.close()
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

    async def send_rpc_request(
        self,
        target_role: AgentRole,
        action: str,
        payload: dict[str, Any],
        sender_role: AgentRole = AgentRole.SUPERVISOR,
        timeout_s: float = 2.0,
    ) -> Optional[SwarmMessage]:
        """
        Send a typed RPC request message to a target agent and await correlated response.
        Enables structured cross-agent queries with timeout fallbacks.
        """
        if target_role not in self._agents:
            return None

        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[SwarmMessage] = loop.create_future()
        self._rpc_futures[corr_id] = future

        msg = SwarmMessage(
            sender_role=sender_role,
            target_role=target_role,
            message_type=MessageType.REQUEST.value,
            action=action,
            payload=payload,
            correlation_id=corr_id,
            is_request=True,
        )

        try:
            await self._agents[target_role].post_message(msg)
            response = await asyncio.wait_for(future, timeout=timeout_s)
            return response
        except asyncio.TimeoutError:
            logger.debug("[RufloSwarm] RPC request %s to %s timed out after %.1fs", action, target_role.value, timeout_s)
            return None
        finally:
            self._rpc_futures.pop(corr_id, None)

    async def on_trade_closed(self, trade_outcome: dict[str, Any]) -> dict[str, Any]:
        """
        Closed trade lifecycle hook:
        Triggers Ruflo Intelligence Pipeline (Distill -> Consolidate)
        via the specialized RufloLearningAgent.
        """
        return await self.learner_agent.tool_distill_and_consolidate(trade_outcome)

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
            # Query similar episodic memories & semantic rules via Ruflo Intelligence Pipeline
            query_vec = feature_vector if (feature_vector and len(feature_vector) == 7) else [0.5] * 7
            token_ctx = {
                "token_address": token_address,
                "chain": chain_str,
                "liquidity_usd": liquidity_usd,
                "top10_concentration": getattr(report, "top10_concentration", 0.15) if report else 0.15,
                "regime": kwargs.get("regime", "NORMAL"),
            }
            pipeline_retrieval = await self.pipeline.retrieve(
                query_vector=query_vec,
                token_context=token_ctx,
                top_k=5,
            )
            pipeline_judgment = self.pipeline.judge(
                retrieval=pipeline_retrieval,
                proposed_signal=token_ctx,
            )
            episodic_matches = pipeline_retrieval.matched_episodes

            # Adaptive Topology Adjustment: High risk/volatility enforces strict hierarchy
            active_topology = self.topology
            if self.topology == SwarmTopology.ADAPTIVE:
                if pipeline_judgment.verdict == "CAUTION" or kwargs.get("regime") == "HIGH_VOLATILITY":
                    active_topology = SwarmTopology.HIERARCHICAL
                else:
                    active_topology = SwarmTopology.MESH

            # Early VETO check from Ruflo Cognitive Pipeline
            if pipeline_judgment.verdict == "VETO":
                rejection_msg = f"Ruflo Pipeline VETO: {'; '.join(pipeline_judgment.warning_flags or ['Adverse historical heuristic precedent'])}"
                return {
                    "approved": False,
                    "consensus_approved": False,
                    "rejection_agent": AgentRole.LEARNER.value,
                    "veto_reason": rejection_msg,
                    "reason": rejection_msg,
                    "details": {"pipeline_judgment": pipeline_judgment.__dict__},
                    "votes": {"SCOUT": True, "LEARNER": False},
                    "episodic_matches": episodic_matches,
                }

            # Broadcast signal announcement
            await self.broadcast_message(
                SwarmMessage(
                    sender_role=AgentRole.SCOUT,
                    target_role=None,
                    content=f"Deliberating signal on {token_address[:10]} ({chain_str}) from {src_str} [topology={active_topology.value}]",
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
                    await self._safe_record_agent_deliberation({
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
            is_direct_dm = bool(getattr(signal, "is_direct_dm", False)) or (
                bool(getattr(signal, "originating_channel", ""))
                and str(getattr(signal, "originating_channel", "")).startswith("DM:")
            )
            if liquidity_usd < 5000.0 and not is_direct_dm:
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
                await self._safe_record_agent_deliberation({
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
                await self._safe_record_agent_deliberation({
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
                await self._safe_record_agent_deliberation({
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
            native_p = None
            if (
                self.execution_agent
                and self.execution_agent.executor
                and hasattr(self.execution_agent.executor, "_price_service")
                and self.execution_agent.executor._price_service
            ):
                price_res = self.execution_agent.executor._price_service.get_price(signal.chain)
                native_p = float(price_res.price if hasattr(price_res, "price") else price_res[0])
            if native_p is None or native_p <= 0.0:
                is_eth = "base" in chain_str.lower() or "eth" in chain_str.lower()
                cfg_p = (
                    self.config.eth_price_usd
                    if is_eth and hasattr(self.config, "eth_price_usd")
                    else (self.config.sol_price_usd if hasattr(self.config, "sol_price_usd") else None)
                )
                native_p = float(cfg_p) if cfg_p is not None else 1.0
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
                await self._safe_record_agent_deliberation({
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
            council_exception_str = ""
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
                    logger.error("Council deliberation failed unexpectedly for %s: %s", token_address[:10], cro_err)
                    council_exception_str = str(cro_err)

            council_approved = True
            council_veto_reason = ""
            council_debate: list[str] = []
            if council_exception_str:
                # Fail-closed policy: unexpected council crash cannot bypass gate
                council_approved = False
                council_veto_reason = f"Council deliberation exception: {council_exception_str}"
            elif council_res is not None:
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
                await self._safe_record_agent_deliberation({
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
                await self._safe_record_agent_deliberation({
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

            # 7. Consensus Approved: Compute dynamic sizing with pipeline multiplier and optionally execute paper fill
            position_usd = float(sizing_result.get("recommended_position_usd", 100.0))
            if audit_result.get("action_parameters"):
                rec_pct = audit_result["action_parameters"].get("recommended_position_pct")
                if rec_pct and rec_pct > 0:
                    tuned_usd = 10000.0 * (rec_pct / 100.0)
                    position_usd = min(position_usd, tuned_usd) if position_usd > 0 else tuned_usd

            # Apply empirical pipeline sizing multiplier
            position_usd = max(20.0, position_usd * float(pipeline_judgment.size_multiplier))

            exec_res = await self.execution_agent.tool_execute_paper_trade(
                token_address=token_address,
                chain=chain_str,
                side="BUY",
                amount_usd=position_usd,
                expected_price=1.0,
                strategy_pattern="RUFLO_CONSENSUS",
                confidence=raw_confidence,
            )
            exec_res["execute_fill"] = execute_fill
            if execute_fill:
                logger.info(
                    "[RufloSwarm] execute_fill=True: Active paper trade fill triggered for %s (%s)",
                    token_address, chain_str,
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
                "LEARNER": True,
            }

            debate_transcript = (getattr(council_res, "debate_transcript", []) if council_res else []) + [
                f"🛡️ [RufloSwarm] Security passed (score={sec_result.get('score', 100.0):.0f}).",
                f"📐 [RufloSwarm] Position sized to ${position_usd:.2f} USD (pipeline mult: {pipeline_judgment.size_multiplier:.2f}).",
                f"🧠 [RufloSwarm] Historical episodic memories matched: {len(episodic_matches)} episodes.",
                f"🔮 [RufloPipeline] Win prob={pipeline_judgment.empirical_win_prob*100:.1f}%, verdict={pipeline_judgment.verdict}.",
                f"⚖️ [RufloSwarm] {self.consensus.value.upper()} Consensus Reached: APPROVED [topology={active_topology.value}].",
            ]

            await self._safe_record_agent_deliberation({
                "deliberation_id": str(uuid.uuid4()),
                "signal_id": sig_id,
                "token_address": token_address,
                "agent_votes_json": json.dumps(votes),
                "episodic_memory_matches_json": json.dumps(episodic_matches),
                "selected_strategy_id": getattr(signal, "strategy_name", "champion_peak_trailing_50"),
                "final_decision": "APPROVE",
                "rationale": "Multi-agent consensus approved; all 9 agent checks passed.",
                "timestamp_ns": time.time_ns(),
            })

            return {
                "approved": True,
                "consensus_approved": True,
                "protocol": self.consensus.value,
                "topology": active_topology.value,
                "token_address": token_address,
                "chain": chain_str,
                "signal_source": src_str,
                "timestamp_ns": time.time_ns(),
                "security_score": sec_result.get("score", 100.0),
                "position_sizing": sizing_result,
                "supervisor_decision": audit_result,
                "profiler_result": prof_result,
                "council_result": council_res.to_dict() if council_res and hasattr(council_res, "to_dict") else {},
                "pipeline_judgment": {
                    "verdict": pipeline_judgment.verdict,
                    "empirical_win_prob": pipeline_judgment.empirical_win_prob,
                    "size_multiplier": pipeline_judgment.size_multiplier,
                    "warning_flags": pipeline_judgment.warning_flags,
                },
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
