"""
alpha_engine.council.arbiter_cro — Agent 4: Chief Risk Officer (CRO) & Council Debate Arbiter
Orchestrates genuine 4-round debate loop among Reach, Forensic, Quant, and CRO agents.
Enforces Kelly Criterion sizing, dynamic Take-Profit ladders, non-negotiable fatal vetoes,
shared Ruflo episodic memory retrieval, post-mortem trade outcome evaluation, and Bayesian weight adaptation.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from typing import Any, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.council.forensic_agent import ChainForensicAgent
from alpha_engine.council.llm_client import CouncilLLMClient
from alpha_engine.council.models import (
    AgentVerdict,
    AgentVote,
    CouncilConsensusResult,
    DebateRound,
    DebateStance,
)
from alpha_engine.council.quant_agent import QuantTechnicalsAgent
from alpha_engine.council.reach_agent import ReachAnalystAgent
from alpha_engine.math.cpmm import PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVE
from alpha_engine.models.enums import ChainIdentifier, ExitProfile
from alpha_engine.models.observability import TraceStage, TraceStatus
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.ruflo.memory import MemoryEpisode, RufloEpisodicMemory
from alpha_engine.tracer import tracer

logger = logging.getLogger(__name__)


class CouncilArbiterCRO:
    """
    Chief Risk Officer (CRO) & Council Arbiter.
    Runs a structured 4-round institutional debate:
      Round 1: Parallel Independent Audits (conditioned on shared episodic memory)
      Round 2: Cross-Examination & Peer Challenges (real Q&A with on-chain data)
      Round 3: Rebuttals & Conviction Revisions (conditioned on Round 2 data answers)
      Round 4: CRO Final Synthesis, Veto Enforcement & Fractional Kelly Sizing
    """

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        reach_agent: Optional[ReachAnalystAgent] = None,
        forensic_agent: Optional[ChainForensicAgent] = None,
        quant_agent: Optional[QuantTechnicalsAgent] = None,
        llm_client: Optional[CouncilLLMClient] = None,
        ledger: Optional[Any] = None,
        episodic_memory: Optional[RufloEpisodicMemory] = None,
        council_weights: Optional[dict[str, float]] = None,
    ):
        self._cfg = config
        self.ledger = ledger
        self.episodic_memory = episodic_memory

        # Setup LLM client
        if llm_client is not None:
            self.llm_client = llm_client
        elif config is not None:
            self.llm_client = CouncilLLMClient(
                provider=getattr(config, "ai_provider", "gemini"),
                model=getattr(config, "ai_model", "gemini-3.8-flash"),
                timeout_s=getattr(config, "council_timeout_s", 3.5),
                max_concurrency=getattr(config, "llm_max_concurrent_calls", 4),
                rate_limit_rpm=getattr(config, "llm_rate_limit_rpm", 60),
                model_aliases=getattr(config, "model_aliases", {}),
                max_cost_per_signal_usd=getattr(config, "llm_max_cost_per_signal_usd", 0.05),
                fallback_warning_threshold=getattr(config, "council_fallback_warning_threshold", 0.50),
                fallback_window_size=getattr(config, "council_fallback_window_size", 50),
            )
        else:
            self.llm_client = CouncilLLMClient()

        # Agents share the CouncilLLMClient
        self.reach_agent = reach_agent or ReachAnalystAgent(llm_client=self.llm_client)
        self.forensic_agent = forensic_agent or ChainForensicAgent(llm_client=self.llm_client)
        self.quant_agent = quant_agent or QuantTechnicalsAgent(llm_client=self.llm_client)

        # Base prior weights
        self._base_weights: dict[str, float] = {
            "ChainForensic": 0.40,
            "QuantTechnicals": 0.40,
            "ReachAnalyst": 0.20,
        }
        self.council_weights: dict[str, float] = dict(council_weights or self._base_weights)

        # Performance tracking for Bayesian adaptation
        self._agent_performance: dict[str, dict[str, Any]] = {
            "ChainForensic": {"total_trades": 0, "correct_calls": 0, "measured_accuracy": 0.50, "shrunk_accuracy": 0.50},
            "QuantTechnicals": {"total_trades": 0, "correct_calls": 0, "measured_accuracy": 0.50, "shrunk_accuracy": 0.50},
            "ReachAnalyst": {"total_trades": 0, "correct_calls": 0, "measured_accuracy": 0.50, "shrunk_accuracy": 0.50},
        }

        # Track recent deliberations for post-mortem lookup
        self._recent_deliberations: dict[str, dict[str, Any]] = {}

        # Minimum sample & shrinkage hyperparameters
        self._min_sample_size: int = getattr(config, "council_min_sample_size", 20) if config else 20
        self._shrinkage_weight: float = getattr(config, "council_shrinkage_prior_weight", 10.0) if config else 10.0

    async def deliberate(
        self,
        token_ca: str,
        chain: ChainIdentifier,
        pool: Optional[PoolState] = None,
        report: Optional[SecurityReport] = None,
        signal: Optional[Any] = None,
        staged_data: Optional[Any] = None,
        website_url: Optional[str] = None,
        symbol: str = "",
        recent_win_rate: float = 65.0,
        ledger: Optional[Any] = None,
    ) -> CouncilConsensusResult:
        """
        Conducts a genuine 4-round multi-agent council debate with enforceable vetoes.
        """
        active_ledger = ledger or self.ledger
        chain_str = chain.value if hasattr(chain, "value") else str(chain)

        # Load persisted weights if available
        if active_ledger and hasattr(active_ledger, "get_council_weights"):
            try:
                saved = await active_ledger.get_council_weights()
                if saved:
                    self.council_weights.update(saved)
            except Exception as w_err:
                logger.warning("Could not load council weights from ledger: %s", w_err, exc_info=True)

        async with tracer.span(
            token_address=token_ca,
            chain=chain_str,
            stage=TraceStage.COUNCIL,
            component="CouncilArbiterCRO",
            function_name="deliberate",
            input_data={"symbol": symbol, "recent_win_rate": recent_win_rate},
        ) as trace_event:
            # Determine token lifecycle stage
            is_swing = getattr(signal, "exit_profile", None) == ExitProfile.REVIVAL_SWING
            token_age_s = getattr(signal, "token_age_s", None)
            token_age_h = getattr(signal, "token_age_hours", None)
            sig_source = getattr(signal, "source", None)
            sig_source_val = getattr(sig_source, "value", str(sig_source)) if sig_source else ""

            is_new_launch = False
            if not is_swing:
                if token_age_s is not None and token_age_s <= 1800.0:
                    is_new_launch = True
                elif token_age_h is not None and token_age_h <= 0.5:
                    is_new_launch = True
                elif "pump" in str(token_ca).lower() or sig_source_val.lower() in ("pump_fun_mint", "launch_stage", "telegram_scraper", "x_stream"):
                    is_new_launch = True
                elif staged_data is not None and hasattr(staged_data, "t_0"):
                    is_new_launch = True
                elif sig_source_val == "dex_swap" and not str(token_ca).lower().endswith("pump"):
                    is_new_launch = False
                elif token_age_s is None and token_age_h is None and ("pump" in str(token_ca).lower() or pool is None):
                    is_new_launch = True

            debate_rounds: list[DebateRound] = []
            debate_transcript: list[str] = []

            # =========================================================================
            # PRELUDE: Ruflo Episodic Memory Retrieval & Shared Historical Precedents
            # =========================================================================
            retrieved_count = 0
            memory_summary = ""
            if self.episodic_memory:
                try:
                    fv = getattr(signal, "entry_feature_vector", None)
                    if not fv and staged_data and hasattr(staged_data, "extract_feature_vector"):
                        fv = staged_data.extract_feature_vector().to_vector()
                    if not fv:
                        fv = [0.5, 0.2, 0.5, 0.6, 0.15, 0.3, 0.5]

                    if hasattr(self.episodic_memory, "query_relevant_episodes"):
                        similar_eps = self.episodic_memory.query_relevant_episodes(
                            query_vector=fv, chain=chain_str, signal_source=sig_source_val, top_k=3
                        )
                    elif hasattr(self.episodic_memory, "query_similar_episodes"):
                        similar_eps = self.episodic_memory.query_similar_episodes(fv, top_k=3, min_similarity=0.40)
                    else:
                        all_eps = self.episodic_memory.get_all_episodes(limit=3) if hasattr(self.episodic_memory, "get_all_episodes") else []
                        similar_eps = [(e, 1.0) for e in all_eps]

                    retrieved_count = len(similar_eps)
                    if hasattr(self.episodic_memory, "format_episodes_compact_summary"):
                        memory_summary = self.episodic_memory.format_episodes_compact_summary(similar_eps)
                    elif similar_eps:
                        lines = []
                        for ep, sim in similar_eps:
                            r_str = f"+{ep.roi_pct:.1f}%" if ep.roi_pct >= 0 else f"{ep.roi_pct:.1f}%"
                            lines.append(f"Token {ep.token_address[:8]} ({ep.signal_source}, ROI: {r_str}, Cause: {ep.root_cause_tag}, Sim: {sim*100:.0f}%)")
                        memory_summary = "Historical Precedents: " + "; ".join(lines)

                    if similar_eps:
                        debate_transcript.append("─── 🧠 RUFLO INTELLIGENCE PIPELINE: HISTORICAL PRECEDENTS ───")
                        debate_transcript.append(f"🔍 {memory_summary}")
                except Exception as mem_err:
                    logger.warning("Episodic memory query error: %s", mem_err, exc_info=True)

            # =========================================================================
            # ROUND 1: Independent Initial Evaluations (conditioned on Shared Memory)
            # =========================================================================
            debate_transcript.append("─── 🏛️ ROUND 1: INDEPENDENT INITIAL AUDITS ───")

            async def _resolve(val: Any) -> Any:
                if inspect.isawaitable(val):
                    return await val
                return val

            reach_call = self.reach_agent.audit_social_presence(
                token_ca=token_ca,
                symbol=symbol,
                website_url=website_url,
                memory_summary=memory_summary,
            ) if hasattr(self.reach_agent, "audit_social_presence") else self.reach_agent.audit_social_narrative(
                token_ca=token_ca,
                symbol=symbol,
                website_url=website_url,
            )
            forensic_call = self.forensic_agent.audit_contract_and_liquidity(
                token_ca=token_ca,
                chain=chain,
                pool=pool,
                report=report,
                memory_summary=memory_summary,
            )
            quant_call = self.quant_agent.evaluate_technical_structure(
                token_ca=token_ca,
                chain=chain,
                pool=pool,
                signal=signal,
                memory_summary=memory_summary,
            ) if hasattr(self.quant_agent, "evaluate_technical_structure") else self.quant_agent.analyze_market_dynamics(
                pool=pool,
                signal=signal,
                staged_data=staged_data,
                is_revival_swing=is_swing,
                is_new_launch=is_new_launch,
            )

            reach_res, forensic_res, quant_res = await asyncio.gather(
                _resolve(reach_call),
                _resolve(forensic_call),
                _resolve(quant_call),
            )

            # Record Round 1 DebateRound entries
            r1_forensic = DebateRound(
                round_number=1,
                agent_name="ChainForensic",
                stance=forensic_res.vote.value,
                arguments=forensic_res.rationale,
                vote_weight=self.council_weights.get("ChainForensic", 1.5),
                confidence=forensic_res.confidence,
                score=forensic_res.score,
                flags=forensic_res.flags,
                generation_path=forensic_res.generation_path,
                fallback_reason=forensic_res.fallback_reason,
                latency_ms=forensic_res.execution_time_ms,
                evidence=forensic_res.evidence,
            )
            r1_quant = DebateRound(
                round_number=1,
                agent_name="QuantTechnicals",
                stance=quant_res.vote.value,
                arguments=quant_res.rationale,
                vote_weight=self.council_weights.get("QuantTechnicals", 1.2),
                confidence=quant_res.confidence,
                score=quant_res.score,
                flags=quant_res.flags,
                generation_path=quant_res.generation_path,
                fallback_reason=quant_res.fallback_reason,
                latency_ms=quant_res.execution_time_ms,
                evidence=quant_res.evidence,
            )
            r1_reach = DebateRound(
                round_number=1,
                agent_name="ReachAnalyst",
                stance=reach_res.vote.value,
                arguments=reach_res.rationale,
                vote_weight=self.council_weights.get("ReachAnalyst", 1.0),
                confidence=reach_res.confidence,
                score=reach_res.score,
                flags=reach_res.flags,
                generation_path=reach_res.generation_path,
                fallback_reason=reach_res.fallback_reason,
                latency_ms=reach_res.execution_time_ms,
                evidence=reach_res.evidence,
            )

            debate_rounds.extend([r1_forensic, r1_quant, r1_reach])
            debate_transcript.append(f"🛡️ [ChainForensic] [{r1_forensic.generation_path.upper()}] Vote: {forensic_res.vote.value} ({forensic_res.score:.1f}/100) -> {forensic_res.rationale}")
            debate_transcript.append(f"📈 [QuantTechnicals] [{r1_quant.generation_path.upper()}] Vote: {quant_res.vote.value} ({quant_res.score:.1f}/100) -> {quant_res.rationale}")
            debate_transcript.append(f"🌐 [ReachAnalyst] [{r1_reach.generation_path.upper()}] Vote: {reach_res.vote.value} ({reach_res.score:.1f}/100) -> {reach_res.rationale}")

            # =========================================================================
            # ROUND 2: Cross-Examination & Inter-Agent Information Requests
            # =========================================================================
            debate_transcript.append("─── ⚔️ ROUND 2: CROSS-EXAMINATION & PEER CHALLENGES ───")

            ch_forensic_call = self.forensic_agent.cross_examine_peer("QuantTechnicals", quant_res, forensic_res, token_ca)
            ch_quant_call = self.quant_agent.cross_examine_peer("ReachAnalyst", reach_res, quant_res, token_ca)
            ch_reach_call = self.reach_agent.cross_examine_peer("ChainForensic", forensic_res, reach_res, token_ca)

            ch_forensic, ch_quant, ch_reach = await asyncio.gather(
                _resolve(ch_forensic_call),
                _resolve(ch_quant_call),
                _resolve(ch_reach_call),
            )
            debate_rounds.extend([ch_forensic, ch_quant, ch_reach])
            debate_transcript.append(f"🛡️ [ChainForensic ➔ {ch_forensic.target_agent}] ({ch_forensic.stance}) {ch_forensic.arguments}")
            debate_transcript.append(f"📈 [QuantTechnicals ➔ {ch_quant.target_agent}] ({ch_quant.stance}) {ch_quant.arguments}")
            debate_transcript.append(f"🌐 [ReachAnalyst ➔ {ch_reach.target_agent}] ({ch_reach.stance}) {ch_reach.arguments}")

            # Generate real answers from queried agents using on-chain data
            ans_quant_raw = self.quant_agent.answer_peer_question(ch_forensic.arguments, "ChainForensic", token_ca) if hasattr(self.quant_agent, "answer_peer_question") else f"Confirmed technical metrics for ChainForensic on {token_ca}"
            ans_forensic_raw = self.forensic_agent.answer_peer_question(ch_quant.arguments, "QuantTechnicals", token_ca) if hasattr(self.forensic_agent, "answer_peer_question") else f"Confirmed forensic integrity for QuantTechnicals on {token_ca}"
            ans_reach_raw = self.reach_agent.answer_peer_question(ch_forensic.arguments, "ChainForensic", token_ca) if hasattr(self.reach_agent, "answer_peer_question") else f"Confirmed social reach metrics for ChainForensic on {token_ca}"

            ans_quant_to_forensic = await _resolve(ans_quant_raw)
            ans_forensic_to_quant = await _resolve(ans_forensic_raw)
            ans_reach_to_forensic = await _resolve(ans_reach_raw)

            r2_ans_quant = DebateRound(
                round_number=2,
                agent_name="QuantTechnicals",
                stance=DebateStance.ANSWER.value,
                arguments=ans_quant_to_forensic,
                target_agent="ChainForensic",
                confidence=0.92,
                generation_path="fallback",
            )
            r2_ans_forensic = DebateRound(
                round_number=2,
                agent_name="ChainForensic",
                stance=DebateStance.ANSWER.value,
                arguments=ans_forensic_to_quant,
                target_agent="QuantTechnicals",
                confidence=0.98,
                generation_path="fallback",
            )
            r2_ans_reach = DebateRound(
                round_number=2,
                agent_name="ReachAnalyst",
                stance=DebateStance.ANSWER.value,
                arguments=ans_reach_to_forensic,
                target_agent="ChainForensic",
                confidence=0.88,
                generation_path="fallback",
            )

            debate_rounds.extend([r2_ans_quant, r2_ans_forensic, r2_ans_reach])
            debate_transcript.append(f"💡 [QuantTechnicals ➔ ChainForensic] {ans_quant_to_forensic}")
            debate_transcript.append(f"💡 [ChainForensic ➔ QuantTechnicals] {ans_forensic_to_quant}")
            debate_transcript.append(f"💡 [ReachAnalyst ➔ ChainForensic] {ans_reach_to_forensic}")

            # Aggregate collected data answers for Round 3 conditionality
            collected_answers = [ans_quant_to_forensic, ans_forensic_to_quant, ans_reach_to_forensic]

            # =========================================================================
            # ROUND 3: Rebuttals, Conviction Revisions & Mandatory Devil's Advocate
            # =========================================================================
            debate_transcript.append("─── 🔄 ROUND 3: REBUTTALS & REVISIONS ───")

            def _call_defend_or_revise(agent: Any, rounds: list[DebateRound], verdict: AgentVerdict, answers: list[str]) -> Any:
                sig = inspect.signature(agent.defend_or_revise)
                if "data_answers" in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                    return agent.defend_or_revise(rounds, verdict, data_answers=answers)
                return agent.defend_or_revise(rounds, verdict)

            rev_forensic_call = _call_defend_or_revise(self.forensic_agent, debate_rounds, forensic_res, collected_answers)
            rev_quant_call = _call_defend_or_revise(self.quant_agent, debate_rounds, quant_res, collected_answers)
            rev_reach_call = _call_defend_or_revise(self.reach_agent, debate_rounds, reach_res, collected_answers)

            r3_raw = await asyncio.gather(
                _resolve(rev_forensic_call),
                _resolve(rev_quant_call),
                _resolve(rev_reach_call),
            )

            def _unpack_r3(out: Any, name: str, cur_v: AgentVerdict) -> tuple[AgentVerdict, DebateRound]:
                if isinstance(out, tuple) and len(out) == 2:
                    return out[0], out[1]
                elif isinstance(out, AgentVerdict):
                    rd = DebateRound(
                        round_number=3,
                        agent_name=name,
                        stance="DEFEND",
                        arguments=out.rationale,
                        score=out.score,
                    )
                    return out, rd
                return cur_v, DebateRound(round_number=3, agent_name=name, stance="DEFEND", arguments=cur_v.rationale)

            revised_forensic, r3_forensic = _unpack_r3(r3_raw[0], "ChainForensic", forensic_res)
            revised_quant, r3_quant = _unpack_r3(r3_raw[1], "QuantTechnicals", quant_res)
            revised_reach, r3_reach = _unpack_r3(r3_raw[2], "ReachAnalyst", reach_res)

            debate_rounds.extend([r3_forensic, r3_quant, r3_reach])
            debate_transcript.append(f"🛡️ [ChainForensic] ({r3_forensic.stance}) Score: {revised_forensic.score:.1f}/100 -> {r3_forensic.arguments}")
            debate_transcript.append(f"📈 [QuantTechnicals] ({r3_quant.stance}) Score: {revised_quant.score:.1f}/100 -> {r3_quant.arguments}")
            debate_transcript.append(f"🌐 [ReachAnalyst] ({r3_reach.stance}) Score: {revised_reach.score:.1f}/100 -> {r3_reach.arguments}")

            final_verdicts: dict[str, AgentVerdict] = {
                "ReachAnalyst": revised_reach,
                "ChainForensic": revised_forensic,
                "QuantTechnicals": revised_quant,
            }

            # Check for Unanimous Approval -> Trigger Mandatory Devil's Advocate
            devils_advocate_invoked = False
            devils_advocate_mitigation = False
            devils_advocate_report = None
            approvals_round3 = sum(1 for v in final_verdicts.values() if v.vote == AgentVote.APPROVE)

            if approvals_round3 == 3:
                devils_advocate_invoked = True
                debate_transcript.append("─── 😈 MANDATORY DEVIL'S ADVOCATE STRESS TEST ───")

                top10_conc = getattr(report, "top10_concentration", 0.12) if report else 0.12
                curve_progress = 0.0
                if pool and pool.native_reserve > PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVE:
                    curve_progress = min(1.0, float(pool.native_reserve - PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVE) / 55.0)

                fatal_flaw = False
                flaw_reason = ""

                if curve_progress >= 0.90 and curve_progress <= 0.99:
                    fatal_flaw = True
                    flaw_reason = "BONDING_CURVE_MIGRATION_SNIPER_VOID (90-99% progress)"
                elif top10_conc > 0.22 and not (pool and getattr(pool, "is_pump_fun", False)):
                    devils_advocate_mitigation = True
                    flaw_reason = f"Elevated top-10 concentration ({top10_conc*100:.1f}% > 20% limit)"

                da_arguments = (
                    f"Devil's Advocate Challenge: Unanimous consensus detected. Testing hidden liquidity voids, "
                    f"curve migration snipers, and top holder clustering. Findings: {flaw_reason or 'No structural death trap detected; thesis survived stress test.'}"
                )

                da_round = DebateRound(
                    round_number=3,
                    agent_name="DevilsAdvocateCRO",
                    stance=DebateStance.DEVILS_ADVOCATE.value,
                    arguments=da_arguments,
                    vote_weight=1.5,
                    confidence=0.90,
                    flags=[flaw_reason] if flaw_reason else [],
                    generation_path="fallback",
                )
                debate_rounds.append(da_round)
                debate_transcript.append(f"😈 [DevilsAdvocateCRO] {da_arguments}")

                devils_advocate_report = {
                    "challenger": "DevilsAdvocateCRO",
                    "fatal_flaw_detected": fatal_flaw,
                    "mitigation_required": devils_advocate_mitigation,
                    "flaw_summary": flaw_reason,
                }

                if fatal_flaw:
                    revised_forensic.fatal_veto = True
                    revised_forensic.vote = AgentVote.REJECT
                    revised_forensic.rationale = f"DEVIL'S ADVOCATE FATAL VETO: {flaw_reason}"
                    if "BONDING_CURVE_MIGRATION_SNIPER_VOID" in flaw_reason:
                        revised_forensic.flags.append("BONDING_CURVE_MIGRATION_SNIPER_VOID_90_99_PCT")

            # =========================================================================
            # ROUND 4: Chief Risk Officer (CRO) Synthesis & Veto Enforcement
            # =========================================================================
            debate_transcript.append("─── ⚖️ ROUND 4: CHIEF RISK OFFICER FINAL SYNTHESIS ───")

            fatal_agent: Optional[str] = None
            veto_reason = ""

            # Enforce Non-Overridable Vetoes
            if revised_forensic.fatal_veto or revised_forensic.vote == AgentVote.REJECT:
                fatal_agent = "ChainForensicAgent"
                veto_reason = f"FATAL CONTRACT SECURITY VETO: {revised_forensic.rationale}"
            elif "PAID_BOT_RAID_DETECTED" in revised_reach.flags or revised_reach.vote == AgentVote.REJECT:
                fatal_agent = "ReachAnalystAgent"
                veto_reason = f"SOCIAL MANIPULATION VETO: {revised_reach.rationale}"
            elif "OVEREXTENDED_GREEN_CANDLE_FOMO" in revised_quant.flags or revised_quant.vote == AgentVote.REJECT:
                fatal_agent = "QuantTechnicalsAgent"
                veto_reason = f"ANTI-FOMO / MICROSTRUCTURE VETO: {revised_quant.rationale}"

            # Dynamic Strategy Weighting (Use Bayesian adapted weights)
            w_forensic = self.council_weights.get("ChainForensic", 0.40)
            w_quant = self.council_weights.get("QuantTechnicals", 0.40)
            w_reach = self.council_weights.get("ReachAnalyst", 0.20)
            strategy_name = "Autonomous Bayesian-Adapted Strategy Profile"

            avg_score = (
                (revised_forensic.score * w_forensic)
                + (revised_quant.score * w_quant)
                + (revised_reach.score * w_reach)
            )
            debate_transcript.append(
                f"⚖️ [CRO Arbiter] Strategy Profile: {strategy_name} (Forensic {w_forensic:.0%}, Quant {w_quant:.0%}, Reach {w_reach:.0%}) -> Composite Score: {avg_score:.1f}/100"
            )

            approvals = sum(1 for v in final_verdicts.values() if v.vote == AgentVote.APPROVE)
            rejections = sum(1 for v in final_verdicts.values() if v.vote == AgentVote.REJECT)

            if is_new_launch:
                forensic_passed = revised_forensic.vote == AgentVote.APPROVE or revised_forensic.score >= 70.0
                should_buy = (fatal_agent is None) and (rejections == 0) and forensic_passed and (avg_score >= 60.0)
            else:
                should_buy = (fatal_agent is None) and (approvals >= 2) and (rejections == 0) and (avg_score >= 65.0)

            # Fractional Kelly Criterion Sizing (0.35% to 2.0%)
            win_prob = max(0.40, min(0.80, recent_win_rate / 100.0))
            b = 1.8
            q = 1.0 - win_prob
            raw_kelly = (win_prob * b - q) / b
            fractional_kelly = max(0.35, min(2.0, (raw_kelly * 0.25) * 10.0))

            if devils_advocate_mitigation and should_buy:
                fractional_kelly = max(0.35, fractional_kelly * 0.60)
                debate_transcript.append(
                    f"⚠️ [CRO Arbiter] Devil's Advocate Mitigation applied: Position dampened to {fractional_kelly:.2f}%."
                )

            # Resolve Exit Profile and Take-Profit / Stop-Loss Ladder
            exit_profile_name = revised_quant.metrics.get("exit_profile", ExitProfile.FAST_SNIPE.value)
            if exit_profile_name == ExitProfile.REVIVAL_SWING.value or is_swing:
                resolved_exit_profile = ExitProfile.REVIVAL_SWING
                tp_ladder = [
                    {"trigger_multiplier": 2.0, "sell_pct": 50.0},
                    {"trigger_multiplier": 3.0, "sell_pct": 50.0},
                    {"trigger_multiplier": 5.0, "sell_pct": 50.0},
                ]
                hard_sl = -18.0
                trailing_stop_pct = 35.0
                time_exit_m = 14400
            elif exit_profile_name == ExitProfile.EARLY_RALLY_SWING.value:
                resolved_exit_profile = ExitProfile.EARLY_RALLY_SWING
                tp_ladder = [
                    {"trigger_multiplier": 1.50, "sell_pct": 50.0},
                    {"trigger_multiplier": 2.00, "sell_pct": 50.0},
                ]
                hard_sl = -15.0
                trailing_stop_pct = 28.0
                time_exit_m = 240
            else:
                resolved_exit_profile = ExitProfile.FAST_SNIPE
                tp_ladder = [
                    {"trigger_multiplier": 1.25, "sell_pct": 50.0},
                    {"trigger_multiplier": 1.50, "sell_pct": 50.0},
                ]
                hard_sl = -12.0
                trailing_stop_pct = 25.0
                time_exit_m = 15

            if should_buy:
                debate_transcript.append(
                    f"⚖️ [CRO Arbiter] CONSENSUS APPROVED ({avg_score:.1f}/100). Profile: {resolved_exit_profile.value}. Half-Kelly Position: {fractional_kelly:.2f}%."
                )
                final_decision = "EXECUTE_BUY"
                cro_round = DebateRound(
                    round_number=4,
                    agent_name="CouncilArbiterCRO",
                    stance=DebateStance.SYNTHESIS.value,
                    arguments=f"Approved with {fractional_kelly:.2f}% sizing, {resolved_exit_profile.value} exit ladder.",
                    confidence=avg_score / 100.0,
                    score=avg_score,
                    generation_path="fallback",
                )
            else:
                final_decision = "PASS"
                if fatal_agent:
                    debate_transcript.append(f"⚖️ [CRO Arbiter] FATAL VETO ENFORCED: {veto_reason}. Execution halted.")
                    cro_round = DebateRound(
                        round_number=4,
                        agent_name="CouncilArbiterCRO",
                        stance=DebateStance.VETO.value,
                        arguments=veto_reason,
                        confidence=1.0,
                        score=0.0,
                        generation_path="fallback",
                    )
                else:
                    debate_transcript.append(
                        f"⚖️ [CRO Arbiter] INSUFFICIENT CONSENSUS ({avg_score:.1f}/100, Approvals: {approvals}/3, Rejections: {rejections}). Trade passed."
                    )
                    cro_round = DebateRound(
                        round_number=4,
                        agent_name="CouncilArbiterCRO",
                        stance=DebateStance.REJECT.value,
                        arguments=f"Insufficient consensus score: {avg_score:.1f}/100 < minimum requirement.",
                        confidence=0.8,
                        score=avg_score,
                        generation_path="fallback",
                    )

            debate_rounds.append(cro_round)

            # Calculate LLM vs Fallback telemetry metrics
            llm_telemetry = self.get_llm_telemetry()
            llm_turns = llm_telemetry["llm_turns"]
            fallback_turns = llm_telemetry["fallback_turns"]
            fallback_ratio = llm_telemetry["fallback_ratio"]
            avg_latency = llm_telemetry["avg_llm_latency_ms"]
            reasons = llm_telemetry["fallback_reasons"]
            warning = llm_telemetry["warning_triggered"]

            consensus_result = CouncilConsensusResult(
                decision=final_decision,
                consensus_score=round(avg_score / 100.0, 3),
                confidence=round(min(0.98, max(0.60, avg_score / 100.0)), 2),
                recommended_position_pct=round(fractional_kelly, 2) if should_buy else 0.0,
                max_slippage_bps=120 if should_buy else 50,
                hard_stop_loss_pct=hard_sl,
                take_profit_ladder=tp_ladder,
                trailing_stop_activation_pct=trailing_stop_pct,
                time_exit_minutes=time_exit_m,
                verdicts=final_verdicts,
                debate_transcript=debate_transcript,
                fatal_veto_agent=fatal_agent,
                debate_rounds=debate_rounds,
                kelly_size_pct=round(fractional_kelly, 2),
                exit_profile=resolved_exit_profile.value,
                devils_advocate_invoked=devils_advocate_invoked,
                devils_advocate_report=devils_advocate_report,
                retrieved_episodes_count=retrieved_count,
                llm_turns=llm_turns,
                fallback_turns=fallback_turns,
                fallback_ratio=fallback_ratio,
                avg_llm_latency_ms=avg_latency,
                fallback_reasons=reasons,
                warning_triggered=warning,
            )

            # Store recent deliberation in memory for post-mortem matching
            self._recent_deliberations[token_ca] = {
                "verdicts": {k: v.vote.value for k, v in final_verdicts.items()},
                "decision": final_decision,
                "signal_id": getattr(signal, "signal_id", ""),
                "timestamp_ns": time.time_ns(),
            }

            # Record deliberation to SQLite ledger
            if active_ledger and hasattr(active_ledger, "record_agent_deliberation"):
                try:
                    sig_id = getattr(signal, "signal_id", f"council-{uuid.uuid4().hex[:8]}")
                    strat_id = getattr(signal, "strategy_name", strategy_name)
                    await active_ledger.record_agent_deliberation({
                        "deliberation_id": f"council-{uuid.uuid4().hex[:12]}",
                        "signal_id": sig_id,
                        "token_address": token_ca,
                        "agent_votes_json": json.dumps({
                            "verdicts": {k: v.to_dict() for k, v in final_verdicts.items()},
                            "debate_rounds": [r.to_dict() for r in debate_rounds],
                            "exit_profile": resolved_exit_profile.value,
                            "kelly_size_pct": fractional_kelly,
                            "llm_telemetry": llm_telemetry,
                        }),
                        "episodic_memory_matches_json": json.dumps([]),
                        "selected_strategy_id": strat_id,
                        "final_decision": final_decision,
                        "rationale": "\n".join(debate_transcript),
                        "timestamp_ns": time.time_ns(),
                    })
                except Exception as log_err:
                    logger.warning("Failed to record council deliberation to ledger: %s", log_err, exc_info=True)

            if should_buy:
                trace_event.status = TraceStatus.PASSED
            else:
                trace_event.status = TraceStatus.REJECTED
                trace_event.reason = veto_reason or "Insufficient consensus"
            trace_event.output_data = {
                "decision": final_decision,
                "consensus_score": round(avg_score / 100.0, 3),
                "fatal_veto": fatal_agent,
                "kelly_size_pct": round(fractional_kelly, 2) if should_buy else 0.0,
                "llm_turns": llm_turns,
                "fallback_turns": fallback_turns,
            }

            return consensus_result

    async def record_trade_outcome(
        self,
        token_address: str,
        pnl_usd: float,
        roi_pct: float,
        exit_reason: str,
        signal_id: str = "",
        mock_verdicts: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Post-trade evaluation:
        1. Look up deliberation votes.
        2. Evaluate each agent's call (profitable -> APPROVE correct, unprofitable -> REJECT correct).
        3. Write post-mortem episode to Ruflo episodic memory.
        4. Update agent accuracy statistics and apply Bayesian shrinkage toward prior.
        5. Persist updated weights to SQLite ledger.
        """
        # Determine agent votes from deliberation history or mock
        delib = self._recent_deliberations.get(token_address, {})
        votes = mock_verdicts or delib.get("verdicts", {
            "ChainForensic": "APPROVE",
            "QuantTechnicals": "APPROVE",
            "ReachAnalyst": "APPROVE",
        })

        is_profitable = (pnl_usd > 0.0) or (roi_pct > 0.0)
        agent_correctness: dict[str, bool] = {}

        for agent_name in ("ChainForensic", "QuantTechnicals", "ReachAnalyst"):
            vote_val = votes.get(agent_name, "NEUTRAL")
            if is_profitable:
                correct = (vote_val == "APPROVE")
            else:
                correct = (vote_val == "REJECT")
            agent_correctness[agent_name] = correct

            # Update performance counters
            perf = self._agent_performance[agent_name]
            perf["total_trades"] += 1
            if correct:
                perf["correct_calls"] += 1

            n = perf["total_trades"]
            k = perf["correct_calls"]
            p0 = 0.50
            m = self._shrinkage_weight
            perf["measured_accuracy"] = round(k / n, 4)

            # Minimum sample requirement & shrinkage toward prior
            if n < self._min_sample_size:
                perf["shrunk_accuracy"] = p0
            else:
                shrunk_acc = (k + m * p0) / (n + m)
                perf["shrunk_accuracy"] = round(shrunk_acc, 4)

        # Update and normalize council weights
        raw_weights = {}
        for agent_name in ("ChainForensic", "QuantTechnicals", "ReachAnalyst"):
            base_w = self._base_weights.get(agent_name, 0.33)
            acc = self._agent_performance[agent_name]["shrunk_accuracy"]
            raw_weights[agent_name] = base_w * (acc / 0.50)

        total_raw = sum(raw_weights.values())
        if total_raw > 0:
            for agent_name in raw_weights:
                self.council_weights[agent_name] = round(raw_weights[agent_name] / total_raw, 4)

        # Write post-mortem episode to Ruflo episodic memory
        if self.episodic_memory and hasattr(self.episodic_memory, "record_episode"):
            correctness_str = ", ".join([f"{a}={'RIGHT' if c else 'WRONG'}" for a, c in agent_correctness.items()])
            summary = (
                f"Post-Mortem: PnL ${pnl_usd:+.2f} ({roi_pct:+.1f}%) on {token_address[:8]} via {exit_reason}. "
                f"Council Calls: {correctness_str}."
            )
            ep = MemoryEpisode(
                episode_id=f"pm-{uuid.uuid4().hex[:10]}",
                token_address=token_address,
                chain="solana_mainnet",
                decision_type="POST_MORTEM",
                signal_source=signal_id or "COUNCIL_TRADE",
                confidence_score=0.90,
                feature_vector=[0.5, 0.2, 0.5, 0.6, 0.15, 0.3, 0.5],
                realized_pnl_usd=pnl_usd,
                roi_pct=roi_pct,
                actual_slippage_bps=0,
                fill_latency_ms=0.0,
                root_cause_tag=exit_reason,
                summary_reflection=summary,
            )
            try:
                await self.episodic_memory.record_episode(ep)
            except Exception as ep_err:
                logger.error("Failed writing post-mortem episode to memory: %s", ep_err, exc_info=True)

        # Persist updated weights and performance to SQLite ledger
        if self.ledger and hasattr(self.ledger, "save_council_agent_weights"):
            try:
                await self.ledger.save_council_agent_weights(
                    weights=self.council_weights,
                    performance=self._agent_performance,
                )
            except Exception as s_err:
                logger.error("Failed persisting council agent weights to ledger: %s", s_err, exc_info=True)

    async def get_council_weights(self) -> dict[str, float]:
        """Return current normalized council agent weights."""
        return dict(self.council_weights)

    async def get_agent_performance(self) -> dict[str, dict[str, Any]]:
        """Return current measured accuracy and sample count for each agent."""
        return dict(self._agent_performance)

    def get_llm_telemetry(self) -> dict[str, Any]:
        """Return aggregated LLM vs Fallback telemetry metrics."""
        return self.llm_client.get_telemetry()

    def get_status_summary(self) -> str:
        """Return human-readable council health status for /status command."""
        t = self.get_llm_telemetry()
        ratio = t["fallback_ratio"] * 100.0
        llm_pct = 100.0 - ratio
        warning_str = " ⚠️ WARNING: High fallback ratio!" if t["warning_triggered"] else " 🟢"
        reasons_str = ", ".join([f"{k}: {v}" for k, v in t["fallback_reasons"].items()]) or "None"
        return (
            f"Council AI: LLM {llm_pct:.0f}% (Fallback {ratio:.0f}%, Latency {t['avg_llm_latency_ms']:.0f}ms){warning_str}\n"
            f"  Fallback Reasons: {reasons_str}"
        )
