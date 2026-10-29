"""
alpha_engine.council.arbiter_cro — Agent 4: Chief Risk Officer (CRO) & Council Debate Arbiter
Orchestrates genuine 4-round debate loop among Reach, Forensic, Quant, and CRO agents.
Enforces Kelly Criterion sizing, dynamic Take-Profit ladders, and absolute fatal vetoes.
Persists verbatim debate transcripts and round structures into SQLite ledger.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Optional

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
from alpha_engine.models.enums import ChainIdentifier, ExitProfile
from alpha_engine.models.observability import TraceStage, TraceStatus
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.tracer import tracer

logger = logging.getLogger(__name__)


class CouncilArbiterCRO:
    """
    Chief Risk Officer (CRO) & Council Arbiter.
    Runs a structured 4-round institutional debate:
      Round 1: Parallel Independent Audits
      Round 2: Cross-Examination & Peer Challenges
      Round 3: Rebuttals & Conviction Revisions
      Round 4: CRO Final Synthesis, Veto Enforcement & Fractional Kelly Sizing
    """

    def __init__(
        self,
        reach_agent: Optional[ReachAnalystAgent] = None,
        forensic_agent: Optional[ChainForensicAgent] = None,
        quant_agent: Optional[QuantTechnicalsAgent] = None,
        llm_client: Optional[CouncilLLMClient] = None,
        ledger: Optional[Any] = None,
    ):
        self.llm_client = llm_client or CouncilLLMClient()
        self.reach_agent = reach_agent or ReachAnalystAgent(llm_client=self.llm_client)
        self.forensic_agent = forensic_agent or ChainForensicAgent(llm_client=self.llm_client)
        self.quant_agent = quant_agent or QuantTechnicalsAgent(llm_client=self.llm_client)
        self.ledger = ledger

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
            # ROUND 1: Independent Initial Evaluations
            # =========================================================================
            debate_transcript.append("─── 🏛️ ROUND 1: INDEPENDENT INITIAL AUDITS ───")

            reach_task = self.reach_agent.audit_social_narrative(token_ca=token_ca, symbol=symbol, website_url=website_url)
            forensic_res: AgentVerdict = self.forensic_agent.audit_contract_and_liquidity(token_ca=token_ca, chain=chain, pool=pool, report=report)
            quant_res: AgentVerdict = self.quant_agent.analyze_market_dynamics(
                pool=pool, signal=signal, staged_data=staged_data, is_revival_swing=is_swing, is_new_launch=is_new_launch
            )
            reach_res: AgentVerdict = await reach_task

            # Record Round 1
            r1_forensic = DebateRound(
                round_number=1,
                agent_name="ChainForensic",
                stance=forensic_res.vote.value,
                arguments=forensic_res.rationale,
                vote_weight=1.5,
                confidence=forensic_res.confidence,
                score=forensic_res.score,
                flags=forensic_res.flags,
            )
            r1_quant = DebateRound(
                round_number=1,
                agent_name="QuantTechnicals",
                stance=quant_res.vote.value,
                arguments=quant_res.rationale,
                vote_weight=1.2,
                confidence=quant_res.confidence,
                score=quant_res.score,
                flags=quant_res.flags,
            )
            r1_reach = DebateRound(
                round_number=1,
                agent_name="ReachAnalyst",
                stance=reach_res.vote.value,
                arguments=reach_res.rationale,
                vote_weight=1.0,
                confidence=reach_res.confidence,
                score=reach_res.score,
                flags=reach_res.flags,
            )

            debate_rounds.extend([r1_forensic, r1_quant, r1_reach])
            debate_transcript.append(f"🛡️ [ChainForensic] Vote: {forensic_res.vote.value} ({forensic_res.score:.1f}/100) -> {forensic_res.rationale}")
            debate_transcript.append(f"📈 [QuantTechnicals] Vote: {quant_res.vote.value} ({quant_res.score:.1f}/100) -> {quant_res.rationale}")
            debate_transcript.append(f"🌐 [ReachAnalyst] Vote: {reach_res.vote.value} ({reach_res.score:.1f}/100) -> {reach_res.rationale}")

            # =========================================================================
            # ROUND 2: Cross-Examination & Peer Challenges
            # =========================================================================
            debate_transcript.append("─── ⚔️ ROUND 2: CROSS-EXAMINATION & CHALLENGES ───")

            ch_forensic, ch_quant, ch_reach = await asyncio.gather(
                self.forensic_agent.cross_examine_peer("QuantTechnicals", quant_res, forensic_res, token_ca),
                self.quant_agent.cross_examine_peer("ReachAnalyst", reach_res, quant_res, token_ca),
                self.reach_agent.cross_examine_peer("QuantTechnicals", quant_res, reach_res, token_ca),
            )

            debate_rounds.extend([ch_forensic, ch_quant, ch_reach])
            debate_transcript.append(f"🛡️ [ChainForensic ➔ {ch_forensic.target_agent}] ({ch_forensic.stance}) {ch_forensic.arguments}")
            debate_transcript.append(f"📈 [QuantTechnicals ➔ {ch_quant.target_agent}] ({ch_quant.stance}) {ch_quant.arguments}")
            debate_transcript.append(f"🌐 [ReachAnalyst ➔ {ch_reach.target_agent}] ({ch_reach.stance}) {ch_reach.arguments}")

            # =========================================================================
            # ROUND 3: Rebuttals & Conviction Revisions
            # =========================================================================
            debate_transcript.append("─── 🔄 ROUND 3: REBUTTALS & REVISIONS ───")

            (revised_forensic, r3_forensic), (revised_quant, r3_quant), (revised_reach, r3_reach) = await asyncio.gather(
                self.forensic_agent.defend_or_revise(debate_rounds, forensic_res),
                self.quant_agent.defend_or_revise(debate_rounds, quant_res),
                self.reach_agent.defend_or_revise(debate_rounds, reach_res),
            )

            debate_rounds.extend([r3_forensic, r3_quant, r3_reach])
            debate_transcript.append(f"🛡️ [ChainForensic] ({r3_forensic.stance}) Score: {revised_forensic.score:.1f}/100 -> {r3_forensic.arguments}")
            debate_transcript.append(f"📈 [QuantTechnicals] ({r3_quant.stance}) Score: {revised_quant.score:.1f}/100 -> {r3_quant.arguments}")
            debate_transcript.append(f"🌐 [ReachAnalyst] ({r3_reach.stance}) Score: {revised_reach.score:.1f}/100 -> {r3_reach.arguments}")

            final_verdicts: dict[str, AgentVerdict] = {
                "ReachAnalyst": revised_reach,
                "ChainForensic": revised_forensic,
                "QuantTechnicals": revised_quant,
            }

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

            # Dynamic Strategy Weighting
            if is_new_launch:
                # Fast-Launch: Forensic 60%, Quant 30%, Reach 10%
                w_forensic, w_quant, w_reach = 0.60, 0.30, 0.10
                strategy_name = "Fast-Launch Early Momentum Strategy"
            else:
                # Professional Market: Forensic 35%, Quant 45%, Reach 20%
                w_forensic, w_quant, w_reach = 0.35, 0.45, 0.20
                strategy_name = "Professional Market Strategy"

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
            b = 1.8  # win/loss payoff ratio
            q = 1.0 - win_prob
            raw_kelly = (win_prob * b - q) / b
            fractional_kelly = max(0.35, min(2.0, (raw_kelly * 0.25) * 10.0))

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
                    )

            debate_rounds.append(cro_round)

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
            )

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
                        }),
                        "episodic_memory_matches_json": json.dumps([]),
                        "selected_strategy_id": strat_id,
                        "final_decision": final_decision,
                        "rationale": "\n".join(debate_transcript),
                        "timestamp_ns": time.time_ns(),
                    })
                except Exception as log_err:
                    logger.debug("Failed to record council deliberation to ledger: %s", log_err)

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
            }

            return consensus_result
