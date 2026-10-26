"""
alpha_engine.council.arbiter_cro — Agent 4: Chief Risk Officer (CRO) & Council Debate Arbiter
Orchestrates debate consensus among Reach, Forensic, and Quant agents.
Enforces Kelly Criterion sizing, dynamic Take-Profit ladders, and absolute veto power.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from alpha_engine.council.forensic_agent import ChainForensicAgent
from alpha_engine.council.models import AgentVote, CouncilConsensusResult
from alpha_engine.council.quant_agent import QuantTechnicalsAgent
from alpha_engine.council.reach_agent import ReachAnalystAgent
from alpha_engine.models.enums import ChainIdentifier, ExitProfile
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class CouncilArbiterCRO:
    def __init__(self, reach_agent: Optional[ReachAnalystAgent] = None):
        self.reach_agent = reach_agent or ReachAnalystAgent()
        self.forensic_agent = ChainForensicAgent()
        self.quant_agent = QuantTechnicalsAgent()

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
    ) -> CouncilConsensusResult:
        """
        Orchestrates parallel multi-agent evaluation and conducts debate.
        """
        # 1. Lifecycle detection
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

        # Parallel evaluation by Reach, Forensic, and Quant agents
        reach_task = self.reach_agent.audit_social_narrative(token_ca=token_ca, symbol=symbol, website_url=website_url)
        forensic_res = self.forensic_agent.audit_contract_and_liquidity(token_ca=token_ca, chain=chain, pool=pool, report=report)
        quant_res = self.quant_agent.analyze_market_dynamics(
            pool=pool, signal=signal, staged_data=staged_data, is_revival_swing=is_swing, is_new_launch=is_new_launch
        )

        reach_res = await reach_task

        verdicts = {
            "ReachAnalyst": reach_res,
            "ChainForensic": forensic_res,
            "QuantTechnicals": quant_res,
        }

        # 2. Structured Debate Transcript
        debate: list[str] = [
            f"👤 [ChainForensic] Vote: {forensic_res.vote.value} ({forensic_res.score}/100) -> {forensic_res.rationale}",
            f"👤 [QuantTechnicals] Vote: {quant_res.vote.value} ({quant_res.score}/100) -> {quant_res.rationale}",
            f"👤 [ReachAnalyst] Vote: {reach_res.vote.value} ({reach_res.score}/100) -> {reach_res.rationale}",
        ]

        # 3. Hard Veto Checks (CRO Rules)
        fatal_agent = None
        if forensic_res.vote == AgentVote.REJECT:
            fatal_agent = "ChainForensicAgent"
            debate.append("⚖️ [CRO Arbiter] FATAL VETO: Chain Forensic failed security gates. Execution blocked.")
        elif "PAID_BOT_RAID_DETECTED" in reach_res.flags:
            fatal_agent = "ReachAnalystAgent"
            debate.append("⚖️ [CRO Arbiter] VETO: Reach Analyst detected paid promotion bot raid. Capital protected.")
        elif quant_res.vote == AgentVote.REJECT and "OVEREXTENDED_GREEN_CANDLE_FOMO" in quant_res.flags:
            fatal_agent = "QuantTechnicalsAgent"
            debate.append("⚖️ [CRO Arbiter] VETO: Quant Analyst detected vertical green candle FOMO wick.")

        # 4. Differentiated Lifecycle Scoring & Voting
        if is_new_launch:
            # Fast-Launch Early Momentum Strategy: 60% Forensic, 30% Quant, 10% Reach
            w_forensic = 0.60
            w_quant = 0.30
            w_reach = 0.10
            strategy_name = "Fast-Launch Early Momentum Strategy"
        else:
            # Professional Market Strategy: 35% Forensic, 45% Quant, 20% Reach
            w_forensic = 0.35
            w_quant = 0.45
            w_reach = 0.20
            strategy_name = "Professional Market Strategy"

        avg_score = (forensic_res.score * w_forensic) + (quant_res.score * w_quant) + (reach_res.score * w_reach)
        debate.append(f"⚖️ [CRO Arbiter] Strategy: {strategy_name} (Forensic {w_forensic:.0%}, Quant {w_quant:.0%}, Reach {w_reach:.0%}) -> Score {avg_score:.1f}/100")

        approvals = sum(1 for v in verdicts.values() if v.vote == AgentVote.APPROVE)
        rejections = sum(1 for v in verdicts.values() if v.vote == AgentVote.REJECT)

        if is_new_launch:
            forensic_passed = forensic_res.vote == AgentVote.APPROVE or forensic_res.score >= 70.0
            should_buy = (fatal_agent is None) and (rejections == 0) and forensic_passed and (avg_score >= 60.0)
        else:
            should_buy = (fatal_agent is None) and (approvals >= 2) and (rejections == 0) and (avg_score >= 65.0)

        # 5. Position Sizing via Fractional Kelly Criterion
        win_prob = max(0.40, min(0.80, recent_win_rate / 100.0))
        b = 1.8  # win ratio
        q = 1.0 - win_prob
        raw_kelly = (win_prob * b - q) / b
        fractional_kelly = max(0.35, min(2.0, (raw_kelly * 0.25) * 10.0))

        is_swing = getattr(signal, "exit_profile", None) == ExitProfile.REVIVAL_SWING
        if is_swing:
            tp_ladder = [
                {"trigger_multiplier": 2.0, "sell_pct": 50.0},
                {"trigger_multiplier": 3.0, "sell_pct": 50.0},
                {"trigger_multiplier": 5.0, "sell_pct": 50.0},
            ]
            hard_sl = -18.0
            time_exit_m = 14400
        else:
            tp_ladder = [
                {"trigger_multiplier": 1.25, "sell_pct": 50.0},
                {"trigger_multiplier": 1.50, "sell_pct": 50.0},
            ]
            hard_sl = -12.0
            time_exit_m = 15

        if should_buy:
            debate.append(f"⚖️ [CRO Arbiter] CONSENSUS APPROVED (Score: {avg_score:.1f}/100). Authorized Half-Kelly Size: {fractional_kelly:.2f}%.")
            final_decision = "EXECUTE_BUY"
        else:
            if not fatal_agent:
                debate.append(f"⚖️ [CRO Arbiter] INSUFFICIENT CONSENSUS (Score: {avg_score:.1f}/100, Approvals: {approvals}/3). Trade passed.")
            final_decision = "PASS"

        return CouncilConsensusResult(
            decision=final_decision,
            consensus_score=round(avg_score / 100.0, 3),
            confidence=round(min(0.98, max(0.60, avg_score / 100.0)), 2),
            recommended_position_pct=round(fractional_kelly, 2) if should_buy else 0.0,
            max_slippage_bps=120 if should_buy else 50,
            hard_stop_loss_pct=hard_sl,
            take_profit_ladder=tp_ladder,
            trailing_stop_activation_pct=25.0 if not is_swing else 40.0,
            time_exit_minutes=time_exit_m,
            verdicts=verdicts,
            debate_transcript=debate,
            fatal_veto_agent=fatal_agent,
        )
