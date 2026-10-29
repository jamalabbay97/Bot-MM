"""
alpha_engine.council.quant_agent — Agent 3: Quantitative Technical & Revival Strategy Agent
Calculates volume surges, order-flow imbalance (OFI), RSI overbought exhaustion, and Wave-2/CTO revival inflection.
Enforces anti-FOMO guards and recommends exit profiles.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.council.llm_client import CouncilLLMClient
from alpha_engine.council.models import AgentVerdict, AgentVote, DebateRound, DebateStance
from alpha_engine.models.enums import ExitProfile
from alpha_engine.models.state import PoolState

logger = logging.getLogger(__name__)


class QuantTechnicalsAgent:
    def __init__(self, llm_client: Optional[CouncilLLMClient] = None):
        self.llm_client = llm_client or CouncilLLMClient()

    def analyze_market_dynamics(
        self,
        pool: Optional[PoolState],
        signal: Any,
        staged_data: Optional[Any] = None,
        is_revival_swing: bool = False,
        is_new_launch: Optional[bool] = None,
    ) -> AgentVerdict:
        """Round 1: Quantitative technical analysis, volume surge, and anti-FOMO checks."""
        t0 = time.perf_counter()
        flags = []

        surge_k = float(getattr(signal, "volume_surge_multiplier", 0.0) or (getattr(staged_data, "volume_surge_multiplier", 0.0) if staged_data else 1.0))
        net_buy_delta = float(getattr(signal, "net_buy_delta", 0.5) or (getattr(staged_data, "net_buy_delta", 0.5) if staged_data else 0.5))
        alpha_score = float(getattr(signal, "alpha_score", 0.5) or 0.5)

        # 1. Anti-FOMO & Over-extension Guard
        dormant_mcap = float(getattr(signal, "base_market_cap", 0.0) or 0.0)
        raw_mcap = (
            getattr(signal, "market_cap_usd", 0.0)
            or (getattr(pool, "market_cap_usd", 0.0) if pool else 0.0)
            or (getattr(staged_data, "market_cap_usd", 0.0) if staged_data else 0.0)
            or 0.0
        )
        curr_mcap = float(raw_mcap) if isinstance(raw_mcap, (int, float, Decimal)) else float(Decimal(str(raw_mcap)))
        if dormant_mcap > 0 and curr_mcap > 0 and (curr_mcap / dormant_mcap) > 2.5:
            flags.append("OVEREXTENDED_GREEN_CANDLE_FOMO")

        # 2. RSI Check
        rsi = float(getattr(staged_data, "rsi_5m", 50.0) if staged_data else 50.0)
        if rsi > 82.0:
            flags.append(f"EXTREME_RSI_OVERBOUGHT_{rsi:.1f}")

        is_nl = is_new_launch if is_new_launch is not None else (
            not is_revival_swing
            and (
                getattr(signal, "token_age_s", 0) <= 1800
                or (getattr(signal, "source", None) is not None and getattr(signal.source, "value", str(signal.source)) in ("pump_fun_mint", "dex_swap", "dex_discovery", "launch_stage"))
                or (staged_data is not None and hasattr(staged_data, "unique_buyers"))
                or (surge_k <= 1.0 and dormant_mcap == 0.0)
            )
        )

        # 3. Resolve recommended exit profile
        if is_revival_swing or getattr(signal, "exit_profile", None) == ExitProfile.REVIVAL_SWING:
            recommended_profile = ExitProfile.REVIVAL_SWING
        elif getattr(signal, "exit_profile", None) == ExitProfile.EARLY_RALLY_SWING:
            recommended_profile = ExitProfile.EARLY_RALLY_SWING
        elif is_nl:
            recommended_profile = ExitProfile.FAST_SNIPE
        else:
            recommended_profile = ExitProfile.PEAK_TRAILING

        if is_nl:
            # Micro-Velocity Strategy for Newly Launched Tokens
            unique_buyers_count = len(getattr(staged_data, "unique_buyers", set())) if staged_data else (
                len(getattr(signal, "unique_buyers", set())) if hasattr(signal, "unique_buyers") else 2
            )
            net_buy_passed = net_buy_delta >= 0.50
            entropy_passed = unique_buyers_count >= 2

            score = (alpha_score * 35.0) + (net_buy_delta * 40.0) + (min(2.0, unique_buyers_count / 2.0) * 25.0)
            if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags or rsi > 82.0:
                score = max(0.0, score - 50.0)

            score = max(0.0, min(100.0, score))

            if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags:
                vote = AgentVote.REJECT
                rationale = "Over-extended top wick chase: anti-FOMO veto active."
            elif net_buy_passed and entropy_passed and score >= 55.0:
                vote = AgentVote.APPROVE
                rationale = f"Healthy early micro-velocity: Net Buy Ratio={net_buy_delta*100:.0f}%, Unique Buyers={unique_buyers_count}, Score={score:.1f}/100."
            elif score >= 45.0:
                vote = AgentVote.NEUTRAL
                rationale = f"Moderate micro-velocity (Score={score:.1f}/100, Net Buy Ratio={net_buy_delta*100:.0f}%)."
            else:
                vote = AgentVote.REJECT
                rationale = f"Insufficient micro-velocity (Net Buy Ratio={net_buy_delta*100:.0f}% < 50%)."
        else:
            # Established Tokens: 15-minute SMA surge & volume inflection
            surge_passed = surge_k >= 2.5
            net_buy_passed = net_buy_delta >= 0.65

            score = (alpha_score * 40.0) + (min(2.0, surge_k / 2.5) * 30.0) + (net_buy_delta * 30.0)
            if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags or rsi > 82.0:
                score = max(0.0, score - 50.0)

            score = max(0.0, min(100.0, score))

            if "OVEREXTENDED_GREEN_CANDLE_FOMO" in flags:
                vote = AgentVote.REJECT
                rationale = "Over-extended top wick chase: current mcap is >2.5x dormant consolidation base. Anti-FOMO veto active."
            elif surge_passed and net_buy_passed and score >= 65.0:
                vote = AgentVote.APPROVE
                rationale = f"Confirmed technical inflection: Volume Surge={surge_k:.2f}x, Net Buy Delta={net_buy_delta*100:.0f}%, RSI={rsi:.1f} in healthy momentum band."
            elif score >= 50.0:
                vote = AgentVote.NEUTRAL
                rationale = f"Moderate momentum (Score={score:.1f}/100). Surge={surge_k:.1f}x."
            else:
                vote = AgentVote.REJECT
                rationale = f"Insufficient technical momentum (Score={score:.1f}/100, Buy Delta={net_buy_delta*100:.0f}%)."

        duration_ms = (time.perf_counter() - t0) * 1000
        return AgentVerdict(
            agent_name="QuantTechnicalsAgent",
            vote=vote,
            confidence=round(min(0.95, max(0.50, alpha_score)), 2),
            score=round(score, 1),
            rationale=rationale,
            flags=flags,
            metrics={
                "volume_surge_multiplier": round(surge_k, 2),
                "net_buy_delta_pct": round(net_buy_delta * 100, 1),
                "rsi_5m": round(rsi, 1),
                "exit_profile": recommended_profile.value,
            },
            execution_time_ms=round(duration_ms, 1),
        )

    async def cross_examine_peer(
        self,
        peer_name: str,
        peer_verdict: AgentVerdict,
        my_verdict: AgentVerdict,
        token_ca: str,
    ) -> DebateRound:
        """Round 2: Quant cross-examines peer opinions with market microstructure data."""
        logger.debug("QuantTechnicals cross-examining peer %s for token %s", peer_name, token_ca)
        if "OVEREXTENDED_GREEN_CANDLE_FOMO" in my_verdict.flags:
            stance = DebateStance.CHALLENGE
            args = f"Challenge to {peer_name}: Vertical green candle FOMO detected (>2.5x extension). Chasing entry here risks catching a 40%+ retracement wick."
        elif "PAID_BOT_RAID_DETECTED" in peer_verdict.flags:
            stance = DebateStance.REVISE
            args = f"Conceding to {peer_name}: If volume is paid bot raid, volume surge multiplier is discounted as artificial liquidity manipulation."
        elif peer_verdict.vote == AgentVote.REJECT and my_verdict.vote == AgentVote.APPROVE:
            stance = DebateStance.DEFEND
            args = f"Defending to {peer_name}: Net order-flow delta and volume surge ({my_verdict.metrics.get('volume_surge_multiplier')}x) show institutional accumulation."
        else:
            stance = DebateStance.APPROVE
            args = f"Technicals align with {peer_name} findings."

        return DebateRound(
            round_number=2,
            agent_name="QuantTechnicals",
            stance=stance.value,
            arguments=args,
            counter_arguments=peer_verdict.rationale,
            target_agent=peer_name,
            vote_weight=1.2,
            confidence=my_verdict.confidence,
            score=my_verdict.score,
            flags=my_verdict.flags,
        )

    async def defend_or_revise(
        self,
        peer_rounds: list[DebateRound],
        current_verdict: AgentVerdict,
    ) -> tuple[AgentVerdict, DebateRound]:
        """Round 3: Rebuttal/Revision based on peer cross-examinations."""
        revised_score = current_verdict.score
        revised_vote = current_verdict.vote

        # Check for paid bot raids from ReachAnalyst
        paid_raid = any("PAID_BOT_RAID_DETECTED" in str(r.flags) or "paid bot raid" in r.arguments.lower() for r in peer_rounds)
        if paid_raid:
            revised_score = max(10.0, current_verdict.score - 40.0)
            revised_vote = AgentVote.REJECT
            stance = DebateStance.REVISE
            args = "Revising downward: Volume surge invalidated due to confirmed bot farm raid."
        elif "OVEREXTENDED_GREEN_CANDLE_FOMO" in current_verdict.flags:
            stance = DebateStance.DEFEND
            args = "Defending anti-FOMO veto: Risk/reward ratio remains unacceptable on extended candle."
        else:
            stance = DebateStance.DEFEND
            args = f"Defending quantitative momentum model (Score: {revised_score:.1f}/100)."

        revised_verdict = AgentVerdict(
            agent_name="QuantTechnicalsAgent",
            vote=revised_vote,
            confidence=current_verdict.confidence,
            score=round(revised_score, 1),
            rationale=args,
            flags=current_verdict.flags,
            metrics=current_verdict.metrics,
            execution_time_ms=current_verdict.execution_time_ms,
        )
        round_entry = DebateRound(
            round_number=3,
            agent_name="QuantTechnicals",
            stance=stance.value,
            arguments=args,
            confidence=revised_verdict.confidence,
            score=revised_verdict.score,
            flags=revised_verdict.flags,
        )
        return revised_verdict, round_entry
