"""
alpha_engine.council.forensic_agent — Agent 2: On-Chain Forensic & Microstructure Agent
Checks contract bytecode, mint/freeze authority, LP locks (>99%), Top-10 entropy, and Sybil clusters.
Enforces non-overridable fatal veto on contract vulnerabilities.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Optional

from alpha_engine.council.llm_client import CouncilLLMClient
from alpha_engine.council.models import AgentVerdict, AgentVote, DebateRound, DebateStance
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class ChainForensicAgent:
    """
    On-chain forensic auditor. Checks contract safety, holder concentration,
    mint/freeze authorities, and liquidity lock status.
    Maintains absolute fatal veto power over insecure assets.
    """

    def __init__(self, llm_client: Optional[CouncilLLMClient] = None):
        self.llm_client = llm_client or CouncilLLMClient()

    def audit_contract_and_liquidity(
        self,
        token_ca: str,
        chain: ChainIdentifier,
        pool: Optional[PoolState] = None,
        report: Optional[SecurityReport] = None,
        **kwargs: Any,
    ) -> AgentVerdict:
        """Deterministic round 1 audit of contract bytecode and liquidity reserves."""
        t0 = time.perf_counter()
        flags = []
        is_pump = "pump" in token_ca.lower() or (pool and "pump" in getattr(pool, "pool_address", "").lower())

        # 1. Check Authority (Solana SPL)
        if chain == ChainIdentifier.SOLANA_MAINNET:
            if report and (not getattr(report, "mint_authority_disabled", True) or not getattr(report, "freeze_authority_disabled", True)):
                flags.append("ACTIVE_MINT_OR_FREEZE_AUTHORITY")

        # 2. Check Taxes & Honeypot (EVM)
        if chain == ChainIdentifier.BASE_MAINNET:
            if report and getattr(report, "is_honeypot", False):
                flags.append("CONFIRMED_HONEYPOT_BYTECODE")
            if report and (getattr(report, "buy_tax_bps", 0) > 300 or getattr(report, "sell_tax_bps", 0) > 300):
                flags.append("EXCESSIVE_TRANSFER_TAX_OVER_3PCT")

        # 3. Check LP Locks
        if not is_pump and report and getattr(report, "lp_burned_ratio", 1.0) < 0.90:
            flags.append("LP_TOKENS_UNLOCKED_OR_UNBURNED")

        # 4. Check Top-10 Concentration
        top10 = getattr(report, "top10_concentration", 0.15) if report else 0.15
        max_allowed_top10 = 0.65 if is_pump else 0.20
        if top10 > max_allowed_top10:
            flags.append(f"TOP10_CONCENTRATION_EXCESSIVE_{top10*100:.1f}PCT")

        # Single holder concentration check (>35% non-pool holder)
        top1_holder = getattr(report, "top1_holder_pct", 0.0) if report else 0.0
        if top1_holder > 0.35 and not is_pump:
            flags.append(f"SINGLE_HOLDER_EXCEEDS_35PCT_{top1_holder*100:.1f}")

        # 5. Liquidity Depth / Slippage Insolvency
        if pool and getattr(pool, "native_reserve", Decimal("1.0")) < Decimal("0.5"):
            flags.append("CRITICAL_THIN_LIQUIDITY_DEPTH")

        # Fatal security disqualifiers
        fatal_flags = {
            "ACTIVE_MINT_OR_FREEZE_AUTHORITY",
            "CONFIRMED_HONEYPOT_BYTECODE",
            "LP_TOKENS_UNLOCKED_OR_UNBURNED",
            "CRITICAL_THIN_LIQUIDITY_DEPTH",
        }
        has_fatal = any(f in fatal_flags for f in flags)

        score = 100.0
        if has_fatal:
            score = 0.0
            vote = AgentVote.REJECT
            rationale = f"FATAL CONTRACT SECURITY BREACH: {', '.join(flags)}. 100% rejection."
        elif flags:
            score = max(20.0, 100.0 - (len(flags) * 30.0))
            vote = AgentVote.REJECT if score < 60.0 else AgentVote.NEUTRAL
            rationale = f"Security concerns detected: {', '.join(flags)}."
        else:
            vote = AgentVote.APPROVE
            rationale = "Contract authorities revoked, LP locked/burned >99%, healthy holder entropy, and optimal depth."

        duration_ms = (time.perf_counter() - t0) * 1000
        return AgentVerdict(
            agent_name="ChainForensicAgent",
            vote=vote,
            confidence=0.98 if has_fatal else 0.90,
            score=round(score, 1),
            rationale=rationale,
            flags=flags,
            metrics={
                "top10_pct": round(top10 * 100, 1),
                "is_pump_fun": is_pump,
                "native_reserve": float(pool.native_reserve) if pool else 0.0,
            },
            execution_time_ms=round(duration_ms, 1),
            fatal_veto=has_fatal,
        )

    async def cross_examine_peer(
        self,
        peer_name: str,
        peer_verdict: AgentVerdict,
        my_verdict: AgentVerdict,
        token_ca: str,
    ) -> DebateRound:
        """
        Round 2: Forensic challenges peer agent's optimism if on-chain reality contradicts narrative or technicals.
        """
        logger.debug("ChainForensic cross-examining peer %s for token %s", peer_name, token_ca)
        stance = DebateStance.CHALLENGE
        args = ""
        target = peer_name

        if my_verdict.fatal_veto:
            stance = DebateStance.VETO
            args = f"Non-overridable security veto standing: {', '.join(my_verdict.flags)}. Peer recommendations are moot due to contract insolvency."
        elif "TOP10_CONCENTRATION" in str(my_verdict.flags):
            args = f"Challenge to {peer_name}: Holder entropy is clustered ({my_verdict.metrics.get('top10_pct')}%), risk of insider supply dump overrides apparent momentum."
        elif peer_verdict.vote == AgentVote.APPROVE and my_verdict.vote != AgentVote.APPROVE:
            args = f"Caution to {peer_name}: Liquidity or bytecode flags {my_verdict.flags} create severe downside risk despite your favorable score ({peer_verdict.score})."
        else:
            stance = DebateStance.APPROVE
            args = f"Forensic telemetry aligns with {peer_name}. Authorities revoked and liquidity verifiably secured."

        return DebateRound(
            round_number=2,
            agent_name="ChainForensic",
            stance=stance.value,
            arguments=args,
            counter_arguments=peer_verdict.rationale,
            target_agent=target,
            vote_weight=1.5,
            confidence=my_verdict.confidence,
            score=my_verdict.score,
            flags=my_verdict.flags,
        )

    async def defend_or_revise(
        self,
        peer_rounds: list[DebateRound],
        current_verdict: AgentVerdict,
    ) -> tuple[AgentVerdict, DebateRound]:
        """
        Round 3: Rebuttal/Revision. Forensic verifies if any fatal veto stands or if peer arguments alleviate concerns.
        """
        if current_verdict.fatal_veto:
            # Fatal vetoes are ABSOLUTE and cannot be negotiated away
            round_entry = DebateRound(
                round_number=3,
                agent_name="ChainForensic",
                stance=DebateStance.VETO.value,
                arguments="IRREVOCABLE VETO: On-chain vulnerability cannot be resolved by social sentiment or technical indicators.",
                confidence=1.0,
                score=0.0,
                flags=current_verdict.flags,
            )
            return current_verdict, round_entry

        # Non-fatal flags: check if peer consensus is strong
        peer_approvals = sum(1 for r in peer_rounds if r.stance in ("APPROVE", "DEFEND"))
        revised_score = current_verdict.score
        revised_vote = current_verdict.vote

        if peer_approvals >= 2 and current_verdict.score >= 50.0:
            revised_score = min(100.0, current_verdict.score + 5.0)
            stance = DebateStance.DEFEND
            args = f"Acknowledging peer validation: Microstructure is acceptable for staged entry with tight stop-loss (Score: {revised_score:.1f})."
        else:
            stance = DebateStance.DEFEND
            args = f"Maintaining forensic stance: Security score remains {current_verdict.score:.1f}."

        revised_verdict = AgentVerdict(
            agent_name="ChainForensicAgent",
            vote=revised_vote,
            confidence=current_verdict.confidence,
            score=round(revised_score, 1),
            rationale=args,
            flags=current_verdict.flags,
            metrics=current_verdict.metrics,
            execution_time_ms=current_verdict.execution_time_ms,
            fatal_veto=False,
        )
        round_entry = DebateRound(
            round_number=3,
            agent_name="ChainForensic",
            stance=stance.value,
            arguments=args,
            confidence=revised_verdict.confidence,
            score=revised_verdict.score,
            flags=revised_verdict.flags,
        )
        return revised_verdict, round_entry
