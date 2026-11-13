"""
alpha_engine.council.models — Data schemas for Council of 4 Autonomous Agents
Includes multi-turn debate rounds, agent verdicts, and consensus synthesis.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional


class AgentVote(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    NEUTRAL = "NEUTRAL"


class DebateStance(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    NEUTRAL = "NEUTRAL"
    VETO = "VETO"
    CHALLENGE = "CHALLENGE"
    DEFEND = "DEFEND"
    REVISE = "REVISE"
    QUESTION = "QUESTION"
    ANSWER = "ANSWER"
    DEVILS_ADVOCATE = "DEVILS_ADVOCATE"
    SYNTHESIS = "SYNTHESIS"


@dataclass
class InformationRequest:
    """Inter-agent request for specific telemetry or audit clarification."""
    request_id: str
    requester_agent: str
    target_agent: str
    question: str
    context: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InformationResponse:
    """Response to an inter-agent information request."""
    request_id: str
    responder_agent: str
    answer: str
    confidence: float = 0.90
    data_points: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DevilsAdvocateStressTest:
    """Stress-test objections generated when Council reaches unanimous approval."""
    challenger_agent: str
    liquidity_challenge: str
    insider_cluster_challenge: str
    narrative_novelty_challenge: str
    fatal_flaw_detected: bool = False
    flaw_summary: str = ""
    mitigation_required: bool = False
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentVerdict:
    agent_name: str
    vote: AgentVote
    confidence: float  # 0.0 to 1.0
    score: float       # 0.0 to 100.0
    rationale: str
    flags: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    fatal_veto: bool = False
    generation_path: str = "fallback"  # "llm" or "fallback"
    fallback_reason: Optional[str] = None
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["vote"] = self.vote.value if hasattr(self.vote, "value") else str(self.vote)
        return d


@dataclass
class DebateRound:
    """Represents a discrete turn/round in multi-agent deliberation."""
    round_number: int  # 1: Initial evaluation, 2: Cross-examination / Q&A, 3: Rebuttal/Revision/Devil's Advocate, 4: Arbiter Synthesis
    agent_name: str    # "ReachAnalyst", "ChainForensic", "QuantTechnicals", "CouncilArbiterCRO"
    stance: str        # "APPROVE", "REJECT", "CHALLENGE", "DEFEND", "REVISE", "VETO", "DEVILS_ADVOCATE", "SYNTHESIS"
    arguments: str
    counter_arguments: Optional[str] = None
    target_agent: Optional[str] = None
    vote_weight: float = 1.0
    confidence: float = 0.8
    score: float = 50.0
    flags: list[str] = field(default_factory=list)
    generation_path: str = "fallback"  # "llm" or "fallback"
    fallback_reason: Optional[str] = None
    latency_ms: float = 0.0
    evidence: list[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CouncilConsensusResult:
    decision: str  # "EXECUTE_BUY" | "PASS"
    consensus_score: float  # 0.0 to 1.0
    confidence: float
    recommended_position_pct: float
    max_slippage_bps: int
    hard_stop_loss_pct: float
    take_profit_ladder: list[dict[str, float]]
    trailing_stop_activation_pct: float
    time_exit_minutes: int
    verdicts: dict[str, AgentVerdict]
    debate_transcript: list[str]
    fatal_veto_agent: Optional[str] = None
    debate_rounds: list[DebateRound] = field(default_factory=list)
    kelly_size_pct: float = 0.0
    exit_profile: Optional[str] = None
    devils_advocate_invoked: bool = False
    devils_advocate_report: Optional[dict[str, Any]] = None
    retrieved_episodes_count: int = 0
    llm_turns: int = 0
    fallback_turns: int = 0
    fallback_ratio: float = 0.0
    avg_llm_latency_ms: float = 0.0
    fallback_reasons: dict[str, int] = field(default_factory=dict)
    warning_triggered: bool = False
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "consensus_score": self.consensus_score,
            "confidence": self.confidence,
            "recommended_position_pct": self.recommended_position_pct,
            "max_slippage_bps": self.max_slippage_bps,
            "hard_stop_loss_pct": self.hard_stop_loss_pct,
            "take_profit_ladder": self.take_profit_ladder,
            "trailing_stop_activation_pct": self.trailing_stop_activation_pct,
            "time_exit_minutes": self.time_exit_minutes,
            "verdicts": {k: v.to_dict() for k, v in self.verdicts.items()},
            "debate_transcript": self.debate_transcript,
            "fatal_veto_agent": self.fatal_veto_agent,
            "debate_rounds": [r.to_dict() for r in self.debate_rounds],
            "kelly_size_pct": self.kelly_size_pct,
            "exit_profile": self.exit_profile,
            "devils_advocate_invoked": self.devils_advocate_invoked,
            "devils_advocate_report": self.devils_advocate_report,
            "retrieved_episodes_count": self.retrieved_episodes_count,
            "llm_turns": self.llm_turns,
            "fallback_turns": self.fallback_turns,
            "fallback_ratio": self.fallback_ratio,
            "avg_llm_latency_ms": self.avg_llm_latency_ms,
            "fallback_reasons": self.fallback_reasons,
            "warning_triggered": self.warning_triggered,
            "created_at": self.created_at,
        }
