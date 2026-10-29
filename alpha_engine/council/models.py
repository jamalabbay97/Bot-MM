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
    SYNTHESIS = "SYNTHESIS"


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

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["vote"] = self.vote.value
        return d


@dataclass
class DebateRound:
    """Represents a discrete turn/round in multi-agent deliberation."""
    round_number: int  # 1: Initial evaluation, 2: Cross-examination, 3: Rebuttal/Revision, 4: Arbiter Synthesis
    agent_name: str    # "ReachAnalyst", "ChainForensic", "QuantTechnicals", "CouncilArbiterCRO"
    stance: str        # "APPROVE", "REJECT", "CHALLENGE", "DEFEND", "REVISE", "VETO", "SYNTHESIS"
    arguments: str
    counter_arguments: Optional[str] = None
    target_agent: Optional[str] = None
    vote_weight: float = 1.0
    confidence: float = 0.8
    score: float = 50.0
    flags: list[str] = field(default_factory=list)
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
            "created_at": self.created_at,
        }
