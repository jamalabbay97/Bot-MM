"""
alpha_engine.council.models — Data schemas for Council of 4 Autonomous Agents
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class AgentVote(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    NEUTRAL = "NEUTRAL"


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
    created_at: float = field(default_factory=time.time)
