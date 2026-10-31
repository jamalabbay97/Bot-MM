"""
alpha_engine.ruflo.protocol — Ruflo Multi-Agent Swarm Orchestration Protocol
=============================================================================
Multi-Chain (SVM & EVM) Autonomous Trading Swarm
Python 3.11+ | ruflo meta-framework protocol | Typed agent & tool schemas
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from alpha_engine.models.base import _STRICT_MODEL_CFG


class AgentRole(str, Enum):
    """Specialized Ruflo swarm agent roles."""

    SCOUT = "SCOUT"                      # Ingestion & Scouting Swarm
    PROFILER = "PROFILER"                # Smart Money & Whale Profiler
    GATEKEEPER = "GATEKEEPER"            # Security & Honeypot Gatekeeper
    QUANT = "QUANT"                      # Quant Math & Dynamic Risk Sizing
    EXECUTOR = "EXECUTOR"                # Execution & Ledger Journaling
    SUPERVISOR = "SUPERVISOR"            # 24/7 Self-Learning & Reflection Loop
    LEARNER = "LEARNER"                  # Distillation, Consolidation & Heuristics Tuner


class MessageType(str, Enum):
    """Categorized swarm message interaction types."""

    SIGNAL = "SIGNAL"                    # New candidate alpha signal announcement
    PROPOSAL = "PROPOSAL"                # Trade proposal for consensus voting
    VOTE = "VOTE"                        # Agent vote (APPROVE / REJECT / ABSTAIN)
    REQUEST = "REQUEST"                  # Synchronous or async RPC query to peer agent
    RESPONSE = "RESPONSE"                # RPC query response
    RULE_CONSOLIDATION = "RULE_CONSOLIDATION" # Distilled semantic rule update
    VETO = "VETO"                        # Unconditional circuit-breaker veto
    HEARTBEAT = "HEARTBEAT"              # Liveness and telemetry heartbeat


class SwarmTopology(str, Enum):
    """Ruflo swarm coordination topologies."""

    HIERARCHICAL = "hierarchical"        # Supervisor / Queen directs specialized agents
    MESH = "mesh"                        # Peer-to-peer cooperative mesh with shared state
    ADAPTIVE = "adaptive"                # Dynamically rebalances between mesh and hierarchy


class ConsensusProtocol(str, Enum):
    """Consensus mechanisms for trade approval and parameter validation."""

    BFT = "bft"                          # Byzantine Fault Tolerance (2/3 quorum required)
    RAFT = "raft"                        # Leader-election based consensus
    GOSSIP = "gossip"                    # Epidemic broadcast for state dissemination


class ToolDefinition(BaseModel):
    """
    Structured Model Context Protocol (MCP) compatible tool definition.
    Exposes input schema, permissions, concurrency safety, and execution handler.
    """

    model_config = _STRICT_MODEL_CFG

    name: str
    description: str
    category: str
    parameters_schema: dict[str, Any] = Field(default_factory=dict)
    requires_lock: bool = False
    rate_limit_per_min: int = 120
    is_idempotent: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


@dataclass
class SwarmMessage:
    """Inter-agent message passed through the swarm message bus."""

    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender_role: AgentRole = AgentRole.SUPERVISOR
    target_role: Optional[AgentRole] = None  # None indicates broadcast
    message_type: str = "SIGNAL"
    action: Optional[str] = None
    content: Optional[str] = None
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    status: str = "OK"
    priority: int = 1                    # 1=Normal, 2=High, 3=Critical/Veto
    is_request: bool = False
    timestamp_ns: int = field(default_factory=time.time_ns)

    def __post_init__(self) -> None:
        if self.action:
            self.message_type = self.action

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "sender_role": self.sender_role.value if hasattr(self.sender_role, "value") else str(self.sender_role),
            "target_role": self.target_role.value if (self.target_role and hasattr(self.target_role, "value")) else ("BROADCAST" if not self.target_role else str(self.target_role)),
            "message_type": self.message_type,
            "content": self.content,
            "payload": self.payload,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "status": self.status,
            "priority": self.priority,
            "is_request": self.is_request,
            "timestamp_ns": self.timestamp_ns,
        }


class AgentStatus(str, Enum):
    INITIALIZING = "INITIALIZING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"
    TERMINATED = "STOPPED"
