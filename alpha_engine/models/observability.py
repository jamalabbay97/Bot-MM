"""
alpha_engine.models.observability — Token Lifecycle Observability Models
========================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field
from alpha_engine.models.base import _STRICT_MODEL_CFG


class TraceStage(str, Enum):
    DETECTION = "DETECTION"
    INGESTION = "INGESTION"
    DEDUPLICATION = "DEDUPLICATION"
    SECURITY_SCREENING = "SECURITY_SCREENING"
    FILTERING = "FILTERING"
    STAGING = "STAGING"
    AI_ANALYSIS = "AI_ANALYSIS"
    DECISION = "DECISION"
    EXECUTION = "EXECUTION"


class TraceStatus(str, Enum):
    PENDING = "PENDING"
    PASSED = "PASSED"
    REJECTED = "REJECTED"
    IGNORED = "IGNORED"
    ERROR = "ERROR"


class LifecycleEvent(BaseModel):
    """A single traced event in a token's journey through the engine."""

    # Derive from strict model config but permit span mutation during lifecycle tracking
    model_config = ConfigDict(**{**_STRICT_MODEL_CFG, "frozen": False})

    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    token_address: str
    chain: str
    stage: TraceStage
    component: str
    function_name: str
    status: TraceStatus = TraceStatus.PENDING
    reason: Optional[str] = None
    input_data: Dict[str, Any] = Field(default_factory=dict)
    output_data: Dict[str, Any] = Field(default_factory=dict)
    duration_ms: float = 0.0
    timestamp_ns: int = Field(default_factory=time.time_ns)
