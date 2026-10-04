"""
alpha_engine.models.ai — Domain Models for AlphaSupervisor-AI
============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2 | Strict JSON Schema Compliance
"""

from __future__ import annotations

import json
from decimal import Decimal
from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field

from alpha_engine.models.base import _STRICT_MODEL_CFG


class AISupervisorDecisionEnum(str, Enum):
    """Deterministic action decisions emitted by AlphaSupervisor-AI."""

    EXECUTE_BUY = "EXECUTE_BUY"
    EXECUTE_SELL = "EXECUTE_SELL"
    PASS = "PASS"
    UPDATE_RISK_PARAMS = "UPDATE_RISK_PARAMS"


class WalletRiskClassification(str, Enum):
    """Smart Money & Entity Profiling classifications."""

    ORGANIC_SMART_MONEY = "ORGANIC_SMART_MONEY"
    CABAL_INSIDER = "CABAL_INSIDER"
    MEV_BOT = "MEV_BOT"
    WASH_TRADER = "WASH_TRADER"
    NOVICE_LUCKY = "NOVICE_LUCKY"


class HoneypotRisk(str, Enum):
    """Honeypot risk level."""

    NONE = "NONE"
    SUSPICIOUS = "SUSPICIOUS"
    CONFIRMED = "CONFIRMED"


class LiquidityHealth(str, Enum):
    """Pool liquidity health assessment."""

    OPTIMAL = "OPTIMAL"
    THIN = "THIN"
    UNLOCKED = "UNLOCKED"


class GlobalRiskMode(str, Enum):
    """Feedback-driven global risk appetite tuning."""

    EXPAND = "EXPAND"
    NEUTRAL = "NEUTRAL"
    DEFENSIVE = "DEFENSIVE"


class LossAttribution(str, Enum):
    """Post-mortem root cause failure modes."""

    LATE_ENTRY = "LATE_ENTRY"
    RUG_PULL = "RUG_PULL"
    MEV_SANDWICH = "MEV_SANDWICH"
    VOLUME_DRYOUT = "VOLUME_DRYOUT"
    CABAL_DUMP = "CABAL_DUMP"


class TakeProfitStage(BaseModel):
    """Stage in the multi-stage take-profit ladder."""

    model_config = _STRICT_MODEL_CFG

    trigger_multiplier: Annotated[float, Field(ge=1.0, description="Price multiplier relative to entry (e.g. 2.0 = +100%)")]
    sell_pct: Annotated[float, Field(ge=1.0, le=100.0, description="Percentage of current position to scale out")]


class ActionParameters(BaseModel):
    """Deterministic execution and risk parameters output by AlphaSupervisor-AI."""

    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        validate_default=True,
        populate_by_name=True,
    )

    target_token_address: str = Field(description="Target token mint or contract address")
    recommended_position_pct: Annotated[float, Field(ge=0.0, le=10.0, description="Fractional Kelly position size clamped 0.25% - 2.0%")] = 1.0
    max_slippage_bps: Annotated[int, Field(ge=50, le=500, description="Max execution slippage in basis points")] = 150
    priority_fee_multiplier: Annotated[float, Field(ge=1.0, le=3.0, description="Dynamic network priority gas / Jito tip multiplier")] = 1.2
    take_profit_ladder: list[TakeProfitStage] = Field(
        default_factory=lambda: [
            TakeProfitStage(trigger_multiplier=2.0, sell_pct=40.0),
            TakeProfitStage(trigger_multiplier=3.5, sell_pct=30.0),
        ],
        description="Multi-stage take-profit ladder",
    )
    hard_stop_loss_pct: Annotated[float, Field(le=0.0, ge=-50.0, description="Initial hard stop loss percentage (e.g. -15.0)")] = -15.0
    trailing_stop_activation_pct: Annotated[float, Field(ge=0.0, description="Gain percentage required to activate trailing stop and breakeven shift")] = 40.0
    time_exit_minutes: Annotated[int, Field(ge=1, le=120, description="Stagnation timeout in minutes to force exit if target gain not achieved")] = 15


class WalletAudit(BaseModel):
    """Comprehensive wallet and on-chain profiling audit."""

    model_config = _STRICT_MODEL_CFG

    is_wallet_reputable: bool = Field(description="True if wallet meets legitimate smart money criteria")
    risk_classification: WalletRiskClassification | str = Field(description="Entity classification")
    rationale: str = Field(description="Concise summary of on-chain verification")


class SecurityAssessment(BaseModel):
    """On-chain contract security, LP depth, and pre-flight evaluation."""

    model_config = _STRICT_MODEL_CFG

    is_secure: bool = Field(description="True if contract passes all security and liquidity pre-flights")
    honeypot_risk: HoneypotRisk | str = Field(description="NONE, SUSPICIOUS, or CONFIRMED")
    liquidity_health: LiquidityHealth | str = Field(description="OPTIMAL, THIN, or UNLOCKED")
    flags: list[str] = Field(default_factory=list, description="Detected security anomalies or warnings")


class FeedbackTuning(BaseModel):
    """Closed-loop feedback and hyperparameter auto-tuning directives."""

    model_config = _STRICT_MODEL_CFG

    adjust_global_risk: GlobalRiskMode | str = Field(description="EXPAND, NEUTRAL, or DEFENSIVE")
    blacklisted_entities: list[str] = Field(default_factory=list, description="Toxic addresses or cabal identifiers to blacklist")
    insights_learned: str = Field(description="Key strategic takeaway to persist into local state")


class AISupervisorResponse(BaseModel):
    """
    Standardized, strict JSON schema output emitted by AlphaSupervisor-AI.
    Matches the exact schema specified in the supervisory directive.
    """

    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        validate_default=True,
        populate_by_name=True,
    )

    decision: AISupervisorDecisionEnum | str = Field(
        description="EXECUTE_BUY | EXECUTE_SELL | PASS | UPDATE_RISK_PARAMS"
    )
    confidence_score: Annotated[float, Field(ge=0.0, le=1.0, description="Confidence score from 0.00 to 1.00")]
    action_parameters: ActionParameters
    wallet_audit: WalletAudit
    security_assessment: SecurityAssessment
    feedback_tuning: FeedbackTuning

    def to_strict_json(self) -> str:
        """Serialize into exact strict JSON string required by prompt specification."""
        return json.dumps(self.model_dump(mode="json"), indent=2)

    @classmethod
    def from_strict_json(cls, json_str: str) -> "AISupervisorResponse":
        """Parse from raw JSON string, stripping any accidental markdown fences or enclosing commentary."""
        clean = json_str.strip()
        start = clean.find("{")
        end = clean.rfind("}")
        if start != -1 and end != -1 and end > start:
            clean = clean[start : end + 1]
        data = json.loads(clean)
        return cls.model_validate(data)


class AuditedTokenOutcome(BaseModel):
    """Tracks token audit decision and subsequent on-chain price evolution for self-learning."""

    model_config = ConfigDict(frozen=False, extra="forbid")

    token_address: str = Field(description="Audited token mint or address")
    chain: str = Field(description="Chain identifier string")
    decision: str = Field(description="EXECUTE_BUY | PASS")
    initial_price: Decimal = Field(description="Spot price at the moment of audit")
    audit_timestamp: float = Field(description="Timestamp in seconds when audit occurred")
    flags: list[str] = Field(default_factory=list, description="Rejection flags or audit flags")
    confidence: float = Field(default=0.90, description="Confidence score emitted at audit")
    peak_price: Decimal = Field(default=Decimal("0"), description="Highest observed price post-audit")
    min_price: Decimal = Field(default=Decimal("0"), description="Lowest observed price post-audit")
    latest_price: Decimal = Field(default=Decimal("0"), description="Most recent price update")
    peak_multiplier: float = Field(default=1.0, description="Highest multiplier reached (peak / initial)")
    max_drawdown_pct: float = Field(default=0.0, description="Maximum drawdown percentage ((initial - min) / initial * 100)")
    is_finalized: bool = Field(default=False, description="True if outcome observation window has matured")
    outcome_label: Optional[str] = Field(default=None, description="TRUE_POSITIVE | FALSE_POSITIVE | TRUE_NEGATIVE | FALSE_NEGATIVE")


class OutcomeTrackerMetrics(BaseModel):
    """Aggregate telemetry from the self-learning outcome tracking loop."""

    model_config = _STRICT_MODEL_CFG

    total_tracked: int = 0
    finalized_count: int = 0
    true_positives: int = 0
    false_positives: int = 0
    true_negatives: int = 0
    false_negatives: int = 0
    accuracy_pct: float = 100.0
    win_rate_pct: float = 0.0
    veto_efficiency_pct: float = 100.0
    false_negative_rate_pct: float = 0.0
    false_positive_rate_pct: float = 0.0
    calibration_status: str = "BALANCED"

