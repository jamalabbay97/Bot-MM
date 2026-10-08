"""
alpha_engine.models.decisions — Structured Decision Audit Trail & Pattern Store Models
=====================================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2 | Immutable Audit Logging
"""

from __future__ import annotations

import math
import time
import uuid
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    OrderSide,
    SignalStrength,
    StrategyHorizon,
)


class DecisionType(str, Enum):
    """Categorical evaluation outcome for trade signals and positions."""

    ENTER = "ENTER"
    SKIP = "SKIP"
    EXIT = "EXIT"
    TRAIL_STOP = "TRAIL_STOP"


class PatternFeatureVector(BaseModel):
    """
    Normalized multi-dimensional feature vector of a token during consolidation
    and breakout. Used in the vectorized pattern store for pattern matching and
    hyperparameter tuning.
    """

    model_config = _STRICT_MODEL_CFG

    token_address: str
    consolidation_duration_s: float = Field(ge=0.0, description="Duration of price consolidation in seconds")
    dip_depth_pct: float = Field(ge=0.0, le=100.0, description="Drawdown percentage from initial peak/mint to trough")
    volume_surge_multiplier: float = Field(ge=0.0, description="Volume 5m over SMA 15m multiplier")
    net_buy_delta: float = Field(ge=0.0, le=1.0, description="Ratio of buy volume to total volume (0.0 to 1.0)")
    top10_concentration: float = Field(ge=0.0, le=1.0, description="Top-10 non-pool holder concentration")
    liquidity_to_mc_ratio: float = Field(ge=0.0, description="Pool liquidity depth relative to market cap")
    smart_wallet_inflows: float = Field(ge=0.0, description="Cumulative smart wallet native inflows")
    peak_gain_multiplier: float = Field(default=1.0, ge=0.0, description="Realized multiplier peak post-breakout")
    timestamp_ns: int = Field(default_factory=time.time_ns)

    def to_vector(self) -> list[float]:
        """Convert key numerical features into a normalized float vector."""
        # Normalization scalers to balance magnitude across dimensions
        norm_consolidation = min(1.0, self.consolidation_duration_s / 7200.0)  # 0 to 2h
        norm_dip = self.dip_depth_pct / 100.0                                 # 0 to 1
        norm_surge = min(1.0, self.volume_surge_multiplier / 10.0)            # 0 to 10x
        norm_delta = self.net_buy_delta                                        # 0 to 1
        norm_conc = self.top10_concentration                                   # 0 to 1
        norm_liq = min(1.0, self.liquidity_to_mc_ratio / 0.50)                 # 0 to 50%
        norm_smart = min(1.0, self.smart_wallet_inflows / 50.0)                # 0 to 50 SOL
        return [
            norm_consolidation,
            norm_dip,
            norm_surge,
            norm_delta,
            norm_conc,
            norm_liq,
            norm_smart,
        ]

    @classmethod
    def from_signal(
        cls,
        signal: Any,
        sec: Optional[Any] = None,
        wallet_profile: Optional[Any] = None,
    ) -> "PatternFeatureVector":
        """
        Safely construct a PatternFeatureVector from a SignalEvent and optional SecurityReport.
        Guards against AttributeError if sec is None or lacks attributes.
        """
        token_address = getattr(signal, "token_address", str(signal))
        top10 = getattr(sec, "top10_concentration", 0.15) if sec is not None else 0.15
        if top10 is None:
            top10 = 0.15
        top10_val = max(0.0, min(1.0, float(top10)))

        smart_inflows = 5.0
        if wallet_profile is not None:
            try:
                smart_inflows = float(getattr(wallet_profile, "total_pnl_usd", 10.0)) / 1000.0
            except (TypeError, ValueError):
                smart_inflows = 5.0

        cons_dur = getattr(signal, "consolidation_duration_s", None)
        if cons_dur is None:
            cons_dur = 1800.0
        dip_pct = getattr(signal, "dip_depth_pct", None)
        if dip_pct is None:
            dip_pct = 40.0
        surge_mult = getattr(signal, "volume_surge_multiplier", None)
        if surge_mult is None:
            surge_mult = 2.5
        net_delta = getattr(signal, "net_buy_delta", None)
        if net_delta is None:
            net_delta = 0.70
        liq_ratio = getattr(signal, "liquidity_to_mc_ratio", None)
        if liq_ratio is None:
            liq_ratio = 0.20
        peak_gain = getattr(signal, "peak_gain_multiplier", None)
        if peak_gain is None:
            peak_gain = 1.0

        return cls(
            token_address=str(token_address),
            consolidation_duration_s=float(cons_dur),
            dip_depth_pct=float(dip_pct),
            volume_surge_multiplier=float(surge_mult),
            net_buy_delta=float(net_delta),
            top10_concentration=top10_val,
            liquidity_to_mc_ratio=float(liq_ratio),
            smart_wallet_inflows=max(0.0, float(smart_inflows)),
            peak_gain_multiplier=float(peak_gain),
        )

    def cosine_similarity(self, other: "PatternFeatureVector") -> float:
        """Calculate cosine similarity [0.0, 1.0] against another feature vector."""
        v1 = self.to_vector()
        v2 = other.to_vector()
        dot_product = sum(a * b for a, b in zip(v1, v2))
        norm_a = math.sqrt(sum(a * a for a in v1))
        norm_b = math.sqrt(sum(b * b for b in v2))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        similarity = dot_product / (norm_a * norm_b)
        return max(0.0, min(1.0, similarity))



class DecisionRecord(BaseModel):
    """
    Immutable structured decision record capturing the exact system state,
    active rule triggers, quantitative metrics, and explicit mathematical
    rationale for every single evaluation event.
    """

    model_config = _STRICT_MODEL_CFG

    decision_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: int = Field(default_factory=time.time_ns)
    token_address: str
    chain: str = "solana_mainnet"
    decision_type: DecisionType
    strategy_pattern: str = "wave2_breakout"
    token_symbol: Optional[str] = None
    signal_id: Optional[str] = None
    market_cap_usd: Optional[float] = None
    volume_5m_usd: Optional[float] = None
    volume_1h_usd: Optional[float] = None
    liquidity_pool_depth_usd: Optional[float] = None
    liquidity_usd: Optional[float] = None
    active_rules: dict[str, Any] = Field(default_factory=dict)
    rule_triggers: dict[str, Any] = Field(default_factory=dict)
    confidence_score: float = 0.0
    pattern_match_score: Optional[float] = None
    reason: str = Field(description="Human-readable explicit rationale explaining exact mathematical condition")
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: int = Field(default_factory=lambda: int(time.time()))

    @model_validator(mode="before")
    @classmethod
    def _normalize_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Normalize liquidity
            if "liquidity_usd" in data and "liquidity_pool_depth_usd" not in data:
                data["liquidity_pool_depth_usd"] = data["liquidity_usd"]
            elif "liquidity_pool_depth_usd" in data and "liquidity_usd" not in data:
                data["liquidity_usd"] = data["liquidity_pool_depth_usd"]

            # Normalize rules
            if "rule_triggers" in data and "active_rules" not in data:
                data["active_rules"] = data["rule_triggers"]
            elif "active_rules" in data and "rule_triggers" not in data:
                data["rule_triggers"] = data["active_rules"]

            # Normalize chain if enum passed
            if hasattr(data.get("chain"), "value"):
                data["chain"] = data["chain"].value
        return data


class RevivalPatternFeatureVector(BaseModel):
    """
    Closed-loop feature storage vector for Revival & CTO Breakout swing trades.
    Recorded upon position closure to train the adaptive tuner.
    """

    model_config = _STRICT_MODEL_CFG

    token_address: str
    token_age_hours: float = Field(ge=0.0, description="Age of token in hours since creation")
    consolidation_length_hours: float = Field(ge=0.0, description="Length of dormant consolidation in hours")
    base_mcap_usd: float = Field(ge=0.0, description="Market cap at the consolidation floor in USD")
    volume_surge_multiplier: float = Field(ge=0.0, description="Breakout volume multiplier over SMA 1h")
    net_buy_ratio: float = Field(ge=0.0, le=1.0, description="Organic net buy volume ratio")
    peak_roi_pct: float = Field(description="Highest peak ROI reached since entry in percent")
    realized_pnl_pct: float = Field(description="Final realized PnL in percent")
    timestamp_ns: int = Field(default_factory=time.time_ns)

    def to_vector(self) -> list[float]:
        """Convert revival features into normalized vector."""
        norm_age = min(1.0, self.token_age_hours / 240.0)             # 0 to 10 days
        norm_cons = min(1.0, self.consolidation_length_hours / 72.0)  # 0 to 3 days
        norm_mcap = min(1.0, self.base_mcap_usd / 200_000.0)         # 0 to $200k base
        norm_surge = min(1.0, self.volume_surge_multiplier / 10.0)    # 0 to 10x
        norm_delta = self.net_buy_ratio                               # 0 to 1
        norm_peak = min(1.0, max(0.0, self.peak_roi_pct / 800.0))    # 0 to +800%
        return [norm_age, norm_cons, norm_mcap, norm_surge, norm_delta, norm_peak]


class DecisionSignal(BaseModel):
    """
    Comprehensive multi-chain autonomous decision signal for dual-horizon execution.
    Encapsulates quantitative metrics, horizon, execution venue, confidence bounds,
    smart-money clustering, and estimated market impact.
    """

    model_config = _STRICT_MODEL_CFG

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: int = Field(default_factory=time.time_ns)
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET
    token_address: str
    pool_address: str
    strategy_horizon: StrategyHorizon = StrategyHorizon.SHORT_TERM_SCALP
    execution_venue: ExecutionVenue = ExecutionVenue.RAYDIUM_AMM
    suggested_side: OrderSide = OrderSide.BUY
    signal_strength: SignalStrength = SignalStrength.STRONG
    confidence_interval: tuple[float, float] = (0.50, 0.95)
    trigger_reason: str = Field(description="Explicit mathematical or anomaly trigger reason")
    smart_money_wallet_cluster: list[str] = Field(default_factory=list)
    estimated_price_impact: float = Field(default=0.0, ge=0.0)
    alpha_score: float = Field(default=0.50, ge=0.0, le=1.0)
    pool_state: Optional[Any] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def strength(self) -> SignalStrength:
        return self.signal_strength

    def to_signal_event(self, pool_state: Any, security_report: Any) -> Any:
        from alpha_engine.models.enums import ExitProfile, SignalSource
        from alpha_engine.models.events import SignalEvent

        return SignalEvent(
            signal_id=self.signal_id,
            timestamp_ns=self.timestamp_ns,
            chain=self.chain,
            pool_address=self.pool_address,
            token_address=self.token_address,
            suggested_side=self.suggested_side,
            pool_state=pool_state,
            security_report=security_report,
            strength=self.signal_strength,
            alpha_score=self.alpha_score,
            source=SignalSource.DEX_SWAP,
            execution_venue=self.execution_venue,
            exit_profile=ExitProfile.FAST_SNIPE,
            strategy_pattern="dex_scalp",
        )



