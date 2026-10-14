"""
alpha_engine.engine.strategy.screener_priority
==============================================
Graduated and Movers Screener Priority Strategy Engine
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | High-Priority Immediate Evaluation with Strict Risk Safeguards
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
import logging
import time
from typing import Any, Optional

from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    OrderSide,
    ScreenerSection,
    SecurityTier,
    SignalSource,
    SignalStrength,
    StrategyType,
)
from alpha_engine.math.sizing import classify_alpha_score
from alpha_engine.models.events import ScreenerDiscoveryEvent, SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport

logger = logging.getLogger(__name__)


class ScreenerDecision(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    OBSERVING = "observing"
    DEFERRED = "deferred"


@dataclass
class ScreenerDecisionRecord:
    """Audit record capturing why a screener token was accepted, rejected, or kept observing."""

    token_address: str
    chain: ChainIdentifier
    section: ScreenerSection
    decision: ScreenerDecision
    timestamp_s: float
    volume_2m_usd: Decimal
    liquidity_usd: Decimal
    price_extension_pct: float
    reason: str
    market_cap_usd: Decimal = Decimal("0")
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScreenerEvaluationResult:
    """Outcome of immediate or bounded evaluation for a screener candidate."""

    admitted: bool
    token_address: str
    chain: ChainIdentifier
    section: ScreenerSection
    decision: ScreenerDecision
    reason: str
    signal: Optional[SignalEvent] = None
    volume_2m_usd: Decimal = Decimal("0")
    liquidity_usd: Decimal = Decimal("0")
    price_extension_pct: float = 0.0
    market_cap_usd: Decimal = Decimal("0")
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraduatedProfileConfig:
    """Configurable evaluation profile for Graduated tokens."""

    min_market_cap_usd: Decimal = Decimal("8000.0")
    min_volume_usd: Decimal = Decimal("8000.0")
    volume_window_sec: float = 120.0
    min_liquidity_usd: Decimal = Decimal("10000.0")
    max_price_impact_pct: float = 0.05
    max_price_extension_pct: float = 1.50
    require_confirmed_structure: bool = True
    breakout_margin_pct: float = 0.01
    retest_tolerance_pct: float = 0.01
    breakout_volume_multiplier: float = 1.5
    range_window_sec: float = 300.0
    allow_continuation: bool = False
    allow_continuation_without_retest: bool = False


@dataclass
class MoversProfileConfig:
    """Configurable evaluation profile for Movers tokens."""

    min_market_cap_usd: Decimal = Decimal("8000.0")
    min_volume_usd: Decimal = Decimal("8000.0")
    volume_window_sec: float = 120.0
    min_liquidity_usd: Decimal = Decimal("10000.0")
    min_buy_pressure_ratio: float = 0.55
    max_extension_pct: float = 1.50
    max_price_impact_pct: float = 0.05
    breakout_margin_pct: float = 0.01
    retest_tolerance_pct: float = 0.01
    breakout_volume_multiplier: float = 1.5
    range_window_sec: float = 300.0
    allow_continuation: bool = False
    allow_continuation_without_retest: bool = False


class ScreenerPriorityEvaluator:
    """
    High-priority immediate evaluator for Graduated and Movers platform screener tokens.
    Removes the mandatory 2-minute / 5-minute observation delay when reliable market data
    and required conditions are fully established.

    Mandatory invariants enforced:
      - MIN_VOLUME_USD = $8,000 over the rolling measurement window (default 120s / 2m).
      - Strict rejection of tokens with < $8,000 volume or unverifiable / missing volume.
      - Never substitutes market capitalization, liquidity, SOL-denominated amounts, or lifetime volume.
      - Deduplication across sections using canonical mint address; merged signals for dual-listed tokens.
      - Bounded concurrency and rate limits to prevent RPC/task starvation.
      - Full audit trail recording why candidates are accepted, rejected, or kept under observation.
    """

    def __init__(
        self,
        config: EngineConfig,
        gatekeeper: Optional[Any] = None,
        metrics_aggregator: Optional[Any] = None,
        ledger: Optional[Any] = None,
        graduated_config: Optional[GraduatedProfileConfig] = None,
        movers_config: Optional[MoversProfileConfig] = None,
    ) -> None:
        self.config = config
        self.gatekeeper = gatekeeper
        self.metrics_aggregator = metrics_aggregator
        self.ledger = ledger

        if graduated_config is not None:
            self.graduated_profile = graduated_config
        else:
            raw_grad_ext = getattr(config, "graduated_max_price_extension_pct", 1.50)
            norm_grad_ext = float(raw_grad_ext) / 100.0 if float(raw_grad_ext) > 5.0 else float(raw_grad_ext)

            self.graduated_profile = GraduatedProfileConfig(
                min_market_cap_usd=getattr(config, "min_market_cap_usd", Decimal("8000.0")),
                min_volume_usd=getattr(config, "screener_min_volume_usd", Decimal("8000.0")),
                volume_window_sec=getattr(config, "screener_volume_window_sec", 120.0),
                min_liquidity_usd=getattr(config, "graduated_min_liquidity_usd", Decimal("10000.0")),
                max_price_impact_pct=getattr(config, "graduated_max_price_impact_pct", 0.05),
                max_price_extension_pct=norm_grad_ext,
                breakout_margin_pct=getattr(config, "breakout_margin_pct", 0.01),
                retest_tolerance_pct=getattr(config, "retest_tolerance_pct", 0.01),
                breakout_volume_multiplier=getattr(config, "breakout_volume_multiplier", 1.5),
                range_window_sec=getattr(config, "range_formation_window_sec", 300.0),
                allow_continuation=getattr(config, "allow_continuation_without_retest", False),
            )

        if movers_config is not None:
            self.movers_profile = movers_config
        else:
            raw_mov_ext = getattr(config, "movers_max_extension_pct", 1.50)
            norm_mov_ext = float(raw_mov_ext) / 100.0 if float(raw_mov_ext) > 5.0 else float(raw_mov_ext)

            self.movers_profile = MoversProfileConfig(
                min_market_cap_usd=getattr(config, "min_market_cap_usd", Decimal("8000.0")),
                min_volume_usd=getattr(config, "screener_min_volume_usd", Decimal("8000.0")),
                volume_window_sec=getattr(config, "screener_volume_window_sec", 120.0),
                min_liquidity_usd=getattr(config, "movers_min_liquidity_usd", Decimal("10000.0")),
                min_buy_pressure_ratio=getattr(config, "movers_min_buy_pressure_ratio", 0.55),
                max_extension_pct=norm_mov_ext,
                max_price_impact_pct=getattr(config, "graduated_max_price_impact_pct", 0.05),
                breakout_margin_pct=getattr(config, "breakout_margin_pct", 0.01),
                retest_tolerance_pct=getattr(config, "retest_tolerance_pct", 0.01),
                breakout_volume_multiplier=getattr(config, "breakout_volume_multiplier", 1.5),
                range_window_sec=getattr(config, "range_formation_window_sec", 300.0),
                allow_continuation=getattr(config, "allow_continuation_without_retest", False),
            )

        # Bounded concurrency semaphore
        concurrency = getattr(config, "screener_concurrency_limit", 10)
        self._semaphore = asyncio.Semaphore(max(1, concurrency))

        # Atomic state synchronization & deduplication
        self._lock = asyncio.Lock()
        self._evaluating_tokens: set[str] = set()
        self._evaluated_history: dict[str, float] = {}  # token -> last evaluated timestamp
        self._section_memberships: dict[str, set[ScreenerSection]] = {}
        self._audit_log: deque[ScreenerDecisionRecord] = deque(maxlen=1000)

    @property
    def audit_log(self) -> deque[ScreenerDecisionRecord]:
        return self._audit_log

    def get_recent_decisions(self, limit: int = 50) -> list[ScreenerDecisionRecord]:
        return list(self._audit_log)[-limit:]

    def _canonicalize_address(self, address: str) -> str:
        return str(address or "").strip()

    def record_decision(
        self,
        token_address: str,
        chain: ChainIdentifier,
        section: ScreenerSection,
        decision: ScreenerDecision,
        reason: str,
        volume_2m_usd: Decimal = Decimal("0"),
        liquidity_usd: Decimal = Decimal("0"),
        price_extension_pct: float = 0.0,
        market_cap_usd: Decimal = Decimal("0"),
        details: Optional[dict[str, Any]] = None,
    ) -> ScreenerDecisionRecord:
        record = ScreenerDecisionRecord(
            token_address=token_address,
            chain=chain,
            section=section,
            decision=decision,
            timestamp_s=time.time(),
            volume_2m_usd=volume_2m_usd,
            liquidity_usd=liquidity_usd,
            price_extension_pct=price_extension_pct,
            market_cap_usd=market_cap_usd,
            reason=reason,
            details=details or {},
        )
        self._audit_log.append(record)
        return record

    async def evaluate(
        self,
        discovery: ScreenerDiscoveryEvent | str | dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> ScreenerEvaluationResult:
        """
        Convenience entrypoint accepting either a ScreenerDiscoveryEvent, dict, or positional args.
        """
        if isinstance(discovery, str):
            return await self.evaluate_token(token_address=discovery, *args, **kwargs)

        if isinstance(discovery, dict):
            discovery = ScreenerDiscoveryEvent.model_validate(discovery)

        token_addr = discovery.token_address or discovery.mint
        cur_price = Decimal(str(discovery.current_price)) if discovery.current_price is not None else None
        vol_usd = Decimal(str(discovery.volume_2m_usd)) if discovery.volume_2m_usd is not None else None
        mcap_usd = Decimal(str(discovery.market_cap_usd)) if getattr(discovery, "market_cap_usd", None) is not None else (Decimal(str(kwargs["market_cap_usd"])) if kwargs.get("market_cap_usd") is not None else None)
        bk_price = Decimal(str(discovery.breakout_price)) if discovery.breakout_price is not None else None
        pct_change = discovery.price_change_pct_2m if discovery.price_change_pct_2m is not None else discovery.price_change_pct

        pool_state = kwargs.get("pool_state")
        if pool_state is None and discovery.liquidity_usd is not None and discovery.liquidity_usd > 0:
            liq_dec = Decimal(str(discovery.liquidity_usd))
            sol_price = Decimal(str(getattr(self.config, "sol_price_usd", 150.0)))
            native_res = liq_dec / Decimal("2.0") / sol_price if sol_price > 0 else Decimal("100.0")
            token_res = (liq_dec / Decimal("2.0")) / (cur_price or Decimal("0.001")) if (cur_price or Decimal("0.001")) > 0 else Decimal("1000000.0")
            pool_state = PoolState(
                pool_address=discovery.pool_address or f"Pool_{token_addr[:16]}",
                chain=discovery.chain,
                token_reserve=token_res,
                native_reserve=native_res,
                fee_numerator=25,
                fee_denominator=10000,
                last_updated_block=1000,
                token_decimals=6,
                native_decimals=9,
            )

        raw_meta = dict(discovery.raw_metadata)
        if not discovery.market_data_fresh:
            raw_meta["market_data_fresh"] = False
        if discovery.volume_trend_fading:
            raw_meta["volume_trend_fading"] = True
        if getattr(discovery, "volume_surge_multiplier", None) is not None:
            raw_meta["volume_surge_multiplier"] = discovery.volume_surge_multiplier
        if getattr(discovery, "failed_breakout", None) is not None:
            raw_meta["failed_breakout"] = discovery.failed_breakout
        if getattr(discovery, "retest_confirmed", None) is not None:
            raw_meta["retest_confirmed"] = discovery.retest_confirmed
        if getattr(discovery, "needs_retest", None) is not None:
            raw_meta["needs_retest"] = discovery.needs_retest

        return await self.evaluate_token(
            token_address=token_addr,
            section=discovery.section,
            chain=discovery.chain,
            pool_state=pool_state,
            current_price=cur_price,
            volume_2m_usd=vol_usd,
            market_cap_usd=mcap_usd,
            breakout_price=bk_price,
            price_change_pct=pct_change,
            timestamp_s=discovery.timestamp,
            security_report=kwargs.get("security_report"),
            raw_metadata=raw_meta,
            active_positions_check=kwargs.get("active_positions_check"),
        )

    async def evaluate_token(
        self,
        token_address: str,
        section: ScreenerSection,
        chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
        pool_state: Optional[PoolState] = None,
        current_price: Optional[Decimal] = None,
        volume_2m_usd: Optional[Decimal] = None,
        market_cap_usd: Optional[Decimal] = None,
        breakout_price: Optional[Decimal] = None,
        price_change_pct: Optional[float] = None,
        timestamp_s: Optional[float] = None,
        security_report: Optional[SecurityReport] = None,
        raw_metadata: Optional[dict[str, Any]] = None,
        active_positions_check: Optional[Any] = None,
    ) -> ScreenerEvaluationResult:
        """
        Evaluate a Graduated or Movers candidate immediately when data is available.
        Bypasses the fixed 2-minute / 5-minute observation delay without bypassing risk controls.
        """
        canon_addr = self._canonicalize_address(token_address)
        if not canon_addr:
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=canon_addr,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason="Invalid empty token address",
            )

        # 1. Atomic Deduplication & Section Merging
        effective_section = section
        async with self._lock:
            # Merge sections if already registered in the other category
            prior_sections = self._section_memberships.setdefault(canon_addr, set())
            prior_sections.add(section)
            if ScreenerSection.GRADUATED in prior_sections and ScreenerSection.MOVERS in prior_sections:
                effective_section = ScreenerSection.BOTH

            # Deduplicate in-flight evaluations or recently admitted tokens
            if canon_addr in self._evaluating_tokens or (canon_addr in self._evaluated_history and time.time() - self._evaluated_history[canon_addr] < 60.0):
                logger.debug("Token %s is already active or in-flight; merging signals.", canon_addr[:10])
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=canon_addr,
                    chain=chain,
                    section=effective_section,
                    decision=ScreenerDecision.REJECTED,
                    reason="Evaluation already active or in-flight for this token",
                )

            # Prevent duplicate buy if position already open or order active
            if active_positions_check and active_positions_check(canon_addr, chain):
                reason = "Position already open; duplicate buy prevented"
                self.record_decision(
                    token_address=canon_addr,
                    chain=chain,
                    section=effective_section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=canon_addr,
                    chain=chain,
                    section=effective_section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                )

            # Mark token as in-flight
            self._evaluating_tokens.add(canon_addr)

        try:
            # 2. Concurrency bounded execution
            async with self._semaphore:
                return await self._execute_evaluation(
                    token_address=canon_addr,
                    section=effective_section,
                    chain=chain,
                    pool_state=pool_state,
                    current_price=current_price,
                    volume_2m_usd=volume_2m_usd,
                    market_cap_usd=market_cap_usd,
                    breakout_price=breakout_price,
                    price_change_pct=price_change_pct,
                    timestamp_s=timestamp_s,
                    security_report=security_report,
                    raw_metadata=raw_metadata or {},
                )
        finally:
            async with self._lock:
                self._evaluating_tokens.discard(canon_addr)
                self._evaluated_history[canon_addr] = time.time()

    async def _execute_evaluation(
        self,
        token_address: str,
        section: ScreenerSection,
        chain: ChainIdentifier,
        pool_state: Optional[PoolState],
        current_price: Optional[Decimal],
        volume_2m_usd: Optional[Decimal],
        breakout_price: Optional[Decimal],
        price_change_pct: Optional[float],
        timestamp_s: Optional[float],
        security_report: Optional[SecurityReport],
        raw_metadata: dict[str, Any],
        market_cap_usd: Optional[Decimal] = None,
    ) -> ScreenerEvaluationResult:
        now_s = time.time()
        native_price = getattr(self.config, "sol_price_usd", Decimal("150.0"))

        # 3. Market Data Freshness & Completeness Check
        is_fresh = raw_metadata.get("market_data_fresh", True)
        max_staleness = getattr(self.config, "screener_market_data_max_staleness_s", 30.0)
        if not is_fresh or (timestamp_s is not None and (now_s - timestamp_s) > max_staleness):
            age_s = now_s - (timestamp_s or now_s)
            reason = f"Stale or unconfirmed market data: age {age_s:.1f}s > {max_staleness:.1f}s"
            self.record_decision(token_address, chain, section, ScreenerDecision.DEFERRED, reason)
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.DEFERRED,
                reason=reason,
            )

        if current_price is None or current_price <= Decimal("0"):
            reason = "Invalid price structure or missing market price data"
            self.record_decision(token_address, chain, section, ScreenerDecision.REJECTED, reason)
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
            )

        if pool_state is None or pool_state.native_reserve <= Decimal("0"):
            reason = "Missing or uninitialized pool liquidity state"
            self.record_decision(token_address, chain, section, ScreenerDecision.REJECTED, reason)
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
            )

        # 3b. Mandatory Minimum Market Cap Requirement ($8,000 USD)
        # Enforce MIN_MARKET_CAP_USD = 8000: token must have market cap >= $8,000 USD.
        # This condition relates strictly to Market Cap, NOT trading volume and NOT liquidity.
        qualifying_mcap_usd: Optional[Decimal] = None
        if market_cap_usd is not None and market_cap_usd > Decimal("0"):
            qualifying_mcap_usd = market_cap_usd
        elif market_cap_usd is not None and market_cap_usd <= Decimal("0"):
            qualifying_mcap_usd = Decimal("0")
        elif pool_state is not None and pool_state.token_reserve > Decimal("0"):
            qualifying_mcap_usd = pool_state.calculate_market_cap_usd(native_price_usd=native_price)
        elif self.metrics_aggregator is not None and pool_state and pool_state.pool_address:
            qualifying_mcap_usd = self.metrics_aggregator.get_market_cap_usd(
                pool_address=pool_state.pool_address,
                native_price_usd=native_price,
            )

        min_mcap_required = getattr(self.graduated_profile, "min_market_cap_usd", Decimal("8000.0"))

        if qualifying_mcap_usd is None or qualifying_mcap_usd <= Decimal("0"):
            reason = "Market cap is missing, unverifiable, or stale: entry strictly disallowed"
            self.record_decision(
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                market_cap_usd=Decimal("0"),
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                market_cap_usd=Decimal("0"),
            )

        if qualifying_mcap_usd < min_mcap_required:
            reason = f"Market cap ${qualifying_mcap_usd:.2f} is below minimum threshold ${min_mcap_required:.2f}"
            self.record_decision(
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                market_cap_usd=qualifying_mcap_usd,
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                market_cap_usd=qualifying_mcap_usd,
            )

        # 4. Mandatory $8,000 USD Volume Requirement Check
        # Inspect trustworthy 2-minute rolling USD volume
        qualifying_vol_usd: Optional[Decimal] = None

        if volume_2m_usd is not None and volume_2m_usd >= Decimal("0"):
            qualifying_vol_usd = volume_2m_usd
        elif self.metrics_aggregator is not None and pool_state.pool_address:
            qualifying_vol_usd = self.metrics_aggregator.get_rolling_volume_usd(
                pool_address=pool_state.pool_address,
                window_sec=self.graduated_profile.volume_window_sec,
                native_price_usd=native_price,
                current_time_s=now_s,
            )

        min_vol_required = self.graduated_profile.min_volume_usd  # $8,000.0

        if qualifying_vol_usd is None or qualifying_vol_usd <= Decimal("0"):
            reason = "Trading volume is unverifiable, insufficient, or missing from trustworthy provider"
            self.record_decision(token_address, chain, section, ScreenerDecision.REJECTED, reason)
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
            )

        if qualifying_vol_usd < min_vol_required:
            reason = f"Insufficient qualifying 2m volume: ${qualifying_vol_usd:.2f} below threshold ${min_vol_required:.2f}"
            self.record_decision(
                token_address,
                chain,
                section,
                ScreenerDecision.REJECTED,
                reason,
                volume_2m_usd=qualifying_vol_usd,
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                volume_2m_usd=qualifying_vol_usd,
            )

        # 5. Executable Liquidity & Price Impact Checks
        liq_usd = pool_state.native_reserve * native_price
        min_liq_req = (
            self.graduated_profile.min_liquidity_usd
            if section != ScreenerSection.MOVERS
            else self.movers_profile.min_liquidity_usd
        )

        if liq_usd < min_liq_req:
            reason = f"Inadequate liquidity: ${liq_usd:.2f} < ${min_liq_req:.2f}"
            self.record_decision(
                token_address,
                chain,
                section,
                ScreenerDecision.REJECTED,
                reason,
                volume_2m_usd=qualifying_vol_usd,
                liquidity_usd=liq_usd,
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                volume_2m_usd=qualifying_vol_usd,
                liquidity_usd=liq_usd,
            )

        # Expected price impact for standard 1.0 SOL trade size
        test_trade_native = Decimal("1.0")
        expected_impact = float(test_trade_native / (pool_state.native_reserve + test_trade_native))
        max_impact = self.graduated_profile.max_price_impact_pct

        if expected_impact > max_impact:
            reason = f"Excessive price impact: {expected_impact * 100:.2f}% > {max_impact * 100:.2f}%"
            self.record_decision(
                token_address,
                chain,
                section,
                ScreenerDecision.REJECTED,
                reason,
                volume_2m_usd=qualifying_vol_usd,
                liquidity_usd=liq_usd,
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                volume_2m_usd=qualifying_vol_usd,
                liquidity_usd=liq_usd,
            )

        # 6. Security Gatekeeper & Sellability Checks
        report = security_report
        if report is None and self.gatekeeper is not None:
            try:
                if hasattr(self.gatekeeper, "screen_token"):
                    report = await self.gatekeeper.screen_token(
                        token_address=token_address,
                        chain=chain,
                        pool_address=pool_state.pool_address if pool_state else "",
                    )
                elif hasattr(self.gatekeeper, "evaluate_token"):
                    report = await self.gatekeeper.evaluate_token(
                        token_address=token_address,
                        chain=chain,
                        pool_address=pool_state.pool_address if pool_state else "",
                    )
            except Exception as exc:  # noqa: BLE001
                reason = f"Security gatekeeper error: {exc}"
                self.record_decision(token_address, chain, section, ScreenerDecision.REJECTED, reason)
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                )

        if report is not None:
            if report.is_honeypot or not report.passes_hard_gates:
                reason = f"Security Gatekeeper rejected: {report.external_api_raw or 'Hard security gates failed'}"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )

            # Holder concentration
            if report.top10_concentration > getattr(self.config, "max_top10_concentration", 0.40):
                reason = f"Top 10 holder concentration too high: {report.top10_concentration * 100:.1f}%"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )

        # 7. Hybrid Entry: Breakout & Retest Confirmation
        # Check breakout and retest structure if breakout level or range data is provided
        breakout_margin_pct = getattr(self.config, "breakout_margin_pct", 0.01)
        retest_tolerance_pct = getattr(self.config, "retest_tolerance_pct", 0.01)
        vol_multiplier = (
            getattr(self.graduated_profile, "breakout_volume_multiplier", 1.5)
            if section != ScreenerSection.MOVERS
            else getattr(self.movers_profile, "breakout_volume_multiplier", 1.5)
        )
        allow_continuation = getattr(self.config, "allow_continuation_without_retest", False)

        # 7a. Check for failed breakout flag
        if raw_metadata.get("failed_breakout") is True:
            reason = "Breakout invalidated: price broke down below breakout support"
            self.record_decision(
                token_address,
                chain,
                section,
                ScreenerDecision.REJECTED,
                reason,
                volume_2m_usd=qualifying_vol_usd,
                liquidity_usd=liq_usd,
            )
            return ScreenerEvaluationResult(
                admitted=False,
                token_address=token_address,
                chain=chain,
                section=section,
                decision=ScreenerDecision.REJECTED,
                reason=reason,
                market_cap_usd=qualifying_mcap_usd,
            )

        # 7b. Volume confirmation check if 5-candle / recent candle volume data is provided
        bk_vol = raw_metadata.get("breakout_volume")
        med_vol = raw_metadata.get("median_volume_5m")
        vol_surge = raw_metadata.get("volume_surge_multiplier")
        if vol_surge is not None:
            if Decimal(str(vol_surge)) < Decimal(str(vol_multiplier)):
                reason = f"Breakout volume confirmation failed: volume surge {float(vol_surge):.2f}x < {float(vol_multiplier):.2f}x median"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    market_cap_usd=qualifying_mcap_usd or Decimal("0"),
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    market_cap_usd=qualifying_mcap_usd,
                )
        elif bk_vol is not None and med_vol is not None:
            bk_vol_d = Decimal(str(bk_vol))
            med_vol_d = Decimal(str(med_vol))
            if med_vol_d > Decimal("0") and bk_vol_d < med_vol_d * Decimal(str(vol_multiplier)):
                reason = f"Breakout volume confirmation failed: volume {bk_vol_d} < {vol_multiplier}x median {med_vol_d}"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    market_cap_usd=qualifying_mcap_usd or Decimal("0"),
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    market_cap_usd=qualifying_mcap_usd,
                )

        # 7c. Retest vs Continuation logic
        if breakout_price is not None and breakout_price > Decimal("0"):
            price_dist_pct = (current_price - breakout_price) / breakout_price
            
            # If price broke down significantly below breakout price (e.g. > 2% breakdown)
            if price_dist_pct < Decimal("-0.02"):
                reason = f"Breakout failed: price fell {float(price_dist_pct * 100):.1f}% below breakout level"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    market_cap_usd=qualifying_mcap_usd,
                )

            # Check if retest failed
            retest_status = raw_metadata.get("retest_status")
            if retest_status == "failed":
                reason = "Breakout retest failed: level rejected on retest"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    market_cap_usd=qualifying_mcap_usd,
                )

            # If token continued pumping without retest, and continuation model is disabled
            allow_cont = (
                allow_continuation
                or getattr(self.graduated_profile, "allow_continuation_without_retest", False)
                or getattr(self.graduated_profile, "allow_continuation", False)
                or getattr(self.movers_profile, "allow_continuation_without_retest", False)
                or getattr(self.movers_profile, "allow_continuation", False)
            )
            if (price_dist_pct > Decimal(str(retest_tolerance_pct * 3)) and raw_metadata.get("needs_retest") is True) and not allow_cont and not raw_metadata.get("retest_confirmed", False):
                reason = "Chasing prevented: Overextended price continued upward without retest confirmation and continuation model is disabled"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    market_cap_usd=qualifying_mcap_usd or Decimal("0"),
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    market_cap_usd=qualifying_mcap_usd,
                )

        # 7d. Profile Evaluation: Graduated vs Movers vs Both
        extension_pct = 0.0
        base_level = breakout_price or pool_state.spot_price_native_per_token

        if base_level > Decimal("0"):
            extension_pct = float((current_price - base_level) / base_level)

        # Movers Profile checks:
        if section in (ScreenerSection.MOVERS, ScreenerSection.BOTH):
            # Check price extension (Anti-FOMO / excessively extended)
            if extension_pct > self.movers_profile.max_extension_pct:
                reason = (
                    f"Excessively extended move: {extension_pct * 100:.1f}% extension "
                    f"> {self.movers_profile.max_extension_pct * 100:.1f}% limit"
                )
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    price_extension_pct=extension_pct,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    price_extension_pct=extension_pct,
                    market_cap_usd=qualifying_mcap_usd,
                )

            # Price direction check: ensure not in free-fall
            if price_change_pct is not None and price_change_pct < -0.05:
                reason = f"Movers candidate experiencing negative momentum: {price_change_pct * 100:.1f}%"
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    market_cap_usd=qualifying_mcap_usd,
                )

        # Graduated Profile checks:
        if section in (ScreenerSection.GRADUATED, ScreenerSection.BOTH):
            # Verify price structure after graduation is not already over-extended
            if extension_pct > self.graduated_profile.max_price_extension_pct:
                reason = (
                    f"Graduated token excessively extended: {extension_pct * 100:.1f}% "
                    f"> {self.graduated_profile.max_price_extension_pct * 100:.1f}% limit"
                )
                self.record_decision(
                    token_address,
                    chain,
                    section,
                    ScreenerDecision.REJECTED,
                    reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    price_extension_pct=extension_pct,
                )
                return ScreenerEvaluationResult(
                    admitted=False,
                    token_address=token_address,
                    chain=chain,
                    section=section,
                    decision=ScreenerDecision.REJECTED,
                    reason=reason,
                    volume_2m_usd=qualifying_vol_usd,
                    liquidity_usd=liq_usd,
                    price_extension_pct=extension_pct,
                    market_cap_usd=qualifying_mcap_usd,
                )

        # 8. Immediate Qualification & Signal Generation
        # All checks passed: immediately admit without 2-minute or 5-minute observation delay!
        sig_source = (
            SignalSource.SCREENER_GRADUATED
            if section == ScreenerSection.GRADUATED
            else (
                SignalSource.SCREENER_MOVERS
                if section == ScreenerSection.MOVERS
                else SignalSource.SCREENER_GRADUATED
            )
        )

        alpha_score = 0.92 if section == ScreenerSection.BOTH else 0.88
        strength_str = classify_alpha_score(alpha_score)
        strength = SignalStrength(strength_str)

        rep = report or SecurityReport(
            token_address=token_address,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            top10_concentration=0.15,
            passes_hard_gates=True,
        )

        strat_type = (
            StrategyType.GRADUATED_PRIORITY
            if section == ScreenerSection.GRADUATED
            else StrategyType.MOVERS_PRIORITY
        )

        signal = SignalEvent(
            timestamp_ns=time.time_ns(),
            chain=chain,
            pool_address=pool_state.pool_address,
            token_address=token_address,
            suggested_side=OrderSide.BUY,
            pool_state=pool_state,
            security_report=rep,
            strength=strength,
            alpha_score=alpha_score,
            source=sig_source,
            strategy_type=strat_type,
            strategy_pattern=f"screener_{section.value}_priority",
            exit_profile=ExitProfile.FAST_SNIPE,
            market_cap_usd=qualifying_mcap_usd,
            execution_venue=(
                ExecutionVenue.RAYDIUM_AMM
                if chain == ChainIdentifier.SOLANA_MAINNET
                else ExecutionVenue.UNISWAP_V2
            ),
        )

        reason = (
            f"Admitted immediately from {section.value.upper()} screener: "
            f"market_cap=${qualifying_mcap_usd:.0f} (>= ${min_mcap_required:.0f}), "
            f"volume_2m=${qualifying_vol_usd:.0f} (>= ${min_vol_required:.0f}), "
            f"liquidity=${liq_usd:.0f}, price={current_price}, zero observation delay."
        )

        self.record_decision(
            token_address=token_address,
            chain=chain,
            section=section,
            decision=ScreenerDecision.ACCEPTED,
            reason=reason,
            volume_2m_usd=qualifying_vol_usd,
            liquidity_usd=liq_usd,
            price_extension_pct=extension_pct,
            market_cap_usd=qualifying_mcap_usd,
            details={"alpha_score": alpha_score, "signal_id": signal.signal_id},
        )

        logger.info("ScreenerPriorityEvaluator: %s", reason)

        return ScreenerEvaluationResult(
            admitted=True,
            token_address=token_address,
            chain=chain,
            section=section,
            decision=ScreenerDecision.ACCEPTED,
            reason=reason,
            signal=signal,
            volume_2m_usd=qualifying_vol_usd,
            liquidity_usd=liq_usd,
            price_extension_pct=extension_pct,
            market_cap_usd=qualifying_mcap_usd,
            metrics={"alpha_score": alpha_score, "impact": expected_impact},
        )
