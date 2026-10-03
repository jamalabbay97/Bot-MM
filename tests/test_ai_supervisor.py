"""
tests.test_ai_supervisor — Unit & Integration Test Suite for AlphaSupervisor-AI
================================================================================
Tests all 5 supervisory sections:
  1. Comprehensive Wallet & On-Chain Profiling Audit (Smart Money Deception Filter)
  2. On-Chain Security, Liquidity Depth & Pre-Flight Verification
  3. Sentiment Velocity, News & Narrative Novelty Decoder
  4. Adaptive Execution, Position Sizing & Dynamic Exit Matrix
  5. Closed-Loop Feedback & Hyperparameter Auto-Tuning
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import time
from typing import Any

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import ALPHA_SUPERVISOR_SYSTEM_PROMPT, AlphaSupervisorAI
from alpha_engine.engine.feedback import TradeReflection
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.models.ai import (
    ActionParameters,
    AISupervisorDecisionEnum,
    AISupervisorResponse,
    FeedbackTuning,
    GlobalRiskMode,
    HoneypotRisk,
    LiquidityHealth,
    LossAttribution,
    SecurityAssessment,
    TakeProfitStage,
    WalletAudit,
    WalletRiskClassification,
)
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
    WalletClassification,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.news import NewsEvent
from alpha_engine.models.profiler import WalletProfile
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport


def _create_mock_config() -> EngineConfig:
    return EngineConfig(
        ai_supervisor_enabled=True,
        ai_strict_veto=True,
        ai_provider="gemini",
        ai_timeout_s=2.0,
    )


def _create_mock_wallet_profile(
    wallet_address: str = "CabalFunder11111111111111111111111111111111",
    chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET,
    classification: WalletClassification = WalletClassification.APPROVED,
    is_whitelisted: bool = True,
    total_trades: int = 50,
    winning_trades: int = 35,
    losing_trades: int = 15,
    win_rate_pct: float = 70.0,
    total_pnl_usd: Decimal = Decimal("50000"),
    max_single_trade_pnl_usd: Decimal = Decimal("5000"),
    outlier_pnl_ratio: float = 0.1,
    median_holding_time_seconds: float = 300.0,
    active_days: float = 60.0,
    days_since_last_active: float = 1.0,
    first_tx_timestamp: int = 1700000000,
    last_tx_timestamp: int = 1705000000,
    custom_metadata: dict[str, Any] | None = None,
) -> WalletProfile:
    return WalletProfile(
        wallet_address=wallet_address,
        chain=chain,
        classification=classification,
        is_whitelisted=is_whitelisted,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        win_rate_pct=win_rate_pct,
        total_pnl_usd=total_pnl_usd,
        max_single_trade_pnl_usd=max_single_trade_pnl_usd,
        outlier_pnl_ratio=outlier_pnl_ratio,
        median_holding_time_seconds=median_holding_time_seconds,
        active_days=active_days,
        days_since_last_active=days_since_last_active,
        first_tx_timestamp=first_tx_timestamp,
        last_tx_timestamp=last_tx_timestamp,
        custom_metadata=custom_metadata or {},
    )


def _create_clean_pool_state(chain: ChainIdentifier = ChainIdentifier.SOLANA_MAINNET) -> PoolState:
    return PoolState(
        pool_address="0x" + "1" * 40 if chain == ChainIdentifier.BASE_MAINNET else "P" * 32,
        chain=chain,
        token_reserve=Decimal("1000000000.0"),
        native_reserve=Decimal("50.0") if chain == ChainIdentifier.SOLANA_MAINNET else Decimal("5.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100,
    )


def _create_clean_security_report(chain: ChainIdentifier, token_address: str) -> SecurityReport:
    return SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.10,
        mint_authority_disabled=True,
        verified_source_code=True,
    )


def _create_signal(chain: ChainIdentifier, token_address: str, pool: PoolState, report: SecurityReport) -> SignalEvent:
    return SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=chain,
        pool_address=pool.pool_address,
        token_address=token_address,
        suggested_side=OrderSide.BUY,
        pool_state=pool,
        security_report=report,
        strength=SignalStrength.STRONG,
        alpha_score=0.88,
    )


# =============================================================================
# 1. Output Schema Validation & Serialization Tests
# =============================================================================
def test_ai_response_strict_schema_serialization():
    """Verify strict JSON serialization matches user specification exactly."""
    resp = AISupervisorResponse(
        decision=AISupervisorDecisionEnum.EXECUTE_BUY,
        confidence_score=0.92,
        action_parameters=ActionParameters(
            target_token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            recommended_position_pct=1.5,
            max_slippage_bps=150,
            priority_fee_multiplier=1.2,
            take_profit_ladder=[
                TakeProfitStage(trigger_multiplier=2.0, sell_pct=40.0),
                TakeProfitStage(trigger_multiplier=3.5, sell_pct=30.0),
            ],
            hard_stop_loss_pct=-15.0,
            trailing_stop_activation_pct=40.0,
            time_exit_minutes=15,
        ),
        wallet_audit=WalletAudit(
            is_wallet_reputable=True,
            risk_classification=WalletRiskClassification.ORGANIC_SMART_MONEY,
            rationale="Authentic smart money distribution over 40+ trades.",
        ),
        security_assessment=SecurityAssessment(
            is_secure=True,
            honeypot_risk=HoneypotRisk.NONE,
            liquidity_health=LiquidityHealth.OPTIMAL,
            flags=[],
        ),
        feedback_tuning=FeedbackTuning(
            adjust_global_risk=GlobalRiskMode.NEUTRAL,
            blacklisted_entities=[],
            insights_learned="High confidence organic entry.",
        ),
    )

    json_str = resp.to_strict_json()
    assert '"decision": "EXECUTE_BUY"' in json_str
    assert '"confidence_score": 0.92' in json_str
    assert '"recommended_position_pct": 1.5' in json_str
    assert '"hard_stop_loss_pct": -15.0' in json_str
    assert '"trailing_stop_activation_pct": 40.0' in json_str
    assert '"time_exit_minutes": 15' in json_str

    parsed = AISupervisorResponse.from_strict_json(json_str)
    assert parsed.decision == AISupervisorDecisionEnum.EXECUTE_BUY
    assert parsed.action_parameters.recommended_position_pct == 1.5
    assert len(parsed.action_parameters.take_profit_ladder) == 2
    assert "AlphaSupervisor-AI" in ALPHA_SUPERVISOR_SYSTEM_PROMPT


def test_ai_response_markdown_fence_stripping():
    """Verify that JSON wrapped in markdown backticks is parsed seamlessly."""
    raw = """```json
    {
      "decision": "PASS",
      "confidence_score": 0.99,
      "action_parameters": {
        "target_token_address": "0x1111111111111111111111111111111111111111",
        "recommended_position_pct": 0.0,
        "max_slippage_bps": 50,
        "priority_fee_multiplier": 1.0,
        "take_profit_ladder": [],
        "hard_stop_loss_pct": -15.0,
        "trailing_stop_activation_pct": 40.0,
        "time_exit_minutes": 15
      },
      "wallet_audit": {
        "is_wallet_reputable": false,
        "risk_classification": "CABAL_INSIDER",
        "rationale": "Disperse aggregator detected."
      },
      "security_assessment": {
        "is_secure": false,
        "honeypot_risk": "CONFIRMED",
        "liquidity_health": "THIN",
        "flags": ["HONEYPOT"]
      },
      "feedback_tuning": {
        "adjust_global_risk": "DEFENSIVE",
        "blacklisted_entities": ["0x1111111111111111111111111111111111111111"],
        "insights_learned": "Honeypot token blocked."
      }
    }
    ```"""
    parsed = AISupervisorResponse.from_strict_json(raw)
    assert parsed.decision == AISupervisorDecisionEnum.PASS
    assert parsed.wallet_audit.risk_classification == "CABAL_INSIDER"
    assert parsed.security_assessment.honeypot_risk == "CONFIRMED"
    assert parsed.security_assessment.liquidity_health == LiquidityHealth.THIN


# =============================================================================
# 2. Section 1 Tests: Comprehensive Wallet & Smart Money Audit
# =============================================================================
def test_wallet_audit_cabal_insider_funding_aggregator():
    """Flag as CABAL_INSIDER when funding traces to Disperse or privacy mixer."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        pool = _create_clean_pool_state()
        report = _create_clean_security_report(ChainIdentifier.SOLANA_MAINNET, "Cabal11111111111111111111111111111111111111")
        sig = _create_signal(ChainIdentifier.SOLANA_MAINNET, "Cabal11111111111111111111111111111111111111", pool, report)

        wallet_profile = _create_mock_wallet_profile(
            wallet_address="CabalFunder11111111111111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            custom_metadata={"is_disperse_funded": True},
        )

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report, wallet_profile=wallet_profile)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert resp.wallet_audit.is_wallet_reputable is False
        assert resp.wallet_audit.risk_classification == WalletRiskClassification.CABAL_INSIDER
        assert "SYBIL_FUNDING_AGGREGATOR" in resp.security_assessment.flags

    asyncio.run(_run())


def test_wallet_audit_mev_sandwich_bot():
    """Flag as MEV_BOT when median holding time is < 45s or 0-slot atomic execution is detected."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        pool = _create_clean_pool_state()
        report = _create_clean_security_report(ChainIdentifier.SOLANA_MAINNET, "MevTarget1111111111111111111111111111111111")
        sig = _create_signal(ChainIdentifier.SOLANA_MAINNET, "MevTarget1111111111111111111111111111111111", pool, report)

        wallet_profile = _create_mock_wallet_profile(
            wallet_address="JitoMevBot111111111111111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            total_trades=100,
            median_holding_time_seconds=12.0,  # < 45s
            custom_metadata={"is_mev_bot": True},
        )

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report, wallet_profile=wallet_profile)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert resp.wallet_audit.risk_classification == WalletRiskClassification.MEV_BOT
        assert resp.wallet_audit.is_wallet_reputable is False

    asyncio.run(_run())


def test_wallet_audit_wash_trading_volume_manipulation():
    """Flag as WASH_TRADER when profit factor is low despite high volume."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        pool = _create_clean_pool_state()
        report = _create_clean_security_report(ChainIdentifier.SOLANA_MAINNET, "WashTarget111111111111111111111111111111111")
        sig = _create_signal(ChainIdentifier.SOLANA_MAINNET, "WashTarget111111111111111111111111111111111", pool, report)

        wallet_profile = _create_mock_wallet_profile(
            wallet_address="WashTrader11111111111111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            total_trades=150,
            win_rate_pct=35.0,
            custom_metadata={"profit_factor": 0.98},
        )

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report, wallet_profile=wallet_profile)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert resp.wallet_audit.risk_classification == WalletRiskClassification.WASH_TRADER

    asyncio.run(_run())


# =============================================================================
# 3. Section 2 Tests: On-Chain Security & Liquidity Pre-Flight
# =============================================================================
def test_security_spl_active_mint_or_freeze_authority_reject():
    """Solana SPL tokens with active mint or freeze authority MUST be rejected immediately."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        pool = _create_clean_pool_state(ChainIdentifier.SOLANA_MAINNET)
        token = "MintAuthActiveToken1111111111111111111111111"
        report = SecurityReport(
            token_address=token,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=0,
            sell_tax_bps=0,
            lp_burned_ratio=1.0,
            top10_concentration=0.10,
            mint_authority_disabled=False,  # Active mint authority!
            verified_source_code=True,
        )
        sig = _create_signal(ChainIdentifier.SOLANA_MAINNET, token, pool, report)

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert resp.security_assessment.is_secure is False
        assert resp.security_assessment.honeypot_risk == HoneypotRisk.CONFIRMED
        assert "ACTIVE_MINT_OR_FREEZE_AUTHORITY" in resp.security_assessment.flags

    asyncio.run(_run())


def test_security_evm_transfer_tax_exceeds_2_percent():
    """EVM tokens with transfer tax > 2% (200 bps) MUST be rejected."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        chain = ChainIdentifier.BASE_MAINNET
        token = "0x" + "2" * 40
        pool = _create_clean_pool_state(chain)
        report = SecurityReport(
            token_address=token,
            chain=chain,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=300,  # 3% tax > 2%
            sell_tax_bps=300,
            lp_burned_ratio=1.0,
            top10_concentration=0.10,
            mint_authority_disabled=True,
            verified_source_code=True,
        )
        sig = _create_signal(chain, token, pool, report)

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert "TRANSFER_TAX_EXCEEDS_2_PCT" in resp.security_assessment.flags

    asyncio.run(_run())


def test_security_bonding_curve_migration_sniper_void():
    """Tokens within 90-99% curve migration progress trigger warning."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        chain = ChainIdentifier.SOLANA_MAINNET
        token = "MigrationNearPump1111111111111111111111111"
        pool = PoolState(
            pool_address="P" * 32,
            chain=chain,
            token_reserve=Decimal("100000000.0"),
            native_reserve=Decimal("82.0"),  # ~94.5% progress
            fee_numerator=3,
            fee_denominator=1000,
            last_updated_block=500,
        )
        report = _create_clean_security_report(chain, token)
        sig = _create_signal(chain, token, pool, report)

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report)
        assert "BONDING_CURVE_MIGRATION_SNIPER_VOID_90_99_PCT" in resp.security_assessment.flags
        assert resp.security_assessment.liquidity_health == LiquidityHealth.THIN

    asyncio.run(_run())


# =============================================================================
# 4. Section 3 Tests: Sentiment Velocity & News Novelty
# =============================================================================
def test_sentiment_expired_news_catalyst():
    """News catalyst older than 300s is flagged as expired local top."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        chain = ChainIdentifier.SOLANA_MAINNET
        token = "NewsToken111111111111111111111111111111111"
        pool = _create_clean_pool_state(chain)
        report = _create_clean_security_report(chain, token)
        sig = _create_signal(chain, token, pool, report)

        news = NewsEvent(
            source_channel="TreeNewsFeed",
            raw_text=f"Breaking token launch: {token}",
            sentiment_score=0.85,
            timestamp=time.time() - 400.0,  # 400s old > 300s
        )

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report, news_event=news)
        assert resp.decision == AISupervisorDecisionEnum.PASS
        assert "NEWS_CATALYST_EXPIRED_LOCAL_TOP" in resp.security_assessment.flags

    asyncio.run(_run())


# =============================================================================
# 5. Section 4 Tests: Adaptive Execution & Dynamic Exit Architecture
# =============================================================================
def test_adaptive_fractional_kelly_sizing():
    """Position sizing is strictly clamped between 0.25% and 2.0%."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        chain = ChainIdentifier.SOLANA_MAINNET
        token = "ValidAlphaToken111111111111111111111111111"
        pool = _create_clean_pool_state(chain)
        report = _create_clean_security_report(chain, token)
        sig = _create_signal(chain, token, pool, report)

        resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report, recent_win_rate=80.0)
        assert resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY
        assert 0.25 <= resp.action_parameters.recommended_position_pct <= 2.0
        assert resp.action_parameters.hard_stop_loss_pct == -15.0
        assert resp.action_parameters.trailing_stop_activation_pct == 40.0
        assert resp.action_parameters.time_exit_minutes == 15

        tp_ladder = resp.action_parameters.take_profit_ladder
        assert len(tp_ladder) == 2
        assert tp_ladder[0].trigger_multiplier == 2.0
        assert tp_ladder[0].sell_pct == 40.0
        assert tp_ladder[1].trigger_multiplier == 3.5
        assert tp_ladder[1].sell_pct == 30.0

    asyncio.run(_run())


# =============================================================================
# 6. Section 5 Tests: Closed-Loop Feedback & Post-Mortem Attribution
# =============================================================================
def test_closed_loop_slippage_leakage_and_mev_sandwich_attribution():
    """Post-mortem trade reflection detects slippage leakage > 3% and MEV sandwich."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        reflection = TradeReflection(
            trade_id="trade-123",
            token_address="SlippedToken111111111111111111111111111111",
            chain=ChainIdentifier.SOLANA_MAINNET,
            signal_source=SignalSource.DEX_SWAP,
            entry_price=Decimal("1.0"),
            exit_price=Decimal("0.85"),
            expected_slippage_bps=150,
            actual_slippage_bps=650,  # 500 bps leakage = 5.0% > 3.0%
            realized_pnl_usd=Decimal("-15.0"),
            realized_pnl_native=Decimal("-0.1"),
            is_win=False,
            roi_pct=-15.0,
        )

        tuning = await supervisor.reflect_on_trade(reflection)
        assert tuning.adjust_global_risk == GlobalRiskMode.DEFENSIVE
        assert LossAttribution.MEV_SANDWICH.value in tuning.insights_learned
        assert supervisor.get_status()["global_risk_mode"] == GlobalRiskMode.DEFENSIVE.value

    asyncio.run(_run())


def test_closed_loop_rug_pull_attribution_and_blacklisting():
    """Severe loss (e.g. -85%) attributes RUG_PULL and blacklists token."""
    async def _run():
        supervisor = AlphaSupervisorAI(config=_create_mock_config())
        token = "RuggedToken1111111111111111111111111111111"
        reflection = TradeReflection(
            trade_id="rug-999",
            token_address=token,
            chain=ChainIdentifier.SOLANA_MAINNET,
            signal_source=SignalSource.DEX_SWAP,
            entry_price=Decimal("1.0"),
            exit_price=Decimal("0.10"),
            expected_slippage_bps=150,
            actual_slippage_bps=200,
            realized_pnl_usd=Decimal("-100.0"),
            realized_pnl_native=Decimal("-0.9"),
            is_win=False,
            roi_pct=-90.0,
        )

        tuning = await supervisor.reflect_on_trade(reflection)
        assert token in tuning.blacklisted_entities
        assert LossAttribution.RUG_PULL.value in tuning.insights_learned
        assert token in supervisor._blacklisted_entities

        # Next signal for this token is immediately vetoed
        pool = _create_clean_pool_state()
        report = _create_clean_security_report(ChainIdentifier.SOLANA_MAINNET, token)
        sig = _create_signal(ChainIdentifier.SOLANA_MAINNET, token, pool, report)
        veto_resp = await supervisor.audit_signal(signal=sig, pool_state=pool, security_report=report)
        assert veto_resp.decision == AISupervisorDecisionEnum.PASS
        assert "ENTITY_BLACKLISTED" in veto_resp.security_assessment.flags

    asyncio.run(_run())


# =============================================================================
# 7. Position Book Dynamic Exit Integration
# =============================================================================
def test_position_book_ai_dynamic_exits():
    """PositionBook evaluates AI inactivity stop, AI hard-stop, and AI breakeven trigger."""
    book = PositionBook()
    now_ns = time.time_ns()
    fill_ts = now_ns - int(1000 * 1e9)  # 1000 seconds ago (>15 mins)
    fill = PaperFill(
        token_address="AIToken11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("0.001"),
        signal_timestamp_ns=fill_ts - 100_000_000,
        fill_timestamp_ns=fill_ts,
    )

    lot = book.open_lot(
        fill=fill,
        signal_id="sig-ai-1",
        initial_pool_reserve_native=Decimal("50.0"),
        bonding_curve_mode=True,
        ai_hard_stop_loss_pct=-15.0,
        ai_trailing_stop_activation_pct=40.0,
        ai_time_exit_minutes=15.0,
    )

    assert isinstance(lot, OpenLot)
    assert lot.ai_hard_stop_loss_pct == -15.0
    assert lot.ai_time_exit_minutes == 15.0
    assert lot.ai_trailing_stop_activation_pct == 40.0

    # 1. Stagnation Exit (>15 mins and pnl < 15%)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00102"),  # +2% gain (< +15%)
        current_pool_reserve_native=Decimal("50.0"),
        tick_timestamp_s=time.time(),
        current_timestamp_ns=now_ns,
        bonding_curve_mode=True,
    )
    assert decision is not None
    assert decision.should_exit is True
    assert decision.exit_reason == TradeExitReason.TIMEOUT_VELOCITY_DECAY


def test_position_book_ai_breakeven_trigger():
    """PositionBook activates trailing stop and shifts stop to breakeven at +40% gain."""
    book = PositionBook()
    now_ns = time.time_ns()
    fill_ts = now_ns - int(30 * 1e9)
    fill = PaperFill(
        token_address="AIToken22222222222222222222222222222222",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("0.001"),
        signal_timestamp_ns=fill_ts - 100_000_000,
        fill_timestamp_ns=fill_ts,
    )

    lot = book.open_lot(
        fill=fill,
        signal_id="sig-ai-2",
        initial_pool_reserve_native=Decimal("50.0"),
        bonding_curve_mode=True,
        ai_trailing_stop_activation_pct=40.0,
    )

    # Price up +45% (0.00145 >= 0.00140)
    decision = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.00145"),
        current_pool_reserve_native=Decimal("52.0"),
        tick_timestamp_s=time.time(),
        current_timestamp_ns=now_ns,
        bonding_curve_mode=True,
    )
    assert lot.trailing_stop_active is True
    assert lot.trailing_stop_price >= lot.entry_price  # Stop shifted to breakeven buffer
