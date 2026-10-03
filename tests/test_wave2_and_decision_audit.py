"""
tests.test_wave2_and_decision_audit — Comprehensive Test Suite for Wave-2 Breakout,
Decision Audit Trail, Pattern Memory Store, Dynamic Tuner & Conversational Supervisor.
======================================================================================
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import os
import tempfile
import time

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.engine.feedback import (
    DynamicParameterTuner,
    MissedOpportunityAnalyzer,
    PatternMemoryStore,
)
from alpha_engine.engine.signals import PendingLaunchBuffer
from alpha_engine.engine.staging import Wave2StagingBuffer
from alpha_engine.execution.book import PositionBook, RunningMetrics
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.decisions import DecisionRecord, DecisionType, PatternFeatureVector
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SwapEvent
from alpha_engine.models.state import PoolState, SecurityReport


@pytest.fixture
def clean_security_report() -> SecurityReport:
    return SecurityReport(
        token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.18,
        mint_authority_disabled=True,
        freeze_authority_disabled=True,
        verified_source_code=True,
    )


@pytest.fixture
def sample_pool(clean_security_report: SecurityReport) -> PoolState:
    return PoolState(
        pool_address="Pool111111111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("30.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        token_address=clean_security_report.token_address,
    )


# =============================================================================
# 1. Wave-2 Staging Buffer & Breakout Tests
# =============================================================================

def test_wave2_staging_and_ttl(clean_security_report: SecurityReport):
    buffer = Wave2StagingBuffer(ttl_seconds=3600.0)
    token_addr = clean_security_report.token_address

    staged = buffer.stage_token(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="Pool111111111111111111111111111111111111111",
        initial_price=Decimal("0.000030"),
        report=clean_security_report,
        t_staged=time.time() - 4000.0,  # Expired
    )
    assert buffer.is_staged(token_addr)
    assert staged.initial_price == Decimal("0.000030")

    # Sweep expired
    expired = buffer.sweep_expired()
    assert token_addr in expired
    assert not buffer.is_staged(token_addr)


def test_wave2_accumulation_breakout_detection(clean_security_report: SecurityReport, sample_pool: PoolState):
    buffer = Wave2StagingBuffer(
        ttl_seconds=7200.0,
        volume_surge_multiplier_k=2.5,
        min_net_buy_delta_pct=65.0,
        max_top10_concentration_pct=25.0,
        min_consolidation_duration_s=60.0,
    )
    token_addr = clean_security_report.token_address
    t_stage = time.time() - 700.0

    buffer.stage_token(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=sample_pool.pool_address,
        initial_price=Decimal("0.000030"),
        report=clean_security_report,
        t_staged=t_stage,
    )

    # 1. Simulate initial dump & consolidation trades (baseline trades 10 minutes ago, inside 15m SMA but outside 5m)
    base_time_ns = int((time.time() - 600.0) * 1e9)
    for i in range(10):
        ts = base_time_ns + i * 5_000_000_000
        is_buy = i % 2 == 0
        swap = SwapEvent(
            block_number=100 + i,
            timestamp_ns=ts,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tx_hash=f"TxHash111111111111111111111111111111_{i:02d}",
            pool_address=sample_pool.pool_address,
            sender=f"WalletAddress11111111111111111111111_{i:02d}",
            token_in="So11111111111111111111111111111111111111112" if is_buy else token_addr,
            token_out=token_addr if is_buy else "So11111111111111111111111111111111111111112",
            amount_in=Decimal("0.1") if is_buy else Decimal("3333.0"),
            amount_out=Decimal("3333.0") if is_buy else Decimal("0.1"),
        )
        sig = buffer.record_trade(swap, pool=sample_pool)
        assert sig is None  # Still consolidating

    # 2. Simulate Wave-2 Breakout: massive smart buy volume surge (> k * SMA, buy delta > 65%)
    breakout_time_ns = int(time.time() * 1e9)
    breakout_sig = None
    for j in range(5):
        swap_surge = SwapEvent(
            block_number=200 + j,
            timestamp_ns=breakout_time_ns + j * 1_000_000_000,
            chain=ChainIdentifier.SOLANA_MAINNET,
            tx_hash=f"TxBreakout111111111111111111111111111_{j:02d}",
            pool_address=sample_pool.pool_address,
            sender=f"SmartWhale11111111111111111111111111_{j:02d}",
            token_in="So11111111111111111111111111111111111111112",
            token_out=token_addr,
            amount_in=Decimal("5.0"),  # Heavy buy surge
            amount_out=Decimal("150000.0"),
        )
        sig = buffer.record_trade(swap_surge, pool=sample_pool)
        if sig is not None:
            breakout_sig = sig
            break

    assert breakout_sig is not None
    assert breakout_sig.suggested_side == OrderSide.BUY
    assert breakout_sig.source == SignalSource.WAVE2_BREAKOUT
    assert breakout_sig.strength == SignalStrength.STRONG
    assert breakout_sig.alpha_score >= 0.75
    assert not buffer.is_staged(token_addr)  # Graduated and cleared from staging


def test_wave2_pending_launch_buffer_handoff(clean_security_report: SecurityReport):
    """Test that dumped/expired clean tokens from PendingLaunchBuffer hand off into Wave2StagingBuffer."""
    wave2_buf = Wave2StagingBuffer(ttl_seconds=3600.0)
    launch_buf = PendingLaunchBuffer(min_age_s=10.0, max_age_s=30.0, min_buys=5)
    launch_buf.set_wave2_staging_buffer(wave2_buf)

    token_addr = clean_security_report.token_address
    t_0 = time.time() - 40.0  # Expired launch
    launch_buf.stage_token(
        token_address=token_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="Pool111111111111111111111111111111111111111",
        initial_price=Decimal("0.000030"),
        report=clean_security_report,
        t_0=t_0,
    )

    # Sweep pending launch buffer
    dropped = launch_buf.sweep_expired()
    assert token_addr in dropped
    # Must now be present in wave2 buffer
    assert wave2_buf.is_staged(token_addr)


# =============================================================================
# 2. SQLite Ledger Structured Audit Log Tests
# =============================================================================

def test_decision_record_audit_log_persistence():
    async def _run():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_ledger.db")
            async with SQLiteLedger(db_path) as ledger:
                rec = DecisionRecord(
                    decision_type=DecisionType.ENTER,
                    token_address="TokenAudit11111111111111111111111111111111",
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    signal_id="sig_test_123",
                    token_symbol="TESTWAVE",
                    market_cap_usd=45000.0,
                    liquidity_usd=12000.0,
                    volume_5m_usd=8500.0,
                    volume_1h_usd=25000.0,
                    rule_triggers={
                        "WhaleInflow": True,
                        "DevExitConfirmed": True,
                        "PatternMatchScore": 0.89,
                    },
                    confidence_score=0.92,
                    pattern_match_score=0.89,
                    reason="Wave-2 Accumulation Breakout verified: V5m/SMA15m=3.5x > 2.5x, BuyDelta=78.2% > 65%, Dev balance=0%",
                )
                await ledger.record_decision(rec)

                # Query decisions
                decisions = await ledger.get_decisions(limit=10)
                assert len(decisions) == 1
                d = decisions[0]
                assert d.decision_type == DecisionType.ENTER
                assert d.token_symbol == "TESTWAVE"
                assert d.rule_triggers["WhaleInflow"] is True
                assert d.confidence_score == 0.92
                assert d.pattern_match_score == 0.89
                assert "Wave-2 Accumulation Breakout" in d.reason

                # Query token history
                history = await ledger.get_token_decision_history("TokenAudit11111111111111111111111111111111")
                assert len(history) == 1

                # Query stats
                stats = await ledger.get_decision_stats(hours=24)
                assert stats["total_evaluations"] == 1
                assert stats["breakdown"].get("ENTER") == 1

    asyncio.run(_run())


def test_pattern_feature_store_persistence():
    async def _run():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_pattern_ledger.db")
            async with SQLiteLedger(db_path) as ledger:
                vec = PatternFeatureVector(
                    token_address="TokenVec111111111111111111111111111111111",
                    consolidation_duration_s=2400.0,
                    dip_depth_pct=48.5,
                    volume_surge_multiplier=3.8,
                    net_buy_delta=0.76,
                    top10_concentration=0.16,
                    liquidity_to_mc_ratio=0.24,
                    smart_wallet_inflows=22.0,
                    peak_gain_multiplier=4.2,
                )
                await ledger.record_pattern_vector(vec)

                stored = await ledger.get_pattern_vectors(limit=10)
                assert len(stored) == 1
                assert stored[0].token_address == "TokenVec111111111111111111111111111111111"
                assert stored[0].peak_gain_multiplier == 4.2

    asyncio.run(_run())


# =============================================================================
# 3. Vectorized Pattern Store & Hyperparameter Tuner Tests
# =============================================================================

def test_pattern_memory_store_cosine_similarity():
    store = PatternMemoryStore()
    assert len(store.get_patterns()) >= 2  # Has baseline archetypes

    candidate_similar = PatternFeatureVector(
        token_address="CandidateSimilar",
        consolidation_duration_s=2600.0,
        dip_depth_pct=53.0,
        volume_surge_multiplier=3.3,
        net_buy_delta=0.73,
        top10_concentration=0.18,
        liquidity_to_mc_ratio=0.21,
        smart_wallet_inflows=17.0,
        peak_gain_multiplier=1.0,
    )
    score = store.calculate_pattern_match(candidate_similar)
    assert 0.80 <= score <= 1.0  # High match with CTO archetype

    candidate_different = PatternFeatureVector(
        token_address="CandidateDivergent",
        consolidation_duration_s=10.0,
        dip_depth_pct=2.0,
        volume_surge_multiplier=0.4,
        net_buy_delta=0.20,
        top10_concentration=0.85,
        liquidity_to_mc_ratio=0.01,
        smart_wallet_inflows=0.0,
        peak_gain_multiplier=1.0,
    )
    score_diff = store.calculate_pattern_match(candidate_different)
    assert score_diff < score


def test_dynamic_parameter_tuner_adaptation():
    tuner = DynamicParameterTuner(smoothing_alpha=0.30)
    initial_params = tuner.current_params
    assert initial_params.confidence_multiplier == 1.0

    # Winning streak: expand confidence, loosen surge k
    for _ in range(5):
        tuner.update_from_performance(win_rate_24h=85.0, realized_pnl_usd=500.0, total_trades_24h=12)

    winning_params = tuner.current_params
    assert winning_params.confidence_multiplier > 1.0
    assert winning_params.wave2_surge_k < 2.5  # Loosened surge threshold to capture Wave-2 sooner
    winning_k = winning_params.wave2_surge_k

    # Losing streak: tighten confidence, tighten stop loss, increase surge k
    for _ in range(10):
        tuner.update_from_performance(win_rate_24h=30.0, realized_pnl_usd=-150.0, total_trades_24h=15)

    losing_params = tuner.current_params
    assert losing_params.confidence_multiplier < 1.0
    assert losing_params.wave2_surge_k > winning_k
    # Strictly respect boundaries
    assert 0.70 <= losing_params.confidence_multiplier <= 1.40
    assert 1.8 <= losing_params.wave2_surge_k <= 3.5


def test_missed_opportunity_post_mortem():
    async def _run():
        store = PatternMemoryStore()
        analyzer = MissedOpportunityAnalyzer(pattern_store=store)

        market_gainers = [
            {
                "token_address": "TokenSurgedAfterSkip",
                "symbol": "SURGE",
                "multiplier": 4.5,
                "volume_surge_multiplier": 3.2,
                "net_buy_delta": 0.75,
                "dip_depth_pct": 50.0,
                "consolidation_duration_s": 2000.0,
            }
        ]

        class FakeOutcome:
            decision = "SKIP"
            flags = ["Low Initial Volume"]

        audited = {"TokenSurgedAfterSkip": FakeOutcome()}
        staged = {}

        missed = await analyzer.cross_reference_market_gainers(market_gainers, audited, staged)
        assert len(missed) == 1
        assert missed[0].token_address == "TokenSurgedAfterSkip"
        assert missed[0].peak_multiplier == 4.5
        # Feature vector saved into store
        patterns = store.get_patterns()
        assert any(p.token_address == "TokenSurgedAfterSkip" for p in patterns)

    asyncio.run(_run())


# =============================================================================
# 4. Conversational Supervisor AI Explainer Tests
# =============================================================================

def test_conversational_supervisor_tools():
    async def _run():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_chat.db")
            async with SQLiteLedger(db_path) as ledger:
                # Seed audit decisions
                rec1 = DecisionRecord(
                    decision_type=DecisionType.SKIP,
                    token_address="TokenChatDemo",
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    token_symbol="CHATDEMO",
                    market_cap_usd=20000.0,
                    liquidity_usd=5000.0,
                    volume_5m_usd=200.0,
                    volume_1h_usd=800.0,
                    rule_triggers={"VolumeSurge": False},
                    confidence_score=0.40,
                    pattern_match_score=0.35,
                    reason="Skipped: V5m/SMA15m=0.8x < 2.5x threshold. Inadequate buying pressure.",
                )
                rec2 = DecisionRecord(
                    decision_type=DecisionType.ENTER,
                    token_address="TokenChatDemo",
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    token_symbol="CHATDEMO",
                    market_cap_usd=35000.0,
                    liquidity_usd=9000.0,
                    volume_5m_usd=5200.0,
                    volume_1h_usd=14000.0,
                    rule_triggers={"VolumeSurge": True, "NetBuyDelta": True},
                    confidence_score=0.88,
                    pattern_match_score=0.82,
                    reason="Entered: V5m/SMA15m=3.8x > 2.5x, BuyDelta=74% > 65%. Wave-2 consolidation breakout confirmed.",
                )
                await ledger.record_decision(rec1)
                await ledger.record_decision(rec2)

                staging_buf = Wave2StagingBuffer()
                staging_buf.stage_token(
                    token_address="StagedTokenDemo11111111111111111111111",
                    chain=ChainIdentifier.SOLANA_MAINNET,
                    pool_address="Pool111111111111111111111111111111111111111",
                    initial_price=Decimal("0.000030"),
                )
                tuner = DynamicParameterTuner()
                supervisor = ConversationalSupervisor(
                    ledger=ledger,
                    position_book=PositionBook(),
                    staging_buffer=staging_buf,
                    parameter_tuner=tuner,
                    metrics=RunningMetrics(peak_equity_usd=Decimal("1000.0")),
                )

                # Test 1: Why decision tool
                why_res = await supervisor.tool_why_decision("TokenChatDemo")
                assert "TokenChatDem" in why_res
                assert "CHATDEMO" in why_res
                assert "SKIP" in why_res
                assert "ENTER" in why_res

                # Test 2: Strategy performance tool
                perf_res = await supervisor.tool_strategy_performance(hours=24)
                assert "Evaluations" in perf_res

                # Test 3: List staged tokens tool
                staged_res = await supervisor.tool_list_staged_tokens()
                assert "Staging Watchlist" in staged_res

                # Test 4: Override parameter tool
                override_res = await supervisor.tool_override_parameter("surge_k", 2.2)
                assert "2.2" in override_res
                assert tuner.current_params.wave2_surge_k == 2.2

                # Test 5: Natural language 'ask' routing
                nl_resp1 = await supervisor.ask("Why did you skip TokenChatDemo?")
                assert "TokenChatDem" in nl_resp1

                nl_resp2 = await supervisor.ask("What is the current staging buffer?")
                assert "Staging Watchlist" in nl_resp2

                nl_resp3 = await supervisor.ask("What was the strategy win rate over the last 12 hours?")
                assert "Evaluations" in nl_resp3

    asyncio.run(_run())
