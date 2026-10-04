"""
tests/test_conversational_and_localization.py
=============================================
Verification of Telegram conversational pipeline, Arabic localization,
supervisor resilience, and free-form query handling.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import time
from typing import Any

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.engine.feedback import DynamicHyperparameters, DynamicParameterTuner, PatternMemoryStore
from alpha_engine.engine.staging import Wave2StagingBuffer
from alpha_engine.execution.book import PositionBook, RunningMetrics
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.models.decisions import PatternFeatureVector
from alpha_engine.models.enums import ChainIdentifier, OrderSide, SecurityTier, SignalStrength
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport


class MockDMEvent:
    def __init__(self, text: str, sender_id: int = 123456789, out: bool = False):
        self.raw_text = text
        self.sender_id = sender_id
        self.is_private = True
        self.out = out
        self.replies: list[str] = []

    async def reply(self, msg: str, buttons: Any = None, **kwargs: Any):
        self.replies.append(msg)


@pytest.mark.anyio
async def test_supervisor_answer_user_query_offline_arabic():
    """Verify that answer_user_query returns rich Arabic text even with no LLM key."""
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)
    supervisor._api_key = None  # Ensure deterministic fallback is invoked

    # 1. Status query
    ans_status = await supervisor.answer_user_query("ما هي حالة المحرك ومستوى المخاطرة الحالي؟")
    assert "AlphaSupervisor-AI" in ans_status
    assert "وضع المخاطرة" in ans_status or "NEUTRAL" in ans_status

    # 2. Veto query
    supervisor._recent_vetoes.append({
        "token": "DemoVetoToken1111111111111111111111111111",
        "chain": "solana_mainnet",
        "flags": ["HONEYPOT_DETECTED"],
        "rationale": "High honeypot probability",
        "timestamp": time.time(),
    })
    ans_veto = await supervisor.answer_user_query("لماذا قمت برفض الإشارة الأخيرة؟")
    assert "HONEYPOT_DETECTED" in ans_veto or "DemoVetoToken" in ans_veto or "رفض" in ans_veto

    # 3. General query
    ans_gen = await supervisor.answer_user_query("كيف تختار الصفقات وتتجنب الفخاخ؟")
    assert len(ans_gen) > 50
    assert any(w in ans_gen for w in ("فحص", "المخاطر", "السيولة", "العقد", "AlphaSupervisor", "المشرف"))


@pytest.mark.anyio
async def test_supervisor_audit_sec_none_hardening():
    """Ensure audit_signal / audit_trade_candidate and PatternFeatureVector never crash when sec is None."""
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)
    store = PatternMemoryStore()
    supervisor._pattern_store = store

    pool = PoolState(
        pool_address="0x" + "1" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("5.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100,
    )
    sec_rep = SecurityReport(
        token_address="0x" + "2" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.12,
        mint_authority_disabled=True,
        verified_source_code=True,
    )
    sig = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address=pool.pool_address,
        token_address="0x" + "2" * 40,
        suggested_side=OrderSide.BUY,
        pool_state=pool,
        security_report=sec_rep,
        strength=SignalStrength.STRONG,
        alpha_score=0.88,
    )

    # 1. Feature vector extraction with sec=None
    feat_vec = PatternFeatureVector.from_signal(signal=sig, sec=None)
    assert feat_vec is not None
    assert feat_vec.top10_concentration == 0.15

    # 2. Pattern match calculation with feat_vec
    match_score = store.calculate_pattern_match(feat_vec)
    assert 0.0 <= match_score <= 1.0

    # 3. Audit trade candidate with sec=None
    res = await supervisor.audit_trade_candidate(signal=sig, pool_state=pool, sec=None)
    assert res is not None
    assert res.decision is not None
    assert res.security_assessment is not None


def test_wave2_staging_buffer_config_resilience():
    """Verify Wave2StagingBuffer handles EngineConfig, dict, or None gracefully."""
    # 1. With EngineConfig
    cfg = EngineConfig(wave2_staging_enabled=True, wave2_ttl_seconds=3600.0)
    buf1 = Wave2StagingBuffer(config=cfg)
    assert buf1.ttl_seconds == 3600.0

    # 2. With dict using alias
    buf2 = Wave2StagingBuffer(config={"wave2_staging_ttl_seconds": 450.0, "wave2_surge_k": 2.5})
    assert buf2.ttl_seconds == 450.0
    assert buf2.volume_surge_multiplier_k == 2.5

    # 3. With None
    buf3 = Wave2StagingBuffer(config=None)
    assert buf3.ttl_seconds == 10800.0
    assert buf3.volume_surge_multiplier_k == 2.5


@pytest.mark.anyio
async def test_chat_interface_arabic_and_supervisor_routing(tmp_path):
    """Verify ConversationalSupervisor routes Arabic queries and falls back to LLM endpoint."""
    db_file = tmp_path / "chat_test.db"
    async with SQLiteLedger(db_path=str(db_file)) as ledger:
        cfg = EngineConfig(ai_supervisor_enabled=True)
        ai_sup = AlphaSupervisorAI(config=cfg)
        ai_sup._api_key = None

        conv_sup = ConversationalSupervisor(
            ledger=ledger,
            supervisor=ai_sup,
            staging_buffer=Wave2StagingBuffer(config=cfg),
            position_book=PositionBook(),
        )

        # 1. Arabic staging query
        ans_stage = await conv_sup.ask("ما هي قائمة العملات في مرحلة المراقبة والتجميع؟")
        assert "Staging" in ans_stage or "المراقبة" in ans_stage

        # 2. Arabic performance query
        ans_perf = await conv_sup.ask("ما هي أفضل استراتيجية حققت أعلى نسبة فوز خلال 12 ساعة؟")
        assert "Evaluations" in ans_perf or "الاستراتيجية" in ans_perf

        # 3. Free-form general question routed to ai_supervisor.answer_user_query
        ans_free = await conv_sup.ask("ما هو رأيك في السوق الآن وكيف تحمي رأس المال؟")
        assert len(ans_free) > 20
        assert any(c in ans_free for c in ("محفظة", "مخاطر", "AlphaSupervisor", "السيولة", "NEUTRAL", "المشرف"))


@pytest.mark.anyio
async def test_telegram_dm_free_form_no_silent_drop():
    """Verify that an arbitrary private message is NEVER silently dropped."""
    queue = asyncio.Queue()
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)
    supervisor._api_key = None

    ingester = TelegramIngester(
        event_queue=queue,
        ai_supervisor=supervisor,
    )

    # Free-form natural language query in Arabic
    evt1 = MockDMEvent("مرحبا، هل المحرك يعمل الآن وما هي إعدادات المخاطرة؟")
    await ingester._handle_dm_message(evt1)
    assert len(evt1.replies) == 1
    assert len(evt1.replies[0]) > 0
    assert "NEUTRAL" in evt1.replies[0] or "المخاطرة" in evt1.replies[0] or "AlphaSupervisor" in evt1.replies[0]

    # Arabic command alias
    evt2 = MockDMEvent("حالة")
    await ingester._handle_dm_message(evt2)
    assert len(evt2.replies) == 1
    assert "تقرير حالة المحرك" in evt2.replies[0]
    assert "Active Lots:**" in evt2.replies[0]

    # Unknown slash command
    evt3 = MockDMEvent("/unknown_action_xyz")
    await ingester._handle_dm_message(evt3)
    assert len(evt3.replies) == 1
    assert "أمر غير معروف" in evt3.replies[0]


def test_running_metrics_alias_and_resilient_pnl():
    """Verify total_realized_pnl_usd alias and resilient extraction."""
    metrics = RunningMetrics()
    metrics.record_closed_trade(
        realized_pnl_usd=Decimal("25.50"),
        gas_cost_usd=Decimal("0.50"),
        current_equity_usd=Decimal("10025.00"),
    )
    assert metrics.cumulative_realized_usd == Decimal("25.00")
    assert metrics.total_realized_pnl_usd == 25.0

    # Test resilient PnL extraction pattern
    realized_pnl = float(
        getattr(metrics, "cumulative_realized_usd",
        getattr(metrics, "total_realized_pnl_usd", 0.0))
    )
    assert realized_pnl == 25.0


def test_missed_opportunity_watchdog_list_handling():
    """Verify that Wave2StagingBuffer get_all_staged / get_staged_tokens return list and iterate safely."""
    buf = Wave2StagingBuffer(config=EngineConfig())
    buf.stage_token("TokenTest111111111111111111111111111111111", initial_price=Decimal("0.000000028"))

    staged_tokens = buf.get_all_staged()
    assert isinstance(staged_tokens, list)
    assert len(staged_tokens) == 1

    # Alias check
    staged_alias = buf.get_staged_tokens()
    assert isinstance(staged_alias, list)
    assert len(staged_alias) == 1

    # Safe iteration pattern from runner watchdog
    staged_list = list(staged_tokens.values()) if isinstance(staged_tokens, dict) else list(staged_tokens)
    for staged in staged_list:
        assert staged.initial_price > Decimal("0")
        assert not staged.dropped
        # Test highest_price alias and setter
        assert staged.highest_price == staged.peak_price
        staged.highest_price = Decimal("0.005")
        assert staged.peak_price == Decimal("0.005")

        # Test extract_feature_vector
        vec = staged.extract_feature_vector(peak_gain_multiplier=2.5)
        assert vec.token_address == "TokenTest111111111111111111111111111111111"
        assert vec.peak_gain_multiplier == 2.5
        assert vec.dip_depth_pct >= 0.0
        assert 0.0 <= vec.net_buy_delta <= 1.0


@pytest.mark.anyio
async def test_ai_supervisor_dynamic_tuning_no_pydantic_freeze():
    """Verify that AI supervisor audit_signal updates ActionParameters without frozen_instance validation errors."""
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)
    supervisor._api_key = None

    tuner = DynamicParameterTuner()
    tuner.current_params = DynamicHyperparameters(
        max_slippage_bps=220,
        hard_stop_loss_pct=-12.5,
        trailing_stop_activation_pct=35.0,
    )
    supervisor.set_parameter_tuner(tuner)

    pool = PoolState(
        pool_address="0x" + "1" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("5.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=100,
    )
    sec_rep = SecurityReport(
        token_address="0x" + "2" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.12,
        mint_authority_disabled=True,
        verified_source_code=True,
    )
    sig = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address=pool.pool_address,
        token_address="0x" + "2" * 40,
        suggested_side=OrderSide.BUY,
        pool_state=pool,
        security_report=sec_rep,
        strength=SignalStrength.STRONG,
        alpha_score=0.90,
    )

    resp = await supervisor.audit_signal(signal=sig, pool_state=pool)
    assert resp is not None
    assert resp.action_parameters.max_slippage_bps == 220
    assert resp.action_parameters.hard_stop_loss_pct == -12.5
    assert resp.action_parameters.trailing_stop_activation_pct == 35.0


@pytest.mark.anyio
async def test_telegram_arabic_conversational_extended_coverage(tmp_path):
    """Verify Arabic conversational routing for questions with لماذا and رفض, and outgoing message handling."""
    db_file = tmp_path / "chat_arabic.db"
    async with SQLiteLedger(db_path=str(db_file)) as ledger:
        cfg = EngineConfig(ai_supervisor_enabled=True)
        ai_sup = AlphaSupervisorAI(config=cfg)
        ai_sup._api_key = None
        staging = Wave2StagingBuffer(config=cfg)
        book = PositionBook()

        conv_sup = ConversationalSupervisor(
            ledger=ledger,
            supervisor=ai_sup,
            staging_buffer=staging,
            position_book=book,
        )

        queue = asyncio.Queue()
        ingester = TelegramIngester(
            event_queue=queue,
            ai_supervisor=ai_sup,
            admin_ids=[123456789],
        )
        ingester.set_chat_explainer(conv_sup)

        # 1. Arabic query with 'لماذا' without a CA address
        evt_why = MockDMEvent("لماذا لم تدخل في هذه الصفقة؟", sender_id=123456789)
        await ingester._handle_dm_message(evt_why)
        assert len(evt_why.replies) == 1
        assert "AlphaSupervisor" in evt_why.replies[0] or "المشرف" in evt_why.replies[0]

        # 2. Arabic query with 'رفض' in conversation
        evt_reject = MockDMEvent("ما هي شروط رفض الصفقات لديك؟", sender_id=123456789)
        await ingester._handle_dm_message(evt_reject)
        assert len(evt_reject.replies) == 1

        # 3. Outgoing message is ignored
        evt_out = MockDMEvent("مرحبا", sender_id=123456789, out=True)
        await ingester._handle_dm_message(evt_out)
        assert len(evt_out.replies) == 0

        # 4. Long reply is properly chunked without throwing RPC/length errors
        long_text = "سطر طويل للاختبار والتأكد من التقسيم.\n" * 200
        evt_long = MockDMEvent("long", sender_id=123456789)
        await ingester._safe_reply(evt_long, long_text)
        assert len(evt_long.replies) > 1

