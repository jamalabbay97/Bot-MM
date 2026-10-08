"""
tests/test_lifecycle_tracer.py
==============================
Verification of Token Lifecycle Observability, Event Tracer, SQLite Ledger
persistence, and Conversational Investigation UI (/assets & /investigate).
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import ChainIdentifier, OrderSide, SecurityTier, SignalSource, SignalStrength
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.observability import LifecycleEvent, TraceStage, TraceStatus
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.tracer import LifecycleTracer, tracer


@pytest.mark.anyio
async def test_lifecycle_models_and_mutability():
    """Verify LifecycleEvent defaults, enum values, and span mutability."""
    event = LifecycleEvent(
        token_address="0xTestToken123",
        chain="base",
        stage=TraceStage.DETECTION,
        component="TelegramIngester",
        function_name="_handle_channel_message",
        input_data={"channel": "alpha_calls"},
    )
    assert event.status == TraceStatus.PENDING
    assert event.duration_ms == 0.0
    assert event.trace_id is not None
    assert event.timestamp_ns > 0

    # Ensure field mutation is allowed during span lifecycle execution
    event.status = TraceStatus.PASSED
    event.reason = "Approved by gatekeeper"
    event.output_data = {"score": 95}
    event.duration_ms = 12.5

    assert event.status == TraceStatus.PASSED
    assert event.reason == "Approved by gatekeeper"
    assert event.output_data["score"] == 95
    assert event.duration_ms == 12.5


@pytest.mark.anyio
async def test_tracer_span_success_and_error():
    """Verify LifecycleTracer.span wraps execution, records duration, and handles exceptions."""
    custom_tracer = LifecycleTracer()

    # 1. Success span
    async with custom_tracer.span(
        token_address="TokenA",
        chain="solana",
        stage=TraceStage.SECURITY_SCREENING,
        component="SecurityGatekeeper",
        function_name="screen_token",
        input_data={"pool": "pool_1"},
    ) as span:
        await asyncio.sleep(0.01)
        span.output_data = {"top10": 0.12}

    assert not custom_tracer._queue.empty()
    queued_event = custom_tracer._queue.get_nowait()
    assert queued_event.token_address == "TokenA"
    assert queued_event.status == TraceStatus.PASSED
    assert queued_event.duration_ms >= 5.0
    assert queued_event.output_data == {"top10": 0.12}

    # 2. Rejection span explicitly set
    async with custom_tracer.span(
        token_address="TokenB",
        chain="base",
        stage=TraceStage.FILTERING,
        component="BlacklistFilter",
        function_name="filter_token",
    ) as span:
        span.status = TraceStatus.REJECTED
        span.reason = "Honeypot flag detected"

    queued_event2 = custom_tracer._queue.get_nowait()
    assert queued_event2.status == TraceStatus.REJECTED
    assert queued_event2.reason == "Honeypot flag detected"

    # 3. Exception span
    with pytest.raises(ValueError, match="Simulated engine failure"):
        async with custom_tracer.span(
            token_address="TokenC",
            chain="solana",
            stage=TraceStage.AI_ANALYSIS,
            component="AlphaSupervisorAI",
            function_name="audit_signal",
        ) as span:
            raise ValueError("Simulated engine failure")

    queued_event3 = custom_tracer._queue.get_nowait()
    assert queued_event3.status == TraceStatus.ERROR
    assert "Simulated engine failure" in queued_event3.reason
    assert "traceback" in queued_event3.output_data


@pytest.mark.anyio
async def test_ledger_trace_storage_and_queries():
    """Verify SQLiteLedger records trace events and retrieves asset lists and timelines."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_traces.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            # Record events for token 1
            e1 = LifecycleEvent(
                token_address="0xTokenABC",
                chain="base",
                stage=TraceStage.DETECTION,
                component="TelegramIngester",
                function_name="_handle_channel_message",
                status=TraceStatus.PASSED,
                duration_ms=1.2,
                timestamp_ns=1_000_000_000,
            )
            e2 = LifecycleEvent(
                token_address="0xTokenABC",
                chain="base",
                stage=TraceStage.SECURITY_SCREENING,
                component="SecurityGatekeeper",
                function_name="screen_token",
                status=TraceStatus.PASSED,
                output_data={"flags": []},
                duration_ms=15.4,
                timestamp_ns=2_000_000_000,
            )
            e3 = LifecycleEvent(
                token_address="0xTokenABC",
                chain="base",
                stage=TraceStage.AI_ANALYSIS,
                component="AlphaSupervisorAI",
                function_name="audit_signal",
                status=TraceStatus.PASSED,
                output_data={"decision": "BUY", "confidence_score": 0.88, "rationale": "Strong momentum"},
                duration_ms=45.0,
                timestamp_ns=3_000_000_000,
            )

            # Record event for token 2 (rejected)
            e4 = LifecycleEvent(
                token_address="So111RejectedToken",
                chain="solana",
                stage=TraceStage.SECURITY_SCREENING,
                component="SecurityGatekeeper",
                function_name="screen_token",
                status=TraceStatus.REJECTED,
                reason="Failed Tier 1 Hard Gates",
                output_data={"top10": 0.85},
                duration_ms=8.0,
                timestamp_ns=4_000_000_000,
            )

            await ledger.record_trace_event(e1)
            await ledger.record_trace_event(e2)
            await ledger.record_trace_event(e3)
            await ledger.record_trace_event(e4)

            # Test get_recent_assets
            assets = await ledger.get_recent_assets(limit=10)
            assert len(assets) == 2
            assert "So111RejectedToken" in assets
            assert "0xTokenABC" in assets

            # Test get_asset_trace for 0xTokenABC
            timeline = await ledger.get_asset_trace("0xTokenABC")
            assert len(timeline) == 3
            assert timeline[0]["stage"] == "DETECTION"
            assert timeline[1]["stage"] == "SECURITY_SCREENING"
            assert timeline[2]["stage"] == "AI_ANALYSIS"
            assert timeline[2]["component"] == "AlphaSupervisorAI"

            # Case insensitivity check for EVM address
            timeline_lower = await ledger.get_asset_trace("0xtokenabc")
            assert len(timeline_lower) == 3


@pytest.mark.anyio
async def test_tracer_background_worker_flush():
    """Verify background worker flushes events to ledger seamlessly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_worker.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            tracer_inst = LifecycleTracer(ledger=ledger)
            tracer_inst.start_worker()

            async with tracer_inst.span("TokenFlush1", "solana", TraceStage.INGESTION, "Coord", "step1"):
                pass
            async with tracer_inst.span("TokenFlush1", "solana", TraceStage.STAGING, "Wave2", "step2"):
                pass

            # Stop worker will drain the queue
            await tracer_inst.stop_worker()

            traces = await ledger.get_asset_trace("TokenFlush1")
            assert len(traces) == 2
            assert traces[0]["stage"] == "INGESTION"
            assert traces[1]["stage"] == "STAGING"


@pytest.mark.anyio
async def test_conversational_supervisor_assets_and_investigate():
    """Verify /assets and /investigate in ConversationalSupervisor."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_chat.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            # Seed traces
            event1 = LifecycleEvent(
                token_address="0xTargetAlphaContract",
                chain="base",
                stage=TraceStage.DETECTION,
                component="TelegramIngester",
                function_name="_handle_channel_message",
                status=TraceStatus.PASSED,
                duration_ms=2.1,
                timestamp_ns=time.time_ns() - 10_000_000_000,
            )
            event2 = LifecycleEvent(
                token_address="0xTargetAlphaContract",
                chain="base",
                stage=TraceStage.SECURITY_SCREENING,
                component="SecurityGatekeeper",
                function_name="screen_token",
                status=TraceStatus.PASSED,
                duration_ms=18.5,
                timestamp_ns=time.time_ns() - 5_000_000_000,
            )
            event3 = LifecycleEvent(
                token_address="0xTargetAlphaContract",
                chain="base",
                stage=TraceStage.AI_ANALYSIS,
                component="AlphaSupervisorAI",
                function_name="audit_signal",
                status=TraceStatus.PASSED,
                output_data={
                    "decision": "EXECUTE_BUY",
                    "confidence_score": 0.92,
                    "rationale": "Whale cluster buy detected with locked liquidity",
                },
                duration_ms=65.2,
                timestamp_ns=time.time_ns(),
            )
            await ledger.record_trace_event(event1)
            await ledger.record_trace_event(event2)
            await ledger.record_trace_event(event3)

            supervisor = ConversationalSupervisor(ledger=ledger)

            # 1. Ask /assets
            assets_resp = await supervisor.ask("/assets")
            assert "العملات المكتشفة مؤخراً" in assets_resp
            assert "0xTargetAlphaContract" in assets_resp
            assert "1." in assets_resp

            # 2. Ask /investigate 1 (numerical ID mapped)
            inv_resp_id = await supervisor.ask("/investigate 1")
            assert "تحقيق شامل حول العملة" in inv_resp_id
            assert "0xTargetAlphaContract" in inv_resp_id
            assert "DETECTION" in inv_resp_id
            assert "SECURITY_SCREENING" in inv_resp_id
            assert "AI_ANALYSIS" in inv_resp_id
            assert "EXECUTE_BUY" in inv_resp_id
            assert "Whale cluster buy detected" in inv_resp_id

            # 3. Ask /investigate with direct CA
            inv_resp_ca = await supervisor.ask("/investigate 0xTargetAlphaContract")
            assert "0xTargetAlphaContract" in inv_resp_ca
            assert "DETECTION" in inv_resp_ca

            # 4. Arabic command: تحقيق 1
            inv_resp_ar = await supervisor.ask("تحقيق 1")
            assert "تحقيق شامل حول العملة" in inv_resp_ar

            # 5. Non-existent asset
            inv_missing = await supervisor.ask("/investigate 999")
            assert "لم يتم العثور على تتبع للعملة" in inv_missing


class MockTelegramEvent:
    def __init__(self, text: str):
        self.raw_text = text
        self.is_private = True
        self.out = False
        self.replies: list[str] = []

    async def reply(self, msg: str, buttons: Any = None, **kwargs: Any):
        self.replies.append(msg)


@pytest.mark.anyio
async def test_telegram_direct_commands_assets_and_investigate():
    """Verify TelegramIngester routes /assets and /investigate commands in DMs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_tg.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            ev = LifecycleEvent(
                token_address="So111TelegramTestCA",
                chain="solana",
                stage=TraceStage.DETECTION,
                component="TelegramIngester",
                function_name="_handle_channel_message",
                status=TraceStatus.PASSED,
                duration_ms=1.5,
                timestamp_ns=time.time_ns(),
            )
            await ledger.record_trace_event(ev)

            explainer = ConversationalSupervisor(ledger=ledger)

            from alpha_engine.ingestion.telegram import TelegramIngester
            q = asyncio.Queue()
            ingester = TelegramIngester(event_queue=q)
            ingester.set_chat_explainer(explainer)

            # Test /assets in Telegram
            event_assets = MockTelegramEvent("/assets")
            await ingester._handle_dm_message(event_assets)
            assert len(event_assets.replies) == 1
            assert "So111TelegramTestCA" in event_assets.replies[0]

            # Test /investigate 1 in Telegram
            event_inv = MockTelegramEvent("/investigate 1")
            await ingester._handle_dm_message(event_inv)
            assert len(event_inv.replies) == 1
            assert "So111TelegramTestCA" in event_inv.replies[0]
            assert "DETECTION" in event_inv.replies[0]


@pytest.mark.anyio
async def test_ai_supervisor_auditing_traces():
    """Verify AlphaSupervisorAI emits AI_ANALYSIS trace spans during audit_signal."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ai_trace.db"
        async with SQLiteLedger(db_path=db_path) as ledger:
            tracer.ledger = ledger
            tracer.start_worker()

            cfg = EngineConfig(ai_supervisor_enabled=True)
            supervisor = AlphaSupervisorAI(config=cfg)
            supervisor.set_ledger(ledger)

            pool = PoolState(
                pool_address="0x6c561b446416e1a00e8e93e221854d6eA4171372",
                chain=ChainIdentifier.BASE_MAINNET,
                token_reserve=Decimal("1000000"),
                native_reserve=Decimal("100"),
                fee_numerator=3,
                fee_denominator=1000,
                last_updated_block=123456,
                token_decimals=18,
                native_decimals=18,
            )
            test_token = "0x1111111111111111111111111111111111111111"
            sec_rep = SecurityReport(
                token_address=test_token,
                chain=ChainIdentifier.BASE_MAINNET,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                buy_tax_bps=0,
                sell_tax_bps=0,
                lp_burned_ratio=1.0,
                mint_authority_disabled=True,
                top10_concentration=0.15,
            )

            signal = SignalEvent(
                timestamp_ns=time.time_ns(),
                chain=ChainIdentifier.BASE_MAINNET,
                token_address=test_token,
                pool_address="0x6c561b446416e1a00e8e93e221854d6eA4171372",
                suggested_side=OrderSide.BUY,
                strength=SignalStrength.STRONG,
                alpha_score=0.88,
                source=SignalSource.WHALE_WALLET,
                pool_state=pool,
                security_report=sec_rep,
            )

            resp = await supervisor.audit_signal(signal)
            assert resp is not None

            await tracer.stop_worker()

            traces = await ledger.get_asset_trace(test_token)
            assert len(traces) >= 1
            ai_traces = [t for t in traces if t["stage"] == "AI_ANALYSIS"]
            assert len(ai_traces) >= 1
            ai_trace = ai_traces[0]
            assert ai_trace["stage"] == "AI_ANALYSIS"
            assert ai_trace["component"] == "AlphaSupervisorAI"
            assert ai_trace["function_name"] == "audit_signal"
            assert ai_trace["status"] in ("PASSED", "REJECTED")
            await supervisor.close()
