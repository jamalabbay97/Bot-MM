"""
tests/test_lifecycle_observability_and_telegram_ui.py
=====================================================
Comprehensive verification of Phase 3:
1. 7-stage token lifecycle tracing (tool_trace_lifecycle).
2. ASCII / emoji pipeline throughput & drop-off diagram (tool_pipeline_flow).
3. Deep institutional status telemetry card (tool_status_detailed).
4. Telegram UI: persistent reply keyboard & inline trade alert buttons.
5. Telegram callback query handlers (trace:, debate:) and DM command routing.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.ai import GlobalRiskMode
from alpha_engine.models.observability import TraceStage, TraceStatus


@pytest.fixture
def temp_db_path(tmp_path):
    return str(tmp_path / "test_observability.db")


@pytest.mark.asyncio
async def test_tool_trace_lifecycle_empty(temp_db_path):
    """Test tool_trace_lifecycle when no traces or deliberations exist."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        chat = ConversationalSupervisor(ledger=ledger)
        res = await chat.tool_trace_lifecycle("UnknownCA111111111111111111111111111111111")
        assert "لم يتم العثور على سجلات تتبع" in res or "No trace" in res
    finally:
        await ledger.close()


@pytest.mark.asyncio
async def test_tool_trace_lifecycle_with_7_stages(temp_db_path):
    """Test tool_trace_lifecycle returns all 7 chronological stages correctly formatted."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        chat = ConversationalSupervisor(ledger=ledger)
        token_ca = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
        chain = "solana"
        now_ns = time.time_ns()

        # Insert events for all 7 stages into token_lifecycle_trace
        stages_to_record = [
            (TraceStage.INGESTION.value, "RaydiumPoolListener", "on_pool_created", TraceStatus.PASSED.value, 12.5, None, {"pair": "SOL/TEST"}),
            (TraceStage.SECURITY.value, "SecurityGatekeeper", "audit_contract", TraceStatus.PASSED.value, 85.0, None, {"mint_authority": False}),
            (TraceStage.STAGING.value, "Wave2StagingBuffer", "stage_token", TraceStatus.PASSED.value, 4.2, None, {"volume_spike_ratio": 2.8}),
            (TraceStage.COUNCIL.value, "CouncilArbiterCRO", "deliberate", TraceStatus.PASSED.value, 210.0, None, {"decision": "EXECUTE_BUY"}),
            (TraceStage.SUPERVISOR.value, "AlphaSupervisorAI", "evaluate_macro_regime", TraceStatus.PASSED.value, 45.0, None, {"risk_mode": "NORMAL"}),
            (TraceStage.EXECUTION.value, "PaperExecutor", "execute_signal", TraceStatus.PASSED.value, 95.0, None, {"fill_price": 0.00045}),
            (TraceStage.FEEDBACK.value, "AdaptiveFeedbackEngine", "record_closed_trade", TraceStatus.PASSED.value, 8.0, None, {"realized_pnl_usd": float(Decimal("125.5"))}),
        ]

        for idx, (stg, comp, fn, status, dur, reason, out_d) in enumerate(stages_to_record):
            serialized_out = json.loads(json.dumps(out_d))
            await ledger.record_trace_event({
                "trace_id": f"trace-{idx}",
                "token_address": token_ca,
                "chain": chain,
                "stage": stg,
                "component": comp,
                "function_name": fn,
                "status": status,
                "duration_ms": dur,
                "reason": reason,
                "input_data": {},
                "output_data": serialized_out,
                "timestamp_ns": now_ns + (idx * 1_000_000_000),
            })

        trace_card = await chat.tool_trace_lifecycle(token_ca)
        assert token_ca in trace_card
        assert "7-Stage Institutional Lifecycle Trace" in trace_card
        assert "INGESTION" in trace_card
        assert "SECURITY" in trace_card
        assert "STAGING" in trace_card
        assert "COUNCIL" in trace_card
        assert "SUPERVISOR" in trace_card
        assert "EXECUTION" in trace_card
        assert "FEEDBACK" in trace_card
        assert "إجمالي زمن المعالجة عبر المراحل" in trace_card
    finally:
        await ledger.close()


@pytest.mark.asyncio
async def test_tool_pipeline_flow(temp_db_path):
    """Test tool_pipeline_flow generates formatted ASCII throughput diagram."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        chat = ConversationalSupervisor(ledger=ledger)
        # Insert mock records
        for i in range(10):
            await ledger.record_trace_event({
                "trace_id": f"flow-{i}",
                "token_address": f"token-{i}",
                "chain": "solana",
                "stage": TraceStage.INGESTION.value,
                "component": "Ingestion",
                "function_name": "detect",
                "status": TraceStatus.PASSED.value,
                "duration_ms": 10.0,
                "reason": None,
                "input_data": {},
                "output_data": {},
                "timestamp_ns": time.time_ns(),
            })

        for i in range(7):
            await ledger.record_trace_event({
                "trace_id": f"sec-{i}",
                "token_address": f"token-{i}",
                "chain": "solana",
                "stage": TraceStage.SECURITY.value,
                "component": "Gatekeeper",
                "function_name": "screen",
                "status": TraceStatus.PASSED.value if i < 4 else TraceStatus.REJECTED.value,
                "duration_ms": 15.0,
                "reason": "Honeypot" if i >= 4 else None,
                "input_data": {},
                "output_data": {},
                "timestamp_ns": time.time_ns(),
            })

        diagram = await chat.tool_pipeline_flow(lookback_hours=24.0)
        assert "مخطط تدفق خط الأنابيب المؤسسي" in diagram
        assert "INGESTION" in diagram
        assert "SECURITY" in diagram
        assert "COUNCIL" in diagram
        assert "EXECUTION" in diagram
        assert "FEEDBACK" in diagram
        assert "Passed:" in diagram
        assert "Rejected / Filtered:" in diagram
    finally:
        await ledger.close()


@pytest.mark.asyncio
async def test_tool_status_detailed(temp_db_path):
    """Test tool_status_detailed renders institutional status card."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        chat = ConversationalSupervisor(ledger=ledger)
        await ledger.set_setting("global_risk_mode", GlobalRiskMode.AGGRESSIVE.value)

        card = await chat.tool_status_detailed()
        assert "Detailed Institutional Status" in card
        assert "AGGRESSIVE" in card
        assert "Active Champion" in card
        assert "تحليل أسباب الخسارة الجذري" in card
    finally:
        await ledger.close()


def test_telegram_keyboards_and_buttons():
    """Test persistent reply keyboard and inline trade alert buttons generation."""
    from alpha_engine.ingestion.telegram import TelegramIngester

    q1 = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q1)

    # 1. Persistent reply keyboard
    kb = ingester._get_persistent_reply_keyboard()
    assert kb is not None
    assert len(kb) == 2
    # Row 1: Status, Staging, Council
    row1_texts = [getattr(b, "button", b).text for b in kb[0]]
    assert "📊 Status" in row1_texts
    assert "🌊 Staging" in row1_texts
    assert "🏛️ Council" in row1_texts
    # Row 2: Strategies, Flow, Risk Mode
    row2_texts = [getattr(b, "button", b).text for b in kb[1]]
    assert "🏆 Strategies" in row2_texts
    assert "🔬 Flow" in row2_texts
    assert "⚙️ Risk Mode" in row2_texts

    # 2. Trade alert action buttons
    ca = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    btns = ingester._get_trade_alert_inline_buttons(ca)
    assert btns is not None
    assert len(btns) == 1
    action_texts = [b.text for b in btns[0]]
    assert "🔍 Trace" in action_texts
    assert "🏛️ Debate" in action_texts
    assert "🔴 Exit Now" in action_texts
    # Check data payload
    data0 = btns[0][0].type.data if hasattr(btns[0][0], "type") else getattr(btns[0][0], "data", None)
    data1 = btns[0][1].type.data if hasattr(btns[0][1], "type") else getattr(btns[0][1], "data", None)
    assert data0 == f"trace:{ca}".encode("utf-8")
    assert data1 == f"debate:{ca}".encode("utf-8")


@pytest.mark.asyncio
async def test_telegram_callback_and_dm_routing():
    """Test Telegram callback query handler for trace: and debate: and DM dispatch."""
    from alpha_engine.ingestion.telegram import TelegramIngester

    q1 = asyncio.Queue()
    ingester = TelegramIngester(event_queue=q1, admin_ids=[12345])

    mock_chat = MagicMock()
    mock_chat.tool_trace_lifecycle = AsyncMock(return_value="Trace lifecycle output for CA")
    mock_chat.tool_council_debate = AsyncMock(return_value="Council debate output for CA")
    mock_chat.tool_pipeline_flow = AsyncMock(return_value="Pipeline flow output")
    mock_chat.tool_status_detailed = AsyncMock(return_value="Detailed status output")

    ingester._chat_explainer = mock_chat
    ingester._safe_reply = AsyncMock()

    # 1. Test callback query for trace:
    mock_event = MagicMock()
    mock_event.sender_id = 12345
    mock_event.data = b"trace:TestCA123"

    await ingester._handle_callback_query(mock_event)
    mock_chat.tool_trace_lifecycle.assert_awaited_once_with("TestCA123")
    ingester._safe_reply.assert_awaited_with(mock_event, "Trace lifecycle output for CA")

    # 2. Test callback query for debate:
    mock_event.data = b"debate:TestCA456"
    await ingester._handle_callback_query(mock_event)
    mock_chat.tool_council_debate.assert_awaited_once_with("TestCA456")
    ingester._safe_reply.assert_awaited_with(mock_event, "Council debate output for CA")

    # 3. Test DM dispatch for /flow
    mock_dm_event = MagicMock()
    mock_dm_event.is_private = True
    mock_dm_event.out = False
    mock_dm_event.sender_id = 12345
    mock_dm_event.raw_text = "/flow"

    await ingester._handle_dm_message(mock_dm_event)
    mock_chat.tool_pipeline_flow.assert_awaited_once()
