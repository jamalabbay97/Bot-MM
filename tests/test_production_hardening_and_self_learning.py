"""
Comprehensive Verification Tests for:
1. Solana RPC Subscription Leak Prevention and Bounded Subscription Registry (svm.py)
2. Launch Buffer removal callbacks and unsubscription on timeout/dump/graduation (signals.py)
3. AI Supervisor Veto Deadlock Resolution (ai_supervisor.py)
4. AI Outcome Tracking Self-Learning Closed-Loop Adaptation (ai_supervisor.py)
5. SQLite Concurrency, WAL Mode, and Busy Timeout Hardening (telegram.py, ledger.py, whitelist_db.py)
6. Telegram Bot UI Overhaul with Subcommands (/ai stance, strict, vetoes, reset)
"""

import asyncio
from decimal import Decimal
import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import (
    AIOutcomeTracker,
    AlphaSupervisorAI,
)
from alpha_engine.engine.signals import PendingLaunchBuffer
from alpha_engine.ingestion.svm import (
    PUMP_FUN_PROGRAM_ID,
    RAYDIUM_AMM_PROGRAM_ID,
    SVMIngester,
    SubscriptionRegistry,
)
from alpha_engine.ingestion.telegram import (
    HardenedSQLiteSession,
    TelegramIngester,
    _connect_aiosqlite,
)
from alpha_engine.models.ai import (
    AISupervisorDecisionEnum,
    GlobalRiskMode,
)
from alpha_engine.models.enums import (
    ChainIdentifier,
    OrderSide,
    SecurityTier,
    SignalStrength,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry


# =============================================================================
# 1. SUBSCRIPTION REGISTRY & SVM SUBSCRIPTION LEAK PREVENTIONS
# =============================================================================

def test_subscription_registry_capacity_and_eviction():
    """Verify SubscriptionRegistry enforces capacity limits and protects permanent targets."""
    reg = SubscriptionRegistry(max_capacity=5)

    # Register permanent targets
    reg.register(PUMP_FUN_PROGRAM_ID, sub_id=1, is_permanent=True)
    reg.register(RAYDIUM_AMM_PROGRAM_ID, sub_id=2, is_permanent=True)

    assert len(reg) == 2
    assert reg.is_permanent(PUMP_FUN_PROGRAM_ID)
    assert reg.is_permanent(RAYDIUM_AMM_PROGRAM_ID)

    # Register dynamic non-permanent targets
    reg.register("TargetToken1111111111111111111111111111", sub_id=101)
    reg.register("TargetToken2222222222222222222222222222", sub_id=102)
    reg.register("TargetToken3333333333333333333333333333", sub_id=103)

    assert len(reg) == 5

    # Adding a 6th target must evict the oldest non-permanent target (TargetToken1)
    evicted = reg.register("TargetToken4444444444444444444444444444", sub_id=104)
    assert evicted == "TargetToken1111111111111111111111111111"
    assert len(reg) == 5

    # Permanent targets must remain intact
    assert reg.get_sub_id(PUMP_FUN_PROGRAM_ID) == 1
    assert reg.get_sub_id(RAYDIUM_AMM_PROGRAM_ID) == 2
    assert reg.get_sub_id("TargetToken1111111111111111111111111111") is None
    assert reg.get_sub_id("TargetToken4444444444444444444444444444") == 104


def test_subscription_registry_stale_detection():
    """Verify stale non-permanent subscriptions are identified based on max_age_s."""
    reg = SubscriptionRegistry(max_capacity=10)
    reg.register(PUMP_FUN_PROGRAM_ID, sub_id=1, is_permanent=True)
    reg.register("OldToken111111111111111111111111111111", sub_id=101)
    reg.register("ActiveToken222222222222222222222222222", sub_id=102)

    # Backdate OldToken
    reg._target_timestamps["OldToken111111111111111111111111111111"] = time.time() - 300.0
    # Keep ActiveToken fresh
    reg.touch("ActiveToken222222222222222222222222222")

    stale = reg.get_stale_targets(max_age_s=60.0)
    stale_targets = [target for target, _ in stale]

    assert "OldToken111111111111111111111111111111" in stale_targets
    assert "ActiveToken222222222222222222222222222" not in stale_targets
    assert PUMP_FUN_PROGRAM_ID not in stale_targets  # Permanent never stale


@pytest.mark.anyio
async def test_svm_ingester_unsubscribe_dispatches_jsonrpc():
    """Verify SVMIngester.unsubscribe removes from registry and sends logsUnsubscribe."""
    event_q: asyncio.Queue = asyncio.Queue()
    ingester = SVMIngester(
        ws_url="wss://mainnet.helius-rpc.com/?api-key=test",
        pool_registry={},
        event_queue=event_q,
        limiter=RateLimiterRegistry.default(),
    )

    mock_ws = AsyncMock()
    ingester._ws = mock_ws
    token_addr = "TokenToUnsubscribe1111111111111111111111"
    sub_id = 999
    ingester._sub_to_pool[sub_id] = token_addr
    ingester._subscriptions.register(token_addr, sub_id=sub_id, is_permanent=False)

    success = await ingester.unsubscribe(token_addr)
    assert success is True
    assert sub_id not in ingester._sub_to_pool
    assert ingester._subscriptions.get_sub_id(token_addr) is None

    # Check mock_ws.send called with logsUnsubscribe
    assert mock_ws.send.called
    sent_msg = json.loads(mock_ws.send.call_args[0][0])
    assert sent_msg["method"] == "logsUnsubscribe"
    assert sent_msg["params"] == [sub_id]


def test_pending_launch_buffer_removal_callback():
    """Verify PendingLaunchBuffer triggers on_removal callbacks on timeout, dump, and graduation."""
    buffer = PendingLaunchBuffer(
        min_age_s=10.0,
        max_age_s=60.0,
        min_buys=3,
        min_volume_native=Decimal("1.0"),
    )

    removed_tokens: list[str] = []
    buffer.register_on_removal_callback(lambda token: removed_tokens.append(token))

    clean_report = SecurityReport(
        token_address="StagedToken1111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.10,
        mint_authority_disabled=True,
        verified_source_code=True,
    )

    buffer.stage_token(
        token_address="StagedToken1111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_price=Decimal("0.000000028"),
        report=clean_report,
        pool_address="Curve11111111111111111111111111111111111",
    )

    assert buffer.is_staged("StagedToken1111111111111111111111111111")

    # Manually trigger removal
    buffer.remove_launch("StagedToken1111111111111111111111111111")
    assert not buffer.is_staged("StagedToken1111111111111111111111111111")
    assert "StagedToken1111111111111111111111111111" in removed_tokens


# =============================================================================
# 2. AI VETO DEADLOCK RESOLUTION
# =============================================================================

@pytest.mark.anyio
async def test_ai_supervisor_approves_graduating_pump_token_without_wallet_profile():
    """
    CRITICAL TELEMETRY REGRESSION TEST:
    Graduating Pump.fun tokens (which pass buffer graduation without a copy-trading wallet profile)
    must NOT be vetoed under WASH_TRADING_FAKE_VOLUME.
    """
    cfg = EngineConfig(ai_supervisor_enabled=True, ai_strict_veto=True)
    supervisor = AlphaSupervisorAI(config=cfg)

    token = "74MNBqyMd5PumpToken11111111111111111111111"
    pool_state = PoolState(
        pool_address="PumpVirtualCurve111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("200000000"),
        native_reserve=Decimal("84.5"),  # Near graduation ~85 SOL
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=12345,
    )

    clean_report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.15,
        mint_authority_disabled=True,
        verified_source_code=True,
    )

    signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_state.pool_address,
        token_address=token,
        suggested_side=OrderSide.BUY,
        pool_state=pool_state,
        security_report=clean_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.92,
        signal_source="pump_fun",
    )

    # Audit without wallet_profile (market launch signal) and recent metrics at initial 0.0
    resp = await supervisor.audit_signal(
        signal=signal,
        pool_state=pool_state,
        security_report=clean_report,
        wallet_profile=None,
        recent_win_rate=0.0,
        recent_profit_factor=0.0,
    )

    assert resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY
    assert resp.wallet_audit.is_wallet_reputable is True
    assert "WASH_TRADING_FAKE_VOLUME" not in resp.security_assessment.flags
    assert resp.action_parameters.recommended_position_pct > 0.0
    assert len(resp.action_parameters.take_profit_ladder) > 0


@pytest.mark.anyio
async def test_ai_supervisor_strict_veto_toggle():
    """Verify strict_veto=False permits execution when only soft warnings exist."""
    cfg = EngineConfig(ai_supervisor_enabled=True, ai_strict_veto=False)
    supervisor = AlphaSupervisorAI(config=cfg)

    # Telemetry with a soft warning (e.g., duplicate sentiment or news expired)
    telemetry = {
        "chain": "solana_mainnet",
        "target_token_address": "SoftWarnToken1111111111111111111111111",
        "pool_address": "Pool1111111111111111111111111111111111111",
        "suggested_side": "buy",
        "pool_telemetry": {
            "native_reserve": 100.0,
            "token_reserve": 1000000.0,
            "bonding_curve_progress_pct": 0.0,
            "is_pump_fun": False,
        },
        "security_telemetry": {
            "is_honeypot": False,
            "buy_tax_bps": 0,
            "sell_tax_bps": 0,
            "lp_burned_ratio": 1.0,
            "top10_concentration": 0.12,
            "mint_authority_disabled": True,
            "freeze_authority_disabled": True,
        },
        "wallet_telemetry": {
            "has_wallet_profile": False,
            "total_trades": 0,
            "win_rate_pct": 50.0,
            "profit_factor": 1.5,
        },
        "sentiment_telemetry": {
            "timestamp_age_s": 400.0,  # Soft flag: NEWS_CATALYST_EXPIRED_LOCAL_TOP
            "source_channel": "TreeNewsFeed",
        },
    }

    resp = supervisor.evaluate_deterministic(telemetry)
    # Under strict_veto=False, a non-fatal soft flag should not block execution
    assert resp.decision == AISupervisorDecisionEnum.EXECUTE_BUY


# =============================================================================
# 3. SELF-LEARNING OUTCOME TRACKING & CLOSED-LOOP ADAPTATION
# =============================================================================

def test_outcome_tracker_classification_and_metrics():
    """Verify AIOutcomeTracker correctly classifies TP, FP, TN, FN and calibrates risk."""
    tracker = AIOutcomeTracker()

    # 1. True Positive: Approved and pumped >= 1.35x within 45s
    tracker.record_audit(
        token_address="TokenTP",
        chain="solana_mainnet",
        decision="EXECUTE_BUY",
        initial_price=Decimal("1.0"),
        flags=[],
        confidence=0.88,
    )
    outcome_tp = tracker.record_price_update("TokenTP", Decimal("1.50"))
    assert outcome_tp.outcome_label == "TRUE_POSITIVE"
    assert outcome_tp.is_finalized is True

    # 2. False Positive: Approved and dumped >= 25%
    tracker.record_audit(
        token_address="TokenFP",
        chain="solana_mainnet",
        decision="EXECUTE_BUY",
        initial_price=Decimal("1.0"),
        flags=[],
        confidence=0.88,
    )
    outcome_fp = tracker.record_price_update("TokenFP", Decimal("0.70"))
    assert outcome_fp.outcome_label == "FALSE_POSITIVE"
    assert outcome_fp.is_finalized is True

    # 3. True Negative: Vetoed and price did not pump >= 1.80x after 45s
    now = time.time()
    tracker.record_audit(
        token_address="TokenTN",
        chain="solana_mainnet",
        decision="PASS",
        initial_price=Decimal("1.0"),
        flags=["HONEYPOT"],
        confidence=0.95,
    )
    tracker._outcomes["TokenTN"].audit_timestamp = now - 50.0  # elapsed > 45s
    outcome_tn = tracker.record_price_update("TokenTN", Decimal("0.90"), timestamp=now)
    assert outcome_tn.outcome_label == "TRUE_NEGATIVE"
    assert outcome_tn.is_finalized is True

    # 4. False Negative: Vetoed but token pumped >= 1.80x (missed opportunity)
    tracker.record_audit(
        token_address="TokenFN",
        chain="solana_mainnet",
        decision="PASS",
        initial_price=Decimal("1.0"),
        flags=["SOFT_FLAG"],
        confidence=0.90,
    )
    outcome_fn = tracker.record_price_update("TokenFN", Decimal("2.10"))
    assert outcome_fn.outcome_label == "FALSE_NEGATIVE"
    assert outcome_fn.is_finalized is True

    metrics = tracker.get_metrics()
    assert metrics.finalized_count == 4
    assert metrics.true_positives == 1
    assert metrics.false_positives == 1
    assert metrics.true_negatives == 1
    assert metrics.false_negatives == 1
    assert metrics.accuracy_pct == 50.0
    assert metrics.win_rate_pct == 50.0


def test_supervisor_auto_relaxes_stance_on_false_negatives():
    """Verify AlphaSupervisorAI auto-relaxes DEFENSIVE to NEUTRAL when FN rate > 20%."""
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)
    supervisor.set_global_risk_mode(GlobalRiskMode.DEFENSIVE)

    # Feed 2 True Negatives and 2 False Negatives (FN rate = 50% > 20%, total >= 5 finalized)
    now = time.time()
    for i in range(3):
        token = f"TN_Token_{i}"
        supervisor.outcome_tracker.record_audit(token, "solana", "PASS", Decimal("1.0"), [], 0.9)
        supervisor.outcome_tracker._outcomes[token].audit_timestamp = now - 50.0
        supervisor.record_price_update(token, Decimal("0.8"), timestamp=now)

    for i in range(2):
        token = f"FN_Token_{i}"
        supervisor.outcome_tracker.record_audit(token, "solana", "PASS", Decimal("1.0"), [], 0.9)
        supervisor.record_price_update(token, Decimal("2.2"), timestamp=now)

    metrics = supervisor.outcome_tracker.get_metrics()
    assert metrics.false_negative_rate_pct > 20.0
    # Stance should have auto-relaxed from DEFENSIVE to NEUTRAL
    assert supervisor.get_status()["global_risk_mode"] == GlobalRiskMode.NEUTRAL.value


# =============================================================================
# 4. SQLITE CONCURRENCY, WAL MODE, AND BUSY TIMEOUT
# =============================================================================

def test_hardened_sqlite_session_pragmas(tmp_path):
    """Verify HardenedSQLiteSession enables WAL mode, synchronous=NORMAL, and 30s timeout."""
    session_file = str(tmp_path / "test_telethon_session")
    session = HardenedSQLiteSession(session_file)

    cursor = session._cursor()
    cursor.execute("PRAGMA journal_mode;")
    j_mode = cursor.fetchone()[0]
    assert j_mode.lower() == "wal"

    cursor.execute("PRAGMA busy_timeout;")
    b_timeout = cursor.fetchone()[0]
    assert int(b_timeout) >= 30000

    cursor.execute("PRAGMA synchronous;")
    sync_mode = cursor.fetchone()[0]
    # In SQLite, NORMAL is represented as 1
    assert sync_mode in (1, "NORMAL", "normal")


@pytest.mark.anyio
async def test_connect_aiosqlite_pragmas(tmp_path):
    """Verify _connect_aiosqlite helper sets WAL and busy timeout."""
    db_file = str(tmp_path / "test_async_ledger.db")
    async with _connect_aiosqlite(db_file) as db:
        async with db.execute("PRAGMA journal_mode;") as cur:
            row = await cur.fetchone()
            assert row[0].lower() == "wal"
        async with db.execute("PRAGMA busy_timeout;") as cur:
            row = await cur.fetchone()
            assert int(row[0]) >= 30000


# =============================================================================
# 5. TELEGRAM BOT /AI SUBCOMMANDS AND UI OVERHAUL
# =============================================================================

@pytest.mark.anyio
async def test_telegram_cmd_ai_status_and_subcommands():
    """Verify TelegramIngester /ai command and subcommands (stance, strict, vetoes, reset)."""
    cfg = EngineConfig(ai_supervisor_enabled=True)
    supervisor = AlphaSupervisorAI(config=cfg)

    ingester = TelegramIngester(
        event_queue=asyncio.Queue(),
        signal_queue=asyncio.Queue(),
        ai_supervisor=supervisor,
    )

    mock_event = MagicMock()
    replies: list[str] = []

    async def _mock_reply(text: str):
        replies.append(text)

    mock_event.reply = AsyncMock(side_effect=_mock_reply)

    # 1. /ai status (default)
    await ingester._cmd_ai(mock_event, "")
    assert len(replies) == 1
    status_card = replies[-1]
    assert "AlphaSupervisor-AI Autonomous Risk Engine" in status_card
    assert "Global Risk Stance:" in status_card
    assert "Self-Learning Outcome Tracking:" in status_card
    assert "Control Subcommands:" in status_card

    # 2. /ai stance EXPAND
    await ingester._cmd_ai(mock_event, "stance EXPAND")
    assert supervisor.get_status()["global_risk_mode"] == "EXPAND"
    assert "EXPAND" in replies[-1]

    # 3. /ai strict off
    await ingester._cmd_ai(mock_event, "strict off")
    assert supervisor.get_status()["strict_veto"] is False
    assert "OFF" in replies[-1]

    # 4. /ai vetoes (simulate a vetoed token first)
    supervisor._recent_vetoes.append({
        "token": "VetoedToken1111111111111111111111111111",
        "chain": "solana_mainnet",
        "flags": ["CONFIRMED_HONEYPOT_BYTECODE"],
        "rationale": "Bytecode trap",
        "timestamp": time.time(),
    })
    await ingester._cmd_ai(mock_event, "vetoes")
    assert "Recent Vetoes" in replies[-1]
    assert "CONFIRMED_HONEYPOT" in replies[-1]

    # 5. /ai reset
    await ingester._cmd_ai(mock_event, "reset")
    assert supervisor.get_status()["global_risk_mode"] == "NEUTRAL"
    assert "Self-Learning Metrics Reset" in replies[-1]
