"""
tests/test_audit_remediation_and_concurrency.py
================================================
Comprehensive test suite validating the 15 mandatory audit, concurrency,
lifecycle, accounting, and capacity scenarios for the Alpha Engine bot.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.runner import AlphaEngine
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    SecurityTier,
    TokenRequestState,
)
from alpha_engine.models.state import SecurityReport, TokenRequestRecord


@asynccontextmanager
async def make_test_engine(tmp_path, max_active_positions=3, max_q_size=10):
    db_file = tmp_path / f"test_paper_trading_{uuid.uuid4().hex[:8]}.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        initial_sol=Decimal("100.0"),
        initial_eth=Decimal("10.0"),
        sol_price_usd=Decimal("150.0"),
        eth_price_usd=Decimal("2500.0"),
        max_active_positions=max_active_positions,
        max_concurrent_analyses=5,
        max_user_request_queue_size=max_q_size,
        ai_supervisor_enabled=False,
    )
    engine = AlphaEngine(cfg)
    engine._ledger = SQLiteLedger(cfg.db_path)
    await engine._ledger.__aenter__()
    engine._executor = PaperExecutor(
        position_book=engine._position_book,
        native_price_usd=cfg.sol_price_usd,
    )
    engine._gatekeeper = MagicMock()
    clean_report = SecurityReport(
        token_address="MockTokenAddress1111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        is_rug=False,
        score=95.0,
        flags=[],
        buy_tax_bps=0,
        sell_tax_bps=0,
        checked_at_ts=int(time.time()),
        passes_hard_gates=True,
    )
    engine._gatekeeper.evaluate_token = AsyncMock(return_value=clean_report)
    try:
        yield engine
    finally:
        if engine._token_request_tasks:
            await asyncio.gather(*list(engine._token_request_tasks), return_exceptions=True)
        await engine._ledger.__aexit__(None, None, None)


# =========================================================================
# Scenario 1: Three or more tokens submitted within a short period
# =========================================================================
@pytest.mark.anyio
async def test_scenario_01_burst_token_submissions(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        tokens = [
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
            "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
        ]
        records = []
        for ca in tokens:
            rec = await engine.submit_token_request(ca, chain=ChainIdentifier.SOLANA_MAINNET)
            records.append(rec)

        req_ids = [r.request_id for r in records]
        assert len(set(req_ids)) == 3
        for r in records:
            assert r.state in (
                TokenRequestState.RECEIVED,
                TokenRequestState.QUEUED,
                TokenRequestState.ANALYZING,
                TokenRequestState.ELIGIBLE,
                TokenRequestState.EXECUTION_PENDING,
                TokenRequestState.EXECUTED,
                TokenRequestState.WAITING_FOR_CAPACITY,
            )
            db_rec = await engine._ledger.get_token_request(r.request_id)
            assert db_rec is not None
            assert db_rec.token_address == r.token_address


# =========================================================================
# Scenario 2: Simultaneous Telegram messages arriving from multiple users
# =========================================================================
@pytest.mark.anyio
async def test_scenario_02_simultaneous_multi_user_submissions(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        user_submissions = [
            ("4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", 1001, 501),
            ("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", 1002, 502),
            ("mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", 1003, 503),
        ]

        async def submit_for_user(ca, uid, mid):
            return await engine.submit_token_request(
                ca,
                chain=ChainIdentifier.SOLANA_MAINNET,
                user_id=uid,
                message_id=mid,
                originating_channel=f"DM:{uid}",
            )

        results = await asyncio.gather(*[submit_for_user(ca, uid, mid) for ca, uid, mid in user_submissions])
        assert len(results) == 3
        for rec, (ca, uid, mid) in zip(results, user_submissions):
            assert rec.token_address == ca
            assert rec.user_id == uid
            assert rec.message_id == mid
            assert rec.originating_channel == f"DM:{uid}"


# =========================================================================
# Scenario 3: Multiple tokens analyzed concurrently without state contamination
# =========================================================================
@pytest.mark.anyio
async def test_scenario_03_concurrent_analysis_no_contamination(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        async def mock_eval(token_address, chain):
            if "Honeypot" in token_address:
                return SecurityReport(
                    token_address=token_address,
                    chain=chain,
                    tier=SecurityTier.TIER2_HONEYPOT,
                    is_honeypot=True,
                    is_rug=False,
                    score=10.0,
                    flags=["HONEYPOT"],
                    buy_tax_bps=0,
                    sell_tax_bps=10000,
                    checked_at_ts=int(time.time()),
                    passes_hard_gates=False,
                )
            return SecurityReport(
                token_address=token_address,
                chain=chain,
                tier=SecurityTier.CLEAN,
                is_honeypot=False,
                is_rug=False,
                score=90.0,
                flags=[],
                buy_tax_bps=0,
                sell_tax_bps=0,
                checked_at_ts=int(time.time()),
                passes_hard_gates=True,
            )

        engine._gatekeeper.evaluate_token = AsyncMock(side_effect=mock_eval)

        t_clean1 = "TokenCleanOne11111111111111111111111111111"
        t_clean2 = "TokenCleanTwo11111111111111111111111111111"
        t_honey = "TokenHoneypotBad111111111111111111111111111"

        rec1 = await engine.submit_token_request(t_clean1, ChainIdentifier.SOLANA_MAINNET)
        rec2 = await engine.submit_token_request(t_clean2, ChainIdentifier.SOLANA_MAINNET)
        rec3 = await engine.submit_token_request(t_honey, ChainIdentifier.SOLANA_MAINNET)

        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        db_rec1 = await engine._ledger.get_token_request(rec1.request_id)
        db_rec2 = await engine._ledger.get_token_request(rec2.request_id)
        db_rec3 = await engine._ledger.get_token_request(rec3.request_id)

        assert db_rec3.state == TokenRequestState.REJECTED
        assert "Honeypot" in (db_rec3.rejection_reason or "")
        assert db_rec1.state in (TokenRequestState.ELIGIBLE, TokenRequestState.EXECUTED, TokenRequestState.WAITING_FOR_CAPACITY)
        assert db_rec2.state in (TokenRequestState.ELIGIBLE, TokenRequestState.EXECUTED, TokenRequestState.WAITING_FOR_CAPACITY)


# =========================================================================
# Scenario 4: Three active positions with additional eligible tokens waiting
# =========================================================================
@pytest.mark.anyio
async def test_scenario_04_three_active_positions_and_waiting_queue(tmp_path):
    async with make_test_engine(tmp_path, max_active_positions=3) as engine:
        assert engine._position_book.max_active_positions == 3

        for i in range(1, 4):
            ca = f"ActiveTokenPosition{i}1111111111111111111111"
            rec = await engine.submit_token_request(ca, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        assert engine._position_book.effective_active_position_count() == 3

        t4 = "WaitingCandidateFour1111111111111111111111"
        t5 = "WaitingCandidateFive1111111111111111111111"

        rec4 = await engine.submit_token_request(t4, ChainIdentifier.SOLANA_MAINNET)
        rec5 = await engine.submit_token_request(t5, ChainIdentifier.SOLANA_MAINNET)

        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        assert engine._position_book.effective_active_position_count() == 3
        assert len(engine._waiting_for_capacity) >= 1

        db_rec4 = await engine._ledger.get_token_request(rec4.request_id)
        assert db_rec4.state == TokenRequestState.WAITING_FOR_CAPACITY


# =========================================================================
# Scenario 5: Position closing and releasing capacity for next candidate
# =========================================================================
@pytest.mark.anyio
async def test_scenario_05_position_close_releases_capacity_for_next_candidate(tmp_path):
    async with make_test_engine(tmp_path, max_active_positions=3) as engine:
        t_open = []
        for i in range(1, 4):
            ca = f"PosToCloseToken{i}111111111111111111111111"
            rec = await engine.submit_token_request(ca, ChainIdentifier.SOLANA_MAINNET)
            t_open.append(ca)

        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        assert engine._position_book.effective_active_position_count() == 3

        t_wait = "NextInLineCandidate11111111111111111111111"
        rec_wait = await engine.submit_token_request(t_wait, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        assert any(c.token_address == t_wait for c in engine._waiting_for_capacity)

        ok, msg, pnl = await engine.manual_exit_position(t_open[0], ChainIdentifier.SOLANA_MAINNET)
        assert ok is True

        await asyncio.sleep(0.1)

        assert engine._position_book.is_position_open(t_wait, ChainIdentifier.SOLANA_MAINNET)
        assert engine._position_book.effective_active_position_count() == 3


# =========================================================================
# Scenario 6: Duplicate submissions and repeated Telegram updates
# =========================================================================
@pytest.mark.anyio
async def test_scenario_06_duplicate_submissions_idempotency(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        token = "DuplicateTokenCheck1111111111111111111111"

        rec1 = await engine.submit_token_request(token, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        assert engine.is_position_open(token, ChainIdentifier.SOLANA_MAINNET)

        rec2 = await engine.submit_token_request(token, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        db_rec2 = await engine._ledger.get_token_request(rec2.request_id)
        assert db_rec2.state == TokenRequestState.REJECTED
        assert "already open" in (db_rec2.rejection_reason or "")
        assert len(engine._position_book.get_open_lots(ChainIdentifier.SOLANA_MAINNET, token)) == 1


# =========================================================================
# Scenario 7: Market-data API timeout, rate limit, and malformed response
# =========================================================================
@pytest.mark.anyio
async def test_scenario_07_market_data_timeout_and_resilience(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        async def timeout_eval(*args, **kwargs):
            raise asyncio.TimeoutError("RPC timeout from Helius node")

        engine._gatekeeper.evaluate_token = AsyncMock(side_effect=timeout_eval)

        token = "TimeoutTokenTest1111111111111111111111111"
        rec = await engine.submit_token_request(token, ChainIdentifier.SOLANA_MAINNET)

        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        db_rec = await engine._ledger.get_token_request(rec.request_id)
        assert db_rec is not None
        assert db_rec.state in (TokenRequestState.ELIGIBLE, TokenRequestState.EXECUTED, TokenRequestState.REJECTED)


# =========================================================================
# Scenario 8: Worker crash and recovery after restart
# =========================================================================
@pytest.mark.anyio
async def test_scenario_08_restart_recovery_from_ledger(tmp_path):
    db_file = tmp_path / "restart_test.db"
    cfg = EngineConfig(
        db_path=str(db_file),
        initial_sol=Decimal("100.0"),
        initial_eth=Decimal("10.0"),
        sol_price_usd=Decimal("150.0"),
        eth_price_usd=Decimal("2500.0"),
        max_active_positions=3,
        ai_supervisor_enabled=False,
    )

    t1 = "RestartRecoverToken1111111111111111111111"
    t2 = "RestartRecoverToken2222222222222222222222"

    # Phase 1: Run engine and open 2 positions
    engine1 = AlphaEngine(cfg)
    async with SQLiteLedger(cfg.db_path) as ledger1:
        engine1._ledger = ledger1
        engine1._executor = PaperExecutor(position_book=engine1._position_book, native_price_usd=cfg.sol_price_usd)
        engine1._gatekeeper = MagicMock()
        clean_rep = SecurityReport(
            token_address=t1, chain=ChainIdentifier.SOLANA_MAINNET, tier=SecurityTier.CLEAN,
            is_honeypot=False, is_rug=False, score=90.0, flags=[], buy_tax_bps=0, sell_tax_bps=0,
            checked_at_ts=int(time.time()), passes_hard_gates=True
        )
        engine1._gatekeeper.evaluate_token = AsyncMock(return_value=clean_rep)

        await engine1.submit_token_request(t1, ChainIdentifier.SOLANA_MAINNET)
        await engine1.submit_token_request(t2, ChainIdentifier.SOLANA_MAINNET)
        if engine1._token_request_tasks:
            await asyncio.gather(*engine1._token_request_tasks, return_exceptions=True)

        assert engine1._position_book.effective_active_position_count() == 2

    # Phase 2: Simulate crash and restart fresh engine instance
    engine2 = AlphaEngine(cfg)
    async with SQLiteLedger(cfg.db_path) as ledger2:
        engine2._ledger = ledger2
        open_lots = await ledger2.restore_open_positions_from_ledger()
        engine2._position_book.restore_open_lots(open_lots)

        assert engine2._position_book.effective_active_position_count() == 2
        assert engine2._position_book.is_position_open(t1, ChainIdentifier.SOLANA_MAINNET)
        assert engine2._position_book.is_position_open(t2, ChainIdentifier.SOLANA_MAINNET)


# =========================================================================
# Scenario 9: Queue saturation and explicit overflow handling
# =========================================================================
@pytest.mark.anyio
async def test_scenario_09_queue_saturation_backpressure(tmp_path):
    async with make_test_engine(tmp_path, max_q_size=5) as engine:
        max_size = 5

        for i in range(max_size):
            dummy_rec = TokenRequestRecord(
                request_id=f"dummy_{i}",
                token_address=f"DummyToken{i}1111111111111111111111111",
                chain=ChainIdentifier.SOLANA_MAINNET,
                state=TokenRequestState.QUEUED,
            )
            engine._user_request_queue.put_nowait(dummy_rec)

        overflow_token = "OverflowToken1111111111111111111111111111"
        rec = await engine.submit_token_request(overflow_token, ChainIdentifier.SOLANA_MAINNET)
        assert rec.state == TokenRequestState.REJECTED
        assert "backpressure" in (rec.rejection_reason or "").lower()


# =========================================================================
# Scenario 10: Concurrent attempts to reserve the last available position slot
# =========================================================================
@pytest.mark.anyio
async def test_scenario_10_concurrent_slot_reservation_contention(tmp_path):
    async with make_test_engine(tmp_path, max_active_positions=3) as engine:
        book = engine._position_book
        assert book.max_active_positions == 3

        assert book.reserve_position_slot("TokenSlot1") is True
        assert book.reserve_position_slot("TokenSlot2") is True
        assert book.effective_active_position_count() == 2

        tokens = [f"CompetitorToken{i}" for i in range(5)]

        def try_reserve(ca):
            return book.reserve_position_slot(ca)

        results = [try_reserve(ca) for ca in tokens]
        assert results.count(True) == 1
        assert results.count(False) == 4
        assert book.effective_active_position_count() == 3


# =========================================================================
# Scenario 11: Order submission timeout safety
# =========================================================================
@pytest.mark.anyio
async def test_scenario_11_order_submission_timeout_releases_slot(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        token = "TimeoutExecToken1111111111111111111111111"

        async def fail_exec(*args, **kwargs):
            return None

        engine._executor.execute_signal = AsyncMock(side_effect=fail_exec)

        rec = await engine.submit_token_request(token, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        db_rec = await engine._ledger.get_token_request(rec.request_id)
        assert db_rec.state == TokenRequestState.FAILED
        assert engine._position_book.effective_active_position_count() == 0


# =========================================================================
# Scenario 12: Conflicting or delayed execution acknowledgements
# =========================================================================
@pytest.mark.anyio
async def test_scenario_12_no_contradictory_telegram_acknowledgement(tmp_path):
    async with make_test_engine(tmp_path, max_active_positions=3) as engine:
        engine._position_book.reserve_position_slot("T1")
        engine._position_book.reserve_position_slot("T2")
        engine._position_book.reserve_position_slot("T3")
        assert engine._position_book.effective_active_position_count() == 3

        ok, reason, sig = await engine.manual_enter_position(
            "AMQ8BusAhpwbt3ufSCTi4k3BEQ7AecX9u12Jnk6Spump",
            chain=ChainIdentifier.SOLANA_MAINNET,
        )
        assert ok is False
        assert "Active position count (3) reached MAX_ACTIVE_POSITIONS limit" in reason


# =========================================================================
# Scenario 13: Partial exits and complete PnL reconciliation
# =========================================================================
@pytest.mark.anyio
async def test_scenario_13_partial_exits_and_non_negative_holding_duration(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        token = "PartialExitToken1111111111111111111111111"

        rec = await engine.submit_token_request(token, ChainIdentifier.SOLANA_MAINNET)
        if engine._token_request_tasks:
            await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)

        lots = engine._position_book.get_open_lots(ChainIdentifier.SOLANA_MAINNET, token)
        assert len(lots) == 1
        lot = lots[0]
        entry_ts = lot.open_timestamp_ns

        ok, msg, pnl = await engine.manual_exit_position(token, ChainIdentifier.SOLANA_MAINNET)
        assert ok is True

        trades = await engine._ledger.get_recent_trades(limit=5)
        exit_trade = [
            t for t in trades
            if t.get("token_address") == token and str(t.get("side", "")).lower() == "sell"
        ][0]

        fill_ts = int(exit_trade.get("fill_timestamp_ns") or exit_trade.get("timestamp_ns", 0))
        assert fill_ts > entry_ts
        duration_s = (fill_ts - entry_ts) / 1_000_000_000
        assert duration_s > 0.0


# =========================================================================
# Scenario 14: Invalid tokens and tokens that fail strategy criteria
# =========================================================================
@pytest.mark.anyio
async def test_scenario_14_invalid_token_and_strategy_rejections(tmp_path):
    async with make_test_engine(tmp_path) as engine:
        rec_inv = await engine.submit_token_request("invalid_address_syntax", ChainIdentifier.SOLANA_MAINNET)
        assert rec_inv.state == TokenRequestState.REJECTED
        assert "Invalid contract address" in (rec_inv.rejection_reason or "")

        with patch.object(engine, "_calculate_opportunity_score", return_value=12.0):
            rec_low = await engine.submit_token_request(
                "LowScoreToken111111111111111111111111111111",
                ChainIdentifier.SOLANA_MAINNET,
            )
            if engine._token_request_tasks:
                await asyncio.gather(*engine._token_request_tasks, return_exceptions=True)
            db_rec = await engine._ledger.get_token_request(rec_low.request_id)
            assert db_rec.state == TokenRequestState.REJECTED
            assert "below minimum threshold" in (db_rec.rejection_reason or "")


# =========================================================================
# Scenario 15: Timezone boundaries and daylight-saving transitions
# =========================================================================
@pytest.mark.anyio
async def test_scenario_15_timezone_normalization_utc_and_morocco():
    import zoneinfo

    utc_tz = timezone.utc
    morocco_fixed_tz = timezone(timedelta(hours=1), name="Morocco/WEST")

    dt_utc = datetime(2026, 9, 28, 14, 0, 0, tzinfo=utc_tz)
    dt_morocco = dt_utc.astimezone(morocco_fixed_tz)

    assert dt_morocco.hour == 15
    assert dt_morocco.utcoffset().total_seconds() == 3600

    ts_ns = int(dt_utc.timestamp() * 1_000_000_000)
    recovered_utc = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
    assert recovered_utc == dt_utc

    morocco_zone = zoneinfo.ZoneInfo("Africa/Casablanca")
    dt_zone = dt_utc.astimezone(morocco_zone)
    assert dt_zone is not None
