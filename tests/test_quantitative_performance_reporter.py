"""
tests/test_quantitative_performance_reporter.py
================================================
Comprehensive verification of Phase 4:
1. QuantitativePerformanceReporter portfolio metrics computation
2. Daily executive digest generation
3. Weekly institutional audit report generation
4. CSV & JSON data export validation
5. Security hardening & regex credential scrubber (LogCredentialFilter)
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from decimal import Decimal

import pytest

from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.execution.reports import QuantitativePerformanceReporter
from alpha_engine.logging_config import LogCredentialFilter, scrub_sensitive_data


@pytest.fixture
def temp_db_path(tmp_path):
    return str(tmp_path / "test_reports.db")


@pytest.mark.asyncio
async def test_reporter_empty_portfolio(temp_db_path):
    """Test metrics and report generation on empty ledger."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        reporter = QuantitativePerformanceReporter(ledger=ledger)
        metrics = await reporter.compute_portfolio_metrics(lookback_hours=24.0)
        assert metrics["total_trades"] == 0
        assert metrics["net_pnl_usd"] == 0.0

        daily = await reporter.generate_daily_digest(lookback_hours=24.0)
        assert "Daily Quantitative Performance Digest" in daily
        assert "$0.00" in daily

        weekly = await reporter.generate_weekly_digest(lookback_days=7.0)
        assert "Weekly Quantitative Audit Report" in weekly
    finally:
        await ledger.close()


@pytest.mark.asyncio
async def test_reporter_with_closed_trades(temp_db_path):
    """Test metrics computation and digests with winning and losing closed trades."""
    ledger = SQLiteLedger(db_path=temp_db_path)
    await ledger.initialize()
    try:
        now_ns = time.time_ns()
        # Seed trade records
        # Trade 1: +$150, Trade 2: +$200, Trade 3: -$50 (net +$300)
        trades_to_insert = [
            ("t1", "token1", "solana", "buy", "0.001", "0.002", "150.00", "50.0", 120.0, "tp_ladder", "dex_swap", "Fast-Launch Early Momentum Strategy", now_ns - 10_000_000_000),
            ("t2", "token2", "solana", "buy", "0.005", "0.007", "200.00", "40.0", 300.0, "tp_ladder", "raydium_new_pool", "Wave-2 Breakout Strategy", now_ns - 5_000_000_000),
            ("t3", "token3", "solana", "buy", "0.010", "0.008", "-50.00", "-20.0", 60.0, "emergency_drain", "wave2_breakout", "Fast-Launch Early Momentum Strategy", now_ns - 1_000_000_000),
        ]

        for tid, ca, chn, side, ep, xp, pnl, roi, dur, reason, src, strat, ts in trades_to_insert:
            await ledger._conn.execute(
                """
                INSERT INTO trades (
                    trade_id, order_id, signal_id, chain, token_address, pool_address, side,
                    native_spent, tokens_delta, effective_price, price_impact_bps, gas_cost_usd,
                    fill_latency_ms, kelly_fraction, portfolio_equity_usd, realized_pnl_usd,
                    signal_timestamp_ns, fill_timestamp_ns, created_at, platform, strategy_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tid, f"ord-{tid}", f"sig-{tid}", chn, ca, "pool-1", "sell",
                    "0.0", "-1000", xp, 10, "0.01",
                    50, 0.5, "10000.0", pnl,
                    ts - 1000, ts, ts, "RaydiumCPMM", strat,
                ),
            )
            await ledger.record_lot_transition(
                lot_id=f"lot-{tid}",
                token_address=ca,
                chain=chn,
                status="CLOSED",
                exit_reason=reason,
                pnl_usd=Decimal(str(pnl)),
            )
        await ledger._conn.commit()

        reporter = QuantitativePerformanceReporter(ledger=ledger)
        metrics = await reporter.compute_portfolio_metrics(lookback_hours=24.0)

        assert metrics["total_trades"] == 3
        assert metrics["winning_trades"] == 2
        assert metrics["losing_trades"] == 1
        assert metrics["win_rate_pct"] == 66.7
        assert metrics["net_pnl_usd"] == 300.00
        assert metrics["gross_profit_usd"] == 350.00
        assert metrics["gross_loss_usd"] == 50.00
        assert metrics["profit_factor"] == 7.00
        assert metrics["payoff_ratio"] == 3.50  # avg win $175 / avg loss $50

        # Digests
        daily = await reporter.generate_daily_digest(lookback_hours=24.0)
        assert "$300.00" in daily
        assert "Fast-Launch Early Momentum Strategy" in daily
        assert "Wave-2 Breakout Strategy" in daily

        weekly = await reporter.generate_weekly_digest(lookback_days=7.0)
        assert "Weekly Quantitative Audit Report" in weekly
        assert "$300.00" in weekly

        # CSV Export
        csv_str = await reporter.export_report_csv(lookback_hours=24.0)
        reader = csv.DictReader(io.StringIO(csv_str))
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["trade_id"] in ("t1", "t2", "t3")
        assert "realized_pnl_usd" in rows[0]

        # JSON Export
        json_obj = await reporter.export_report_json(lookback_hours=24.0)
        assert "summary_metrics" in json_obj
        assert json_obj["summary_metrics"]["net_pnl_usd"] == 300.00
        assert len(json_obj["closed_trades"]) == 3
    finally:
        await ledger.close()


def test_credential_scrubber_patterns():
    """Test regex masking of credentials, keys, and tokens without corrupting public CAs."""
    # 1. Telegram bot token
    tg_token = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz123456789"
    scrubbed_tg = scrub_sensitive_data(f"Connecting to TG with {tg_token} on port 443")
    assert tg_token not in scrubbed_tg
    assert "[REDACTED_TG_BOT_TOKEN]" in scrubbed_tg

    # 2. Google / Gemini API key
    gemini_key = "AIzaSyD1234567890123456789012345678901"
    scrubbed_gem = scrub_sensitive_data(f"GEMINI_API_KEY={gemini_key}")
    assert gemini_key not in scrubbed_gem
    assert "[REDACTED_GEMINI_KEY]" in scrubbed_gem

    # 3. OpenAI API key
    openai_key = "sk-1234567890abcdef1234567890abcdef12"
    scrubbed_oai = scrub_sensitive_data(f"OPENAI_API_KEY={openai_key}")
    assert openai_key not in scrubbed_oai
    assert "[REDACTED_OPENAI_KEY]" in scrubbed_oai

    # 4. Anthropic API key
    anthropic_key = "sk-ant-1234567890abcdef1234567890abcdef12"
    scrubbed_ant = scrub_sensitive_data(f"Authorization: Bearer {anthropic_key}")
    assert anthropic_key not in scrubbed_ant
    assert "[REDACTED_ANTHROPIC_KEY]" in scrubbed_ant

    # 5. Groq API key
    groq_key = "gsk_1234567890abcdef1234567890abcdef12"
    scrubbed_groq = scrub_sensitive_data(f"GROQ_KEY={groq_key}")
    assert groq_key not in scrubbed_groq
    assert "[REDACTED_GROQ_KEY]" in scrubbed_groq

    # 6. Explicit private key assignment
    hex_pk = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    scrubbed_pk = scrub_sensitive_data(f"private_key = {hex_pk}")
    assert hex_pk not in scrubbed_pk
    assert "[REDACTED_PRIVATE_KEY]" in scrubbed_pk

    # 7. Standalone Solana 88-char private keypair
    sol_pk = "4Z7Pz27N1h9J2V34567890123456789012345678901234567890123456789012345678901234567890123456"
    assert len(sol_pk) == 88
    scrubbed_sol = scrub_sensitive_data(f"Loaded signer keypair: {sol_pk}")
    assert sol_pk not in scrubbed_sol
    assert "[REDACTED_SOLANA_KEY]" in scrubbed_sol

    # 8. Crucial check: Solana public contract addresses (32-44 chars) MUST NOT BE REDACTED
    token_ca = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert len(token_ca) == 44
    scrubbed_ca = scrub_sensitive_data(f"Detected new Raydium pool for {token_ca}")
    assert token_ca in scrubbed_ca


def test_log_credential_filter_integration():
    """Test LogCredentialFilter on logging.LogRecord."""
    cred_filter = LogCredentialFilter()

    # Test msg scrubbing
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Using Gemini token AIzaSyD1234567890123456789012345678901 to analyze token",
        args=(),
        exc_info=None,
    )
    assert cred_filter.filter(record) is True
    assert "AIzaSyD" not in record.msg
    assert "[REDACTED_GEMINI_KEY]" in record.msg

    # Test args scrubbing (tuple)
    secret = "sk-1234567890abcdef1234567890abcdef12"
    record_with_args = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=20,
        msg="Failed connection with key %s",
        args=(secret,),
        exc_info=None,
    )
    assert cred_filter.filter(record_with_args) is True
    assert secret not in record_with_args.args[0]
    assert "[REDACTED_OPENAI_KEY]" in record_with_args.args[0]
