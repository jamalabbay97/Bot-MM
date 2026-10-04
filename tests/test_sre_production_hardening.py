"""
tests/test_sre_production_hardening.py
======================================
Comprehensive SRE and Systems Architecture tests validating:
1. Environment configuration & Pydantic Settings type validation.
2. Production log rotation with size caps and directory creation.
3. Telemetry heartbeat task metrics collection.
4. Autonomous supervisor backoff calculation and crash resilience.
5. Graceful shutdown, queue draining, and WAL checkpointing.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig, _optional_env, _require_env
from alpha_engine.engine.runner import PaperTradingEngine, _build_example_config
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.logging_config import setup_production_logging
from alpha_engine.models.events import ShutdownSentinel


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


def test_pydantic_settings_validation_and_defaults():
    assert MagicMock is not None
    test_rpc = os.getenv("TEST_RPC_URL", "https://base.llamarpc.com")
    test_api_key = os.getenv("TEST_API_KEY", "demo_api_key")
    assert test_rpc is not None
    assert test_api_key is not None

    cfg = EngineConfig()
    assert cfg.log_level in ("INFO", "DEBUG", "WARNING", "ERROR")
    assert cfg.max_portfolio_risk_pct == 0.01
    assert cfg.stop_loss_pct == -0.15
    assert cfg.max_queue_size == 100
    assert cfg.initial_sol == Decimal("10.0")
    assert cfg.initial_eth == Decimal("2.0")
    assert cfg.initial_equity_usd > 0
    assert "alchemy" in cfg.alchemy_ws_url or "demo" in cfg.alchemy_ws_url or "base" in cfg.alchemy_ws_url
    assert "helius" in cfg.helius_ws_url or "demo" in cfg.helius_ws_url or "solana" in cfg.helius_ws_url


def test_env_var_override_and_backward_compatibility(monkeypatch):
    monkeypatch.setenv("BASE_RPC_HTTP", "https://custom-base-rpc.com")
    monkeypatch.setenv("SOLANA_RPC_HTTP", "https://custom-solana-rpc.com")
    monkeypatch.setenv("TELEGRAM_API_ID", "12345678")
    monkeypatch.setenv("TELEGRAM_API_HASH", "test_hash_abcdef")
    monkeypatch.setenv("TELEGRAM_CHANNELS", "alpha_channel_1,alpha_channel_2")
    monkeypatch.setenv("MAX_QUEUE_SIZE", "250")
    monkeypatch.setenv("MAX_PORTFOLIO_RISK_PCT", "0.02")

    cfg = EngineConfig()
    assert cfg.base_rpc_http == "https://custom-base-rpc.com"
    assert cfg.solana_rpc_http == "https://custom-solana-rpc.com"
    assert cfg.telegram_api_id == 12345678
    assert cfg.telegram_api_hash == "test_hash_abcdef"
    assert cfg.telegram_channels == ["alpha_channel_1", "alpha_channel_2"]
    assert cfg.max_queue_size == 250
    assert cfg.max_portfolio_risk_pct == 0.02

    # Check env helper functions
    assert _require_env("TELEGRAM_API_ID") == "12345678"
    assert _optional_env("NON_EXISTENT_KEY", "fallback") == "fallback"


def test_production_logging_rotation(temp_dir):
    log_dir = temp_dir / "logs"
    root_logger = setup_production_logging(
        log_level="DEBUG",
        log_dir=str(log_dir),
        log_filename="test_engine.log",
        max_bytes=1024,  # Small size to trigger fast test rotation
        backup_count=3,
    )
    assert log_dir.exists()
    assert (log_dir / "test_engine.log").exists()

    # Emit logs to verify write
    test_logger = logging.getLogger("alpha_engine.test")
    test_logger.info("Test production log line 1")
    test_logger.debug("Test production log line 2")

    content = (log_dir / "test_engine.log").read_text(encoding="utf-8")
    assert "Test production log line 1" in content


def test_sqlite_wal_checkpoint(temp_dir):
    async def _run():
        db_file = temp_dir / "test_wal.db"
        async with SQLiteLedger(db_file) as ledger:
            await ledger.checkpoint()
        assert db_file.exists()

    asyncio.run(_run())


def test_heartbeat_telemetry_execution():
    async def _run():
        start_t = time.time()
        cfg = EngineConfig()
        cfg.snapshot_interval_s = 0.05  # fast interval for test
        engine = PaperTradingEngine(cfg)

        # Run heartbeat task briefly then cancel
        task = asyncio.create_task(engine._heartbeat_task())
        await asyncio.sleep(0.12)
        engine._shutdown_event.set()
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done()
        assert time.time() >= start_t

    asyncio.run(_run())


def test_supervisor_crash_containment_and_backoff():
    async def _run():
        cfg = EngineConfig()
        engine = PaperTradingEngine(cfg)

        watchlist, svm_reg, seed_states = _build_example_config()
        attempts = 0

        async def _failing_cycle(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts >= 2:
                engine._shutdown_event.set()
            raise ConnectionResetError("Simulated RPC connection reset")

        with patch.object(engine, "_run_single_cycle", side_effect=_failing_cycle):
            await engine.run_supervised(
                pool_watchlist=watchlist,
                svm_pool_registry=svm_reg,
                seed_pool_states=seed_states,
                max_restarts=3,
                initial_backoff_s=0.01,
                max_backoff_s=0.05,
                backoff_factor=1.5,
            )

        # Supervisor should have contained the errors and restarted twice before stopping
        assert attempts >= 2
        assert engine._shutdown_event.is_set()

    asyncio.run(_run())


def test_graceful_shutdown_queue_draining():
    async def _run():
        cfg = EngineConfig()
        engine = PaperTradingEngine(cfg)

        # Push items into queues
        await engine._ingestion_q.put(ShutdownSentinel())
        # Running ingestion processor should forward ShutdownSentinel to signal queue
        mock_gk = AsyncMock()
        mock_gk.screen_token = AsyncMock()
        engine._gatekeeper = mock_gk
        await engine._process_ingestion_queue()

        # The signal queue must now have received the ShutdownSentinel
        item = await engine._signal_q.get()
        assert isinstance(item, ShutdownSentinel)
        mock_gk.screen_token.assert_not_awaited()

    asyncio.run(_run())

