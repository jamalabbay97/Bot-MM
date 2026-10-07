"""
tests.test_claude_asymmetric_ema_and_backpressure
=================================================
Unit and integration test suite covering:
1. Anthropic Claude 3.5 Integration in AlphaSupervisorAI & EngineConfig
2. Asymmetric EMA DynamicParameterTuner (fast risk cut 0.35, slow risk expansion 0.10)
3. Queue Backpressure Warning in PaperTradingEngine heartbeat
4. Exception tracking in IngestionCoordinator background screening tasks
5. Asyncio Debug Mode configuration
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
import json
import logging
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.ai_supervisor import AlphaSupervisorAI, ALPHA_SUPERVISOR_SYSTEM_PROMPT
from alpha_engine.engine.feedback import DynamicParameterTuner, DynamicHyperparameters
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.ingestion.coordinator import IngestionCoordinator
from alpha_engine.models.ai import AISupervisorDecisionEnum, AISupervisorResponse
from alpha_engine.models.enums import ChainIdentifier


def _create_test_coordinator() -> IngestionCoordinator:
    return IngestionCoordinator(
        evm_ws_url="wss://base-mainnet.g.alchemy.com/v2/demo",
        svm_ws_url="wss://mainnet.helius-rpc.com/?api-key=demo",
        pool_watchlist=[],
        pool_registry={},
        limiter=MagicMock(),
    )


# =============================================================================
# 1. Anthropic Claude 3.5 Integration Tests
# =============================================================================

def test_anthropic_config_and_default_model():
    """Verify EngineConfig anthropic_api_key and AlphaSupervisorAI default model."""
    cfg = EngineConfig(
        ai_provider="anthropic",
        anthropic_api_key="sk-ant-api03-test-key",
    )
    assert cfg.anthropic_api_key == "sk-ant-api03-test-key"
    assert "anthropic" in cfg.ai_provider

    supervisor = AlphaSupervisorAI(cfg)
    assert supervisor._provider == "anthropic"
    assert supervisor._api_key == "sk-ant-api03-test-key"
    assert supervisor._model == "claude-3-5-sonnet-latest"


def test_anthropic_custom_model_preserved():
    """Custom Claude model override is preserved."""
    cfg = EngineConfig(
        ai_provider="anthropic",
        anthropic_api_key="sk-ant-api03-test-key",
        ai_model="claude-3-haiku-20240307",
    )
    supervisor = AlphaSupervisorAI(cfg)
    assert supervisor._model == "claude-3-haiku-20240307"


def test_anthropic_call_llm_payload_and_headers():
    """Verify _call_llm formats headers, JSON payload, and parses response correctly."""
    async def _run():
        cfg = EngineConfig(
            ai_provider="anthropic",
            anthropic_api_key="sk-ant-api03-test-key-mock",
            ai_timeout_s=3.0,
        )
        supervisor = AlphaSupervisorAI(cfg)

        mock_llm_json_payload = {
            "decision": "EXECUTE_BUY",
            "confidence_score": 0.88,
            "action_parameters": {
                "target_token_address": "TestClaudeTokenAddress1111111111111111",
                "recommended_position_pct": 1.25,
                "max_slippage_bps": 120,
                "priority_fee_multiplier": 1.5,
                "take_profit_ladder": [
                    {"trigger_multiplier": 2.0, "sell_pct": 40},
                    {"trigger_multiplier": 3.5, "sell_pct": 30},
                ],
                "hard_stop_loss_pct": -12.0,
                "trailing_stop_activation_pct": 40.0,
                "time_exit_minutes": 15,
            },
            "wallet_audit": {
                "is_wallet_reputable": True,
                "risk_classification": "ORGANIC_SMART_MONEY",
                "rationale": "High win rate and organic trade history verified by Claude 3.5",
            },
            "security_assessment": {
                "is_secure": True,
                "honeypot_risk": "NONE",
                "liquidity_health": "OPTIMAL",
                "flags": [],
            },
            "feedback_tuning": {
                "adjust_global_risk": "NEUTRAL",
                "blacklisted_entities": [],
                "insights_learned": "Verified setup",
            },
        }

        anthropic_api_response = {
            "id": "msg_01XyZ",
            "type": "message",
            "role": "assistant",
            "model": "claude-3-5-sonnet-latest",
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(mock_llm_json_payload),
                }
            ],
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=anthropic_api_response)

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_response

        mock_session = MagicMock()
        mock_session.post.return_value = mock_post_cm

        with patch.object(supervisor, "_get_session", new=AsyncMock(return_value=mock_session)):
            telemetry = {
                "target_token_address": "TestClaudeTokenAddress1111111111111111",
                "chain": ChainIdentifier.SOLANA_MAINNET.value,
            }
            result = await supervisor._call_llm(telemetry)

            assert isinstance(result, AISupervisorResponse)
            assert result.decision == AISupervisorDecisionEnum.EXECUTE_BUY
            assert result.confidence_score == 0.88
            assert result.wallet_audit.rationale == "High win rate and organic trade history verified by Claude 3.5"

            # Verify endpoint and headers
            mock_session.post.assert_called_once()
            call_args, call_kwargs = mock_session.post.call_args
            assert call_args[0] == "https://api.anthropic.com/v1/messages"

            headers = call_kwargs["headers"]
            assert headers["x-api-key"] == "sk-ant-api03-test-key-mock"
            assert headers["anthropic-version"] == "2023-06-01"
            assert headers["content-type"] == "application/json"

            # Verify body
            body = call_kwargs["json"]
            assert body["model"] == "claude-3-5-sonnet-latest"
            assert body["max_tokens"] == 1024
            assert body["temperature"] == 0.1
            assert body["system"] == ALPHA_SUPERVISOR_SYSTEM_PROMPT
            assert len(body["messages"]) == 1
            assert body["messages"][0]["role"] == "user"
            assert "TestClaudeTokenAddress1111111111111111" in body["messages"][0]["content"]

    asyncio.run(_run())


def test_anthropic_call_llm_http_error():
    """Verify _call_llm raises RuntimeError on non-200 HTTP status."""
    async def _run():
        cfg = EngineConfig(
            ai_provider="anthropic",
            anthropic_api_key="sk-ant-invalid-key",
        )
        supervisor = AlphaSupervisorAI(cfg)

        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.text = AsyncMock(return_value="Invalid API key")

        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__.return_value = mock_response

        mock_session = MagicMock()
        mock_session.post.return_value = mock_post_cm

        with patch.object(supervisor, "_get_session", new=AsyncMock(return_value=mock_session)):
            telemetry = {"token": "test"}
            with pytest.raises(RuntimeError, match="Anthropic API returned status 401"):
                await supervisor._call_llm(telemetry)

    asyncio.run(_run())


# =============================================================================
# 2. Asymmetric EMA DynamicParameterTuner Tests
# =============================================================================

def test_asymmetric_ema_fast_cut_slow_expand():
    """
    Test Asymmetric EMA behavior:
    - Losing regime cuts risk fast with alpha = 0.35
    - Winning regime expands risk slowly with alpha = 0.10
    """
    tuner = DynamicParameterTuner()
    # Baseline: smoothed_win_rate = 65.0, smoothed_pnl_usd = 0.0

    # 1. Winning step: win rate 85.0 (> 65.0) and PnL 200.0 (> 0.0) -> Winning regime (alpha = 0.10)
    tuner.update_from_performance(win_rate_24h=85.0, realized_pnl_usd=Decimal("200.0"), total_trades_24h=10)
    expected_win_rate_win = 0.10 * 85.0 + 0.90 * 65.0  # 8.5 + 58.5 = 67.0
    expected_pnl_win = 0.10 * 200.0 + 0.90 * 0.0        # 20.0
    assert pytest.approx(tuner._smoothed_win_rate, rel=1e-3) == expected_win_rate_win
    assert pytest.approx(tuner._smoothed_pnl_usd, rel=1e-3) == expected_pnl_win

    # 2. Losing step: win rate drops to 40.0 (< 67.0) -> Losing regime (alpha = 0.35)
    current_smoothed_win = tuner._smoothed_win_rate
    current_smoothed_pnl = tuner._smoothed_pnl_usd

    tuner.update_from_performance(win_rate_24h=40.0, realized_pnl_usd=Decimal("-100.0"), total_trades_24h=10)
    expected_win_rate_loss = 0.35 * 40.0 + 0.65 * current_smoothed_win
    expected_pnl_loss = 0.35 * (-100.0) + 0.65 * current_smoothed_pnl

    assert pytest.approx(tuner._smoothed_win_rate, rel=1e-3) == expected_win_rate_loss
    assert pytest.approx(tuner._smoothed_pnl_usd, rel=1e-3) == expected_pnl_loss


def test_asymmetric_ema_regime_parameter_adaptation():
    """Verify parameter adjustments and safety boundaries in winning vs losing regimes."""
    tuner = DynamicParameterTuner()

    # Apply 10 winning updates to reach strong winning regime
    for _ in range(10):
        tuner.update_from_performance(win_rate_24h=85.0, realized_pnl_usd=Decimal("300.0"), total_trades_24h=15)

    winning_params = tuner.current_params
    assert isinstance(winning_params, DynamicHyperparameters)
    assert winning_params.confidence_multiplier > 1.05
    assert winning_params.max_slippage_bps > 150
    assert winning_params.wave2_surge_k < 2.4

    # Apply 10 losing updates: should quickly cut risk
    for _ in range(10):
        tuner.update_from_performance(win_rate_24h=35.0, realized_pnl_usd=Decimal("-150.0"), total_trades_24h=15)

    losing_params = tuner.current_params
    assert isinstance(losing_params, DynamicHyperparameters)
    assert losing_params.confidence_multiplier < 0.95
    assert losing_params.max_slippage_bps < 130
    assert losing_params.wave2_surge_k > 2.6

    # Verify hard safety boundary clamping
    assert 0.70 <= losing_params.confidence_multiplier <= 1.40
    assert 50 <= losing_params.max_slippage_bps <= 350
    assert 1.8 <= losing_params.wave2_surge_k <= 3.5


# =============================================================================
# 3. Queue Backpressure Warning Tests
# =============================================================================

def test_heartbeat_backpressure_warning(caplog):
    """Verify warning is logged when queue saturation exceeds 80%."""
    async def _run():
        cfg = EngineConfig(max_queue_size=10, snapshot_interval_s=0.01)
        engine = PaperTradingEngine(cfg)

        # Fill ingestion queue to 90% (9 items out of 10)
        for i in range(9):
            engine._ingestion_q.put_nowait(MagicMock())

        with caplog.at_level(logging.WARNING):
            # Run one heartbeat cycle
            task = asyncio.create_task(engine._heartbeat_task())
            await asyncio.sleep(0.05)
            engine._shutdown_event.set()
            await task

        # Verify backpressure log warning
        warning_records = [r for r in caplog.records if "BACKPRESSURE DETECTED" in r.message]
        assert len(warning_records) > 0
        assert "Ingestion Queue: 90%" in warning_records[0].message

    asyncio.run(_run())


# =============================================================================
# 4. Background Screening Exception Handling Tests
# =============================================================================

def test_screening_task_exception_logged(caplog):
    """Verify unhandled exceptions in background screening tasks are logged and discarded."""
    async def _run():
        coordinator = _create_test_coordinator()

        async def _failing_worker():
            raise ValueError("Simulated unhandled GoPlus API timeout")

        task = asyncio.create_task(_failing_worker())
        coordinator._background_tasks.add(task)
        task.add_done_callback(coordinator._on_screening_task_done)

        with caplog.at_level(logging.ERROR):
            # Wait for task to complete and callback to run
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0.01)

        # Task should be discarded from background_tasks
        assert task not in coordinator._background_tasks

        # Exception should be logged
        error_records = [r for r in caplog.records if "Background screening task failed" in r.message]
        assert len(error_records) == 1
        assert "Simulated unhandled GoPlus API timeout" in error_records[0].message

    asyncio.run(_run())


def test_screening_task_cancelled_not_logged_as_error(caplog):
    """Cancelled background tasks do not log false error."""
    async def _run():
        coordinator = _create_test_coordinator()

        async def _slow_worker():
            await asyncio.sleep(10.0)

        task = asyncio.create_task(_slow_worker())
        coordinator._background_tasks.add(task)
        task.add_done_callback(coordinator._on_screening_task_done)

        with caplog.at_level(logging.ERROR):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0.01)

        assert task not in coordinator._background_tasks
        error_records = [r for r in caplog.records if "Background screening task failed" in r.message]
        assert len(error_records) == 0

    asyncio.run(_run())


# =============================================================================
# 5. Asyncio Debug Mode Tests
# =============================================================================

def test_asyncio_debug_config_flag():
    """Verify asyncio_debug flag in EngineConfig and PaperTradingEngine initialization."""
    cfg = EngineConfig(asyncio_debug=True)
    assert cfg.asyncio_debug is True

    # PaperTradingEngine activates debug
    engine = PaperTradingEngine(cfg)
    assert os.environ.get("PYTHONASYNCIODEBUG") == "1"
    assert logging.getLogger("asyncio").level == logging.DEBUG
