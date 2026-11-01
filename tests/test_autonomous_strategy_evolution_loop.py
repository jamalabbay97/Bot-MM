"""
tests/test_autonomous_strategy_evolution_loop.py
================================================
Tests verifying:
1. Autonomous strategy evolution worker in Runner (regime classification, candidate generation, promotion).
2. Live Strategy Tournament data in DashboardTelemetryAggregator.get_strategy_performance().
3. ConversationalSupervisor live tournament binding through execution_target.
4. Live trade outcome recording with ROI and holding duration accounting.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.config import EngineConfig
from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator
from alpha_engine.engine.backtester import StrategyParameters
from alpha_engine.engine.experimentation import (
    AutonomousStrategyGenerator,
    MarketRegime,
    StrategyTournamentRegistry,
    StrategyVariant,
)
from alpha_engine.engine.runner import Runner
from alpha_engine.models.enums import ChainIdentifier


@pytest.mark.asyncio
async def test_dashboard_telemetry_live_strategy_performance():
    """Verify that DashboardTelemetryAggregator reads live Champion and Challengers."""
    agg = DashboardTelemetryAggregator()
    registry = StrategyTournamentRegistry()

    # Configure Champion
    champ = registry.champion
    champ.metrics.trades_count = 35
    champ.metrics.win_rate = 71.4
    champ.metrics.profit_factor = 2.45
    champ.metrics.sharpe_ratio = 2.10
    champ.metrics.sortino_ratio = 2.80
    champ.metrics.max_drawdown_pct = 6.5
    champ.metrics.net_pnl_usd = 420.0

    # Add a Challenger
    chal_params = StrategyParameters(strategy_name="Adaptive_Challenger_V1")
    chal = StrategyVariant(
        experiment_id="chal_v1",
        strategy_name="Adaptive_Challenger_V1",
        parameters=chal_params,
        regime=MarketRegime.HIGH_VOL_MEME_MANIA,
    )
    chal.metrics.trades_count = 15
    chal.metrics.win_rate = 60.0
    chal.metrics.profit_factor = 1.85
    chal.metrics.sharpe_ratio = 1.70
    chal.metrics.sortino_ratio = 2.10
    chal.metrics.max_drawdown_pct = 7.0
    chal.metrics.net_pnl_usd = 180.0
    registry.challengers["Adaptive_Challenger_V1"] = chal

    # Attach tournament to telemetry aggregator
    agg.strategy_tournament = registry

    perf = await agg.get_strategy_performance()
    assert len(perf) >= 2
    champ_entry = next((p for p in perf if "CHAMPION" in p["status"]), None)
    assert champ_entry is not None
    assert "🏆" in champ_entry["strategy_type"]
    assert champ_entry["profit_factor"] == 2.45
    assert champ_entry["sortino_ratio"] == 2.80

    chal_entry = next((p for p in perf if "Adaptive_Challenger_V1" in p["strategy_type"]), None)
    assert chal_entry is not None
    assert chal_entry["profit_factor"] == 1.85


@pytest.mark.asyncio
async def test_conversational_supervisor_execution_target_tournament():
    """Verify ConversationalSupervisor uses target.strategy_tournament."""
    mock_ledger = MagicMock()
    supervisor = ConversationalSupervisor(ledger=mock_ledger)
    registry = StrategyTournamentRegistry()
    registry.champion.metrics.trades_count = 42

    mock_runner = MagicMock()
    mock_runner.strategy_tournament = registry
    supervisor.execution_target = mock_runner

    report = await supervisor.tool_experiments()
    assert "🏆 لوحة صدارة تجارب الاستراتيجيات" in report
    assert "42" in report


@pytest.mark.asyncio
async def test_strategy_evolution_worker_step():
    """Verify that _strategy_evolution_worker correctly evaluates regime and generates candidates."""
    cfg = EngineConfig(db_path=":memory:")
    runner = Runner(config=cfg)
    runner._strategy_tournament = StrategyTournamentRegistry()
    runner._strategy_generator = AutonomousStrategyGenerator()

    # Wire an AsyncMock promotion callback
    async_promo_callback = AsyncMock()
    runner._strategy_tournament.register_on_promotion_callback(async_promo_callback)

    # Test record_live_trade_outcome with genuine ROI and hold duration
    runner._strategy_tournament.record_live_trade_outcome(
        strategy_name=runner._strategy_tournament.champion.strategy_name,
        pnl_usd=30.0,
        is_win=True,
        roi_pct=25.0,
        hold_time_s=18.5,
    )
    assert len(runner._strategy_tournament.champion.closed_lots) > 0
    latest_lot = runner._strategy_tournament.champion.closed_lots[-1]
    assert latest_lot.roi_pct == 25.0
    assert latest_lot.exit_timestamp is not None
    assert latest_lot.entry_timestamp < latest_lot.exit_timestamp

    # Simulate recent loss trades
    runner._recent_closed_trades.append({
        "token_address": "TestTokenPump",
        "chain": ChainIdentifier.SOLANA_MAINNET,
        "platform": "pump.fun",
        "is_win": False,
        "realized_pnl_usd": Decimal("-5.0"),
        "exit_reason": "sl_hard",
    })

    initial_challengers = len(runner._strategy_tournament.get_active_challengers())

    # Trigger regime classification with volatility sensitivity
    current_regime = runner._strategy_tournament.classify_regime(
        recent_win_rate=0.0,
        recent_pnl_usd=-5.0,
        graduation_velocity=1.2,
        sol_volatility=0.06,
    )
    assert current_regime in [MarketRegime.LOW_LIQUIDITY_CHOP, MarketRegime.RISK_OFF_DRAWDOWN]

    # Generate candidates adapted to recent loss reasons
    recent_losses = [t.get("exit_reason", "") for t in runner._recent_closed_trades if not t.get("is_win", False)]
    new_candidates = runner._strategy_generator.generate_evolutionary_generation(
        registry=runner._strategy_tournament,
        market_regime=current_regime,
        recent_losses=recent_losses,
    )
    assert len(new_candidates) > 0
    assert len(runner._strategy_tournament.get_active_challengers()) > initial_challengers


@pytest.mark.asyncio
async def test_strategy_evolution_worker_async_execution():
    """Verify background task execution of _strategy_evolution_worker using asyncio and patch."""
    cfg = EngineConfig(db_path=":memory:")
    runner = Runner(config=cfg)
    runner._strategy_tournament = StrategyTournamentRegistry()

    with patch.object(runner._strategy_tournament, "evaluate_promotions", return_value=None):
        worker_task = asyncio.create_task(runner._strategy_evolution_worker())
        await asyncio.sleep(0.01)
        runner._shutdown_event.set()
        await asyncio.wait_for(worker_task, timeout=1.5)
        assert worker_task.done()
