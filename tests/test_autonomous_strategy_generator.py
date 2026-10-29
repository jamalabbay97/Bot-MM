"""
tests/test_autonomous_strategy_generator.py
============================================
Tests for AutonomousStrategyGenerator & Dynamic Tournament Evolution:
1. Generation across 5 archetypes:
   - FAST_LAUNCH_MOMENTUM
   - WAVE2_ACCUMULATION
   - REVIVAL_CTO_SWING
   - SMART_MONEY_FOLLOW
   - NARRATIVE_BREAKOUT
2. Parameter mutation with bounds enforcement
3. Walk-forward backtest evaluation & anti-overfitting perturbation testing
4. Shadow staging into StrategyTournamentRegistry
5. Portfolio synthesis & SQLite experiment persistence
"""

from __future__ import annotations

import os
import pytest
from alpha_engine.engine.backtester import WalkForwardBacktester
from alpha_engine.engine.experimentation import (
    AutonomousStrategyGenerator,
    ExperimentStatus,
    MarketRegime,
    StrategyArchetype,
    StrategyParameters,
    StrategyTournamentRegistry,
)
from alpha_engine.execution.ledger import SQLiteLedger


@pytest.fixture
def temp_ledger(tmp_path):
    db_file = tmp_path / "test_strategy_gen.db"
    return str(db_file)


def test_generate_all_archetypes():
    gen = AutonomousStrategyGenerator()
    
    # 1. Fast Launch Momentum
    p1 = gen.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM)
    assert isinstance(p1, StrategyParameters)
    assert p1.hard_stop_loss_pct == 0.10
    assert p1.take_profit_pct == 0.35
    assert p1.time_exit_s == 180.0
    assert p1.regime_affinity == MarketRegime.HIGH_VOL_MEME_MANIA

    # Test base_params specialization
    custom_base = StrategyParameters(strategy_name="seed_template", hard_stop_loss_pct=0.12, take_profit_pct=0.40)
    p_derived = gen.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM, base_params=custom_base)
    assert isinstance(p_derived, StrategyParameters)
    assert "seed_template" in p_derived.strategy_name

    # 2. Wave-2 Accumulation
    p2 = gen.generate_strategy_variant(StrategyArchetype.WAVE2_ACCUMULATION)
    assert p2.hard_stop_loss_pct == 0.14
    assert p2.take_profit_pct == 0.65
    assert p2.time_exit_s == 1800.0

    # 3. Revival / CTO Swing
    p3 = gen.generate_strategy_variant(StrategyArchetype.REVIVAL_CTO_SWING)
    assert p3.hard_stop_loss_pct == 0.18
    assert p3.take_profit_pct == 1.50
    assert p3.trailing_stop_from_peak_pct == 0.30
    assert p3.time_exit_s == 86400.0
    assert p3.regime_affinity == MarketRegime.REVIVAL_ROTATION

    # 4. Smart Money Follow
    p4 = gen.generate_strategy_variant(StrategyArchetype.SMART_MONEY_FOLLOW)
    assert p4.hard_stop_loss_pct == 0.12
    assert p4.extra_params.get("max_slippage_bps") == 80

    # 5. Narrative Breakout
    p5 = gen.generate_strategy_variant(StrategyArchetype.NARRATIVE_BREAKOUT)
    assert p5.min_entry_score == 75.0
    assert p5.regime_affinity == MarketRegime.HIGH_VOL_MEME_MANIA


def test_mutate_strategy_bounds():
    gen = AutonomousStrategyGenerator()
    parent = gen.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM)

    for _ in range(10):
        child = gen.mutate_strategy(parent, mutation_rate=0.15)
        assert 0.06 <= child.hard_stop_loss_pct <= 0.25
        assert 0.20 <= child.take_profit_pct <= 3.00
        assert 0.12 <= child.trailing_stop_from_peak_pct <= 0.40
        assert 0.0 <= child.min_entry_score <= 85.0
        assert child.strategy_name != parent.strategy_name


@pytest.mark.asyncio
async def test_evaluate_and_stage_candidate(temp_ledger):
    async with SQLiteLedger(temp_ledger) as ledger:
        registry = StrategyTournamentRegistry(ledger=ledger)
        gen = AutonomousStrategyGenerator(ledger=ledger)

        cand = gen.generate_strategy_variant(StrategyArchetype.WAVE2_ACCUMULATION)
        report = gen.evaluate_and_stage_candidate(cand, registry)

        assert "strategy_name" in report
        assert "experiment_id" in report
        assert "perturbation_robust" in report
        assert cand.strategy_name in registry.challengers
        challenger = registry.get_strategy_by_name(cand.strategy_name)
        assert challenger is not None
        assert challenger.status == ExperimentStatus.SHADOW


@pytest.mark.asyncio
async def test_discover_and_deploy_strategy_portfolio(temp_ledger):
    async with SQLiteLedger(temp_ledger) as ledger:
        registry = StrategyTournamentRegistry(ledger=ledger)
        gen = AutonomousStrategyGenerator(ledger=ledger)

        initial_count = len(registry.challengers)
        deployed = gen.discover_and_deploy_strategy_portfolio(registry)

        assert len(deployed) == 5
        assert len(registry.challengers) == initial_count + 5

        # Check leaderboard outputs new challengers
        leaderboard = registry.get_leaderboard()
        names = [entry["name"] for entry in leaderboard]
        for d in deployed:
            assert d["strategy_name"] in names


@pytest.mark.asyncio
async def test_real_database_backtest_with_strategy_generator():
    real_db = "paper_trading.db"
    if not os.path.exists(real_db):
        pytest.skip("paper_trading.db not available in test environment")

    backtester = WalkForwardBacktester(db_path=real_db)
    loaded = backtester.load_historical_data()
    assert loaded > 0

    gen = AutonomousStrategyGenerator()
    cand = gen.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM)
    res = backtester.evaluate_strategy(cand)

    assert res.total_trades > 0
    assert res.full_sample_metrics.trades_count > 0
    assert hasattr(res.full_sample_metrics, "profit_factor")
    assert hasattr(res.full_sample_metrics, "sortino_ratio")
