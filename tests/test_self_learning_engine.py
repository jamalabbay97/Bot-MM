"""
tests.test_self_learning_engine — Unit & Integration Tests for Self-Learning Engine
===================================================================================
Validates:
1. Per-strategy metrics calculation (Sharpe, Sortino, Drawdown, Expectancy).
2. Root-cause loss attribution taxonomy.
3. Online Reinforcement Learning (Softmax Multi-Armed Bandit weight adaptation).
4. Autonomous Hyperparameter Tuner (Council weight recalibration, slippage adaptation).
5. Strategy ranking & quarantine enforcement.
"""

import os
import tempfile
import pytest

from alpha_engine.engine.self_learning import (
    AutonomousHyperparameterTuner,
    ClosedTradeOutcome,
    ComprehensiveLossAttribution,
    OnlinePolicyLearner,
    SelfLearningEngine,
    StrategyPerformanceTracker,
    StrategyStatus,
)
from alpha_engine.engine.strategy import StrategyRegistry


@pytest.fixture
def clean_strategy_tracker() -> StrategyPerformanceTracker:
    return StrategyPerformanceTracker(history_window=50)


def test_strategy_performance_metrics_and_expectancy(clean_strategy_tracker: StrategyPerformanceTracker):
    tracker = clean_strategy_tracker

    # Simulate 5 trades: 3 wins, 2 losses
    trades = [
        ClosedTradeOutcome(
            trade_id="t1",
            token_address="TokenA",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.40,
            realized_pnl_usd=40.0,
            realized_pnl_native=0.4,
            roi_pct=40.0,
            holding_time_seconds=600.0,
            exit_reason="tp_25",
        ),
        ClosedTradeOutcome(
            trade_id="t2",
            token_address="TokenB",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.70,
            realized_pnl_usd=70.0,
            realized_pnl_native=0.7,
            roi_pct=70.0,
            holding_time_seconds=900.0,
            exit_reason="tp_50",
        ),
        ClosedTradeOutcome(
            trade_id="t3",
            token_address="TokenC",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=0.90,
            realized_pnl_usd=-10.0,
            realized_pnl_native=-0.1,
            roi_pct=-10.0,
            holding_time_seconds=300.0,
            exit_reason="sl_hard",
        ),
        ClosedTradeOutcome(
            trade_id="t4",
            token_address="TokenD",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=0.88,
            realized_pnl_usd=-12.0,
            realized_pnl_native=-0.12,
            roi_pct=-12.0,
            holding_time_seconds=250.0,
            exit_reason="sl_hard",
        ),
        ClosedTradeOutcome(
            trade_id="t5",
            token_address="TokenE",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.30,
            realized_pnl_usd=30.0,
            realized_pnl_native=0.3,
            roi_pct=30.0,
            holding_time_seconds=500.0,
            exit_reason="tp_25",
        ),
    ]

    for t in trades:
        metrics = tracker.record_closed_trade(t)

    assert metrics.total_trades == 5
    assert metrics.winning_trades == 3
    assert metrics.losing_trades == 2
    assert metrics.win_rate_pct == 60.0
    assert metrics.total_realized_pnl_usd == 118.0
    assert metrics.profit_factor > 5.0
    assert metrics.mathematical_expectancy_usd > 0.0
    assert metrics.sharpe_ratio > 0.0
    assert metrics.sortino_ratio > 0.0
    assert metrics.status in (StrategyStatus.BOOSTED, StrategyStatus.ACTIVE)


def test_loss_attribution_diagnostics():
    tracker = StrategyPerformanceTracker()

    # 1. MEV Sandwich loss
    t_mev = ClosedTradeOutcome(
        trade_id="t_mev",
        token_address="TokenMEV",
        chain="solana_mainnet",
        strategy_type="fast_snipe",
        entry_price=1.0,
        exit_price=0.80,
        realized_pnl_usd=-20.0,
        realized_pnl_native=-0.2,
        roi_pct=-20.0,
        holding_time_seconds=40.0,
        exit_reason="sl_hard",
        actual_slippage_bps=450,
        expected_slippage_bps=100,
    )
    attr_mev = tracker._attribute_trade_outcome(t_mev)
    assert attr_mev == ComprehensiveLossAttribution.MEV_SANDWICH

    # 2. Rugpull loss
    t_rug = ClosedTradeOutcome(
        trade_id="t_rug",
        token_address="TokenRug",
        chain="solana_mainnet",
        strategy_type="fast_snipe",
        entry_price=1.0,
        exit_price=0.10,
        realized_pnl_usd=-90.0,
        realized_pnl_native=-0.9,
        roi_pct=-90.0,
        holding_time_seconds=60.0,
        exit_reason="rug_liquidity_drain",
    )
    attr_rug = tracker._attribute_trade_outcome(t_rug)
    assert attr_rug == ComprehensiveLossAttribution.RUG_PULL


def test_online_reinforcement_learning_policy_update():
    learner = OnlinePolicyLearner(learning_rate_eta=0.20)

    # 1. Winning trade boosts strategy weight
    t_win = ClosedTradeOutcome(
        trade_id="t_w",
        token_address="TokenW",
        chain="solana_mainnet",
        strategy_type="momentum_breakout",
        entry_price=1.0,
        exit_price=1.50,
        realized_pnl_usd=50.0,
        realized_pnl_native=0.5,
        roi_pct=50.0,
        holding_time_seconds=300.0,
        exit_reason="tp_50",
    )
    weights_win = learner.update_policy(t_win)
    assert weights_win["momentum_breakout"] > 1.0

    # 2. Severe losing trade depresses strategy weight
    t_loss = ClosedTradeOutcome(
        trade_id="t_l",
        token_address="TokenL",
        chain="solana_mainnet",
        strategy_type="momentum_breakout",
        entry_price=1.0,
        exit_price=0.60,
        realized_pnl_usd=-40.0,
        realized_pnl_native=-0.4,
        roi_pct=-40.0,
        holding_time_seconds=100.0,
        exit_reason="sl_hard",
    )
    # Execute consecutive losses to depress weight
    learner.update_policy(t_loss)
    weights_loss = learner.update_policy(t_loss)
    assert weights_loss["momentum_breakout"] < weights_win["momentum_breakout"]


def test_autonomous_hyperparameter_tuner():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        tuner = AutonomousHyperparameterTuner(settings_file_path=tmp_path, min_trades_between_tuning=2)

        # Feed 2 trades with agent verdict audit
        t1 = ClosedTradeOutcome(
            trade_id="tune1",
            token_address="TokenT1",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.35,
            realized_pnl_usd=35.0,
            realized_pnl_native=0.35,
            roi_pct=35.0,
            holding_time_seconds=400.0,
            exit_reason="tp_25",
            actual_slippage_bps=120,
            council_verdicts={
                "ChainForensic": {"vote": "APPROVE"},
                "QuantTechnicals": {"vote": "APPROVE"},
                "ReachAnalyst": {"vote": "NEUTRAL"},
            },
        )
        t2 = ClosedTradeOutcome(
            trade_id="tune2",
            token_address="TokenT2",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.45,
            realized_pnl_usd=45.0,
            realized_pnl_native=0.45,
            roi_pct=45.0,
            holding_time_seconds=500.0,
            exit_reason="tp_50",
            actual_slippage_bps=130,
            council_verdicts={
                "ChainForensic": {"vote": "APPROVE"},
                "QuantTechnicals": {"vote": "APPROVE"},
                "ReachAnalyst": {"vote": "APPROVE"},
            },
        )

        res1 = tuner.record_trade_outcome(t1)
        assert res1 is None  # Below batch threshold
        res2 = tuner.record_trade_outcome(t2)
        assert res2 is not None  # Batch threshold met; tuned!
        assert "ChainForensic" in res2.council_weights
        assert res2.max_slippage_bps >= 75
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@pytest.mark.asyncio
async def test_self_learning_engine_end_to_end():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        registry = StrategyRegistry()
        engine = SelfLearningEngine(strategy_registry=registry, settings_path=tmp_path)

        trade = ClosedTradeOutcome(
            trade_id="e2e_1",
            token_address="TokenE2E",
            chain="solana_mainnet",
            strategy_type="momentum_breakout",
            entry_price=1.0,
            exit_price=1.60,
            realized_pnl_usd=60.0,
            realized_pnl_native=0.6,
            roi_pct=60.0,
            holding_time_seconds=800.0,
            exit_reason="tp_50",
        )

        out = await engine.on_trade_closed(trade)
        assert "strategy_metrics" in out
        assert out["strategy_metrics"]["total_trades"] == 1
        assert "policy_weights" in out

        report = engine.get_ranked_strategy_report()
        assert len(report) >= 1
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
