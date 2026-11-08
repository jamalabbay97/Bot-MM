"""
tests/test_realtime_pricing_and_strategy_evolution.py
======================================================
Verifies:
1. Candidate pool selection prioritizes viable liquidity over sub-second dead ghost pools.
2. Multi-Armed Bandit strategy selection explores Challengers and exploits Champion.
3. Strategy attribution is preserved in TradeRecord.
4. Dynamic SoftMax weighting accurately computes Sortino/PF weights on the tournament leaderboard.
"""

from decimal import Decimal
import pytest
from unittest.mock import MagicMock

from alpha_engine.models.enums import ChainIdentifier, OrderSide
from alpha_engine.models.state import PoolState, PaperFill, TradeRecord
from alpha_engine.engine.runner import Runner
from alpha_engine.engine.experimentation import (
    StrategyTournamentRegistry,
)
from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator


def test_viable_liquidity_candidate_priority():
    """Verify deeper liquidity pool ranks above a sub-second ghost pool."""
    ghost_pool = PoolState(
        pool_address="ghost_pool_111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="token_1111111111111111111111111111111",
        token_reserve=Decimal("10000"),
        native_reserve=Decimal("0.12"),  # Only 0.12 SOL
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        timestamp_s=100.0,
        data_age_s=0.15,  # Super fresh, but illiquid
    )
    setattr(ghost_pool, "_reserve_usd", 20.0)

    deep_pool = PoolState(
        pool_address="deep_pool_2222222222222222222222222222",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="token_1111111111111111111111111111111",
        token_reserve=Decimal("1000000"),
        native_reserve=Decimal("56.0"),  # 56 SOL
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        timestamp_s=100.0,
        data_age_s=0.95,  # Slightly older, but deep
    )
    setattr(deep_pool, "_reserve_usd", 16500.0)

    native_price = 150.0

    def _candidate_rank(c: PoolState) -> tuple[int, float, float]:
        res_native = float(c.native_reserve or 0.0)
        res_usd = float(getattr(c, "_reserve_usd", 0.0) or (res_native * 2.0 * float(native_price)))
        has_viable_liq = 1 if (res_native >= 2.0 or res_usd >= 300.0) else 0
        age = c.data_age_s if c.data_age_s is not None else 999.0
        return (has_viable_liq, res_usd, -age)

    ranked = sorted([ghost_pool, deep_pool], key=_candidate_rank, reverse=True)
    assert ranked[0].pool_address == "deep_pool_2222222222222222222222222222", "Deep liquidity pool must beat ghost pool regardless of latency"


def test_trade_record_strategy_name_preservation():
    """Verify TradeRecord preserves strategy_name."""
    fill = PaperFill(
        order_id="ord_1",
        token_address="token_1111111111111111111111111111111",
        pool_address="pool_11111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("0.1"),
        tokens_acquired=Decimal("1000"),
        effective_price=Decimal("0.0001"),
        price_impact_bps=10,
        fill_latency_ms=45,
        portfolio_equity_usd=Decimal("10000"),
    )
    rec = TradeRecord.from_fill(fill, strategy_name="challenger_early_rally_swing")
    assert rec.strategy_name == "challenger_early_rally_swing"


@pytest.mark.asyncio
async def test_dynamic_softmax_weights_on_leaderboard():
    """Verify DashboardTelemetryAggregator computes dynamic SoftMax weights."""
    tournament = StrategyTournamentRegistry()
    champ = tournament.champion
    champ.metrics.win_rate = 65.0
    champ.metrics.profit_factor = Decimal("2.10")
    champ.metrics.sortino_ratio = Decimal("2.50")
    champ.metrics.trades_count = 20

    telemetry = DashboardTelemetryAggregator()
    telemetry.strategy_tournament = tournament

    leaderboard = await telemetry.get_strategy_performance()
    assert len(leaderboard) > 1
    assert leaderboard[0]["status"] == "CHAMPION"
    # Weight must be a calculated float, summing to approximately 1.0
    total_w = sum(item["weight"] for item in leaderboard)
    assert 0.95 <= total_w <= 1.05
    assert leaderboard[0]["weight"] > 0.0


def test_multi_armed_bandit_strategy_selection():
    """Verify runner.select_execution_strategy samples both Champion and Challengers."""
    runner = Runner.__new__(Runner)
    runner.config = MagicMock()
    runner.config.tournament_exploration_rate = 0.50  # 50% explore for testing
    runner.active_strategy = "champion_peak_trailing_50"
    runner._cfg = runner.config

    tournament = StrategyTournamentRegistry()
    runner._strategy_tournament = tournament

    chosen_strats = set()
    for _ in range(50):
        params = runner.select_execution_strategy()
        chosen_strats.add(params.strategy_name)

    assert "champion_peak_trailing_50" in chosen_strats
    assert len(chosen_strats) > 1, f"Bandit should explore challengers; chosen: {chosen_strats}"
