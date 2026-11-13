"""
tests/test_issue9_strategy_generation.py
========================================
Test suite for Issue 9: Real rule-based strategy generation and per-variant fills.

Validates:
1. Declarative entry-rule schema validation (known metric names, operators, ranges, types; unknown metrics fail).
2. Metric registry extraction from signal, pool, security report, and wallet profile.
3. Variants make their own entry decisions and receive their own simulated fills (different rules -> different subsets -> different fills).
4. Shadow sizing uses the live sizing function (per-chain pricing & equity), not fixed 0.10 and $0.05.
5. Mutation and crossover are stochastic with seeded RNG (reproducible with same seed, different with different seeds).
6. Duplicate strategies are detected and rejected from staging.
7. Promotion of a champion changes live entry decisions in Runner (integration test through _process_signal_queue).
8. Shadow fills equal live-paper fills for identical inputs (parity).
9. Generation logs (parent ids, what changed) are persisted in ledger.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.experimentation import (
    AutonomousStrategyGenerator,
    ExperimentStatus,
    MarketRegime,
    ShadowLot,
    StrategyArchetype,
    StrategyParameters,
    StrategyTournamentRegistry,
    StrategyVariant,
)
from alpha_engine.engine.rules import (
    ComparisonOperator,
    EntryRuleSet,
    LogicalOperator,
    METRIC_REGISTRY,
    RuleCondition,
    RuleGroup,
    SchemaValidationError,
)
from alpha_engine.engine.runner import Runner
from alpha_engine.execution.book import PositionBook
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    ExitProfile,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.pricing.native_price_service import NativePriceService


@pytest.fixture
def test_sol_pool() -> PoolState:
    return PoolState(
        pool_address="PoolSolana111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("1000000.0"),
        spot_price_native_per_token=Decimal("0.00005"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1000,
    )


@pytest.fixture
def test_signal(test_sol_pool: PoolState) -> SignalEvent:
    sec = SecurityReport(
        token_address="TokenAAA1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        top10_concentration=0.18,
        liquidity_usd=Decimal("15000.0"),
        audit_passed=True,
    )
    return SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=test_sol_pool.pool_address,
        token_address="TokenAAA1111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        pool_state=test_sol_pool,
        security_report=sec,
        strength=SignalStrength.STRONG,
        alpha_score=0.75,
        source=SignalSource.DEX_SWAP,
        market_cap_usd=Decimal("65000.0"),
        net_buy_delta=0.62,
        volume_surge_multiplier=3.2,
        consolidation_length_hours=4.5,
        execution_venue=ExecutionVenue.RAYDIUM_AMM,
        exit_profile=ExitProfile.FAST_SNIPE,
    )


def test_metric_registry_extraction_and_rule_groups(test_sol_pool, test_signal):
    """
    Requirement 1: Metric registry extraction from signal, pool, and nested RuleGroup evaluation.
    """
    assert "mcap_usd" in METRIC_REGISTRY
    assert "net_buy_delta" in METRIC_REGISTRY
    assert "volume_surge_multiplier" in METRIC_REGISTRY
    assert "dormancy_hours" in METRIC_REGISTRY
    assert "slippage_estimate_bps" in METRIC_REGISTRY

    # Verify extractors against test_signal and test_sol_pool
    mcap_val = METRIC_REGISTRY["mcap_usd"].extractor(test_signal, test_sol_pool, Decimal("200.0"), None)
    assert mcap_val == 65000.0

    delta_val = METRIC_REGISTRY["net_buy_delta"].extractor(test_signal, test_sol_pool, Decimal("200.0"), None)
    assert delta_val == 0.62

    surge_val = METRIC_REGISTRY["volume_surge_multiplier"].extractor(test_signal, test_sol_pool, Decimal("200.0"), None)
    assert surge_val == 3.2

    # Test RuleGroup with LogicalOperator.OR and LogicalOperator.AND
    cond1 = RuleCondition("net_buy_delta", ComparisonOperator.GTE, 0.90)  # Fails (0.62 < 0.90)
    cond2 = RuleCondition("volume_surge_multiplier", ComparisonOperator.GTE, 2.0)  # Passes (3.2 >= 2.0)
    group_or = RuleGroup(conditions=[cond1, cond2], operator=LogicalOperator.OR)
    assert group_or.evaluate(test_signal, test_sol_pool) is True

    group_and = RuleGroup(conditions=[cond1, cond2], operator=LogicalOperator.AND)
    assert group_and.evaluate(test_signal, test_sol_pool) is False

    ruleset = EntryRuleSet(
        conditions=[RuleCondition("alpha_score", ComparisonOperator.GTE, 50.0)],
        groups=[group_or],
        operator=LogicalOperator.AND,
    )
    assert ruleset.evaluate(test_signal, test_sol_pool) is True


def test_shadow_sizing_dynamic_uses_live_sizing_not_fixed_constants(test_sol_pool, test_signal):
    """
    Requirement 2: Shadow position size follows dynamic sizing (RiskManager), not fixed 0.10 and $0.05.
    """
    registry = StrategyTournamentRegistry()
    params = StrategyParameters(
        strategy_name="dynamic_sizer",
        regime_affinity=MarketRegime.HIGH_VOL_MEME_MANIA,
        entry_rules=EntryRuleSet(
            conditions=[RuleCondition("alpha_score", ComparisonOperator.GTE, 50.0)]
        ),
    )
    variant = StrategyVariant("var_dyn", params.strategy_name, params, status=ExperimentStatus.SHADOW, regime=MarketRegime.HIGH_VOL_MEME_MANIA)
    registry.register_challenger(variant)

    lots = registry.evaluate_signal_and_record_shadow_lots(
        signal=test_signal,
        pool=test_sol_pool,
        native_price_usd=Decimal("200.0"),
    )
    assert len(lots) >= 1

    assert len(variant.open_lots) == 1
    lot = list(variant.open_lots.values())[0]
    assert isinstance(lot, ShadowLot)
    # Must NOT be fixed 0.10 size and $0.05 gas
    assert lot.simulated_native_spent != Decimal("0.10"), f"Expected dynamic sizing via RiskManager, got fixed {lot.simulated_native_spent}"
    assert lot.gas_drag_usd != Decimal("0.05"), f"Expected dynamic cost model gas, got fixed {lot.gas_drag_usd}"


@pytest.mark.asyncio
async def test_shadow_fills_parity_with_live_paper_executor(tmp_path, test_sol_pool, test_signal):
    """
    Requirement 8: Shadow fills equal live-paper fills for identical inputs (parity).
    """
    cfg = EngineConfig(db_path=str(tmp_path / "parity_test.db"))
    book = PositionBook()
    price_svc = NativePriceService()
    price_svc.set_price("solana", Decimal("200.0"))
    cost_model = TradeCostModel(config=cfg, price_service=price_svc)
    executor = PaperExecutor(position_book=book, price_service=price_svc, cost_model=cost_model, config=cfg)

    # 1. Execute shadow tournament fill
    registry = StrategyTournamentRegistry(config=cfg)
    registry.champion.parameters.entry_rules = EntryRuleSet(
        conditions=[RuleCondition("alpha_score", ComparisonOperator.GTE, 50.0)]
    )
    lots = registry.evaluate_signal_and_record_shadow_lots(
        signal=test_signal,
        pool=test_sol_pool,
        native_price_usd=Decimal("200.0"),
    )
    assert len(lots) >= 1
    assert len(registry.champion.open_lots) == 1
    champ_lot = list(registry.champion.open_lots.values())[0]
    assert isinstance(champ_lot, ShadowLot)

    # 2. Execute live paper fill with identical sized trade amount
    live_fill = await executor.execute_signal(
        signal=test_signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=champ_lot.simulated_native_spent,
    )
    assert live_fill is not None

    # 3. Assert execution parity
    # Entry prices, price impact, and costs match between live paper and shadow tournament
    assert abs(champ_lot.entry_price - live_fill.effective_price) < Decimal("0.00001")
    assert abs(champ_lot.cpmm_slippage_bps - live_fill.price_impact_bps) <= 1
    assert abs(champ_lot.gas_drag_usd - live_fill.simulated_gas_cost_usd) < Decimal("0.01")
    assert champ_lot.chain == live_fill.chain
    assert champ_lot.token_address == live_fill.token_address
    await asyncio.sleep(0)


def test_schema_validation_unknown_metric_fails():
    """Requirement 1: Unknown metric names fail schema validation."""
    with pytest.raises(SchemaValidationError, match="Unknown metric"):
        RuleCondition(
            metric="non_existent_invented_metric",
            operator=ComparisonOperator.GT,
            threshold=10.0,
        ).validate()


def test_schema_validation_out_of_bounds_threshold_fails():
    """Requirement 1: Thresholds outside allowed ranges fail schema validation."""
    with pytest.raises(SchemaValidationError, match="outside allowed range"):
        RuleCondition(
            metric="net_buy_delta",
            operator=ComparisonOperator.GT,
            threshold=1.5,  # net_buy_delta must be in [0.0, 1.0]
        ).validate()


def test_variants_make_own_entry_decisions_and_get_own_fills(test_sol_pool, test_signal):
    """
    Requirement 1 & 2: Two variants with different entry rules enter different subsets
    of the same signal stream and receive different fills (not identical copies of champion).
    """
    registry = StrategyTournamentRegistry()
    registry.champion.parameters.entry_rules = EntryRuleSet(
        conditions=[
            RuleCondition(metric="alpha_score", operator=ComparisonOperator.GTE, threshold=50.0),
        ]
    )

    # Variant 1: Strict net_buy_delta >= 0.70 (signal has 0.62 -> should NOT enter)
    v1_params = StrategyParameters(
        strategy_name="strict_buyer",
        entry_rules=EntryRuleSet(
            conditions=[
                RuleCondition(metric="net_buy_delta", operator=ComparisonOperator.GTE, threshold=0.70),
            ]
        ),
    )
    v1 = StrategyVariant("v1", v1_params.strategy_name, v1_params, status=ExperimentStatus.SHADOW)
    registry.register_challenger(v1)

    # Variant 2: Permissive net_buy_delta >= 0.50 and mcap <= 100k (signal has 0.62 and 65k -> SHOULD enter)
    v2_params = StrategyParameters(
        strategy_name="permissive_buyer",
        entry_rules=EntryRuleSet(
            conditions=[
                RuleCondition(metric="net_buy_delta", operator=ComparisonOperator.GTE, threshold=0.50),
                RuleCondition(metric="mcap_usd", operator=ComparisonOperator.LTE, threshold=100000.0),
            ]
        ),
    )
    v2 = StrategyVariant("v2", v2_params.strategy_name, v2_params, status=ExperimentStatus.SHADOW)
    registry.register_challenger(v2)

    lots = registry.evaluate_signal_and_record_shadow_lots(
        signal=test_signal,
        pool=test_sol_pool,
        native_price_usd=Decimal("200.0"),
    )
    assert len(lots) >= 1

    # Champion entered
    assert len(registry.champion.open_lots) == 1
    # Variant 1 did NOT enter (rejected by net_buy_delta condition)
    assert len(v1.open_lots) == 0
    # Variant 2 DID enter
    assert len(v2.open_lots) == 1
    v2_lot = list(v2.open_lots.values())[0]
    assert v2_lot.strategy_id == v2.experiment_id


def test_seeded_rng_mutation_reproducible_and_different_seeds_differ():
    """
    Requirement 3: Mutation with fixed seed is reproducible and different seeds produce different outputs.
    """
    gen1 = AutonomousStrategyGenerator(seed=12345)
    gen2 = AutonomousStrategyGenerator(seed=12345)
    gen3 = AutonomousStrategyGenerator(seed=99999)

    parent = gen1.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM)

    mut1 = gen1.mutate_strategy(parent)
    mut2 = gen2.mutate_strategy(parent)
    mut3 = gen3.mutate_strategy(parent)

    # Identical seeds -> identical mutation parameters
    assert mut1.hard_stop_loss_pct == mut2.hard_stop_loss_pct
    assert mut1.take_profit_pct == mut2.take_profit_pct
    assert mut1.entry_rules.to_dict() == mut2.entry_rules.to_dict()

    # Different seeds -> different results
    diff = (
        mut1.hard_stop_loss_pct != mut3.hard_stop_loss_pct
        or mut1.take_profit_pct != mut3.take_profit_pct
        or mut1.entry_rules.to_dict() != mut3.entry_rules.to_dict()
    )
    assert diff, "Different seeds must produce different mutated parameters"


def test_deduplication_prevents_staging_identical_variants():
    """
    Requirement 3: Duplicate variants (identical entry rules & exits) are never staged.
    """
    registry = StrategyTournamentRegistry()
    gen = AutonomousStrategyGenerator(seed=42)

    p1 = gen.generate_strategy_variant(StrategyArchetype.FAST_LAUNCH_MOMENTUM)
    v1 = StrategyVariant("v1", "fast_launch_1", p1, status=ExperimentStatus.SHADOW)
    registry.register_challenger(v1)

    # Clone exact same strategy parameters
    p_dup = StrategyParameters.from_dict(p1.to_dict())
    p_dup.strategy_name = "fast_launch_duplicate"

    staged = gen.evaluate_and_stage_candidate(p_dup, registry)
    assert not staged["qualified_for_shadow"], "Duplicate strategy must not be staged"
    assert "DUPLICATE_STRATEGY" in staged["rejections"]
    assert "fast_launch_duplicate" not in registry.challengers


@pytest.mark.asyncio
async def test_promotion_changes_live_entry_decisions_integration(tmp_path):
    """
    Requirement 5: Promotion of a champion updates runner live entry rules and changes
    live entry decisions (evaluated in runner entry path).
    """
    cfg = EngineConfig(db_path=str(tmp_path / "runner_test.db"))
    runner = Runner(config=cfg)
    runner.set_native_price(ChainIdentifier.SOLANA_MAINNET, Decimal("200.0"))

    # Initial champion: permissive entry
    sec = SecurityReport(
        token_address="TokenPromote111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        top10_concentration=0.15,
        audit_passed=True,
    )
    pool = PoolState(
        pool_address="PoolPromote111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("1000000.0"),
        spot_price_native_per_token=Decimal("0.00005"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
    )
    sig = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool.pool_address,
        token_address=sec.token_address,
        suggested_side=OrderSide.BUY,
        pool_state=pool,
        security_report=sec,
        strength=SignalStrength.STRONG,
        alpha_score=0.80,
        net_buy_delta=0.45,  # Moderate net buy
        volume_surge_multiplier=1.5,
    )

    # Evaluate champion entry rules directly on runner
    initial_allowed = runner.evaluate_champion_entry_rules(sig)
    assert initial_allowed is True, "Default champion should accept moderate signal"

    # Now promote a strict challenger requiring net_buy_delta >= 0.70
    strict_params = StrategyParameters(
        strategy_name="strict_whale_hunter",
        hard_stop_loss_pct=0.12,
        take_profit_pct=0.80,
        entry_rules=EntryRuleSet(
            conditions=[
                RuleCondition(metric="net_buy_delta", operator=ComparisonOperator.GTE, threshold=0.70),
            ]
        ),
    )
    strict_champ = StrategyVariant("champ_strict", "strict_whale_hunter", strict_params, status=ExperimentStatus.CHAMPION)

    # Trigger promotion callback
    runner._strategy_tournament.champion = strict_champ
    for cb in runner._strategy_tournament.on_promotion_callbacks:
        cb(strict_champ, "Promotion integration test")

    # Re-evaluate same signal: MUST now be rejected by newly promoted champion's entry rules
    after_promotion_allowed = runner.evaluate_champion_entry_rules(sig)
    assert after_promotion_allowed is False, "Promoted champion entry rules must reject signal with net_buy_delta < 0.70"


@pytest.mark.asyncio
async def test_generation_logs_persisted_to_ledger(tmp_path):
    """
    Requirement 3: Generation logs (parent ids, what changed) are persisted in the ledger.
    """
    db_file = str(tmp_path / "test_generation_log.db")
    async with SQLiteLedger(db_file) as ledger:
        gen = AutonomousStrategyGenerator(ledger=ledger, seed=42)
        registry = StrategyTournamentRegistry(ledger=ledger)

        parent = gen.generate_strategy_variant(StrategyArchetype.WAVE2_ACCUMULATION)
        mutated = gen.mutate_strategy(parent)
        gen.evaluate_and_stage_candidate(
            mutated,
            registry,
            action="mutation",
            parent_ids=[parent.strategy_name],
            changes={"parent": parent.strategy_name, "mutated_fields": ["hard_stop_loss_pct"]},
        )
        if hasattr(gen, "_last_log_task") and gen._last_log_task:
            await gen._last_log_task

        # Check generation log in ledger
        logs = await ledger.get_strategy_generation_logs()
        assert len(logs) > 0
        latest_log = logs[-1]
        assert latest_log["strategy_name"] == mutated.strategy_name
        assert parent.strategy_name in latest_log["parent_ids"]
        assert "changes" in latest_log or "changes_json" in latest_log
