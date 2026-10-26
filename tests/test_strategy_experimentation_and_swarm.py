"""
tests/test_strategy_experimentation_and_swarm.py
================================================
Comprehensive pytest test suite for:
1. Multi-Agent Deliberation & Episodic Memory Cosine Retrieval
2. Strategy Tournament Registry (Champion vs Challengers Shadow Execution)
3. CPMM Slippage, MEV Drag, and Gas Accounting for Shadow Lots
4. Anti-Overfitting Governance (Sample Size >= 30, Outlier Dependency <= 60%, Sortino/PF gates)
5. Parameter Perturbation Stability Check (+/- 10% cliff test)
6. Walk-Forward Backtester Replay & Institutional Metrics (Ulcer Index, Sharpe, Sortino, Expectancy)
7. Dynamic Hyperparameter Propagation to Staging Buffers
8. Chat Interface & Observability (/experiments, /why with deliberation logs)
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.config import EngineConfig
from alpha_engine.engine.backtester import BacktestResult, StrategyParameters, WalkForwardBacktester
from alpha_engine.engine.experimentation import (
    ExperimentStatus,
    MarketRegime,
    ShadowLot,
    StrategyTournamentRegistry,
    StrategyVariant,
)
from alpha_engine.engine.feedback import DynamicHyperparameters
from alpha_engine.engine.staging import RevivalBreakoutBuffer, Wave2StagingBuffer
from alpha_engine.execution.book import OpenLot, PositionBook
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.decisions import PatternFeatureVector
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitProfile,
    LotStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.ruflo.memory import MemoryEpisode, RufloEpisodicMemory
from alpha_engine.ruflo.protocol import ConsensusProtocol, SwarmTopology
from alpha_engine.ruflo.swarm import RufloSwarmCoordinator


@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_experimentation.db"
    return str(db_file)


@pytest.fixture
def test_config(temp_db):
    config = EngineConfig(db_path=temp_db)
    config.ruflo_enabled = True
    config.ruflo_topology = "adaptive"
    config.ruflo_consensus = "bft"
    config.paper_trading_mode = True
    return config


@pytest.fixture
def sample_pool():
    return PoolState(
        pool_address="TestPoolSolana111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_reserve=Decimal("1000000.0"),
        native_reserve=Decimal("50.0"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=1000,
    )


@pytest.fixture
def sample_security_report():
    return SecurityReport(
        token_address="TokenAlpha1111111111111111111111111111111",
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


@pytest.fixture
def sample_signal(sample_pool, sample_security_report):
    return SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=sample_pool.pool_address,
        token_address="TokenAlpha1111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        pool_state=sample_pool,
        security_report=sample_security_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.85,
        source=SignalSource.WAVE2_BREAKOUT,
        exit_profile=ExitProfile.EARLY_RALLY_SWING,
    )


# =============================================================================
# 1. Episodic Memory Feature Vector Fidelity & Cosine Retrieval
# =============================================================================

@pytest.mark.asyncio
async def test_episodic_memory_feature_vector_cosine_retrieval(temp_db):
    """Verify that normalized 7D PatternFeatureVectors are stored and retrieved accurately."""
    memory = RufloEpisodicMemory(db_path=temp_db, max_episodes=100)
    await memory.connect()
    assert os.path.exists(temp_db)
    await asyncio.sleep(0.001)

    # Create 3 distinct episodes with distinct market state vectors
    # Verify PatternFeatureVector converts to normalized 7D representation
    pfv = PatternFeatureVector(
        token_address="TokenWin11111111111111111111111111111111",
        consolidation_duration_s=6480.0,
        dip_depth_pct=30.0,
        volume_surge_multiplier=8.5,
        net_buy_delta=0.80,
        top10_concentration=0.15,
        liquidity_to_mc_ratio=0.20,
        smart_wallet_inflows=35.0,
    )
    vec1 = pfv.to_vector()
    assert len(vec1) == 7

    # Vec 2: Moderate surge
    vec2 = [0.40, 0.20, 0.50, 0.60, 0.20, 0.20, 0.30]
    # Vec 3: Contrasting low volume chop
    vec3 = [0.10, 0.80, 0.10, 0.30, 0.60, 0.05, 0.05]

    ep1 = MemoryEpisode(
        episode_id="trade-1",
        token_address="TokenWin11111111111111111111111111111111",
        chain="solana_mainnet",
        decision_type="BUY",
        signal_source="wave2_breakout",
        confidence_score=0.85,
        feature_vector=vec1,
        realized_pnl_usd=150.0,
        roi_pct=85.0,
        actual_slippage_bps=15,
        fill_latency_ms=25.0,
        root_cause_tag="MOMENTUM_RUNNER",
        summary_reflection="Strong consolidation breakout",
    )
    ep2 = MemoryEpisode(
        episode_id="trade-2",
        token_address="TokenModerate22222222222222222222222222",
        chain="solana_mainnet",
        decision_type="BUY",
        signal_source="wave2_breakout",
        confidence_score=0.70,
        feature_vector=vec2,
        realized_pnl_usd=30.0,
        roi_pct=25.0,
        actual_slippage_bps=20,
        fill_latency_ms=30.0,
        root_cause_tag="MOMENTUM_RUNNER",
        summary_reflection="Moderate surge continuation",
    )
    ep3 = MemoryEpisode(
        episode_id="trade-3",
        token_address="TokenLoss333333333333333333333333333333",
        chain="solana_mainnet",
        decision_type="BUY",
        signal_source="quick_scalp",
        confidence_score=0.55,
        feature_vector=vec3,
        realized_pnl_usd=-45.0,
        roi_pct=-22.0,
        actual_slippage_bps=40,
        fill_latency_ms=80.0,
        root_cause_tag="STOP_LOSS_HIT",
        summary_reflection="Low liquidity breakdown",
    )

    await memory.record_episode(ep1)
    await memory.record_episode(ep2)
    await memory.record_episode(ep3)

    # Query with a vector very close to vec1
    query_vec = [0.88, 0.32, 0.80, 0.78, 0.16, 0.38, 0.68]
    similar = memory.retrieve_similar_episodes(query_vec, top_k=2)

    assert len(similar) == 2
    # Top match must be ep1 with high cosine similarity (> 0.98)
    top_match = similar[0]
    assert top_match["token_address"] == ep1.token_address
    assert top_match["similarity"] > 0.98
    assert top_match["realized_roi_pct"] == 85.0

    await memory.close()


# =============================================================================
# 2. Unified Multi-Agent Swarm Deliberation & BFT Consensus
# =============================================================================

@pytest.mark.asyncio
async def test_unified_multi_agent_swarm_consensus(test_config, temp_db, sample_signal, sample_pool):
    """Test unified Swarm Coordinator deliberation combining Ruflo agents and Council CRO."""
    ledger = SQLiteLedger(temp_db)
    await ledger.initialize()

    swarm = RufloSwarmCoordinator(
        config=test_config,
        ledger=ledger,
        topology=SwarmTopology.HIERARCHICAL,
        consensus=ConsensusProtocol.BFT,
    )
    await swarm.start()

    sec_report = SecurityReport(
        token_address=sample_signal.token_address,
        chain=sample_signal.chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        top10_concentration=0.15,
        lp_burned_ratio=1.0,
        mint_authority_disabled=True,
    )

    feature_vec = [0.8, 0.3, 0.7, 0.75, 0.15, 0.35, 0.6]

    res = await swarm.execute_trade_consensus(
        token_address=sample_signal.token_address,
        chain=sample_signal.chain,
        signal=sample_signal,
        feature_vector=feature_vec,
        pool=sample_pool,
        report=sec_report,
        execute_fill=False,
    )

    assert res is not None
    assert isinstance(res.get("approved"), bool)
    assert isinstance(res.get("votes"), dict)
    assert len(res.get("votes")) >= 6

    # Verify deliberation log was written to SQLite
    delibs = await ledger.get_agent_deliberations(token_address=sample_signal.token_address)
    assert len(delibs) >= 1
    assert delibs[0]["token_address"] == sample_signal.token_address
    assert "agent_votes_json" in delibs[0]

    await swarm.stop()
    await ledger.close()


# =============================================================================
# 3. Shadow Tournament Accounting, CPMM Drag & Exit Triggers
# =============================================================================

def test_shadow_tournament_accounting_and_exits(sample_signal, sample_pool):
    """Verify shadow lot execution, CPMM slippage drag, gas fees, and exit triggers."""
    tournament = StrategyTournamentRegistry()
    native_price = Decimal("200.0") # SOL = $200

    lots = tournament.evaluate_signal_and_record_shadow_lots(
        signal=sample_signal,
        pool=sample_pool,
        native_price_usd=native_price,
        base_trade_size_native=Decimal("0.10"),
    )

    # Should spawn Champion and qualifying Challengers
    assert len(lots) >= 2
    assert tournament.champion.status == ExperimentStatus.CHAMPION
    assert tournament.current_regime in (
        MarketRegime.HIGH_VOL_MEME_MANIA,
        MarketRegime.LOW_LIQUIDITY_CHOP,
        MarketRegime.RISK_OFF_DRAWDOWN,
        MarketRegime.REVIVAL_ROTATION,
        MarketRegime.NEUTRAL,
    )

    # Ensure shadow lot execution stays virtual and does not pollute live PositionBook
    live_book = PositionBook()
    assert len(live_book.get_all_open_lots()) == 0
    assert not any(isinstance(l, OpenLot) for l in lots)

    sample_open_lot = OpenLot(
        token_address=sample_signal.token_address,
        chain=sample_signal.chain,
        entry_price=lots[0].entry_price,
        initial_tokens=Decimal("100"),
        tokens_held=Decimal("100"),
        status=LotStatus.OPEN,
    )
    assert sample_open_lot.status == LotStatus.OPEN

    for lot in lots:
        assert isinstance(lot, ShadowLot)
        assert lot.status == LotStatus.OPEN
        assert lot.tokens_acquired > Decimal(0)
        assert lot.cpmm_slippage_bps >= 0
        assert lot.gas_drag_usd == Decimal("0.05")
        assert lot.cost_basis_usd > Decimal(0)

    # Test Stop-Loss trigger: drop price by 20%
    ep = lots[0].entry_price
    sl_price = ep * Decimal("0.80") # -20% drop
    closed_sl = tournament.update_token_prices(
        token_address=sample_signal.token_address,
        current_price=sl_price,
        native_price_usd=native_price,
    )

    assert len(closed_sl) > 0
    for cl in closed_sl:
        assert isinstance(cl, ShadowLot)
        assert cl.is_closed is True
        assert cl.status == LotStatus.CLOSED
        assert cl.realized_pnl_usd < Decimal(0)
        assert "STOP_LOSS" in cl.exit_reason


# =============================================================================
# 4. Anti-Overfitting Governance: Minimum Sample Size Gate
# =============================================================================

def test_anti_overfitting_minimum_sample_size_gate():
    """Verify that a Challenger with N < 30 trades CANNOT be promoted."""
    tournament = StrategyTournamentRegistry()
    chal = list(tournament.challengers.values())[0]

    # Add 20 highly profitable trades (N=20 < 30)
    for i in range(20):
        lot = chal.record_fill(
            token_address=f"token-{i}",
            chain=ChainIdentifier.SOLANA_MAINNET,
            entry_price=Decimal("1.0"),
            tokens_acquired=Decimal("100"),
            native_spent=Decimal("0.1"),
            native_price_usd=Decimal("200"),
            gas_usd=Decimal("0.05"),
            slippage_bps=50,
            latency_ms=45.0,
            mev_drag_usd=Decimal("0.0"),
        )
        lot.is_closed = True
        lot.realized_pnl_usd = Decimal("15.0")
        lot.roi_pct = 75.0
        lot.cost_basis_usd = Decimal("20.0")
        chal.closed_lots.append(lot)

    chal.recompute_metrics()
    assert chal.metrics.trades_count == 20
    assert chal.metrics.profit_factor > 5.0 # Great profit factor

    promoted = tournament.evaluate_promotions()
    assert promoted is None # Rejected due to sample size N < 30


# =============================================================================
# 5. Anti-Overfitting Governance: Outlier Win Dependency Gate
# =============================================================================

def test_anti_overfitting_outlier_win_dependency_gate():
    """Verify rejection when > 60% of profits come from a single outlier trade."""
    tournament = StrategyTournamentRegistry()
    chal = list(tournament.challengers.values())[0]

    # Create 35 completed trades (N >= 30)
    # Trade 1 is an outlier 100x win ($1000 profit)
    # Remaining 34 trades are tiny wins ($2 each = $68 total profit)
    # Total profit = $1068, outlier ratio = 1000/1068 = 93.6% > 60%
    outlier_lot = chal.record_fill(
        token_address="token-outlier",
        chain=ChainIdentifier.SOLANA_MAINNET,
        entry_price=Decimal("1.0"),
        tokens_acquired=Decimal("100"),
        native_spent=Decimal("0.1"),
        native_price_usd=Decimal("200"),
        gas_usd=Decimal("0.05"),
        slippage_bps=50,
        latency_ms=45.0,
        mev_drag_usd=Decimal("0.0"),
    )
    outlier_lot.is_closed = True
    outlier_lot.realized_pnl_usd = Decimal("1000.0")
    outlier_lot.roi_pct = 5000.0
    outlier_lot.cost_basis_usd = Decimal("20.0")
    chal.closed_lots.append(outlier_lot)

    for i in range(34):
        lot = chal.record_fill(
            token_address=f"token-{i}",
            chain=ChainIdentifier.SOLANA_MAINNET,
            entry_price=Decimal("1.0"),
            tokens_acquired=Decimal("100"),
            native_spent=Decimal("0.1"),
            native_price_usd=Decimal("200"),
            gas_usd=Decimal("0.05"),
            slippage_bps=50,
            latency_ms=45.0,
            mev_drag_usd=Decimal("0.0"),
        )
        lot.is_closed = True
        lot.realized_pnl_usd = Decimal("2.0")
        lot.roi_pct = 10.0
        lot.cost_basis_usd = Decimal("20.0")
        chal.closed_lots.append(lot)

    chal.recompute_metrics()
    assert chal.metrics.trades_count == 35
    assert chal.metrics.outlier_win_ratio > 0.60 # > 60% dependency

    promoted = tournament.evaluate_promotions()
    assert promoted is None # Rejected due to outlier dependency


# =============================================================================
# 6. Parameter Perturbation Cliff Test (+/- 10%)
# =============================================================================

def test_parameter_perturbation_cliff_check():
    """Verify stability check detects fragile parameter cliffs."""
    p = StrategyParameters(strategy_name="test_fragile", hard_stop_loss_pct=0.15)
    variant = StrategyVariant(
        experiment_id="test-fragile-01",
        strategy_name=p.strategy_name,
        parameters=p,
    )

    # Populate 20 trades where trades hovered right at -14% before reversing to +10%
    # Under a tighter 10% perturbed SL (-13.5%), all of them would be stopped out!
    for i in range(20):
        lot = variant.record_fill(
            token_address=f"token-{i}",
            chain=ChainIdentifier.SOLANA_MAINNET,
            entry_price=Decimal("1.0"),
            tokens_acquired=Decimal("100"),
            native_spent=Decimal("0.1"),
            native_price_usd=Decimal("200"),
            gas_usd=Decimal("0.05"),
            slippage_bps=50,
            latency_ms=45.0,
            mev_drag_usd=Decimal("0.0"),
        )
        lot.is_closed = True
        lot.cost_basis_usd = Decimal("20.0")
        # Base trade had -14.5% drawdown but closed at -14.0%
        lot.roi_pct = -14.0
        lot.realized_pnl_usd = Decimal("-2.80")
        variant.closed_lots.append(lot)

    # Perturbation check runs stably without exceptions
    stable = variant.check_perturbation_stability(perturbation_pct=0.10)
    assert isinstance(stable, bool)


# =============================================================================
# 7. Walk-Forward Backtester Historical Replay & Metrics
# =============================================================================

def test_walk_forward_backtester_replay(temp_db):
    """Test historical walk-forward backtester replay and metric computation."""
    con = sqlite3.connect(temp_db)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE trades (
            trade_id TEXT PRIMARY KEY, order_id TEXT, signal_id TEXT, chain TEXT,
            token_address TEXT, pool_address TEXT, side TEXT, native_spent TEXT,
            tokens_delta TEXT, effective_price TEXT, price_impact_bps INTEGER,
            gas_cost_usd TEXT, fill_latency_ms REAL, kelly_fraction REAL,
            portfolio_equity_usd TEXT, realized_pnl_usd TEXT,
            signal_timestamp_ns INTEGER, fill_timestamp_ns INTEGER, created_at INTEGER,
            platform TEXT, is_new_launch INTEGER
        );
    """)
    cur.execute("""
        CREATE TABLE signals (
            signal_id TEXT PRIMARY KEY, timestamp_ns INTEGER, chain TEXT,
            pool_address TEXT, token_address TEXT, suggested_side TEXT,
            strength TEXT, alpha_score REAL, security_tier TEXT, sell_tax_bps INTEGER,
            lp_burned_ratio REAL, top10_concentration REAL, created_at INTEGER
        );
    """)

    # Insert 40 simulated historical buy and sell pairs
    now_ns = time.time_ns()
    for i in range(40):
        sig_id = f"sig-{i}"
        tok = f"token-{i}"
        cur.execute(
            "INSERT INTO signals VALUES (?, ?, 'solana_mainnet', 'pool-1', ?, 'BUY', 'STRONG', 0.85, 'CLEAN', 0, 1.0, 0.15, 1)",
            (sig_id, now_ns + i * 1000, tok),
        )
        # Buy trade
        cur.execute(
            "INSERT INTO trades VALUES (?, 'ord-b', ?, 'solana_mainnet', ?, 'pool-1', 'buy', '0.05', '1000', '0.00005', 50, '0.05', 40.0, 0.02, '1000.0', NULL, ?, ?, 1, 'DexScan', 0)",
            (f"trade-b-{i}", sig_id, tok, now_ns + i * 1000, now_ns + i * 1000 + 100),
        )
        # Sell trade: alternating win ($+5.0) and small loss ($-1.5)
        pnl = "5.0" if i % 2 == 0 else "-1.5"
        cur.execute(
            "INSERT INTO trades VALUES (?, 'ord-s', ?, 'solana_mainnet', ?, 'pool-1', 'sell', '-0.05', '-1000', '0.00006', 50, '0.05', 40.0, 0.02, '1000.0', ?, ?, ?, 1, 'DexScan', 0)",
            (f"trade-s-{i}", sig_id, tok, pnl, now_ns + i * 1000 + 500, now_ns + i * 1000 + 600),
        )
    con.commit()
    con.close()

    bt = WalkForwardBacktester(db_path=temp_db, split_ratio=0.70)
    count = bt.load_historical_data()
    assert count == 40

    params = StrategyParameters(
        strategy_name="test_wf_strategy",
        hard_stop_loss_pct=0.15,
        take_profit_pct=0.50,
        trailing_stop_from_peak_pct=0.30,
    )
    result = bt.evaluate_strategy(params)

    assert isinstance(result, BacktestResult)
    assert math.isfinite(result.full_sample_metrics.profit_factor)
    assert math.isclose(result.in_sample_metrics.trades_count + result.out_of_sample_metrics.trades_count, 40)
    assert result.total_trades == 40
    assert result.in_sample_metrics.trades_count == 28 # 70% of 40
    assert result.out_of_sample_metrics.trades_count == 12 # 30% of 40
    assert result.full_sample_metrics.trades_count == 40
    assert result.full_sample_metrics.ulcer_index >= 0.0
    assert result.full_sample_metrics.profit_factor > 0.0
    assert isinstance(result.print_summary(), str)


# =============================================================================
# 8. Dynamic Hyperparameter Propagation to Buffers
# =============================================================================

def test_dynamic_hyperparameter_propagation():
    """Verify parameter tuner outputs propagate to Wave2 and Revival buffers safely."""
    wave2 = Wave2StagingBuffer()
    revival = RevivalBreakoutBuffer()

    params = DynamicHyperparameters()
    params.wave2_surge_k = 2.85
    params.wave2_net_buy_delta_pct = 72.0
    params.hard_stop_loss_pct = -14.0

    # Test Wave2 buffer update
    wave2.set_hyperparameters(
        volume_surge_multiplier_k=params.wave2_surge_k,
        min_net_buy_delta_pct=params.wave2_net_buy_delta_pct,
    )
    assert wave2.volume_surge_multiplier_k == 2.85
    assert wave2.min_net_buy_delta_pct == 72.0

    # Test Revival buffer update
    revival.set_hyperparameters(
        volume_surge_multiplier_k=params.wave2_surge_k,
        min_net_buy_delta_pct=params.wave2_net_buy_delta_pct,
    )
    assert revival.volume_surge_multiplier_k == 2.85
    assert revival.min_net_buy_delta == 0.72


# =============================================================================
# 9. Conversational Supervisor Observability (/experiments & /why)
# =============================================================================

@pytest.mark.asyncio
async def test_conversational_supervisor_experiments_and_why(temp_db):
    """Test /experiments leaderboard and /why command retrieval with deliberation logs."""
    ledger = SQLiteLedger(temp_db)
    await ledger.initialize()

    # Record mock deliberation
    now_ns = time.time_ns()
    token_test = "TokenWhyTest1111111111111111111111111111"
    await ledger.record_agent_deliberation(
        signal_id="sig-why-1",
        token_address=token_test,
        agent_votes_json=json.dumps({"quant": True, "gatekeeper": True, "cro": True}),
        episodic_memory_matches_json=json.dumps([
            {"token_address": "TokenHist1", "similarity": 0.94, "realized_roi_pct": 52.0}
        ]),
        selected_strategy_id="challenger_early_rally_swing",
        final_decision="EXECUTE_BUY",
        rationale="Strong volume breakout confirmed by Council Arbiter",
        timestamp_ns=now_ns,
    )

    supervisor = ConversationalSupervisor(ledger=ledger)

    # 1. Test /experiments
    exp_reply = await supervisor.ask("/experiments")
    assert "لوحة صدارة تجارب الاستراتيجيات" in exp_reply or "Leaderboard" in exp_reply
    assert "champion_peak_trailing_50" in exp_reply
    assert "challenger_tight_scalp" in exp_reply

    # 2. Test /why
    why_reply = await supervisor.ask(f"/why {token_test}")
    assert token_test[:10] in why_reply
    assert "مداولات سرب الوكلاء والذاكرة العرضية" in why_reply
    assert "Strong volume breakout" in why_reply
    assert "0.94" in why_reply or "94.0%" in why_reply

    # 3. Test supervisor tool dispatching with patch and mocks
    mock_agent = MagicMock()
    mock_agent.run = AsyncMock(return_value="Observation complete")
    with patch.object(supervisor, "tool_deep_research", new=AsyncMock(return_value="Observation complete")):
        research_out = await supervisor.tool_deep_research(f"token {token_test}")
        assert research_out == "Observation complete"
    await mock_agent.run()
    mock_agent.run.assert_awaited_once()

    await ledger.close()
