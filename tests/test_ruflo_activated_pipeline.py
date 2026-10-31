"""
tests/test_ruflo_activated_pipeline.py — Unit & Integration tests for Ruflo Swarm, Vector Store & Pipeline
===========================================================================================================
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.ruflo.agents.learning_agent import RufloLearningAgent
from alpha_engine.ruflo.memory import (
    LightweightVectorStore,
    MemoryEpisode,
    RufloEpisodicMemory,
    RufloIntelligencePipeline,
    RufloSemanticMemory,
    SemanticRule,
)
from alpha_engine.ruflo.protocol import (
    AgentRole,
    ConsensusProtocol,
    MessageType,
    SwarmMessage,
    SwarmTopology,
)
from alpha_engine.ruflo.swarm import RufloSwarmCoordinator


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass


def test_lightweight_vector_store():
    store = LightweightVectorStore(dimension=7)
    v1 = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    v2 = [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0]
    v3 = [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0]

    store.add("item1", v1, metadata={"category": "breakout"})
    store.add("item2", v2, metadata={"category": "breakout"})
    store.add("item3", v3, metadata={"category": "reversion"})

    assert store.size() == 3

    # Query with vector close to item1
    results = store.search([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], top_k=2)
    assert len(results) == 2
    assert results[0][0].item_id == "item1"
    assert results[0][1] > 0.99
    assert results[1][0].item_id == "item2"

    # Query with metadata filter
    filtered = store.search([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], top_k=5, filter_fn=lambda m: m.get("category") == "reversion")
    assert len(filtered) == 0  # v3 has orthogonal similarity (0.0 < 0.50)


@pytest.mark.asyncio
async def test_semantic_memory_lifecycle(temp_db_path: str):
    sem = RufloSemanticMemory(db_path=temp_db_path)
    await sem.connect()

    rule1 = SemanticRule(
        rule_id="rule_slip_1",
        category="SLIPPAGE_GUARD",
        condition="Low liquidity < $5,000",
        action="SCALE_SIZE",
        parameters={"min_liquidity_threshold": 5000.0, "size_dampener": 0.60, "max_slippage_bps": 75},
        confidence=0.85,
        sample_size=3,
        evidence_episodes=["ep1", "ep2"],
    )
    await sem.record_rule(rule1)

    # Query active rules
    rules = sem.query_rules(category="SLIPPAGE_GUARD")
    assert len(rules) == 1
    assert rules[0].rule_id == "rule_slip_1"

    # Evaluate context with low liquidity
    mult, warnings, params = sem.evaluate_rules({"liquidity_usd": 3000.0})
    assert mult <= 0.65
    assert len(warnings) == 1
    assert params.get("max_slippage_bps") == 75

    # Reinforce and decay
    await sem.reinforce_or_decay("SLIPPAGE_GUARD", profitable=True)
    assert sem._rules["rule_slip_1"].confidence > 0.85

    await sem.close()


@pytest.mark.asyncio
async def test_ruflo_intelligence_pipeline(temp_db_path: str):
    episodic = RufloEpisodicMemory(db_path=temp_db_path)
    semantic = RufloSemanticMemory(db_path=temp_db_path)
    pipeline = RufloIntelligencePipeline(episodic_memory=episodic, semantic_memory=semantic)

    await episodic.connect()
    await semantic.connect()

    # Pre-record past winning episode
    ep = MemoryEpisode(
        episode_id="trade_1",
        token_address="TokenA11111111111111111111111111111111",
        chain="solana_mainnet",
        decision_type="ENTER",
        signal_source="MOMENTUM_BREAKOUT",
        confidence_score=0.90,
        feature_vector=[0.7, 0.3, 0.5, 0.6, 0.1, 0.2, 0.5],
        realized_pnl_usd=150.0,
        roi_pct=45.0,
        actual_slippage_bps=50,
        fill_latency_ms=30.0,
        root_cause_tag="TAKE_PROFIT_1",
        summary_reflection="Strong trend continuation",
    )
    await episodic.record_episode(ep)

    # 1. Retrieve
    retrieval = await pipeline.retrieve(
        query_vector=[0.7, 0.3, 0.5, 0.6, 0.1, 0.2, 0.5],
        token_context={"token_address": "TokenA11111111111111111111111111111111", "liquidity_usd": 20000.0},
    )
    assert len(retrieval.matched_episodes) == 1
    assert retrieval.historical_win_rate == 1.0

    # 2. Judge
    judgment = pipeline.judge(
        retrieval=retrieval,
        proposed_signal={"token_address": "TokenA11111111111111111111111111111111", "liquidity_usd": 20000.0},
    )
    assert judgment.verdict in ("PROCEED", "CAUTION")
    assert judgment.size_multiplier >= 0.90

    # 3. Distill from adverse trade with high slippage
    adverse_trade = {
        "trade_id": "trade_bad_1",
        "realized_pnl_usd": -80.0,
        "roi_pct": -25.0,
        "actual_slippage_bps": 220,
        "exit_reason": "STOP_LOSS",
        "signal_source": "TELEGRAM_SCRAPER",
    }
    distilled = await pipeline.distill(adverse_trade)
    assert len(distilled) >= 1
    assert any(r.category == "SLIPPAGE_GUARD" for r in distilled)

    # 4. Consolidate into semantic memory
    res = await pipeline.consolidate(distilled)
    assert res["consolidated_rules"] >= 1

    # Verify rule is now active
    active = semantic.query_rules(category="SLIPPAGE_GUARD")
    assert len(active) >= 1

    await episodic.close()
    await semantic.close()


@pytest.mark.asyncio
async def test_ruflo_learning_agent_tools(temp_db_path: str):
    agent = RufloLearningAgent(
        episodic_memory=RufloEpisodicMemory(db_path=temp_db_path),
        semantic_memory=RufloSemanticMemory(db_path=temp_db_path),
    )
    await agent.start()

    # Tool 1: Distill and consolidate
    trade_info = {
        "trade_id": "trade_surge_1",
        "realized_pnl_usd": 300.0,
        "roi_pct": 75.0,
        "actual_slippage_bps": 40,
        "exit_reason": "RUNNER_TRAILING_HIT",
        "signal_source": "MOMENTUM_BREAKOUT",
    }
    dist_res = await agent.execute_tool("distill_and_consolidate", trade_outcome=trade_info)
    assert dist_res["distilled_rules_count"] >= 1

    # Tool 2: Query semantic rules
    rules = await agent.execute_tool("query_semantic_rules", category="MOMENTUM_CONFIRMATION")
    assert len(rules) >= 1

    # Tool 3: Get pipeline judgment
    judg = await agent.execute_tool(
        "get_pipeline_judgment",
        query_vector=[0.5] * 7,
        proposed_signal={"liquidity_usd": 15000.0, "token_address": "TokenA11111111111111111111111111111111"},
    )
    assert "verdict" in judg
    assert "size_multiplier" in judg

    await agent.stop()
    await agent.episodic_memory.close()
    await agent.semantic_memory.close()


@pytest.mark.asyncio
async def test_ruflo_swarm_coordinator_full_cycle(temp_db_path: str):
    cfg = EngineConfig()
    setattr(cfg, "db_path", temp_db_path)
    setattr(cfg, "ruflo_topology", "adaptive")
    setattr(cfg, "ruflo_consensus", "bft")

    coordinator = RufloSwarmCoordinator(config=cfg)
    assert coordinator.topology == SwarmTopology.ADAPTIVE
    assert coordinator.consensus == ConsensusProtocol.BFT
    assert AgentRole.LEARNER in coordinator.agents

    await coordinator.start()

    # Test consensus execution on clean token
    token = "TokenSolanaClean111111111111111111111111"
    res = await coordinator.execute_trade_consensus(
        token_address=token,
        chain="solana_mainnet",
        liquidity_usd=30000.0,
        raw_confidence=0.88,
        regime="TRENDING",
    )
    assert res["approved"] is True
    assert res["consensus_approved"] is True
    assert res["votes"]["LEARNER"] is True
    assert "pipeline_judgment" in res
    assert res["pipeline_judgment"]["verdict"] in ("PROCEED", "CAUTION")

    # Test closed trade post-mortem hook
    closed_res = await coordinator.on_trade_closed({
        "trade_id": "test_closed_1",
        "realized_pnl_usd": 120.0,
        "roi_pct": 55.0,
        "actual_slippage_bps": 50,
        "exit_reason": "PROFIT_TARGET_HIT",
        "signal_source": "MOMENTUM_BREAKOUT",
    })
    assert closed_res["distilled_rules_count"] >= 1

    # Check status compilation
    status = coordinator.get_swarm_status()
    assert status["agents_count"] == 7
    assert status["is_running"] is True

    await coordinator.stop()


@pytest.mark.asyncio
async def test_swarm_message_protocol_types():
    msg = SwarmMessage(
        sender_role=AgentRole.SCOUT,
        target_role=AgentRole.LEARNER,
        message_type=MessageType.PROPOSAL,
        content="Test pipeline message",
        payload={"action": "test_ping"},
    )
    assert msg.message_type == MessageType.PROPOSAL
    assert msg.sender_role == AgentRole.SCOUT
    await asyncio.sleep(0.01)

