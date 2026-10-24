"""
Unit and Integration Tests for Ruflo Multi-Agent Swarm Orchestration Layer
==========================================================================
Tests the Ruflo protocol, episodic memory, 6 specialized agents, tool & agent registries,
multi-agent consensus engine, self-learning feedback loop, and CLI / chat interfaces.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import ChainIdentifier
from alpha_engine.models.events import SignalSource
from alpha_engine.ruflo.protocol import (
    AgentRole,
    AgentStatus,
    ConsensusProtocol,
    SwarmMessage,
    SwarmTopology,
    ToolDefinition,
)
from alpha_engine.ruflo.memory import RufloEpisodicMemory, MemoryEpisode
from alpha_engine.ruflo.agents.scouting_agent import IngestionScoutingAgent
from alpha_engine.ruflo.agents.profiler_agent import SmartMoneyProfilerAgent
from alpha_engine.ruflo.agents.gatekeeper_agent import SecurityGatekeeperAgent
from alpha_engine.ruflo.agents.quant_agent import QuantRiskSizingAgent
from alpha_engine.ruflo.agents.execution_agent import ExecutionLedgerAgent
from alpha_engine.ruflo.agents.supervisor_agent import RufloSupervisorAgent
from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
from alpha_engine.engine.registry import (
    RufloToolRegistry,
    RufloAgentRegistry,
)
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine
from alpha_engine.chat_interface import ConversationalSupervisor
from alpha_engine.execution.ledger import SQLiteLedger


@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_ruflo.db"
    return str(db_file)


@pytest.fixture
def test_config(temp_db):
    config = EngineConfig(db_path=temp_db)
    config.ruflo_enabled = True
    config.ruflo_topology = "adaptive"
    config.ruflo_consensus = "bft"
    config.ruflo_reflection_interval_s = 1.0
    config.ruflo_max_memory_episodes = 500
    config.paper_trading_mode = True
    return config


# =========================================================================
# 1. Ruflo Protocol & Models Tests
# =========================================================================

def test_ruflo_protocol_definitions():
    """Verify enum roles, topology, consensus, and message schemas."""
    assert AgentRole.SCOUT.value == "SCOUT"
    assert AgentRole.PROFILER.value == "PROFILER"
    assert AgentRole.GATEKEEPER.value == "GATEKEEPER"
    assert AgentRole.QUANT.value == "QUANT"
    assert AgentRole.EXECUTOR.value == "EXECUTOR"
    assert AgentRole.SUPERVISOR.value == "SUPERVISOR"

    assert SwarmTopology.HIERARCHICAL.value == "hierarchical"
    assert SwarmTopology.ADAPTIVE.value == "adaptive"
    assert ConsensusProtocol.BFT.value == "bft"

    tool = ToolDefinition(
        name="test_tool",
        description="A test tool",
        category="math",
        parameters_schema={"type": "object", "properties": {"x": {"type": "integer"}}},
        timeout_seconds=5.0,
    )
    assert tool.name == "test_tool"
    assert tool.category == "math"

    msg = SwarmMessage(
        sender_role=AgentRole.SCOUT,
        target_role=AgentRole.GATEKEEPER,
        action="INSPECT_TOKEN",
        payload={"token": "0x123"},
    )
    assert msg.sender_role == AgentRole.SCOUT
    assert msg.action == "INSPECT_TOKEN"
    assert msg.payload["token"] == "0x123"


# =========================================================================
# 2. Ruflo Episodic Memory Tests
# =========================================================================

@pytest.mark.asyncio
async def test_ruflo_episodic_memory_wal_and_vector_search(temp_db):
    """Test episodic memory persistence, vector search, and performance metrics."""
    memory = RufloEpisodicMemory(db_path=temp_db, max_memory_episodes=50)
    await memory.connect()

    # Record 3 episodes
    ep1 = MemoryEpisode(
        episode_id="trade_1",
        token_address="token_alpha",
        chain="solana_mainnet",
        decision_type="ENTER",
        signal_source="TELEGRAM_SCRAPER",
        confidence_score=0.9,
        feature_vector=[1.0, 0.8, 0.5, 0.2, 0.1],
        realized_pnl_usd=150.0,
        roi_pct=25.0,
        actual_slippage_bps=20,
        fill_latency_ms=30.0,
        root_cause_tag="TAKE_PROFIT_1",
        summary_reflection="Strong momentum breakout copytrade.",
    )
    ep2 = MemoryEpisode(
        episode_id="trade_2",
        token_address="token_beta",
        chain="base_mainnet",
        decision_type="ENTER",
        signal_source="TELEGRAM_SCRAPER",
        confidence_score=0.6,
        feature_vector=[0.1, 0.2, 0.9, 0.8, 0.5],
        realized_pnl_usd=-50.0,
        roi_pct=-10.0,
        actual_slippage_bps=80,
        fill_latency_ms=95.0,
        root_cause_tag="STOP_LOSS",
        summary_reflection="High slippage sandwich attack on Base.",
    )
    ep3 = MemoryEpisode(
        episode_id="trade_3",
        token_address="token_gamma",
        chain="solana_mainnet",
        decision_type="SKIP",
        signal_source="X_STREAM",
        confidence_score=0.3,
        feature_vector=[0.9, 0.7, 0.4, 0.1, 0.0],
        realized_pnl_usd=0.0,
        roi_pct=0.0,
        actual_slippage_bps=0,
        fill_latency_ms=0.0,
        root_cause_tag="SECURITY_VETO",
        summary_reflection="Honeypot vetoed by security gatekeeper.",
    )

    await memory.record_episode(ep1)
    await memory.record_episode(ep2)
    await memory.record_episode(ep3)

    assert await memory.get_episode_count() == 3

    # Vector search with a vector close to ep1
    query_vec = [0.95, 0.75, 0.45, 0.2, 0.1]
    similar = await memory.search_similar_episodes(query_vec, top_k=2)
    assert len(similar) > 0
    top_ep, similarity = similar[0]
    assert top_ep.episode_id in ("trade_1", "trade_3")
    assert similarity > 0.8

    # Query recent reflections
    reflections = await memory.get_recent_reflections(limit=5)
    assert len(reflections) == 3

    # Check source performance analytics
    source_stats = memory.get_source_performance()
    assert "TELEGRAM_SCRAPER" in source_stats
    tg_stats = source_stats["TELEGRAM_SCRAPER"]
    assert tg_stats["total_trades"] == 2
    assert tg_stats["win_rate"] == 0.5
    assert tg_stats["avg_pnl_usd"] == 50.0

    await memory.close()


# =========================================================================
# 3. Tool & Agent Registries with Concurrency Locks
# =========================================================================

@pytest.mark.asyncio
async def test_ruflo_tool_and_agent_registries():
    """Verify tool registration, invocation under concurrency lock, and agent registry."""
    tool_reg = RufloToolRegistry()
    agent_reg = RufloAgentRegistry()

    # Define a tool handler
    async def sample_tool(val: int) -> int:
        return val * 2

    tool_def = ToolDefinition(
        name="double_value",
        description="Multiplies integer by 2",
        category="math",
        parameters_schema={"type": "object", "properties": {"val": {"type": "integer"}}},
    )

    tool_reg.register(
        tool_def=tool_def,
        handler=sample_tool,
        agent_name="QuantAgent",
    )

    assert tool_reg.has_tool("double_value")
    res = await tool_reg.invoke_tool("double_value", val=21)
    assert res == 42

    # Concurrency test: execute multiple tool calls concurrently
    tasks = [tool_reg.invoke_tool("double_value", val=i) for i in range(5)]
    results = await asyncio.gather(*tasks)
    assert results == [0, 2, 4, 6, 8]

    # Agent registry
    dummy_agent = MagicMock()
    dummy_agent.name = "TestScout"
    dummy_agent.role = AgentRole.SCOUT
    dummy_agent.status = AgentStatus.ACTIVE
    dummy_agent.is_healthy = MagicMock(return_value=True)

    agent_reg.register_agent(dummy_agent)
    assert agent_reg.has_role(AgentRole.SCOUT)
    assert agent_reg.get_agent(AgentRole.SCOUT) is dummy_agent
    assert len(agent_reg.get_active_agents()) == 1


# =========================================================================
# 4. Specialized Swarm Agents Tests
# =========================================================================

@pytest.mark.asyncio
async def test_specialized_agents_lifecycle_and_tools(test_config, temp_db):
    """Test all 6 specialized agents' initialization, tool registration, and handlers."""
    # 1. Scouting Agent
    scout = IngestionScoutingAgent(config=test_config)
    await scout.start()
    assert scout.status == AgentStatus.ACTIVE
    assert "scout_stream_status" in scout._tools
    assert "scout_inject_signal" in scout._tools

    # Test tool invocation
    ingest_res = await scout.execute_tool(
        "scout_inject_signal",
        chain="solana_mainnet",
        token_address="TokenA11111111111111111111111111111111111",
        source="TELEGRAM_SCRAPER",
    )
    assert ingest_res["status"] == "SIGNAL_INJECTED"
    assert ingest_res["token"] == "TokenA11111111111111111111111111111111111"

    # 2. Profiler Agent
    profiler = SmartMoneyProfilerAgent(config=test_config)
    await profiler.start()
    assert profiler.status == AgentStatus.ACTIVE
    assert "profiler_evaluate_wallet" in profiler._tools
    assert "profiler_check_whitelist" in profiler._tools

    profile_res = await profiler.execute_tool(
        "profiler_evaluate_wallet",
        wallet_address="Wallet11111111111111111111111111111111111111",
        chain="solana_mainnet",
    )
    assert "eligible_for_copytrade" in profile_res

    # 3. Security Gatekeeper Agent
    gatekeeper = SecurityGatekeeperAgent(config=test_config)
    await gatekeeper.start()
    assert gatekeeper.status == AgentStatus.ACTIVE
    assert "gatekeeper_evaluate_token" in gatekeeper._tools
    assert "gatekeeper_inspect_rugcheck" in gatekeeper._tools

    sec_res = await gatekeeper.execute_tool(
        "gatekeeper_evaluate_token",
        token_address="SafeToken111111111111111111111111111111111",
        chain="solana_mainnet",
        initial_liquidity_usd=25000.0,
    )
    assert "approved" in sec_res

    # 4. Quant Math & Risk Sizing Agent
    quant = QuantRiskSizingAgent(config=test_config)
    await quant.start()
    assert quant.status == AgentStatus.ACTIVE
    assert "quant_calculate_sizing" in quant._tools
    assert "quant_estimate_cpmm_quote" in quant._tools
    assert "quant_evaluate_mev_risk" in quant._tools

    size_res = await quant.execute_tool(
        "quant_calculate_sizing",
        win_rate=0.65,
        win_loss_ratio=2.2,
        confidence=0.85,
        portfolio_equity_usd=10000.0,
        pool_liquidity_usd=50000.0,
    )
    assert size_res["approved"] is True
    assert size_res["recommended_position_usd"] > 0
    assert size_res["stop_loss_pct"] == 0.08

    # 5. Execution & Ledger Agent
    execution = ExecutionLedgerAgent(config=test_config)
    await execution.start()
    assert execution.status == AgentStatus.ACTIVE
    assert "executor_execute_trade" in execution._tools
    assert "executor_get_positions" in execution._tools

    trade_res = await execution.execute_tool(
        "executor_execute_trade",
        token_address="TokenExec111111111111111111111111111111111",
        chain="solana_mainnet",
        side="BUY",
        amount_usd=100.0,
        expected_price=1.50,
        strategy_pattern="RUFLO_CONSENSUS",
        confidence=0.88,
    )
    assert trade_res["status"] == "EXECUTED"
    assert trade_res["trade_id"] is not None

    # 6. Supervisor Agent
    supervisor = RufloSupervisorAgent(config=test_config)
    await supervisor.start()
    assert supervisor.status == AgentStatus.ACTIVE
    assert "supervisor_reflect_trade" in supervisor._tools
    assert "supervisor_audit_signal" in supervisor._tools
    assert "supervisor_evaluate_rpc_health" in supervisor._tools

    refl_res = await supervisor.execute_tool(
        "supervisor_reflect_trade",
        trade_id=trade_res["trade_id"],
        token_address="TokenExec111111111111111111111111111111111",
        realized_pnl_usd=15.5,
        roi_pct=15.5,
        chain="solana_mainnet",
        signal_source="TELEGRAM_SCRAPER",
    )
    assert refl_res["status"] == "RECORDED"

    # Clean up all agents
    for ag in (scout, profiler, gatekeeper, quant, execution, supervisor):
        await ag.stop()
        assert ag.status == AgentStatus.TERMINATED


# =========================================================================
# 5. Ruflo Swarm Coordinator & Multi-Agent Consensus
# =========================================================================

@pytest.mark.asyncio
async def test_ruflo_swarm_coordinator_consensus_flow(test_config):
    """Test full multi-agent consensus trade execution from scouting to execution."""
    coordinator = RufloSwarmCoordinator(config=test_config)
    await coordinator.start()

    status = coordinator.get_swarm_status()
    assert status["is_running"] is True
    assert len(status["agents"]) == 6
    assert status["topology"] == "adaptive"

    # Simulate trade consensus request on a safe token
    consensus_res = await coordinator.execute_trade_consensus(
        token_address="ConsensusToken11111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        source=SignalSource.TELEGRAM_SCRAPER,
        raw_confidence=0.85,
        liquidity_usd=30000.0,
        wallet_address="WhaleWallet11111111111111111111111111111",
    )

    assert consensus_res["approved"] is True
    assert consensus_res["execution_result"] is not None
    assert consensus_res["execution_result"]["status"] == "EXECUTED"
    assert "votes" in consensus_res
    assert consensus_res["votes"]["GATEKEEPER"] is True
    assert consensus_res["votes"]["QUANT"] is True

    # Test veto flow: high rug probability or bad wallet
    veto_res = await coordinator.execute_trade_consensus(
        token_address="VetoToken1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        source=SignalSource.TELEGRAM_SCRAPER,
        raw_confidence=0.85,
        liquidity_usd=500.0,  # Below min liquidity of 5000 -> gatekeeper veto
    )

    assert veto_res["approved"] is False
    assert veto_res["veto_reason"] is not None
    assert "SecurityGatekeeper" in veto_res["veto_reason"]

    await coordinator.stop()
    assert coordinator._is_running is False


# =========================================================================
# 6. Self-Optimization Feedback Loop
# =========================================================================

@pytest.mark.asyncio
async def test_adaptive_feedback_ruflo_audit(temp_db):
    """Test post-trade audit updating source confidence weights and recording episodic memory."""
    memory = RufloEpisodicMemory(db_path=temp_db)
    await memory.connect()

    feedback = AdaptiveFeedbackEngine(episodic_memory=memory)

    # Audit a winning trade from TELEGRAM_SCRAPER
    res1 = await feedback.audit_closed_trade_ruflo(
        trade_id="audit_t1",
        token_address="TokenAudit11111111111111111111111111",
        chain="solana_mainnet",
        signal_source="TELEGRAM_SCRAPER",
        realized_pnl_usd=120.0,
        roi_pct=24.0,
        entry_price=1.0,
        exit_price=1.24,
        actual_slippage_bps=15,
        expected_slippage_bps=20,
        fill_latency_ms=25.0,
    )
    assert res1["status"] == "AUDITED"
    assert res1["slippage_leakage_usd"] == 0.0  # better than expected slippage
    assert feedback.source_confidence_weights["TELEGRAM_SCRAPER"] > 1.0

    # Audit a losing trade from X_STREAM with heavy slippage leakage
    res2 = await feedback.audit_closed_trade_ruflo(
        trade_id="audit_t2",
        token_address="TokenAudit22222222222222222222222222",
        chain="base_mainnet",
        signal_source="X_STREAM",
        realized_pnl_usd=-80.0,
        roi_pct=-16.0,
        entry_price=2.0,
        exit_price=1.68,
        actual_slippage_bps=90,
        expected_slippage_bps=20,
        fill_latency_ms=120.0,
    )
    assert res2["status"] == "AUDITED"
    assert feedback.source_confidence_weights["X_STREAM"] < 1.0

    # Verify memory recorded both episodes
    episodes = await memory.get_episode_count()
    assert episodes == 2

    await memory.close()


# =========================================================================
# 7. Conversational Supervisor & Chat Interface Ruflo Command
# =========================================================================

@pytest.mark.asyncio
async def test_chat_interface_swarm_status_command(test_config, temp_db):
    """Test /swarm command in ConversationalSupervisor."""
    ledger = SQLiteLedger(db_path=temp_db)
    async with ledger:
        coordinator = RufloSwarmCoordinator(config=test_config)
        await coordinator.start()

        chat = ConversationalSupervisor(
            ledger=ledger,
            config=test_config,
            swarm_coordinator=coordinator,
        )

        resp = await chat.ask("/swarm")
        assert "حالة سرب الوكلاء التداولي المتعدد" in resp
        assert "ADAPTIVE" in resp.upper()
        assert "BFT" in resp.upper()
        assert "[SCOUT]" in resp.upper()
        assert "[GATEKEEPER]" in resp.upper()
        assert "[QUANT]" in resp.upper()
        assert "[EXECUTOR]" in resp.upper()
        assert "[SUPERVISOR]" in resp.upper()

        await coordinator.stop()


# =========================================================================
# 8. Rate Limiter Registry Multi-Chain Latency Penalty
# =========================================================================

def test_rate_limiter_registry_ruflo_integration(test_config):
    """Verify RateLimiterRegistry penalize_from_latency adapts dynamic fill penalties."""
    from alpha_engine.rate_limiter.registry import RateLimiterRegistry
    from alpha_engine.rate_limiter.bucket import TokenBucketRateLimiter

    registry = RateLimiterRegistry.from_config(test_config)

    # Initial token bucket
    limiter = registry.get(ChainIdentifier.SOLANA_MAINNET)
    assert isinstance(limiter, TokenBucketRateLimiter)

    # Simulate latency spike over threshold (e.g. 800ms > 500ms threshold)
    registry.penalize_from_latency(ChainIdentifier.SOLANA_MAINNET, latency_ms=850.0, threshold_ms=500.0)
    assert registry.has_penalty(ChainIdentifier.SOLANA_MAINNET)

    # Reset
    registry.reset_penalty(ChainIdentifier.SOLANA_MAINNET)
    assert not registry.has_penalty(ChainIdentifier.SOLANA_MAINNET)
