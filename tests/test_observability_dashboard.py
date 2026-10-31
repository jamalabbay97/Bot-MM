"""
tests/test_observability_dashboard.py — Unit & Integration tests for Dashboard, REST, WebSockets & Telegram
===========================================================================================================
"""

from __future__ import annotations

import asyncio
import pytest
from starlette.testclient import TestClient

from alpha_engine.chat_interface import ChatInterface
from alpha_engine.dashboard.app import create_app
from alpha_engine.dashboard.server import DashboardServer
from alpha_engine.dashboard.telemetry import DashboardTelemetryAggregator


@pytest.mark.asyncio
async def test_telemetry_aggregator():
    agg = DashboardTelemetryAggregator()
    agg.record_stage_event("ingested_signals", 10)
    agg.record_stage_event("security_passed", 8)

    snapshot = await agg.get_full_telemetry_snapshot()
    assert "system" in snapshot
    assert "connections" in snapshot
    assert "pipeline_flow" in snapshot
    assert "strategies" in snapshot
    assert "portfolio" in snapshot
    assert "debates" in snapshot
    assert "alerts" in snapshot

    assert snapshot["system"]["memory_mb"] > 0
    assert len(snapshot["strategies"]) >= 7
    assert snapshot["connections"]["solana_rpc"]["status"] == "HEALTHY"


def test_fastapi_rest_endpoints():
    agg = DashboardTelemetryAggregator()
    app = create_app(agg)
    client = TestClient(app)

    # 1. Root dashboard UI HTML
    res_root = client.get("/")
    assert res_root.status_code == 200
    assert "AlphaBot" in res_root.text

    # 2. Status API
    res_status = client.get("/api/status")
    assert res_status.status_code == 200
    data_status = res_status.json()
    assert "pipeline_flow" in data_status

    # 3. Pipeline flow API
    res_pipeline = client.get("/api/pipeline")
    assert res_pipeline.status_code == 200
    data_pipeline = res_pipeline.json()
    assert len(data_pipeline["stages"]) == 6

    # 4. Strategies API
    res_strat = client.get("/api/strategies")
    assert res_strat.status_code == 200
    data_strat = res_strat.json()
    assert len(data_strat) >= 7

    # 5. Portfolio API
    res_port = client.get("/api/portfolio")
    assert res_port.status_code == 200
    data_port = res_port.json()
    assert "portfolio_equity_usd" in data_port

    # 6. Health API
    res_health = client.get("/api/health")
    assert res_health.status_code == 200
    data_health = res_health.json()
    assert "solana_rpc" in data_health

    # 7. Alerts API
    res_alerts = client.get("/api/alerts")
    assert res_alerts.status_code == 200
    assert isinstance(res_alerts.json(), list)

    # 8. Trades API
    res_trades = client.get("/api/trades")
    assert res_trades.status_code == 200
    data_trades = res_trades.json()
    assert "summary" in data_trades
    assert "open_positions" in data_trades
    assert "recent_trades" in data_trades

    # 9. Agents API
    res_agents = client.get("/api/agents")
    assert res_agents.status_code == 200
    data_agents = res_agents.json()
    assert "agents" in data_agents
    assert len(data_agents["agents"]) >= 6

    # 10. Decisions API
    res_dec = client.get("/api/decisions")
    assert res_dec.status_code == 200
    assert isinstance(res_dec.json(), list)

    # 11. Risk API
    res_risk = client.get("/api/risk")
    assert res_risk.status_code == 200
    data_risk = res_risk.json()
    assert "risk_mode" in data_risk
    assert "global_circuit_breaker" in data_risk

    # 12. Activity Logs API
    res_act = client.get("/api/activity")
    assert res_act.status_code == 200
    assert isinstance(res_act.json(), list)

    # 13. Token Deep-Dive API
    res_tok = client.get("/api/token/4HmTNc9zqaYBfF47mDz6j9Hh3PQPfEdJYfQgfGJHZ5kW")
    assert res_tok.status_code == 200
    assert "token_address" in res_tok.json()


@pytest.mark.asyncio
async def test_dashboard_server_lifecycle():
    agg = DashboardTelemetryAggregator()
    # Choose a high random test port
    server = DashboardServer(host="127.0.0.1", port=18921, aggregator=agg)
    await server.start()
    assert server._task is not None
    await asyncio.sleep(0.1)
    await server.stop()


@pytest.mark.asyncio
async def test_chat_interface_telegram_dashboard_commands():
    from alpha_engine.execution.ledger import SQLiteLedger
    ledger = SQLiteLedger(db_path=":memory:")
    chat = ChatInterface(ledger=ledger)

    # Test /dashboard
    dash_reply = await chat.ask("/dashboard")
    assert "AlphaBot Institutional Observability Hub" in dash_reply
    assert "http://localhost:8000/" in dash_reply

    # Test /strategies
    strat_reply = await chat.ask("/strategies")
    assert "MOMENTUM_BREAKOUT" in strat_reply
    assert "MEAN_REVERSION" in strat_reply
    assert "LIQUIDITY_SWEEP" in strat_reply


@pytest.mark.asyncio
async def test_staging_and_supervisor_telemetry_flow():
    agg = DashboardTelemetryAggregator()
    
    # Record stage events simulating engine lifecycle
    agg.record_stage_event("ingested_signals", 50)
    agg.record_stage_event("security_passed", 30)
    agg.record_stage_event("staging_promoted", 25)
    agg.record_stage_event("council_deliberated", 20)
    agg.record_stage_event("council_approved", 18)
    agg.record_stage_event("supervisor_audited", 18)
    agg.record_stage_event("executed_orders", 15)

    # Record STAGING and SUPERVISOR activities
    agg.record_activity(
        category="STAGING",
        title="Token Staged in Launch Buffer",
        details="Observation window active",
        level="INFO",
        component="PendingLaunchBuffer",
        token_address="TokenStaged111111111111111111111111111111",
    )
    agg.record_activity(
        category="SUPERVISOR",
        title="Supervisor: APPROVED",
        details="Confidence: 0.92 | Pre-flight risk profile optimal",
        level="SUCCESS",
        component="AlphaSupervisorAI",
        token_address="TokenSupervised11111111111111111111111111",
    )

    flow = await agg.get_pipeline_flow()
    stages = {s["name"]: s["throughput_count"] for s in flow["stages"]}
    assert stages["Ingestion"] >= 50
    assert stages["Security Gateway"] >= 30
    assert stages["Staging Buffers"] >= 25
    assert stages["Agent Council"] >= 20
    assert stages["Alpha Supervisor"] >= 18
    assert stages["Execution Engine"] >= 15

    # Verify dropoff statistics are sane
    assert flow["dropoff_stats"]["council_vetoed_pct"] < 50.0

    # Verify activity logs contain STAGING and SUPERVISOR
    logs = await agg.get_activity_logs(limit=20)
    cats = {l["category"] for l in logs}
    assert "STAGING" in cats
    assert "SUPERVISOR" in cats

    # Verify snapshot includes total_decisions
    snapshot = await agg.get_full_telemetry_snapshot()
    assert "total_decisions" in snapshot

