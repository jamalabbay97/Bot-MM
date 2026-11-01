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


def test_instant_trade_execution_endpoints():
    agg = DashboardTelemetryAggregator()
    app = create_app(agg)
    client = TestClient(app)

    # 1. Valid Solana BUY instant execution
    solana_ca = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC on Solana
    res_buy_sol = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": solana_ca,
            "side": "BUY",
            "amount_native": 0.5,
            "slippage_bps": 150,
        },
    )
    assert res_buy_sol.status_code == 200
    data_buy_sol = res_buy_sol.json()
    assert data_buy_sol["status"] == "SUCCESS"
    assert data_buy_sol["token_address"] == solana_ca
    assert data_buy_sol["side"] == "BUY"
    assert data_buy_sol["chain"] == "solana_mainnet"
    assert "tx_hash" in data_buy_sol

    # Verify position is immediately visible in portfolio
    res_port_after_buy = client.get("/api/portfolio")
    assert res_port_after_buy.status_code == 200
    open_pos = res_port_after_buy.json()["open_positions"]
    assert any(p["token_address"] == solana_ca for p in open_pos)

    # 2. Valid EVM (Base) BUY instant execution
    base_ca = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC on Base
    res_buy_base = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": base_ca,
            "side": "BUY",
            "amount_native": 0.05,
            "slippage_bps": 100,
        },
    )
    assert res_buy_base.status_code == 200
    data_buy_base = res_buy_base.json()
    assert data_buy_base["status"] == "SUCCESS"
    assert data_buy_base["token_address"] == base_ca
    assert data_buy_base["chain"] == "base_mainnet"

    # 3. Valid Solana SELL instant execution
    res_sell_sol = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": solana_ca,
            "side": "SELL",
            "amount_usd": 50.0,
        },
    )
    assert res_sell_sol.status_code == 200
    data_sell_sol = res_sell_sol.json()
    assert data_sell_sol["status"] == "SUCCESS"
    assert data_sell_sol["side"] == "SELL"

    # Verify position is no longer in open positions
    res_port_after_sell = client.get("/api/portfolio")
    assert not any(p["token_address"] == solana_ca for p in res_port_after_sell.json()["open_positions"])

    # 3b. Subsequent SELL should reject with No open position found
    res_sell_again = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": solana_ca,
            "side": "SELL",
        },
    )
    assert res_sell_again.status_code == 400
    assert "No open position found" in res_sell_again.json()["message"]

    # 4. Error case: Invalid token address format
    res_invalid_addr = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": "invalid_address_format",
            "side": "BUY",
            "amount_native": 0.1,
        },
    )
    assert res_invalid_addr.status_code == 400
    data_err = res_invalid_addr.json()
    assert data_err["status"] == "ERROR"
    assert "Invalid token address format" in data_err["message"]

    # 5. Error case: Missing token contract address
    res_missing_addr = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": "",
            "side": "BUY",
        },
    )
    assert res_missing_addr.status_code == 400
    assert res_missing_addr.json()["status"] == "ERROR"

    # 6. Error case: Unsupported side
    res_bad_side = client.post(
        "/api/action/trade/execute",
        json={
            "token_address": solana_ca,
            "side": "HOLD",
        },
    )
    assert res_bad_side.status_code == 400
    assert "Unsupported side" in res_bad_side.json()["message"]


@pytest.mark.asyncio
async def test_smart_wallet_endpoints():
    agg = DashboardTelemetryAggregator()
    app = create_app(agg)
    client = TestClient(app)

    # 1. GET /api/wallets
    res_wallets = client.get("/api/wallets")
    assert res_wallets.status_code == 200
    wallets_data = res_wallets.json()
    assert "tracked" in wallets_data
    assert "discovered" in wallets_data
    assert "stats" in wallets_data
    assert isinstance(wallets_data["tracked"], list)
    assert isinstance(wallets_data["discovered"], list)
    assert wallets_data["stats"]["total_discovered"] >= 1
    assert "global_copy_trading" in wallets_data["stats"]

    # 2. POST /api/wallets/track (Solana wallet)
    test_sol_wallet = "3kR5mRkQvVz7P5Ew6oNqZp4v3kR5mRkQvVz7P5Ew6oNq"
    res_track_sol = client.post(
        "/api/wallets/track",
        json={
            "wallet_address": test_sol_wallet,
            "alias": "Test Alpha Sniper",
            "chain": "solana_mainnet",
            "copy_trade_enabled": True,
            "copy_trade_size_native": "0.25 SOL",
            "alerts_enabled": True,
        },
    )
    assert res_track_sol.status_code == 200
    data_track_sol = res_track_sol.json()
    assert data_track_sol["status"] == "SUCCESS"
    assert data_track_sol["wallet_address"] == test_sol_wallet
    assert data_track_sol["is_tracked"] is True
    assert data_track_sol["copy_trade_enabled"] is True

    # 3. POST /api/wallets/track (EVM Base wallet)
    test_evm_wallet = "0x5555555555555555555555555555555555555555"
    res_track_evm = client.post(
        "/api/wallets/track",
        json={
            "wallet_address": test_evm_wallet,
            "alias": "Test Base Whale",
            "chain": "base_mainnet",
            "copy_trade_enabled": False,
        },
    )
    assert res_track_evm.status_code == 200
    assert res_track_evm.json()["status"] == "SUCCESS"

    # 4. Error case: Invalid wallet address
    res_bad_wallet = client.post(
        "/api/wallets/track",
        json={
            "wallet_address": "not_a_real_wallet_address_xyz",
            "alias": "Bad Wallet",
        },
    )
    assert res_bad_wallet.status_code == 400
    assert res_bad_wallet.json()["status"] == "ERROR"

    # 5. POST /api/wallets/toggle_copy (global toggle)
    res_toggle_global = client.post(
        "/api/wallets/toggle_copy",
        json={"enabled": False},
    )
    assert res_toggle_global.status_code == 200
    assert res_toggle_global.json()["status"] == "SUCCESS"
    assert res_toggle_global.json()["global_copy_trading"] is False

    # 6. POST /api/wallets/toggle_copy (per-wallet toggle)
    res_toggle_wallet = client.post(
        "/api/wallets/toggle_copy",
        json={
            "wallet_address": test_sol_wallet,
            "enabled": False,
        },
    )
    assert res_toggle_wallet.status_code == 200
    assert res_toggle_wallet.json()["status"] == "SUCCESS"
    assert res_toggle_wallet.json()["copy_trade_enabled"] is False

    # 7. POST /api/wallets/untrack
    res_untrack = client.post(
        "/api/wallets/untrack",
        json={"wallet_address": test_sol_wallet},
    )
    assert res_untrack.status_code == 200
    assert res_untrack.json()["status"] == "SUCCESS"


