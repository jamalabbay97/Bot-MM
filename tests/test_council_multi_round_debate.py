"""
tests/test_council_multi_round_debate.py — Tests for genuine 4-round Council of Agents debate loop.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
import pytest

from alpha_engine.council.arbiter_cro import CouncilArbiterCRO
from alpha_engine.execution.ledger import SQLiteLedger
from alpha_engine.models.enums import ChainIdentifier, ExitProfile, OrderSide, SecurityTier, SignalSource, SignalStrength
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport


def make_pool(chain=ChainIdentifier.SOLANA_MAINNET, native_reserve="15.5", token_reserve="500000.0", mcap="65000.0"):
    return PoolState(
        pool_address="TestPoolSolana111111111111111111111111",
        chain=chain,
        token_reserve=Decimal(str(token_reserve)),
        native_reserve=Decimal(str(native_reserve)),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=1000,
        market_cap_usd=Decimal(str(mcap)) if mcap else None,
    )


def make_report(chain=ChainIdentifier.SOLANA_MAINNET, token_addr="TokenAlpha1111111111111111111111111111111", **kwargs):
    d = {
        "token_address": token_addr,
        "chain": chain,
        "tier": SecurityTier.CLEAN,
        "is_honeypot": False,
        "buy_tax_bps": 0,
        "sell_tax_bps": 0,
        "lp_burned_ratio": 1.0,
        "top10_concentration": 0.18,
        "mint_authority_disabled": True,
        "verified_source_code": True,
    }
    d.update(kwargs)
    return SecurityReport(**d)


def make_signal(pool: PoolState, report: SecurityReport, **kwargs) -> SignalEvent:
    d = {
        "timestamp_ns": time.time_ns(),
        "token_address": report.token_address,
        "chain": pool.chain,
        "pool_address": pool.pool_address,
        "suggested_side": OrderSide.BUY,
        "source": SignalSource.PAIR_CREATED,
        "strength": SignalStrength.STRONG,
        "pool_state": pool,
        "security_report": report,
        "alpha_score": 0.85,
    }
    d.update(kwargs)
    return SignalEvent(**d)


@pytest.mark.asyncio
async def test_council_four_round_debate_execution(tmp_path):
    """Verify that deliberate() runs all 4 rounds and outputs structured debate rounds."""
    db_file = tmp_path / "test_council.db"
    ledger = await SQLiteLedger(db_path=str(db_file)).initialize()

    arbiter = CouncilArbiterCRO(ledger=ledger)

    token_ca = "TokenAlpha1111111111111111111111111111111"
    pool = make_pool(native_reserve="15.5", mcap="65000.0")
    report = make_report(token_addr=token_ca)
    sig = make_signal(
        pool=pool,
        report=report,
        alpha_score=0.88,
        volume_surge_multiplier=3.2,
        net_buy_delta=0.72,
        token_age_s=600,
    )

    res = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
        symbol="ALPHATEST",
    )

    assert res is not None
    assert res.decision == "EXECUTE_BUY"
    assert res.consensus_score >= 0.60
    assert res.recommended_position_pct > 0.0
    assert len(res.debate_rounds) >= 10  # 3 in R1, 3 in R2, 3 in R3, 1 in R4
    if res.exit_profile:
        assert ExitProfile(res.exit_profile) in (ExitProfile.FAST_SNIPE, ExitProfile.EARLY_RALLY_SWING, ExitProfile.REVIVAL_SWING)

    # Check discrete round numbers
    rounds = [r.round_number for r in res.debate_rounds]
    assert 1 in rounds
    assert 2 in rounds
    assert 3 in rounds
    assert 4 in rounds

    # Check transcript formatting
    assert any("ROUND 1" in line for line in res.debate_transcript)
    assert any("ROUND 2" in line for line in res.debate_transcript)
    assert any("ROUND 3" in line for line in res.debate_transcript)
    assert any("ROUND 4" in line for line in res.debate_transcript)

    # Check SQLite persistence
    delibs = await ledger.get_agent_deliberations(token_address=token_ca)
    assert len(delibs) == 1
    assert delibs[0]["final_decision"] == "EXECUTE_BUY"
    parsed_votes = json.loads(delibs[0]["agent_votes_json"])
    assert "debate_rounds" in parsed_votes
    assert len(parsed_votes["debate_rounds"]) >= 10

    await ledger.close()


@pytest.mark.asyncio
async def test_fractional_kelly_sizing():
    """Verify Kelly Criterion position sizing calculation under high and low win rates."""
    arbiter = CouncilArbiterCRO()
    token_ca = "TokenAlpha1111111111111111111111111111111"
    pool = make_pool(native_reserve="20.0")
    report = make_report(token_addr=token_ca, top10_concentration=0.12)
    sig = make_signal(
        pool=pool,
        report=report,
        alpha_score=0.90,
        volume_surge_multiplier=4.0,
        net_buy_delta=0.80,
    )

    # High win rate (75%)
    res_high = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
        recent_win_rate=75.0,
    )
    assert res_high.decision == "EXECUTE_BUY"
    assert res_high.recommended_position_pct >= 1.0  # High sizing for 75% win rate

    # Lower win rate (45%)
    res_low = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
        recent_win_rate=45.0,
    )
    assert res_low.decision == "EXECUTE_BUY"
    assert res_low.recommended_position_pct <= res_high.recommended_position_pct
