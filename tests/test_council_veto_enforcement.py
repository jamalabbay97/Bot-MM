"""
tests/test_council_veto_enforcement.py — Tests for non-overridable vetoes in Council debate.
"""
from __future__ import annotations

import time
from decimal import Decimal
import pytest

from alpha_engine.council.arbiter_cro import CouncilArbiterCRO
from alpha_engine.models.enums import ChainIdentifier, ExitProfile, OrderSide, SecurityTier, SignalSource, SignalStrength
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, SecurityReport


def make_pool(chain=ChainIdentifier.SOLANA_MAINNET, native_reserve="10.0", token_reserve="1000000.0", mcap=None):
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
        "top10_concentration": 0.10,
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
async def test_forensic_fatal_veto_active_mint():
    """Verify that active mint or freeze authority triggers an absolute, non-overridable fatal veto."""
    arbiter = CouncilArbiterCRO()
    token_ca = "TokenAlpha1111111111111111111111111111111"
    pool = make_pool(native_reserve="10.0")
    # Active mint authority is fatal
    report = make_report(token_addr=token_ca, mint_authority_disabled=False)
    sig = make_signal(
        pool=pool,
        report=report,
        alpha_score=0.95,
        volume_surge_multiplier=5.0,
        net_buy_delta=0.85,
    )

    res = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
    )

    assert res.decision == "PASS"
    assert res.fatal_veto_agent == "ChainForensicAgent"
    assert "ACTIVE_MINT_OR_FREEZE_AUTHORITY" in res.verdicts["ChainForensic"].flags
    assert any("FATAL CONTRACT SECURITY VETO" in line or "FATAL" in line for line in res.debate_transcript)


@pytest.mark.asyncio
async def test_forensic_fatal_veto_honeypot():
    """Verify that honeypot bytecode triggers an irrevocable fatal veto on Base EVM."""
    arbiter = CouncilArbiterCRO()
    token_ca = "0x1234567890123456789012345678901234567890"
    pool = make_pool(chain=ChainIdentifier.BASE_MAINNET, native_reserve="5.0")
    report = make_report(chain=ChainIdentifier.BASE_MAINNET, token_addr=token_ca, is_honeypot=True)
    sig = make_signal(
        pool=pool,
        report=report,
        source=SignalSource.DEX_SWAP,
        alpha_score=0.90,
    )

    res = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.BASE_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
    )

    assert res.decision == "PASS"
    assert res.fatal_veto_agent == "ChainForensicAgent"
    assert "CONFIRMED_HONEYPOT_BYTECODE" in res.verdicts["ChainForensic"].flags


@pytest.mark.asyncio
async def test_quant_anti_fomo_veto():
    """Verify that vertical green candle FOMO (>2.5x from dormant base) triggers an anti-FOMO veto."""
    arbiter = CouncilArbiterCRO()
    token_ca = "TokenAlpha1111111111111111111111111111111"
    pool = make_pool(native_reserve="15.0", mcap="300000.0")
    report = make_report(token_addr=token_ca)
    # Dormant mcap was 100k, current is 300k (3.0x > 2.5x threshold)
    sig = make_signal(
        pool=pool,
        report=report,
        source=SignalSource.DEX_SWAP,
        alpha_score=0.75,
        volume_surge_multiplier=3.0,
        net_buy_delta=0.60,
        base_market_cap=Decimal("100000.0"),
        market_cap_usd=Decimal("300000.0"),
    )

    res = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig,
    )

    assert res.decision == "PASS"
    assert res.fatal_veto_agent == "QuantTechnicalsAgent"
    assert "OVEREXTENDED_GREEN_CANDLE_FOMO" in res.verdicts["QuantTechnicals"].flags


@pytest.mark.asyncio
async def test_exit_profile_ladder_assignment():
    """Verify exit profile assignment matches strategy intent (REVIVAL_SWING vs EARLY_RALLY_SWING vs FAST_SNIPE)."""
    arbiter = CouncilArbiterCRO()
    token_ca = "TokenAlpha1111111111111111111111111111111"
    pool = make_pool(native_reserve="12.0")
    report = make_report(token_addr=token_ca)

    # 1. Revival Swing
    sig_revival = make_signal(
        pool=pool,
        report=report,
        source=SignalSource.REVIVAL_BREAKOUT,
        exit_profile=ExitProfile.REVIVAL_SWING,
        alpha_score=0.85,
        volume_surge_multiplier=3.5,
        net_buy_delta=0.75,
    )
    res_revival = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig_revival,
    )
    assert res_revival.exit_profile == ExitProfile.REVIVAL_SWING.value
    assert res_revival.hard_stop_loss_pct == -18.0
    assert res_revival.trailing_stop_activation_pct == 35.0

    # 2. Early Rally Swing
    sig_rally = make_signal(
        pool=pool,
        report=report,
        source=SignalSource.EARLY_RALLY_BREAKOUT,
        exit_profile=ExitProfile.EARLY_RALLY_SWING,
        alpha_score=0.85,
        volume_surge_multiplier=3.0,
        net_buy_delta=0.70,
    )
    res_rally = await arbiter.deliberate(
        token_ca=token_ca,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool=pool,
        report=report,
        signal=sig_rally,
    )
    assert res_rally.exit_profile == ExitProfile.EARLY_RALLY_SWING.value
    assert res_rally.hard_stop_loss_pct == -15.0
    assert res_rally.trailing_stop_activation_pct == 28.0
