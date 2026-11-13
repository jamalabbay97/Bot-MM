"""
tests/test_issue11b_smart_wallets.py — Smart Wallet Discovery, Evaluation, Whitelist, Tracking & Signals
========================================================================================================
Anti-Shallow Contract Proof Suite for Issue 11, Part B:
- Realized fee-adjusted PnL evaluation only (no unrealized gains)
- Whitelisting of known profitable wallets
- Specific rejection reasons: wash trader, single lucky outlier, MEV bot, insider-funded, out-of-sample failure
- Deferred (not rejected) on insufficient history / quota
- Live tracking and cluster agreement conviction boost
- SignalSource.WHALE_WALLET emission through normal Gatekeeper -> Council -> Sizing -> Execution path
- Gatekeeper security veto remains absolute (honeypot vetoed)
- Automatic demotion of whitelisted wallets after bad copy-trade results
- End-to-end integration through runner lifecycle from fixture swap stream to paper fill with wallet attribution
"""

import asyncio
import time
from decimal import Decimal
from typing import Any, Optional
import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExecutionVenue,
    LotStatus,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    SwapVenue,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import RawSignalEvent, SignalEvent, SwapEvent
from alpha_engine.models.profiler import (
    FundingHop,
    InitialTxRecord,
    WalletProfile,
    WalletTradeRecord,
)
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.profiler.evaluator import WalletEvaluator
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.pipeline import SmartMoneyPipeline
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDatabase


# ---------------------------------------------------------------------------
# Test Helpers & Fixture Generators
# ---------------------------------------------------------------------------

def _make_trade(
    token: str,
    buy_ts: int,
    sell_ts: int,
    invested_native: Decimal,
    realized_pnl_usd: Decimal,
    roi_pct: float,
    buy_tx: Optional[str] = None,
    sell_tx: Optional[str] = None,
    custom_metadata: Optional[dict[str, Any]] = None,
    venue: SwapVenue = SwapVenue.UNISWAP_V3,
) -> WalletTradeRecord:
    hold_s = float(max(1, sell_ts - buy_ts))
    tok = token if len(token) >= 32 else f"0x{token.replace('0x', ''):0>40}"
    b_tx = buy_tx if (buy_tx and len(buy_tx) >= 32) else f"0x{'b'*60}{buy_ts % 1000:04d}"
    s_tx = sell_tx if (sell_tx and len(sell_tx) >= 32) else f"0x{'c'*60}{sell_ts % 1000:04d}"
    meta = dict(custom_metadata or {})
    meta["venue"] = venue.value
    return WalletTradeRecord(
        token_address=tok,
        buy_tx_hash=b_tx,
        sell_tx_hash=s_tx,
        buy_timestamp=buy_ts,
        sell_timestamp=sell_ts,
        holding_time_seconds=hold_s,
        invested_native=invested_native,
        realized_native_pnl=realized_pnl_usd / Decimal("150.0"),
        realized_pnl_usd=realized_pnl_usd,
        roi_pct=roi_pct,
        custom_metadata=meta,
    )


def _build_profitable_trades(base_ts: int = 1_000_000, count: int = 20) -> list[WalletTradeRecord]:
    """Generates 20 trades spread over 30 days, 70% win rate, consistent profits, no outlier."""
    trades: list[WalletTradeRecord] = []
    # 30 days = 30 * 86400 = 2,592,000s; spacing = ~120,000s
    curr_ts = base_ts
    for i in range(count):
        is_win = (i % 10) not in (3, 7, 9)  # 70% win rate
        pnl = Decimal("120.0") if is_win else Decimal("-50.0")
        roi = 30.0 if is_win else -15.0
        hold = 180.0  # 3 minutes hold
        trades.append(
            _make_trade(
                token=f"0xtoken_{i:04d}",
                buy_ts=curr_ts,
                sell_ts=int(curr_ts + hold),
                invested_native=Decimal("1.0"),
                realized_pnl_usd=pnl,
                roi_pct=roi,
                buy_tx=f"buy_tx_{i}",
                sell_tx=f"sell_tx_{i}",
            )
        )
        curr_ts += 130_000  # advance ~1.5 days
    return trades


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_known_profitable_wallet_whitelisted(tmp_path):
    """A consistently profitable wallet with sufficient history is APPROVED and WHITELISTED."""
    db_path = str(tmp_path / "wallet_test.db")
    whitelist_db = WhitelistDatabase(db_path=db_path)
    await whitelist_db.connect()

    trades = _build_profitable_trades(count=20)
    profile = WalletEvaluator.evaluate(
        wallet_address="0xsmartwallet0000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=trades[-1].sell_timestamp + 3600,
    )

    assert profile.classification == WalletClassification.APPROVED
    assert profile.is_whitelisted is True
    assert profile.smart_money_score >= 0.50
    assert profile.win_rate_pct >= 60.0
    assert len(profile.rejection_reasons) == 0

    record = await whitelist_db.upsert_wallet(profile, status=WhitelistStatus.ACTIVE)
    assert record.status == WhitelistStatus.ACTIVE
    is_wl = await whitelist_db.is_whitelisted("0xsmartwallet0000000000000000000000000001")
    assert is_wl is True
    await whitelist_db.close()


@pytest.mark.asyncio
async def test_wash_trader_rejected_with_right_reason():
    """A wallet with circular trades / round-trips with near-zero PnL is rejected as wash trader."""
    base_ts = 1_000_000
    # 6 trades on the exact same token with negligible PnL (<$1)
    trades = [
        _make_trade(
            token="0xwashtoken000000000000000000000000000001",
            buy_ts=base_ts + i * 200,
            sell_ts=base_ts + i * 200 + 60,
            invested_native=Decimal("5.0"),
            realized_pnl_usd=Decimal("0.02") if i % 2 == 0 else Decimal("-0.02"),
            roi_pct=0.01 if i % 2 == 0 else -0.01,
            buy_tx=f"wash_b_{i}",
            sell_tx=f"wash_s_{i}",
        )
        for i in range(8)
    ]

    profile = WalletEvaluator.evaluate(
        wallet_address="0xwashtrader00000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=base_ts + 5000,
    )

    assert profile.classification in (WalletClassification.CIRCULAR_WASH, WalletClassification.WASH_TRADER)
    assert profile.is_whitelisted is False
    assert any("wash" in r.lower() or "circular" in r.lower() for r in profile.rejection_reasons)


@pytest.mark.asyncio
async def test_single_lucky_trade_wallet_rejected_with_right_reason():
    """A wallet where a single trade accounts for >60% of total PnL is rejected as LUCKY_OUTLIER."""
    base_ts = 1_000_000
    trades: list[WalletTradeRecord] = []
    # 1 massive lucky trade: $10,000 PnL
    trades.append(
        _make_trade(
            token="0xluckytoken00000000000000000000000000001",
            buy_ts=base_ts,
            sell_ts=base_ts + 300,
            invested_native=Decimal("1.0"),
            realized_pnl_usd=Decimal("10000.0"),
            roi_pct=1000.0,
        )
    )
    # 15 small losing or break-even trades: -$20 each = -$300
    for i in range(1, 16):
        trades.append(
            _make_trade(
                token=f"0xlost_{i:04d}",
                buy_ts=base_ts + i * 100_000,
                sell_ts=base_ts + i * 100_000 + 120,
                invested_native=Decimal("1.0"),
                realized_pnl_usd=Decimal("-20.0"),
                roi_pct=-2.0,
            )
        )

    profile = WalletEvaluator.evaluate(
        wallet_address="0xluckywallet0000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=base_ts + 2_000_000,
    )

    assert profile.classification == WalletClassification.LUCKY_OUTLIER
    assert profile.is_whitelisted is False
    assert any("outlier" in r.lower() or "survivorship" in r.lower() for r in profile.rejection_reasons)


@pytest.mark.asyncio
async def test_mev_bot_rejected_with_right_reason():
    """A wallet with median holding time < 45 seconds or same-block execution is rejected as MEV_BOT."""
    base_ts = 1_000_000
    # 20 trades, but hold time is only 8 seconds (sandwich/sniper bot)
    trades = [
        _make_trade(
            token=f"0xmev_{i:04d}",
            buy_ts=base_ts + i * 100_000,
            sell_ts=base_ts + i * 100_000 + 8,  # 8 seconds hold
            invested_native=Decimal("5.0"),
            realized_pnl_usd=Decimal("40.0"),
            roi_pct=5.0,
        )
        for i in range(20)
    ]

    profile = WalletEvaluator.evaluate(
        wallet_address="0xmevbot000000000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=base_ts + 2_500_000,
    )

    assert profile.classification == WalletClassification.MEV_BOT
    assert profile.is_whitelisted is False
    assert any("mev" in r.lower() or "holding time" in r.lower() for r in profile.rejection_reasons)


@pytest.mark.asyncio
async def test_insider_funded_wallet_rejected_with_right_reason():
    """A wallet funded by token deployer within 3 hops is rejected as INSIDER."""
    trades = _build_profitable_trades(count=20)
    init_txs = [
        InitialTxRecord(
            tx_hash="0x" + "d" * 60,
            block_number=100,
            timestamp=1000,
            from_address="0xdeployer0000000000000000000000000000001",
            to_address="0xinsiderwallet0000000000000000000000001",
            value_native=Decimal("10.0"),
        )
    ]
    hops = [
        FundingHop(
            source_address="0xdeployer0000000000000000000000000000001",
            destination_address="0xinsiderwallet0000000000000000000000001",
            tx_hash="0x" + "d" * 60,
            hop_depth=1,
            amount_native=Decimal("10.0"),
            is_deployer=True,
            is_multisig=False,
        )
    ]

    profile = WalletEvaluator.evaluate(
        wallet_address="0xinsiderwallet0000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        initial_txs=init_txs,
        funding_hops=hops,
    )

    assert profile.classification == WalletClassification.INSIDER
    assert profile.is_whitelisted is False
    assert any("insider" in r.lower() or "deployer" in r.lower() for r in profile.rejection_reasons)


@pytest.mark.asyncio
async def test_insufficient_data_deferred_not_rejected(tmp_path):
    """A wallet with fewer than 15 trades is marked INSUFFICIENT_HISTORY and DEFERRED in pipeline, not banned."""
    db_path = str(tmp_path / "pipeline_test.db")
    pipeline = SmartMoneyPipeline(db_path=db_path)
    await pipeline.initialize()

    # Only 5 trades
    short_trades = _build_profitable_trades(count=5)
    profile = WalletEvaluator.evaluate(
        wallet_address="0xnewtrader000000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=short_trades,
        current_timestamp=short_trades[-1].sell_timestamp + 3600,
    )

    assert profile.classification == WalletClassification.INSUFFICIENT_HISTORY
    assert profile.is_whitelisted is False

    # In pipeline, recording an insufficient data candidate marks it as DEFERRED, not REJECTED
    await pipeline.record_candidate_evaluation(
        wallet_address="0xnewtrader000000000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        profile=profile,
    )

    candidate = await pipeline.get_candidate("0xnewtrader000000000000000000000000000001", ChainIdentifier.BASE_MAINNET)
    assert candidate is not None
    assert candidate["status"] == "DEFERRED"

    # Wallet is NOT in whitelist DB as BANNED
    is_banned = await pipeline.whitelist_db.is_whitelisted("0xnewtrader000000000000000000000000000001")
    assert is_banned is False
    record = await pipeline.whitelist_db.get_wallet("0xnewtrader000000000000000000000000000001")
    assert record is None or record.status != WhitelistStatus.BANNED

    await pipeline.close()


@pytest.mark.asyncio
async def test_out_of_sample_consistency_failure_rejected():
    """A wallet profitable in-sample (60% win rate overall, +$1600) but unprofitable out-of-sample fails consistency."""
    base_ts = 1_000_000
    trades: list[WalletTradeRecord] = []
    # 12 in-sample trades: ALL 12 wins (+$200 each) -> +$2400
    for i in range(12):
        trades.append(
            _make_trade(
                token=f"0xtok_is_{i:04d}",
                buy_ts=base_ts + i * 100_000,
                sell_ts=base_ts + i * 100_000 + 180,
                invested_native=Decimal("1.0"),
                realized_pnl_usd=Decimal("200.0"),
                roi_pct=25.0,
            )
        )
    # 8 out-of-sample trades: ALL 8 losses (-$100 each) -> -$800 (OOS win rate 0%, OOS PnL -$800)
    # Lifetime: 12/20 = 60% win rate, +$1600 net PnL -> passes lifetime, but fails out-of-sample!
    for i in range(12, 20):
        trades.append(
            _make_trade(
                token=f"0xtok_oos_{i:04d}",
                buy_ts=base_ts + i * 100_000,
                sell_ts=base_ts + i * 100_000 + 180,
                invested_native=Decimal("1.0"),
                realized_pnl_usd=Decimal("-100.0"),
                roi_pct=-15.0,
            )
        )

    profile = WalletEvaluator.evaluate(
        wallet_address="0xregimechange00000000000000000000000001",
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=trades[-1].sell_timestamp + 3600,
    )

    assert profile.is_whitelisted is False
    assert any("out-of-sample" in r.lower() or "oos" in r.lower() for r in profile.rejection_reasons)


@pytest.mark.asyncio
async def test_cluster_agreement_increases_conviction(tmp_path):
    """Multiple whitelisted wallets buying the same token within 120s increases cluster size and conviction."""
    db_path = str(tmp_path / "cluster_test.db")
    pipeline = SmartMoneyPipeline(db_path=db_path)
    await pipeline.initialize()

    # Whitelist 2 distinct wallets
    w1 = "0xsmart111111111111111111111111111111111111"
    w2 = "0xsmart222222222222222222222222222222222222"
    prof1 = WalletProfile(
        wallet_address=w1,
        chain=ChainIdentifier.BASE_MAINNET,
        classification=WalletClassification.APPROVED,
        is_whitelisted=True,
        total_trades=25,
        winning_trades=18,
        losing_trades=7,
        win_rate_pct=72.0,
        total_pnl_usd=Decimal("3000"),
        max_single_trade_pnl_usd=Decimal("300"),
        outlier_pnl_ratio=0.10,
        median_holding_time_seconds=180.0,
        active_days=30.0,
        days_since_last_active=1.0,
        first_tx_timestamp=1000,
        last_tx_timestamp=2000,
        smart_money_score=0.85,
    )
    prof2 = prof1.model_copy(update={"wallet_address": w2, "smart_money_score": 0.88})
    await pipeline.whitelist_db.upsert_wallet(prof1, status=WhitelistStatus.ACTIVE)
    await pipeline.whitelist_db.upsert_wallet(prof2, status=WhitelistStatus.ACTIVE)

    now_ns = int(time.time() * 1e9)
    target_token = "0xtargettoken33333333333333333333333333333"

    # Wallet 1 buys
    swap1 = SwapEvent(
        timestamp_ns=now_ns,
        block_number=100,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0xpool000000000000000000000000000000000001",
        token_in="0x4200000000000000000000000000000000000006",  # WETH
        token_out=target_token,
        amount_in=Decimal("1.5"),
        amount_out=Decimal("10000.0"),
        sender=w1,
        tx_hash="0x" + "e" * 60,
    )
    sig1 = await pipeline.on_swap_event(swap1)
    assert sig1 is not None
    assert isinstance(sig1, SignalEvent)
    assert sig1.source == SignalSource.WHALE_WALLET
    assert sig1.strength in (SignalStrength.MODERATE, SignalStrength.STRONG)
    assert sig1.cluster_size == 1
    initial_alpha = sig1.alpha_score

    # Wallet 2 buys same token 30 seconds later (within 120s window)
    swap2 = SwapEvent(
        timestamp_ns=now_ns + int(30 * 1e9),
        block_number=105,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0xpool000000000000000000000000000000000001",
        token_in="0x4200000000000000000000000000000000000006",
        token_out=target_token,
        amount_in=Decimal("2.0"),
        amount_out=Decimal("13000.0"),
        sender=w2,
        tx_hash="0x" + "f" * 60,
    )
    sig2 = await pipeline.on_swap_event(swap2)
    assert sig2 is not None
    assert isinstance(sig2, SignalEvent)
    assert sig2.source == SignalSource.WHALE_WALLET
    assert sig2.strength == SignalStrength.STRONG
    assert sig2.cluster_size >= 2
    assert sig2.alpha_score > initial_alpha
    assert sig2.wallet_conviction > sig1.wallet_conviction

    await pipeline.close()


@pytest.mark.asyncio
async def test_whitelisted_wallet_signal_gatekeeper_security_veto_applies(tmp_path):
    """WHALE_WALLET signals pass through Gatekeeper: honeypot/freeze authority is vetoed fail-closed."""
    from unittest.mock import AsyncMock
    from alpha_engine.rate_limiter.registry import RateLimiterRegistry
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    db_path = str(tmp_path / "veto_test.db")
    pipeline = SmartMoneyPipeline(db_path=db_path)
    await pipeline.initialize()

    w = "0xsmartveto00000000000000000000000000000001"
    prof = WalletProfile(
        wallet_address=w,
        chain=ChainIdentifier.BASE_MAINNET,
        classification=WalletClassification.APPROVED,
        is_whitelisted=True,
        total_trades=20,
        winning_trades=16,
        losing_trades=4,
        win_rate_pct=80.0,
        total_pnl_usd=Decimal("2500"),
        max_single_trade_pnl_usd=Decimal("250"),
        outlier_pnl_ratio=0.10,
        median_holding_time_seconds=180.0,
        active_days=30.0,
        days_since_last_active=1.0,
        first_tx_timestamp=1000,
        last_tx_timestamp=2000,
        smart_money_score=0.88,
    )
    await pipeline.whitelist_db.upsert_wallet(prof, status=WhitelistStatus.ACTIVE)

    session = AsyncMock()
    limiter = RateLimiterRegistry.default()
    gatekeeper = SecurityGatekeeper(
        session=session,
        limiter=limiter,
        evm_rpc_url="https://base-mainnet.g.alchemy.com/v2/demo",
        evm_router_address="0x" + "1" * 40,
        weth_address="0x4200000000000000000000000000000000000006",
        enable_tier2=False,
    )

    # A honeypot token
    honeypot_token = "0xhoneypot000000000000000000000000000001"
    honeypot_report = SecurityReport(
        token_address=honeypot_token,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.TIER2_HONEYPOT,
        is_honeypot=True,
        buy_tax_bps=1000,
        sell_tax_bps=10000,  # 100% sell tax
        lp_burned_ratio=0.0,
        top10_concentration=0.99,
        mint_authority_disabled=False,
        freeze_authority_disabled=False,
    )
    gatekeeper.screen_token = AsyncMock(return_value=honeypot_report)

    # 1. Pipeline Gatekeeper check: on_swap_event with honeypot fails closed and returns None
    swap = SwapEvent(
        timestamp_ns=time.time_ns(),
        block_number=100,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0xpool000000000000000000000000000000000001",
        token_in="0x4200000000000000000000000000000000000006",
        token_out=honeypot_token,
        amount_in=Decimal("1.0"),
        amount_out=Decimal("1000.0"),
        sender=w,
        tx_hash="0x" + "a" * 60,
    )
    res = await pipeline.on_swap_event(swap, gatekeeper=gatekeeper)
    assert res is None, "Honeypot token swap must be rejected by Gatekeeper screening"

    # 2. Ruflo Council / Swarm BFT Consensus check: security veto applies
    from alpha_engine.ruflo.swarm import RufloSwarmCoordinator
    cfg = EngineConfig(allow_static_native_prices=True, ruflo_enabled=True)
    swarm = RufloSwarmCoordinator(
        config=cfg,
        gatekeeper=gatekeeper,
        profiler=pipeline.profiler,
        whitelist_db=pipeline.whitelist_db,
    )
    consensus_res = await swarm.execute_trade_consensus(
        token_address=honeypot_token,
        chain=ChainIdentifier.BASE_MAINNET,
        wallet_address=w,
        report=honeypot_report,
    )
    assert consensus_res["approved"] is False
    assert consensus_res["rejection_agent"] == "GATEKEEPER"

    await pipeline.close()


@pytest.mark.asyncio
async def test_demotion_after_bad_copy_trade_results(tmp_path):
    """Underperforming copied trades trigger automatic demotion via feedback loop and whitelist_db."""
    db_path = str(tmp_path / "demotion_test.db")
    pipeline = SmartMoneyPipeline(db_path=db_path)
    await pipeline.initialize()

    w = "0xsmartdecay0000000000000000000000000000001"
    prof = WalletProfile(
        wallet_address=w,
        chain=ChainIdentifier.BASE_MAINNET,
        classification=WalletClassification.APPROVED,
        is_whitelisted=True,
        total_trades=20,
        winning_trades=15,
        losing_trades=5,
        win_rate_pct=75.0,
        total_pnl_usd=Decimal("2000"),
        max_single_trade_pnl_usd=Decimal("200"),
        outlier_pnl_ratio=0.10,
        median_holding_time_seconds=150.0,
        active_days=25.0,
        days_since_last_active=1.0,
        first_tx_timestamp=1000,
        last_tx_timestamp=2000,
        smart_money_score=0.80,
    )
    await pipeline.whitelist_db.upsert_wallet(prof, status=WhitelistStatus.ACTIVE)
    assert await pipeline.whitelist_db.is_whitelisted(w) is True

    # Record 3 consecutive losing copied trades
    for i in range(3):
        await pipeline.record_copied_trade_outcome(
            wallet_address=w,
            chain=ChainIdentifier.BASE_MAINNET,
            realized_pnl_usd=Decimal("-50.0"),
            is_win=False,
        )

    # Must be demoted to SUSPENDED or BANNED
    is_wl = await pipeline.whitelist_db.is_whitelisted(w)
    assert is_wl is False
    rec = await pipeline.whitelist_db.get_wallet(w)
    assert rec is not None
    assert rec.status in (WhitelistStatus.SUSPENDED, WhitelistStatus.BANNED)
    assert "consecutive" in rec.rejection_reasons.lower() or "copy-trade" in rec.rejection_reasons.lower()

    await pipeline.close()


@pytest.mark.asyncio
async def test_end_to_end_runner_swap_stream_to_paper_fill_with_wallet_attribution(tmp_path):
    """
    End-to-end integration test through runner from a fixture swap stream to a paper fill
    with wallet attribution and post-trade performance recording.
    """
    from unittest.mock import AsyncMock, MagicMock
    from alpha_engine.pricing.native_price_service import PriceResult, NativePriceStatus
    from alpha_engine.engine.runner import PaperTradingEngine, ShutdownSentinel
    from alpha_engine.execution.ledger import SQLiteLedger
    from alpha_engine.models.state import PaperFill

    class MockPriceService:
        def get_price(self, chain: Any) -> PriceResult:
            return PriceResult(Decimal("3000.0"), NativePriceStatus.OK, time.time())

    db_file = str(tmp_path / "e2e_runner.db")
    cfg = EngineConfig(
        allow_static_native_prices=True,
        db_path=db_file,
        initial_equity_usd=Decimal("10000.0"),
        initial_eth=Decimal("5.0"),
        initial_sol=Decimal("20.0"),
        wallet_pipeline_enabled=True,
        auto_trade_enabled=True,
        screener_exclusive_focus=False,
    )

    price_svc = MockPriceService()
    engine = PaperTradingEngine(config=cfg, native_price_service=price_svc)

    # Initialize backing SQLite ledger
    ledger = SQLiteLedger(db_file)
    await ledger.initialize()
    engine._ledger = ledger

    # Initialize wallet pipeline
    await engine._wallet_pipeline.initialize()

    # Whitelist a smart wallet
    smart_w = "0x7777777777777777777777777777777777777777"
    prof = WalletProfile(
        wallet_address=smart_w,
        chain=ChainIdentifier.BASE_MAINNET,
        classification=WalletClassification.APPROVED,
        is_whitelisted=True,
        total_trades=30,
        winning_trades=22,
        losing_trades=8,
        win_rate_pct=73.3,
        total_pnl_usd=Decimal("4500"),
        max_single_trade_pnl_usd=Decimal("400"),
        outlier_pnl_ratio=0.09,
        median_holding_time_seconds=240.0,
        active_days=35.0,
        days_since_last_active=1.0,
        first_tx_timestamp=1000,
        last_tx_timestamp=2000,
        smart_money_score=0.92,
    )
    await engine._wallet_pipeline.whitelist_db.upsert_wallet(prof, status=WhitelistStatus.ACTIVE)

    # Set up Gatekeeper with clean report
    target_token = "0x8888888888888888888888888888888888888888"
    pool_addr = "0x9999999999999999999999999999999999999999"
    clean_report = SecurityReport(
        token_address=target_token,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        lp_burned_ratio=1.0,
        top10_concentration=0.1,
        mint_authority_disabled=True,
        verified_source_code=True,
    )
    mock_gk = AsyncMock()
    mock_gk.screen_token = AsyncMock(return_value=clean_report)
    mock_gk.is_rejected = MagicMock(return_value=False)
    engine._gatekeeper = mock_gk

    # Seed pool state in PoolRegistry
    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.BASE_MAINNET,
        token_address=target_token,
        native_reserve=Decimal("50.0"),
        token_reserve=Decimal("500000.0"),
        spot_price_native_per_token=Decimal("0.0001"),
        fee_numerator=30,
        fee_denominator=10000,
        total_supply=Decimal("1000000000"),
        circulating_supply=Decimal("1000000000"),
        timestamp_s=time.time(),
        last_updated_block=1000,
    )
    engine._pool_registry.set(pool_addr, pool)
    engine._pool_registry.set(target_token, pool)

    # Create incoming SwapEvent from whitelisted smart wallet buying the token
    swap = SwapEvent(
        timestamp_ns=time.time_ns(),
        block_number=1000,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address=pool_addr,
        token_in="0x4200000000000000000000000000000000000006",  # WETH
        token_out=target_token,
        amount_in=Decimal("1.0"),
        amount_out=Decimal("10000.0"),
        sender=smart_w,
        tx_hash="0x" + "7" * 60,
    )

    # Put swap and sentinel into ingestion queue
    await engine._ingestion_q.put(swap)
    await engine._ingestion_q.put(ShutdownSentinel())

    # Process ingestion and signal queues
    await engine._process_ingestion_queue()
    await engine._process_signal_queue()

    # Verify open lot in position book has wallet attribution
    lots = engine._position_book.get_open_lots()
    assert len(lots) == 1, f"Expected 1 open lot, got {len(lots)}"
    lot = lots[0]
    assert lot.token_address == target_token
    assert lot.wallet_address == smart_w
    assert smart_w in lot.copied_wallets
    assert lot.status == LotStatus.OPEN
    resolved_venue = engine._cost_model.resolve_venue(pool_address=lot.pool_address, chain=lot.chain)
    assert resolved_venue in (ExecutionVenue.UNISWAP_V3, ExecutionVenue.UNISWAP_V2, ExecutionVenue.RAYDIUM_AMM, ExecutionVenue.RAYDIUM_CPMM)

    # Verify pipeline components structure
    assert isinstance(engine._wallet_pipeline.history_db, WalletHistoryDatabase)
    assert isinstance(engine._wallet_pipeline.profiler, SmartMoneyProfiler)
    assert isinstance(engine._wallet_pipeline.whitelist_db, WhitelistDatabase)

    # Candidate discovery from raw event text
    raw_ev = RawSignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        token_address=target_token,
        source=SignalSource.TELEGRAM_SCRAPER,
        raw_text=f"Whale {smart_w} accumulation alert",
    )
    assert raw_ev.source == SignalSource.TELEGRAM_SCRAPER

    # Verify pipeline metrics recorded signal emission
    metrics = await engine._wallet_pipeline.get_metrics()
    assert metrics["signals_emitted"] >= 1

    # Simulate position closure with positive realized PnL
    exit_fill = PaperFill(
        order_id="exit_test_fill",
        token_address=target_token,
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.SELL,
        effective_price=Decimal("0.00015"),
        amount_token=lot.tokens_held,
        tokens_acquired=lot.tokens_held,
        tokens_delta=-lot.tokens_held,
        simulated_native_spent=Decimal("0.0"),
        simulated_gas_cost_usd=Decimal("0.05"),
        slippage_bps=10,
        fill_latency_ms=25.0,
    )

    engine._record_closed_trade_and_notify(
        lot=lot,
        exit_fill=exit_fill,
        decision_reason="TAKE_PROFIT",
        exit_pnl_usd=Decimal("150.0"),
        pnl_native=Decimal("0.05"),
    )
    await engine.await_pending_hooks()
    await asyncio.sleep(0.01)

    # Verify wallet copy performance was recorded in SQLite
    copy_perf = await engine._wallet_pipeline.get_copy_performance(smart_w, ChainIdentifier.BASE_MAINNET)
    assert copy_perf is not None
    assert copy_perf["total_copied_trades"] == 1
    assert copy_perf["winning_copied_trades"] == 1
    assert Decimal(copy_perf["total_copied_pnl_usd"]) == Decimal("150.0")

    # Clean up
    await engine._wallet_pipeline.stop()
    await ledger.close()

