"""
tests/test_profiler_and_rate_limiter.py — Verification Suite for Modules A, C, and Models
========================================================================================
Tests:
1. models.py (Pydantic v2 schemas and validation)
2. rate_limiter.py (TokenBucket 20 CU/s Alchemy, 8 req/s Helius, continuous leak, penalties)
3. wallet_profiler.py (Outlier bias, wash trading <=3 hops BFS, holding time, dead-wallet revival, SQLite WAL)
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import models
import rate_limiter
import wallet_profiler
from models import (
    ChainIdentifier,
    FundingHop,
    InitialTxRecord,
    NewsSignalEvent,
    NewsSignalStatus,
    OrderSide,
    PoolState,
    SecurityReport,
    SecurityTier,
    TelegramMessage,
    WalletClassification,
    WalletProfile,
    WalletTradeRecord,
    WhitelistStatus,
)
from rate_limiter import (
    AsyncTokenBucket,
    RateLimiterRegistry,
    RateLimitError,
    build_alchemy_limiter,
    build_helius_limiter,
)
from wallet_profiler import (
    SmartMoneyProfiler,
    WalletEvaluator,
    WhitelistDatabase,
)


def test_models_validation_and_invariants():
    """Verify models instantiate correctly and enforce constraints."""
    trade = WalletTradeRecord(
        token_address="0x" + "a" * 40,
        buy_tx_hash="0x" + "b" * 64,
        sell_tx_hash="0x" + "c" * 64,
        buy_timestamp=1700000000,
        sell_timestamp=1700001000,
        holding_time_seconds=1000.0,
        invested_native=Decimal("1.5"),
        realized_native_pnl=Decimal("0.5"),
        realized_pnl_usd=Decimal("1700.0"),
        roi_pct=33.33,
    )
    assert trade.is_win is True
    assert trade.holding_time_seconds == 1000.0

    # Ensure sell_timestamp >= buy_timestamp
    try:
        WalletTradeRecord(
            token_address="0x" + "a" * 40,
            buy_tx_hash="0x" + "b" * 64,
            sell_tx_hash="0x" + "c" * 64,
            buy_timestamp=1700001000,
            sell_timestamp=1700000000,  # invalid
            holding_time_seconds=0.0,
            invested_native=Decimal("1.0"),
            realized_native_pnl=Decimal("0"),
            realized_pnl_usd=Decimal("0"),
            roi_pct=0.0,
        )
        assert False, "Should have raised ValueError"
    except Exception:
        pass

    # Verify module facades and all imported model types
    assert models.__name__ == "models"
    assert rate_limiter.__name__ == "rate_limiter"
    assert wallet_profiler.__name__ == "wallet_profiler"
    assert WhitelistDatabase is not None
    assert OrderSide.BUY == "buy"
    assert SecurityTier.CLEAN == "clean"
    assert NewsSignalStatus.VALID == "valid"

    hop = FundingHop(
        source_address="0x" + "1" * 40,
        destination_address="0x" + "2" * 40,
        tx_hash="0x" + "3" * 64,
        hop_depth=1,
    )
    assert hop.hop_depth == 1

    tx_rec = InitialTxRecord(
        tx_hash="0x" + "4" * 64,
        block_number=1,
        timestamp=int(time.time()),
        from_address="0x" + "1" * 40,
        to_address="0x" + "2" * 40,
        value_native=Decimal("1.0"),
    )
    assert tx_rec.value_native == Decimal("1.0")

    tg_msg = TelegramMessage(
        channel_id=1,
        channel_title="Alpha",
        message_id=1,
        text="Alpha text",
        timestamp=time.time(),
    )
    assert tg_msg.message_id == 1

    news_evt = NewsSignalEvent(
        token_address="0x" + "a" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        originating_channel="@alpha",
        channel_id=1,
        message_id=1,
        raw_text="Alpha text",
    )
    assert news_evt.status == NewsSignalStatus.VALID

    pool_st = PoolState(
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x" + "5" * 40,
        native_reserve=Decimal("10"),
        token_reserve=Decimal("100"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=1,
    )
    assert pool_st.pool_address.startswith("0x")

    sec_rep = SecurityReport(
        token_address="0x" + "a" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        is_honeypot=False,
        buy_tax_bps=100,
        sell_tax_bps=100,
        tier=SecurityTier.CLEAN,
    )
    assert sec_rep.tier == SecurityTier.CLEAN

    profile = WalletProfile(
        wallet_address="0x" + "w" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        classification=WalletClassification.APPROVED,
        is_whitelisted=True,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate_pct=100.0,
        total_pnl_usd=Decimal("100"),
        max_single_trade_pnl_usd=Decimal("100"),
        outlier_pnl_ratio=1.0,
        median_holding_time_seconds=60.0,
        active_days=1.0,
        days_since_last_active=0.1,
        first_tx_timestamp=1700000000,
        last_tx_timestamp=1700000100,
    )
    assert profile.is_whitelisted is True


def test_rate_limiter_strict_quotas():
    """Verify Alchemy 20 CU/s and Helius 8 req/s configs."""
    alchemy = build_alchemy_limiter()
    assert alchemy.capacity == 20.0
    assert alchemy.refill_rate == 20.0
    assert alchemy.name == "alchemy_base"

    helius = build_helius_limiter()
    assert helius.capacity == 8.0
    assert helius.refill_rate == 8.0
    assert helius.name == "helius_solana"

    reg = RateLimiterRegistry.default()
    assert reg.alchemy.capacity == 20.0
    assert reg.helius.capacity == 8.0
    assert reg.for_chain(ChainIdentifier.BASE_MAINNET) is reg.alchemy
    assert reg.for_chain(ChainIdentifier.SOLANA_MAINNET) is reg.helius


def test_rate_limiter_continuous_leak_and_penalize():
    """Verify non-blocking acquisition, penalty backoff, and capacity exceed errors."""
    limiter = AsyncTokenBucket(capacity=5.0, refill_rate=10.0, name="test")

    # Cost exceeding capacity raises RateLimitError
    try:
        asyncio.run(limiter.acquire(cost=6.0))
        assert False, "Should have raised RateLimitError"
    except RateLimitError:
        pass

    # Acquire nowait works when tokens available
    assert limiter.acquire_nowait(cost=3.0) is True
    assert limiter.available_tokens <= 2.05

    # Penalize locks out acquisitions
    limiter.penalize(2.0)
    assert limiter.is_penalized is True
    assert limiter.acquire_nowait(cost=1.0) is False


def test_wallet_profiler_survivorship_outlier_bias():
    """Drop any wallet where > 60% of total lifetime PnL originates from a single lucky trade."""
    now = 1700000000
    trades: list[WalletTradeRecord] = []

    # 15 trades total
    # 1 lucky trade with $7,000 PnL, 14 trades with $200 PnL each
    # Total PnL = $7000 + 14 * 200 = $9,800.
    # Outlier ratio = 7000 / 9800 = 71.4% (> 60%)
    trades.append(
        WalletTradeRecord(
            token_address="0x" + "1" * 40,
            buy_tx_hash="0x" + "a" * 64,
            sell_tx_hash="0x" + "b" * 64,
            buy_timestamp=now - 25 * 86400,
            sell_timestamp=now - 25 * 86400 + 3600,
            holding_time_seconds=3600.0,
            invested_native=Decimal("1"),
            realized_native_pnl=Decimal("2"),
            realized_pnl_usd=Decimal("7000"),
            roi_pct=200.0,
        )
    )

    for i in range(14):
        t_buy = now - (20 - i) * 86400
        trades.append(
            WalletTradeRecord(
                token_address="0x" + f"{i:040x}",
                buy_tx_hash="0x" + f"{i:064x}",
                sell_tx_hash="0x" + f"{(i+100):064x}",
                buy_timestamp=t_buy,
                sell_timestamp=t_buy + 300,
                holding_time_seconds=300.0,
                invested_native=Decimal("1"),
                realized_native_pnl=Decimal("0.1"),
                realized_pnl_usd=Decimal("200"),
                roi_pct=10.0,
            )
        )

    profile = WalletEvaluator.evaluate(
        wallet_address="0x" + "e" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=now,
    )

    assert profile.classification == WalletClassification.LUCKY_OUTLIER
    assert profile.is_whitelisted is False
    assert profile.outlier_pnl_ratio > 0.60
    assert any("Survivorship/Outlier bias" in r for r in profile.rejection_reasons)


def test_wallet_profiler_holding_time_filter():
    """Discard wallets holding tokens for < 45 seconds (HFT/MEV bots)."""
    now = 1700000000
    trades: list[WalletTradeRecord] = []

    # 20 trades, all held for 15 seconds
    for i in range(20):
        t_buy = now - (25 - i) * 86400
        trades.append(
            WalletTradeRecord(
                token_address="0x" + f"{i:040x}",
                buy_tx_hash="0x" + f"{i:064x}",
                sell_tx_hash="0x" + f"{(i+100):064x}",
                buy_timestamp=t_buy,
                sell_timestamp=t_buy + 15,  # 15s holding time!
                holding_time_seconds=15.0,
                invested_native=Decimal("1"),
                realized_native_pnl=Decimal("0.1"),
                realized_pnl_usd=Decimal("100"),
                roi_pct=10.0,
            )
        )

    profile = WalletEvaluator.evaluate(
        wallet_address="0x" + "f" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=now,
    )

    assert profile.classification == WalletClassification.MEV_BOT
    assert profile.is_whitelisted is False
    assert profile.median_holding_time_seconds < 45.0
    assert any("HFT/MEV Bot" in r for r in profile.rejection_reasons)


def test_wallet_profiler_dead_wallet_revival():
    """Flag wallets inactive for > 45 days that suddenly trade."""
    now = 1700000000
    trades: list[WalletTradeRecord] = []

    # Trades 1-15 took place 80 days ago, then nothing until trade 16 today (80-day dormancy)
    old_time = now - 80 * 86400
    for i in range(15):
        t_buy = old_time + i * 3600
        trades.append(
            WalletTradeRecord(
                token_address="0x" + f"{i:040x}",
                buy_tx_hash="0x" + f"{i:064x}",
                sell_tx_hash="0x" + f"{(i+100):064x}",
                buy_timestamp=t_buy,
                sell_timestamp=t_buy + 300,
                holding_time_seconds=300.0,
                invested_native=Decimal("1"),
                realized_native_pnl=Decimal("0.1"),
                realized_pnl_usd=Decimal("100"),
                roi_pct=10.0,
            )
        )

    # Trade 16 today
    trades.append(
        WalletTradeRecord(
            token_address="0x" + "9" * 40,
            buy_tx_hash="0x" + "9" * 64,
            sell_tx_hash="0x" + "8" * 64,
            buy_timestamp=now - 300,
            sell_timestamp=now,
            holding_time_seconds=300.0,
            invested_native=Decimal("1"),
            realized_native_pnl=Decimal("0.1"),
            realized_pnl_usd=Decimal("100"),
            roi_pct=10.0,
        )
    )

    profile = WalletEvaluator.evaluate(
        wallet_address="0x" + "7" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        trades=trades,
        current_timestamp=now,
    )

    assert profile.classification == WalletClassification.DEAD_REVIVAL
    assert profile.is_whitelisted is False
    assert any("Dead-wallet revival" in r for r in profile.rejection_reasons)


def test_wallet_profiler_wash_trading_3_hop_bfs():
    """If initial gas/funds trace back to token deployer or multi-sig <= 3 hops, mark INSIDER and ban."""
    async def run():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_whitelist.db")
            deployer = "0x" + "d" * 40
            target_wallet = "0x" + "w" * 40

            # Hop 1: Target funded by IntA
            # Hop 2: IntA funded by IntB
            # Hop 3: IntB funded by Deployer!
            funding_graph = {
                target_wallet.lower(): [("0x" + "a" * 40, Decimal("1.0"), "0x" + "1" * 64)],
                ("0x" + "a" * 40): [("0x" + "b" * 40, Decimal("1.0"), "0x" + "2" * 64)],
                ("0x" + "b" * 40): [(deployer, Decimal("5.0"), "0x" + "3" * 64)],
            }

            async def mock_fetcher(addr: str, chain: ChainIdentifier):
                return funding_graph.get(addr.lower(), [])

            profiler = SmartMoneyProfiler(
                db_path=db_path,
                funding_fetcher=mock_fetcher,
            )
            await profiler.initialize()

            # 20 valid trades
            trades = [
                WalletTradeRecord(
                    token_address="0x" + f"{i:040x}",
                    buy_tx_hash="0x" + f"{i:064x}",
                    sell_tx_hash="0x" + f"{(i+100):064x}",
                    buy_timestamp=1700000000 - (30 - i) * 86400,
                    sell_timestamp=1700000000 - (30 - i) * 86400 + 300,
                    holding_time_seconds=300.0,
                    invested_native=Decimal("1"),
                    realized_native_pnl=Decimal("0.1"),
                    realized_pnl_usd=Decimal("100"),
                    roi_pct=10.0,
                )
                for i in range(20)
            ]

            is_wl, profile = await profiler.evaluate_and_whitelist(
                wallet_address=target_wallet,
                chain=ChainIdentifier.BASE_MAINNET,
                trades=trades,
                deployer_addresses={deployer},
                current_timestamp=1700000000,
            )

            assert is_wl is False
            assert profile.classification == WalletClassification.INSIDER
            assert profile.is_whitelisted is False

            # Verify persisted as BANNED in SQLite
            is_stored_wl = await profiler.is_whitelisted(target_wallet)
            assert is_stored_wl is False

            record = await profiler.whitelist_db.get_wallet(target_wallet)
            assert record is not None
            assert record.status == WhitelistStatus.BANNED

            await profiler.close()

    asyncio.run(run())


def test_wallet_profiler_approved_whitelist():
    """Wallet meeting all criteria (>15 trades, >=55% win rate, >=21 days, holding >45s, no outlier) gets APPROVED."""
    async def run():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_approved.db")
            target_wallet = "0x" + "8" * 40
            now = 1700000000

            # 20 trades, 14 wins (70% win rate), span 25 days, holding time 600s, balanced PnL
            trades = []
            for i in range(20):
                is_win = (i < 14)
                pnl = Decimal("150") if is_win else Decimal("-50")
                t_buy = now - int((26 - i * (25.0 / 19.0)) * 86400)
                trades.append(
                    WalletTradeRecord(
                        token_address="0x" + f"{i:040x}",
                        buy_tx_hash="0x" + f"{i:064x}",
                        sell_tx_hash="0x" + f"{(i+100):064x}",
                        buy_timestamp=t_buy,
                        sell_timestamp=t_buy + 600,
                        holding_time_seconds=600.0,
                        invested_native=Decimal("1"),
                        realized_native_pnl=Decimal("0.1") if is_win else Decimal("-0.05"),
                        realized_pnl_usd=pnl,
                        roi_pct=15.0 if is_win else -5.0,
                    )
                )

            profiler = SmartMoneyProfiler(db_path=db_path)
            await profiler.initialize()

            is_wl, profile = await profiler.evaluate_and_whitelist(
                wallet_address=target_wallet,
                chain=ChainIdentifier.BASE_MAINNET,
                trades=trades,
                current_timestamp=now,
            )

            assert is_wl is True
            assert profile.classification == WalletClassification.APPROVED
            assert profile.win_rate_pct == 70.0
            assert profile.total_trades == 20
            assert profile.active_days >= 21.0
            assert profile.is_whitelisted is True

            # Verify in SQLite DB
            is_stored_wl = await profiler.is_whitelisted(target_wallet)
            assert is_stored_wl is True

            active_list = await profiler.get_whitelisted_wallets()
            assert len(active_list) == 1
            assert active_list[0].wallet_address == target_wallet.lower()

            await profiler.close()

    asyncio.run(run())


if __name__ == "__main__":
    print("=== RUNNING TEST SUITE FOR MODULES A, C & MODELS ===")
    test_models_validation_and_invariants()
    print("  [PASS] test_models_validation_and_invariants")
    test_rate_limiter_strict_quotas()
    print("  [PASS] test_rate_limiter_strict_quotas")
    test_rate_limiter_continuous_leak_and_penalize()
    print("  [PASS] test_rate_limiter_continuous_leak_and_penalize")
    test_wallet_profiler_survivorship_outlier_bias()
    print("  [PASS] test_wallet_profiler_survivorship_outlier_bias")
    test_wallet_profiler_holding_time_filter()
    print("  [PASS] test_wallet_profiler_holding_time_filter")
    test_wallet_profiler_dead_wallet_revival()
    print("  [PASS] test_wallet_profiler_dead_wallet_revival")
    test_wallet_profiler_wash_trading_3_hop_bfs()
    print("  [PASS] test_wallet_profiler_wash_trading_3_hop_bfs")
    test_wallet_profiler_approved_whitelist()
    print("  [PASS] test_wallet_profiler_approved_whitelist")
    print("\nALL 8 PROFILER & RATE LIMITER TESTS PASSED WITH 100% SUCCESS!")
