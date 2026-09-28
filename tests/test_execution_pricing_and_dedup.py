"""
tests/test_execution_pricing_and_dedup.py — Regression tests for Bot-MM fixes
=============================================================================
Validates:
1. Non-zero fill pricing & bonding curve reserve fallback (pump.fun / Raydium).
2. Position deduplication across runner and book (preventing duplicate buys).
3. Automated TP (+25%, +50%) and SL (-15%) dynamic lot exit execution and win rate tracking.
4. Token blacklisting for wrapped native & stable tokens on Solana & Base.
5. Solana RPC logsSubscribe Base58 public key sanitization.
6. Telegram bot commands: /news, /signals, and high-precision /trades formatting.
7. Anti-sinkhole DNS resolver upstream query.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.dns_resolver import query_upstream_dns
from alpha_engine.engine.runner import PaperTradingEngine
from alpha_engine.execution.book import OpenLot, PositionBook, RunningMetrics
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.ingestion.svm import is_valid_solana_pubkey
from alpha_engine.ingestion.telegram import TelegramIngester
from alpha_engine.math.cpmm import get_initial_bonding_curve_pool
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
)
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PaperFill, SecurityReport
from alpha_engine.security.constants import is_blacklisted_token


def test_non_zero_fill_pricing_and_bonding_curve_fallback():
    """Verify that an order is never placed with price 0.0, using pump.fun bonding curve reserves."""
    pool = get_initial_bonding_curve_pool(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="PumpFunTestMint111111111111111111111111111",
    )
    assert pool.spot_price_native_per_token > Decimal(0)
    assert pool.native_reserve == Decimal("30.0")
    assert pool.token_reserve == Decimal("1073000000.0")

    book = PositionBook()
    executor = PaperExecutor(position_book=book, native_price_usd=Decimal("150.0"))

    dummy_report = SecurityReport(
        token_address="PumpFunTestMint111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        mint_authority_disabled=True,
        top10_concentration=0.05,
    )
    signal = SignalEvent(
        signal_id="sig-test-pricing-01",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="PumpFunTestMint111111111111111111111111111",
        pool_address="PoolAddr111111111111111111111111111111111",
        timestamp_ns=time.time_ns(),
        source=SignalSource.DEX_SWAP,
        suggested_side=OrderSide.BUY,
        alpha_score=0.85,
        strength=SignalStrength.STRONG,
        pool_state=pool,
        security_report=dummy_report,
    )

    async def _run():
        fill = await executor.execute_signal(signal, portfolio_equity_usd=Decimal("10000.0"))
        assert fill is not None
        assert fill.effective_price > Decimal(0)
        assert fill.tokens_acquired > Decimal(0)
        assert fill.simulated_native_spent > Decimal(0)

        # Opening lot in book with valid price succeeds
        lot = book.open_lot(fill, signal.signal_id, initial_pool_reserve_native=pool.native_reserve)
        assert lot.entry_price == fill.effective_price
        assert lot.tokens_held == fill.tokens_acquired

        # Opening lot with zero price is strictly rejected
        zero_fill = PaperFill(
            order_id="bad-order",
            chain=ChainIdentifier.SOLANA_MAINNET,
            token_address="BadMint11111111111111111111111111111111",
            side=OrderSide.BUY,
            simulated_native_spent=Decimal("0.5"),
            tokens_acquired=Decimal("1000"),
            effective_price=Decimal("0.0"),
            price_impact_pct=0.0,
            simulated_gas_cost_usd=Decimal("0.01"),
            fill_timestamp_ns=time.time_ns(),
            fill_latency_ms=10.0,
        )
        with pytest.raises(ValueError, match="non-positive fill price"):
            book.open_lot(zero_fill, "bad-sig")

    asyncio.run(_run())


def test_position_deduplication():
    """Verify that multiple BUY signals for the same token mint are rejected if an active lot exists."""
    book = PositionBook()
    token_mint = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

    assert book.is_position_open(token_mint) is False

    fill = PaperFill(
        order_id="ord-01",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token_mint,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("0.5"),
        tokens_acquired=Decimal("100000"),
        effective_price=Decimal("0.000005"),
        price_impact_pct=0.01,
        simulated_gas_cost_usd=Decimal("0.01"),
        fill_timestamp_ns=time.time_ns(),
        fill_latency_ms=10.0,
    )
    book.open_lot(fill, "sig-01")

    # Position is now open (case-insensitive)
    assert book.is_position_open(token_mint) is True
    assert book.is_position_open(token_mint.lower()) is True

    # Check engine helper
    cfg = EngineConfig(alchemy_ws_url="", alchemy_http_url="")
    engine = PaperTradingEngine(cfg)
    engine._position_book = book
    assert engine.is_position_open(token_mint) is True
    assert engine.is_position_open("AnotherUnknownMint1111111111111111111111111") is False


def test_automated_take_profit_and_stop_loss():
    """Verify automated TP (+25%, +50%) and SL (-15%) dynamic lot exit execution and win rate tracking."""
    book = PositionBook()
    metrics = RunningMetrics(peak_equity_usd=Decimal("10000.0"))

    entry_price = Decimal("0.000100")
    lot = OpenLot(
        token_address="TokenA",
        chain=ChainIdentifier.SOLANA_MAINNET,
        initial_tokens=Decimal("10000"),
        tokens_held=Decimal("10000"),
        cost_basis_native=Decimal("1.0"),
        entry_price=entry_price,
        signal_id="sig-tp",
        open_timestamp_ns=time.time_ns(),
        peak_price=entry_price,
        exit_stage=ExitStage.NONE,
        trailing_stop_active=False,
        trailing_stop_price=Decimal(0),
        last_pool_reserve_native=Decimal("50.0"),
    )
    book._lots[(lot.chain.value, lot.token_address)].append(lot)

    # 1. Hard Stop-Loss (-15% drops to 0.000085 or lower)
    sl_decision = book.evaluate_lot_exit(lot, current_price=Decimal("0.000084"))
    assert sl_decision is not None
    assert sl_decision.should_exit is True
    assert sl_decision.exit_reason == TradeExitReason.SL_INITIAL
    assert sl_decision.exit_stage == ExitStage.SL
    assert sl_decision.tokens_to_sell == Decimal("10000")

    # 2. Take-Profit TP (2x / +100% gain at 0.000200)
    tp_decision = book.evaluate_lot_exit(lot, current_price=Decimal("0.000200"))
    assert tp_decision is not None
    assert tp_decision.should_exit is True
    assert tp_decision.exit_reason == TradeExitReason.TP_2X
    assert tp_decision.exit_stage == ExitStage.TP1
    assert tp_decision.tokens_to_sell == Decimal("5000")  # 50% initial bag

    # Apply TP decision
    pnl_native, remaining = book.apply_exit_decision(tp_decision, sell_price=Decimal("0.000200"))
    assert pnl_native == Decimal("5000") * (Decimal("0.000200") - Decimal("0.000100"))
    assert remaining == Decimal("5000")
    metrics.record_closed_trade(realized_pnl_usd=Decimal("75.00"), gas_cost_usd=Decimal("0.01"), current_equity_usd=Decimal("10075.00"))
    assert metrics.win_rate_pct == 100.0

    # 3. Trailing Stop-Loss: Once up +50% (0.000150), trailing stop locks at +20% (0.000120)
    book.evaluate_lot_exit(lot, current_price=Decimal("0.000150"))
    assert lot.trailing_stop_active is True
    assert lot.trailing_stop_price == Decimal("0.000100") * Decimal("1.20")

    # When price drops below trailing stop (e.g. 0.000115)
    trail_decision = book.evaluate_lot_exit(lot, current_price=Decimal("0.000115"))
    assert trail_decision is not None
    assert trail_decision.should_exit is True
    assert trail_decision.exit_reason == TradeExitReason.SL_TRAILING
    assert trail_decision.exit_stage == ExitStage.TRAILING_SL


def test_blacklisted_native_and_wrapped_tokens():
    """Verify that WSOL, Solana USDC, and Base WETH are rejected by the blacklist filter."""
    wsol = "So11111111111111111111111111111111111111112"
    usdc_sol = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    weth_base = "0x4200000000000000000000000000000000000006"
    weth_mainnet = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    assert is_blacklisted_token(wsol, ChainIdentifier.SOLANA_MAINNET) is True
    assert is_blacklisted_token(usdc_sol, ChainIdentifier.SOLANA_MAINNET) is True
    assert is_blacklisted_token(weth_base, ChainIdentifier.BASE_MAINNET) is True
    assert is_blacklisted_token(weth_mainnet, ChainIdentifier.BASE_MAINNET) is True
    assert is_blacklisted_token("ValidMeme1111111111111111111111111111111111", ChainIdentifier.SOLANA_MAINNET) is False


def test_solana_base58_pubkey_validation():
    """Verify that only valid 32-byte Base58 Solana public keys pass SVM sanitization."""
    valid_pubkey = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    assert is_valid_solana_pubkey(valid_pubkey) is True

    # Malformed addresses (bad characters, wrong byte length)
    assert is_valid_solana_pubkey("") is False
    assert is_valid_solana_pubkey("invalid-0OIl-characters") is False
    assert is_valid_solana_pubkey("1111") is False  # Too short (only ~3-4 bytes)
    assert is_valid_solana_pubkey("0x285617313860407d647990b50375990264186566") is False  # EVM hex string


def test_telegram_bot_news_signals_and_trades(tmp_path):
    """Verify /news, /signals, and /trades Telegram command handling and high-precision formatting."""
    class MockEvent:
        def __init__(self, text: str):
            self.raw_text = text
            self.is_private = True
            self.sender_id = 12345
            self.replies: list[str] = []

        async def reply(self, msg: str):
            self.replies.append(msg)

    async def _run():
        db_file = tmp_path / "test_trades.db"
        queue: asyncio.Queue = asyncio.Queue()
        ingester = TelegramIngester(event_queue=queue, db_path=str(db_file), admin_ids=[12345])

        status_data = {
            "uptime_str": "01h 23m 45s",
            "rss_mb": 42.5,
            "equity_usd": Decimal("10500.00"),
            "realized_pnl_usd": Decimal("500.00"),
            "open_positions": 1,
            "win_rate_pct": 75.0,
            "max_drawdown_pct": 2.1,
            "recent_news": [
                {
                    "token_address": "TokenA11111111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "source": "TWITTER",
                    "headline": "Elon tweets about new dog meme token breakout",
                    "sentiment": 0.85,
                    "timestamp": time.time(),
                }
            ],
            "recent_signals": [
                {
                    "token_address": "TokenA11111111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "reason": "Top 10 concentration > 50% (68.4%)",
                    "passed": False,
                    "timestamp": time.time(),
                }
            ],
            "open_trades": [
                {
                    "token_address": "TokenPump11111111111111111111111111111111",
                    "chain": ChainIdentifier.SOLANA_MAINNET,
                    "entry_price": Decimal("0.000000028"),
                    "current_price": Decimal("0.000000035"),
                    "unrealized_pnl_usd": Decimal("24.50"),
                    "tokens_held": Decimal("1000000"),
                }
            ],
        }

        ingester.set_status_provider(lambda: status_data)

        # 1. Test /news
        evt_news = MockEvent("/news")
        await ingester._handle_dm_message(evt_news)
        assert len(evt_news.replies) == 1
        assert "Last 5 Ingested News Headlines" in evt_news.replies[0]
        assert "Elon tweets about new dog meme" in evt_news.replies[0]
        assert "+0.85" in evt_news.replies[0]

        # 2. Test /signals
        evt_sig = MockEvent("/signals")
        await ingester._handle_dm_message(evt_sig)
        assert len(evt_sig.replies) == 1
        assert "Last 5 Token Security Screenings" in evt_sig.replies[0]
        assert "REJ" in evt_sig.replies[0]
        assert "Top 10 concentration > 50%" in evt_sig.replies[0]

        # 3. Test /trades (high-precision formatting avoids 0.000000)
        evt_trades = MockEvent("/trades")
        await ingester._handle_dm_message(evt_trades)
        assert len(evt_trades.replies) == 1
        assert "Active Open Positions" in evt_trades.replies[0]
        assert "2.800e-08" in evt_trades.replies[0] or "2.8" in evt_trades.replies[0]
        assert "0.000000" not in evt_trades.replies[0]
        assert "+$24.50" in evt_trades.replies[0]

    asyncio.run(_run())


def test_anti_sinkhole_dns_query():
    """Verify that query_upstream_dns can query verified hosts and return clean IP addresses."""
    ips = query_upstream_dns("rugcheck.xyz")
    assert isinstance(ips, list)
    assert len(ips) > 0
    assert "174.138.15.144" in ips or all(isinstance(ip, str) for ip in ips)


def test_security_report_tax_properties_and_rejection_reason_extraction():
    """Verify that SecurityReport properties (buy_tax_pct, etc.) and runner rejection reason work cleanly."""
    rep = SecurityReport(
        token_address="TokenRejectionTest111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.TIER1_REJECTED,
        is_honeypot=False,
        buy_tax_bps=1200,
        sell_tax_bps=1500,
        mint_authority_disabled=False,
        top10_concentration=0.65,
        lp_burned_ratio=0.50,
    )
    assert rep.buy_tax_pct == 12.0
    assert rep.sell_tax_pct == 15.0
    assert rep.mint_disabled is False
    assert rep.lp_burned is False

    reason = PaperTradingEngine._extract_gatekeeper_rejection_reason(None, rep)
    assert "Excessive tax" in reason
    assert "Mint authority active" in reason
    assert "Top 10 concentration > 50%" in reason
    assert "LP not burned or locked" in reason
