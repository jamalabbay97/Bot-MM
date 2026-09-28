"""
tests/test_autonomous_engine.py — Comprehensive Autonomous Alpha Engine Test Suite
==================================================================================
Validates:
1. Autonomous Ingestion Architecture (SVM Pump.fun/Raydium, EVM PairCreated, X Anti-Sybil & Velocity).
2. Advanced Security Gatekeeper (Bytecode selectors, tax limits <= 5%, LP locks, mixer tracing).
3. Smart Money Autonomous Profiler & ML Evaluator (Reverse-engineering, 30d stats, circular wash loops).
4. Execution Engine, MEV Protection & Market Drag (Flashbots/Titan/MEV-Blocker, Jito Tip Floor, Paper Drag).
5. Dynamic Trade Management (TP ladder 2x/3x/5x/10x, Trailing SL at +50%/+20%, Emergency Liquidity Drain).
6. Self-Learning Feedback Loop & RPC Failover (Social decay, wallet pruning, latency/lag failover).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root and virtual environment site-packages are always in sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

for _p in (_ROOT / ".venv" / "lib").glob("python*/site-packages"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.engine.feedback import AdaptiveFeedbackEngine, TradeReflection
from alpha_engine.engine.rpc_health import RPCEndpoint, RPCHealthMonitor
from alpha_engine.execution.book import ExitDecision, OpenLot, PositionBook
from alpha_engine.execution.bundle import PrivateTxRouter
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.ingestion.decoders import (
    PUMP_FUN_PROGRAM_ID,
    RAYDIUM_AMM_PROGRAM_ID,
    _decode_evm_pair_created_log,
    _decode_evm_pool_created_log,
    _parse_pump_fun_logs,
    _parse_raydium_initialize2_logs,
)
from alpha_engine.ingestion.x_stream import TweetPayload, XStreamIngester
from alpha_engine.math.sizing import (
    ASSUMED_SLIPPAGE_PCT,
    DEX_SWAP_FEE_PCT,
    MAX_AUTONOMOUS_CAP,
    SIMULATED_GAS_NATIVE,
    apply_paper_trading_drag,
    compute_position_size,
)
from alpha_engine.models.enums import (
    ChainIdentifier,
    ExitStage,
    OrderSide,
    SecurityTier,
    SignalSource,
    SignalStrength,
    TradeExitReason,
    WalletClassification,
    WhitelistStatus,
)
from alpha_engine.models.events import RawSignalEvent, SignalEvent, SwapEvent
from alpha_engine.models.profiler import WalletTradeRecord
from alpha_engine.models.state import PaperFill, PoolState, SecurityReport
from alpha_engine.profiler.evaluator import (
    WalletEvaluator,
    detect_circular_wash_trading,
)
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.profiler.whitelist_db import WhitelistDB
from alpha_engine.security.constants import (
    MIXER_AND_RUG_FUNDING_ADDRESSES,
    TAX_MUTATION_SELECTORS,
    VERIFIED_LP_LOCKERS,
)
from alpha_engine.security.gatekeeper import SecurityGatekeeper
from alpha_engine.security.preflight import inspect_bytecode_for_delayed_taxes


# ==============================================================================
# 1. Autonomous Ingestion Architecture Tests
# ==============================================================================

def test_svm_pump_fun_log_parsing():
    """Verify parsing of Pump.fun mint and bonding curve initialization logs."""
    assert PUMP_FUN_PROGRAM_ID == "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    logs = [
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
        "Program log: Instruction: InitializeMint2",
        "Program log: Create mint 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "Program log: Instruction: Create",
        "Program log: virtual_sol_reserves: 30000000000",
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
    ]
    parsed = _parse_pump_fun_logs(logs)
    assert parsed is not None
    mint, curve_sol = parsed
    assert mint == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert curve_sol == Decimal("30.0")


def test_svm_raydium_amm_pool_parsing():
    """Verify parsing of Raydium AMM pool creation logs."""
    assert RAYDIUM_AMM_PROGRAM_ID == "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    logs = [
        "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 invoke [1]",
        "Program log: initialize2: open_time 1711000000",
        "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success",
    ]
    open_time = _parse_raydium_initialize2_logs(logs)
    assert open_time == 1711000000


def test_evm_pair_created_event_decoding():
    """Verify decoding of PairCreated and PoolCreated event logs across EVM DEXes."""
    # PairCreated(address token0, address token1, address pair, uint)
    # token0 = 0x4200000000000000000000000000000000000006 (WETH)
    # token1 = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 (USDC)
    topic0 = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
    topic1 = "0x0000000000000000000000004200000000000000000000000000000000000006"
    topic2 = "0x000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    pair_addr = "0xd4a0e0b9149bcee3c920d2e00b5de09138fd8bb7"
    data = "0x" + "00" * 12 + pair_addr[2:] + "00" * 31 + "01"

    decoded = _decode_evm_pair_created_log(
        topics=[topic0, topic1, topic2],
        data=data,
        weth_address="0x4200000000000000000000000000000000000006",
    )
    assert decoded is not None
    pair, new_token, is_weth_first = decoded
    assert pair.lower() == pair_addr.lower()
    assert new_token.lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    assert is_weth_first is True

    # PoolCreated(address,address,uint24,int24,address)
    pool_created_log = {
        "topics": [
            "0x783cca1c041245d8083164ea2cbd8e436ab6ae90824b2b740b02830204cc0415",
            topic1,
            topic2,
            "0x00000000000000000000000000000000000000000000000000000000000001f4",
        ],
        "data": "0x" + "00" * 32 + "00" * 12 + pair_addr[2:] + "00" * 32,
        "address": "0xfactory",
        "transactionHash": "0xtx",
        "blockNumber": "0x1",
    }
    pool_decoded = _decode_evm_pool_created_log(pool_created_log)
    assert pool_decoded is not None
    assert pool_decoded["pool"].lower() == pair_addr.lower()


def test_x_stream_anti_sybil_and_engagement_velocity():
    """Verify X-Stream Anti-Sybil filtering, bot farm blacklisting, and regex extraction."""
    queue = asyncio.Queue()
    ingester = XStreamIngester(event_queue=queue)

    # 1. Reject account age < 90 days
    young_tweet = TweetPayload(
        tweet_id="101",
        text="Ape into $ALPHA 0x285617313860407d647990b50375990264186566",
        author_id="author_1",
        author_username="crypto_newbie",
        account_age_days=45.0,  # < 90 days
        followers_count=5_000,
        retweets_count=10,
        quotes_count=5,
        replies_count=5,
    )
    eval_young = ingester.evaluate_tweet(young_tweet)
    assert eval_young.passes_sybil_filter is False
    assert "Young account" in eval_young.rejection_reason

    # 2. Reject accounts with < 1,000 followers
    low_follower_tweet = TweetPayload(
        tweet_id="102",
        text="Check out $GEM 0x285617313860407d647990b50375990264186566",
        author_id="author_2",
        author_username="crypto_anon",
        account_age_days=180.0,
        followers_count=450,  # < 1,000
        retweets_count=10,
        quotes_count=5,
        replies_count=5,
    )
    eval_followers = ingester.evaluate_tweet(low_follower_tweet)
    assert eval_followers.passes_sybil_filter is False
    assert "Insufficient followers" in eval_followers.rejection_reason

    # 3. Detect bot-farm keywords and blacklist the symbol
    bot_farm_tweet = TweetPayload(
        tweet_id="103",
        text="🚀 FAST PUMP GUARANTEED 100x NEXT GEM $SCAM COIN 0x285617313860407d647990b50375990264186566",
        author_id="author_3",
        author_username="alpha_caller",
        account_age_days=300.0,
        followers_count=20_000,
        retweets_count=50,
        quotes_count=10,
        replies_count=10,
    )
    eval_bot = ingester.evaluate_tweet(bot_farm_tweet)
    assert eval_bot.passes_sybil_filter is False
    assert "Bot farm keywords" in eval_bot.rejection_reason
    assert ingester.is_symbol_blacklisted("SCAM") is True

    # 4. Legitimate high-velocity tweet with CA extraction
    clean_tweet = TweetPayload(
        tweet_id="104",
        text="Breakout on $MOON: 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU volume expanding fast!",
        author_id="author_4",
        author_username="smart_trader",
        account_age_days=400.0,
        followers_count=15_000,
        retweets_count=20,
        quotes_count=15,
        replies_count=25,
    )
    eval_clean = ingester.evaluate_tweet(clean_tweet)
    assert eval_clean.passes_sybil_filter is True
    assert eval_clean.solana_ca == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert eval_clean.ticker == "MOON"
    assert eval_clean.velocity_score > 0.0


# ==============================================================================
# 2. Comprehensive Security Gatekeeper Tests
# ==============================================================================

def test_bytecode_delayed_tax_selector_inspection():
    """Verify inspection of EVM bytecode for backdoor tax/trading mutation selectors."""
    assert "9c3d4f19" in TAX_MUTATION_SELECTORS
    assert len(TAX_MUTATION_SELECTORS) > 0
    # Bytecode containing setTax(uint256,uint256) selector: 0x9c3d4f19
    malicious_bytecode = "0x608060405234801561001057600080fd5b509c3d4f19610020"
    is_safe, detected = inspect_bytecode_for_delayed_taxes(malicious_bytecode)
    assert is_safe is False
    assert "setTax(uint256,uint256)" in detected

    clean_bytecode = "0x608060405234801561001057600080fd5b5012345678"
    is_safe_clean, detected_clean = inspect_bytecode_for_delayed_taxes(clean_bytecode)
    assert is_safe_clean is True
    assert len(detected_clean) == 0


def test_security_gatekeeper_hard_gate_tax_enforcement():
    """Verify security report rejects tokens with Buy Tax > 5% or Sell Tax > 5%."""
    # Buy tax 6% (> 500 bps) -> Must fail hard gates
    report_high_buy = SecurityReport(
        token_address="0x285617313860407d647990b50375990264186566",
        chain=ChainIdentifier.BASE_MAINNET,
        is_honeypot=False,
        buy_tax_bps=600,
        sell_tax_bps=200,
        liquidity_usd=Decimal("50000"),
        top10_holder_fraction=0.15,
        tier=SecurityTier.CLEAN,
    )
    assert report_high_buy.passes_hard_gates is False

    # Buy tax 5% and Sell tax 5% -> Passes hard gates
    report_valid = SecurityReport(
        token_address="0x285617313860407d647990b50375990264186566",
        chain=ChainIdentifier.BASE_MAINNET,
        is_honeypot=False,
        buy_tax_bps=500,
        sell_tax_bps=500,
        liquidity_usd=Decimal("50000"),
        top10_holder_fraction=0.15,
        tier=SecurityTier.CLEAN,
    )
    assert report_valid.passes_hard_gates is True


def test_security_gatekeeper_lp_lock_and_mixer_tracing():
    """Verify verified LP lockers (Unicrypt/Team.Finance) and Tornado Cash mixer blacklists."""
    # Check known mixer / rug addresses
    tornado_cash = "0xd90e2f925DA726b50C4Ed8D0Fb90Ad053324F31b"
    assert tornado_cash in MIXER_AND_RUG_FUNDING_ADDRESSES

    # Check verified LP lockers
    unicrypt = "0x663A5C229c09b049E36dCc11a9B0d4a8Eb9db214"
    team_finance = "0xE2FE530C047f2d85298b07D91337b0262C99D255"
    assert unicrypt in VERIFIED_LP_LOCKERS
    assert team_finance in VERIFIED_LP_LOCKERS


# ==============================================================================
# 3. Autonomous Smart Wallet Profiler & ML Evaluator Tests
# ==============================================================================

def test_circular_wash_trading_detection():
    """Verify detection of circular transaction loops among affiliated addresses."""
    # Loop: A -> B -> C -> A
    graph_loop = [
        ("wallet_A", "wallet_B"),
        ("wallet_B", "wallet_C"),
        ("wallet_C", "wallet_A"),
    ]
    is_circular = detect_circular_wash_trading(
        wallet_address="wallet_A",
        trades=[],
        transfer_graph=graph_loop,
    )
    assert is_circular is True

    # Organic: A -> B, C -> D, E -> F
    graph_clean = [
        ("wallet_A", "wallet_B"),
        ("wallet_C", "wallet_D"),
        ("wallet_E", "wallet_F"),
    ]
    assert detect_circular_wash_trading(
        wallet_address="wallet_A",
        trades=[],
        transfer_graph=graph_clean,
    ) is False


def test_autonomous_wallet_evaluator_30d_criteria():
    """Verify 30-day autonomous criteria: >=25 trades, WR >60% at >50% ROI, PF >2.0, MDD <35%, hold >180s."""
    evaluator = WalletEvaluator()
    now_ts = 1700000000

    # 1. Qualifying Smart Money Wallet
    qualifying_trades = []
    for i in range(30):
        is_win = i < 20  # 20/30 = 66.7% win rate (> 60%)
        qualifying_trades.append(
            WalletTradeRecord(
                token_address="0x" + "1" * 40,
                buy_tx_hash="0x" + f"{i:064x}",
                sell_tx_hash="0x" + f"{(i+100):064x}",
                buy_timestamp=now_ts - 86400 * 10 - 300,
                sell_timestamp=now_ts - 86400 * 10,
                holding_time_seconds=300.0,  # 5 minutes (> 180s)
                invested_native=Decimal("1.0"),
                realized_native_pnl=Decimal("0.8") if is_win else Decimal("-0.15"),
                realized_pnl_usd=Decimal("800.0") if is_win else Decimal("-150.0"),
                roi_pct=80.0 if is_win else -15.0,  # Wins are > 50% ROI
            )
        )

    result_qual = evaluator.evaluate_autonomous(
        wallet_address="0x1111111111111111111111111111111111111111",
        trades=qualifying_trades,
        chain=ChainIdentifier.BASE_MAINNET,
        current_timestamp=now_ts,
    )
    assert result_qual.is_whitelisted is True
    assert result_qual.classification == WalletClassification.APPROVED
    assert result_qual.total_trades == 30
    assert result_qual.win_rate_pct > 60.0

    # 2. Micro-second MEV bot (< 180s holding time) -> Must be rejected
    mev_trades = []
    for i in range(30):
        is_win = i < 25
        mev_trades.append(
            WalletTradeRecord(
                token_address="0x" + "2" * 40,
                buy_tx_hash="0x" + f"{(i+200):064x}",
                sell_tx_hash="0x" + f"{(i+300):064x}",
                buy_timestamp=now_ts - 86400 * 5 - 2,
                sell_timestamp=now_ts - 86400 * 5,
                holding_time_seconds=2.0,  # 2 seconds (< 180s)
                invested_native=Decimal("1.0"),
                realized_native_pnl=Decimal("0.6") if is_win else Decimal("-0.05"),
                realized_pnl_usd=Decimal("600.0") if is_win else Decimal("-50.0"),
                roi_pct=60.0 if is_win else -5.0,
            )
        )
    result_mev = evaluator.evaluate_autonomous(
        wallet_address="0x2222222222222222222222222222222222222222",
        trades=mev_trades,
        chain=ChainIdentifier.BASE_MAINNET,
        current_timestamp=now_ts,
    )
    assert result_mev.is_whitelisted is False
    assert result_mev.classification == WalletClassification.MEV_BOT


def test_reverse_engineer_winning_tokens(tmp_path):
    """Verify autonomous reverse-engineering of winning tokens (>300% in 1 hr) discovering early buyers."""
    async def _run():
        db_file = tmp_path / "whitelist_test.db"
        db = WhitelistDB(str(db_file))
        profiler = SmartMoneyProfiler(whitelist_db=db)
        await profiler.initialize()
        try:
            deployer = "0xdeployer00000000000000000000000000000000"
            early_buyer = "0xearlybuyer11111111111111111111111111111111"
            late_buyer = "0xlatebuyer222222222222222222222222222222222"

            creation_ts = 1000
            token_trades = [
                {"wallet": deployer, "block_number": 1, "timestamp": creation_ts + 5},
                {"wallet": early_buyer, "block_number": 3, "timestamp": creation_ts + 30},
                {"wallet": late_buyer, "block_number": 50, "timestamp": creation_ts + 600},
            ]

            discovered = await profiler.reverse_engineer_winning_tokens(
                token_address="0xwinningtoken333333333333333333333333333333",
                chain=ChainIdentifier.BASE_MAINNET,
                price_gain_pct=350.0,  # > 300%
                creation_timestamp=creation_ts,
                early_trades=token_trades,
                deployer_cluster=[deployer],
            )
            assert early_buyer.lower() in [d.lower() for d in discovered]
            assert deployer.lower() not in [d.lower() for d in discovered]
            assert late_buyer.lower() not in [d.lower() for d in discovered]
        finally:
            await db.close()

    asyncio.run(_run())


# ==============================================================================
# 4. Execution Engine, MEV Protection & Market Drag Tests
# ==============================================================================

def test_private_tx_router_endpoints_and_jito_tip():
    """Verify Flashbots/Titan/MEV-Blocker headers and Jito dynamic tip floor estimation."""
    router = PrivateTxRouter(
        flashbots_rpc="https://rpc.flashbots.net",
        titan_rpc="https://rpc.titanbuilder.xyz",
        mev_blocker_rpc="https://rpc.mevblocker.io",
    )
    fb_req = router.prepare_flashbots_request({"jsonrpc": "2.0", "method": "eth_sendRawTransaction", "params": ["0x123"], "id": 1})
    assert fb_req["url"] == "https://rpc.flashbots.net"
    assert "X-Flashbots-Bundle" in fb_req["headers"]

    # Jito Tip Floor mock
    mock_response = [{"landed_tips_50th_percentile": 0.001, "landed_tips_75th_percentile": 0.002, "landed_tips_95th_percentile": 0.005}]
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=mock_response)
    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)
    with patch("aiohttp.ClientSession.get", return_value=mock_ctx):
        tip = asyncio.run(router.get_jito_tip_floor(percentile="p75"))
        assert tip == Decimal("0.002")
        mock_resp.json.assert_awaited_once()


def test_position_sizing_autonomous_cap_and_paper_drag():
    """Verify 2%-5% Kelly cap and paper trading drag deduction (0.3% fee + 0.005 gas + 5% slippage)."""
    assert MAX_AUTONOMOUS_CAP == Decimal("0.05")
    assert SIMULATED_GAS_NATIVE > Decimal(0)
    # 1. Autonomous Kelly sizing with 5% cap
    sizing = compute_position_size(
        kelly_fraction=0.10,  # raw 10%
        portfolio_equity_usd=Decimal("10000"),
        native_price_usd=Decimal("3000"),
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        max_position_fraction=Decimal("0.05"),  # 5% cap
    )
    assert sizing.capped_fraction == 0.05
    assert sizing.usd_at_risk == Decimal("500")

    # 2. Paper trading drag on BUY
    gross_in = Decimal("1.0")  # 1 ETH spent
    exec_price = Decimal("10.0")  # 10 tokens per native
    net_tokens, effective_price, fee = apply_paper_trading_drag(
        gross_amount=gross_in,
        execution_price=exec_price,
        side=OrderSide.BUY,
    )
    # Effective price includes 5% slippage
    assert effective_price == exec_price * (Decimal("1") + ASSUMED_SLIPPAGE_PCT)
    # Net tokens deducted by 0.3% DEX swap fee
    assert fee == gross_in * DEX_SWAP_FEE_PCT
    assert net_tokens < (gross_in / exec_price)


# ==============================================================================
# 5. Dynamic Trade Management (Dynamic Exits) Tests
# ==============================================================================

def test_dynamic_exits_take_profit_ladder():
    """Verify Take Profit Ladder: 50% at 2x, 25% at 3x, 50% remainder at 5x, 100% at 10x."""
    book = PositionBook()
    fill = PaperFill(
        token_address="0xmoontoken",
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("0.001"),  # Entry price
    )
    lot = book.open_lot(fill, signal_id="sig_moon")
    assert isinstance(lot, OpenLot)
    assert PaperExecutor is not None

    # 1. Price reaches 2x (+100%) -> Sell 50% of bag (500 tokens)
    p_2x = Decimal("0.002")
    decision_2x = book.evaluate_lot_exit(lot, current_price=p_2x)
    assert isinstance(decision_2x, ExitDecision)
    assert decision_2x is not None
    assert decision_2x.should_exit is True
    assert decision_2x.exit_reason == TradeExitReason.TP_2X
    assert decision_2x.exit_stage == ExitStage.TP1
    assert decision_2x.tokens_to_sell == Decimal("500.0")

    # Apply 2x exit
    pnl, sold = book.apply_exit_decision(decision_2x, sell_price=p_2x)
    assert sold == Decimal("500.0")
    assert lot.tokens_held == Decimal("500.0")
    assert lot.exit_stage == ExitStage.TP1

    # 2. Price reaches 3x (+200%) -> Sell 25% of initial (250 tokens)
    p_3x = Decimal("0.003")
    decision_3x = book.evaluate_lot_exit(lot, current_price=p_3x)
    assert decision_3x is not None
    assert decision_3x.exit_reason == TradeExitReason.TP_3X
    assert decision_3x.tokens_to_sell == Decimal("250.0")

    pnl_3x, sold_3x = book.apply_exit_decision(decision_3x, sell_price=p_3x)
    assert sold_3x == Decimal("250.0")
    assert lot.tokens_held == Decimal("250.0")
    assert lot.exit_stage == ExitStage.TP2

    # 3. Price reaches 10x (+900%) -> Dump 100% of remaining tokens
    p_10x = Decimal("0.010")
    decision_10x = book.evaluate_lot_exit(lot, current_price=p_10x)
    assert decision_10x is not None
    assert decision_10x.exit_reason == TradeExitReason.TP_10X
    assert decision_10x.tokens_to_sell == Decimal("250.0")

    pnl_10x, sold_10x = book.apply_exit_decision(decision_10x, sell_price=p_10x)
    assert sold_10x == Decimal("250.0")
    assert len(book.get_open_lots()) == 0  # Fully closed


def test_dynamic_exits_trailing_stop_and_hard_stop():
    """Verify Trailing Stop locks at +20% once up +50%, and Hard Stop-Loss (-15%)."""
    book = PositionBook()
    fill = PaperFill(
        token_address="0xtrailtoken",
        chain=ChainIdentifier.BASE_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("1000.0"),
        effective_price=Decimal("1.0"),  # Entry price = 1.0
    )
    lot = book.open_lot(fill, signal_id="sig_trail")

    # 1. Price drops to 0.84 (-16% from entry) before +50% -> Hard Stop triggered
    dec_sl = book.evaluate_lot_exit(lot, current_price=Decimal("0.84"))
    assert dec_sl is not None
    assert dec_sl.exit_reason == TradeExitReason.SL_INITIAL
    assert dec_sl.exit_stage == ExitStage.SL
    assert dec_sl.tokens_to_sell == Decimal("1000.0")

    # Reset lot
    lot.tokens_held = Decimal("1000.0")
    lot.exit_stage = ExitStage.NONE

    # 2. Price climbs to +50% (1.50) -> Trailing stop locked at +20% (1.20)
    book.evaluate_lot_exit(lot, current_price=Decimal("1.50"))
    assert lot.trailing_stop_active is True
    assert lot.trailing_stop_price == Decimal("1.20")

    # Price retraces to 1.15 (< 1.20) -> Trailing Stop Triggered
    dec_trail = book.evaluate_lot_exit(lot, current_price=Decimal("1.15"))
    assert dec_trail is not None
    assert dec_trail.exit_reason == TradeExitReason.SL_TRAILING
    assert dec_trail.exit_stage == ExitStage.TRAILING_SL
    assert dec_trail.tokens_to_sell == Decimal("1000.0")


def test_dynamic_exits_emergency_liquidity_drain():
    """Verify Emergency Liquidity Drain dump if pool reserves decrease by > 30%."""
    book = PositionBook()
    fill = PaperFill(
        token_address="0xdraintoken",
        chain=ChainIdentifier.SOLANA_MAINNET,
        side=OrderSide.BUY,
        simulated_native_spent=Decimal("1.0"),
        tokens_acquired=Decimal("5000.0"),
        effective_price=Decimal("0.0002"),
    )
    # Open lot with 100 SOL initial reserve
    lot = book.open_lot(fill, signal_id="sig_drain", initial_pool_reserve_native=Decimal("100.0"))

    # Pool drops from 100 SOL to 65 SOL (35% drop in single block)
    dec_drain = book.evaluate_lot_exit(
        lot=lot,
        current_price=Decimal("0.0002"),
        current_pool_reserve_native=Decimal("65.0"),
    )
    assert dec_drain is not None
    assert dec_drain.should_exit is True
    assert dec_drain.exit_reason == TradeExitReason.EMERGENCY_DRAIN
    assert dec_drain.exit_stage == ExitStage.RUGPULL
    assert dec_drain.tokens_to_sell == Decimal("5000.0")


# ==============================================================================
# 6. Self-Learning Feedback Loop & RPC Failover Tests
# ==============================================================================

def test_adaptive_feedback_social_weight_decay():
    """Verify social signal weight decays if X Sentiment win-rate < 40% over last 10 trades."""
    async def _run():
        feedback = AdaptiveFeedbackEngine(initial_social_weight=1.0)

        # Record 10 X Sentiment trades with only 2 wins (20% win rate)
        for i in range(10):
            is_win = i < 2
            reflection = TradeReflection(
                trade_id=f"trade_{i}",
                token_address=f"0xtoken_{i}",
                chain=ChainIdentifier.BASE_MAINNET,
                signal_source=SignalSource.X_SENTIMENT,
                entry_price=Decimal("1.0"),
                exit_price=Decimal("1.5") if is_win else Decimal("0.8"),
                realized_pnl_usd=Decimal("50.0") if is_win else Decimal("-20.0"),
                is_win=is_win,
            )
            await feedback.record_closed_trade(reflection)

        # Social weight decayed: 1.0 * 0.8 = 0.8
        assert feedback.get_social_weight() < 1.0
        assert feedback.get_social_weight() == pytest.approx(0.80, rel=1e-2)

    asyncio.run(_run())


def test_adaptive_feedback_wallet_pruning(tmp_path):
    """Verify automated wallet demotion on 3 consecutive losing trades."""
    async def _run():
        db_file = tmp_path / "whitelist_prune.db"
        db = WhitelistDB(str(db_file))
        await db.connect()
        try:
            profiler = SmartMoneyProfiler(whitelist_db=db)

            wallet = "0xwalletconsecutivelosses00000000000000"
            await db.insert_wallet(
                wallet_address=wallet,
                chain=ChainIdentifier.BASE_MAINNET,
                classification=WalletClassification.SMART_MONEY,
                status=WhitelistStatus.ACTIVE,
                total_trades=30,
                win_rate=0.70,
                profit_factor=2.5,
                avg_holding_time_seconds=300,
                max_drawdown_pct=15.0,
            )

            feedback = AdaptiveFeedbackEngine(profiler=profiler)

            # Suffer 3 consecutive losing trades
            for i in range(3):
                reflection = TradeReflection(
                    trade_id=f"loss_{i}",
                    token_address=f"0xtoken_{i}",
                    chain=ChainIdentifier.BASE_MAINNET,
                    signal_source=SignalSource.WHALE_WALLET,
                    wallet_address=wallet,
                    realized_pnl_usd=Decimal("-50.0"),
                    is_win=False,
                )
                await feedback.record_closed_trade(reflection)

            # Check that wallet was demoted in DB
            updated = await db.get_wallet(wallet)
            assert updated is not None
            assert updated["status"] == WhitelistStatus.SUSPENDED.value
        finally:
            await db.close()

    asyncio.run(_run())


def test_rpc_health_failover_latency_and_lag():
    """Verify RPC Health Monitor benchmarking and automatic failover on latency > 400ms or block lag > 2."""
    async def _run():
        primary_rpc = "https://rpc1.primary.org"
        fallback_rpc = "https://rpc2.fallback.org"

        endpoint = RPCEndpoint(url=primary_rpc, chain=ChainIdentifier.BASE_MAINNET)
        assert endpoint.url == primary_rpc

        monitor = RPCHealthMonitor(
            endpoints={ChainIdentifier.BASE_MAINNET: [primary_rpc, fallback_rpc]},
            latency_threshold_ms=400.0,
            max_block_lag=2,
        )
        assert monitor.get_active_rpc(ChainIdentifier.BASE_MAINNET) == primary_rpc

        # Mock benchmarks: primary has latency 450ms (> 400ms) and block lag 3; fallback has 50ms and latest block
        async def mock_benchmark(endpoint, session=None, timeout_seconds=2.0):
            if endpoint.url == primary_rpc:
                endpoint.latency_ms = 450.0
                endpoint.latest_block = 100
                endpoint.is_healthy = True
            else:
                endpoint.latency_ms = 45.0
                endpoint.latest_block = 105
                endpoint.is_healthy = True
            return endpoint

        monitor.benchmark_endpoint = mock_benchmark

        new_active = await monitor.check_and_failover(ChainIdentifier.BASE_MAINNET)
        assert new_active == fallback_rpc
        assert monitor.get_active_rpc(ChainIdentifier.BASE_MAINNET) == fallback_rpc

    asyncio.run(_run())


def test_security_gatekeeper_screen_token_optional_pool_address():
    """Verify SecurityGatekeeper.screen_token can be called with or without pool_address."""
    from alpha_engine.rate_limiter.registry import RateLimiterRegistry

    async def _run():
        session = AsyncMock()
        limiter = RateLimiterRegistry.default()
        gk = SecurityGatekeeper(
            session=session,
            limiter=limiter,
            evm_rpc_url="https://base-mainnet.g.alchemy.com/v2/demo",
            evm_router_address="0x" + "1" * 40,
            weth_address="0x" + "2" * 40,
            enable_tier2=False,  # Disable tier 2 so we test screen_token signature directly
        )

        mock_report = SecurityReport(
            token_address="0x" + "3" * 40,
            chain=ChainIdentifier.BASE_MAINNET,
            tier=SecurityTier.CLEAN,
            is_honeypot=False,
            buy_tax_bps=100,
            sell_tax_bps=100,
            lp_burned_ratio=1.0,
            top10_concentration=0.1,
            mint_authority_disabled=True,
            verified_source_code=True,
        )
        gk._run_tier1 = AsyncMock(return_value=mock_report)

        # 1. Call without pool_address (default empty string)
        rep1 = await gk.screen_token(
            token_address="0x" + "3" * 40,
            chain=ChainIdentifier.BASE_MAINNET,
        )
        assert rep1.passes_hard_gates is True

        # 2. Call with pool_address
        rep2 = await gk.screen_token(
            token_address="0x" + "3" * 40,
            chain=ChainIdentifier.BASE_MAINNET,
            pool_address="0x" + "4" * 40,
        )
        assert rep2.passes_hard_gates is True
        assert gk._run_tier1.await_count == 2

        # 3. Test RawSignalEvent carrying pool_address
        raw_event = RawSignalEvent(
            chain=ChainIdentifier.BASE_MAINNET,
            token_address="0x" + "3" * 40,
            pool_address="0x" + "4" * 40,
            source=SignalSource.PAIR_CREATED,
        )
        assert raw_event.pool_address == "0x" + "4" * 40

    asyncio.run(_run())


def test_dns_resolver_anti_sinkhole_patch():
    """Verify anti-sinkhole DNS resolver intercepts Cisco Umbrella IPs and resolves real edge IPs."""
    import socket
    from alpha_engine.dns_resolver import patch_dns_resolvers

    patch_dns_resolvers()

    # Querying helius-rpc.com through socket.getaddrinfo
    addrs = socket.getaddrinfo("mainnet.helius-rpc.com", 443)
    assert len(addrs) > 0
    resolved_ip = addrs[0][4][0]
    # Ensure it is NOT a Cisco Umbrella sinkhole IP (146.112.*)
    assert not resolved_ip.startswith("146.112.")
    assert resolved_ip in ("104.18.36.169", "172.64.151.87")


def test_trade_evaluation_pipeline_signal_strength_validation():
    """Verify SignalStrength integration into trade evaluation pipeline before order placement."""
    from alpha_engine.engine.runner import PaperTradingEngine
    from alpha_engine.engine.signals import SignalGenerator

    fallback_rpc = os.getenv("TEST_BASE_RPC_HTTP", "https://base.llamarpc.com")
    cfg = EngineConfig(base_rpc_http=fallback_rpc)
    assert cfg.base_rpc_http == fallback_rpc
    assert time.time() > 0

    engine = PaperTradingEngine(config=cfg)
    generator = SignalGenerator()

    pool_state = PoolState(
        pool_address="0x" + "4" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        token_reserve=Decimal("1000000"),
        native_reserve=Decimal("100"),
        fee_numerator=3,
        fee_denominator=1000,
        last_updated_block=12345,
        token_decimals=18,
        native_decimals=18,
    )
    swap_event = SwapEvent(
        timestamp_ns=time.time_ns(),
        block_number=12345,
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x" + "4" * 40,
        token_in="0x" + "2" * 40,
        token_out="0x" + "3" * 40,
        amount_in=Decimal("1.0"),
        amount_out=Decimal("1000.0"),
        sender="0x" + "5" * 40,
        tx_hash="0x" + "6" * 64,
        log_index=0,
    )
    security_report = SecurityReport(
        token_address="0x" + "3" * 40,
        chain=ChainIdentifier.BASE_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=100,
        sell_tax_bps=100,
    )

    weak_signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x" + "4" * 40,
        token_address="0x" + "3" * 40,
        suggested_side=OrderSide.BUY,
        trigger_swap=swap_event,
        pool_state=pool_state,
        security_report=security_report,
        strength=SignalStrength.WEAK,
        alpha_score=0.3,
    )

    strong_signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x" + "4" * 40,
        token_address="0x" + "3" * 40,
        suggested_side=OrderSide.BUY,
        trigger_swap=swap_event,
        pool_state=pool_state,
        security_report=security_report,
        strength=SignalStrength.STRONG,
        alpha_score=0.9,
    )

    sell_signal = SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=ChainIdentifier.BASE_MAINNET,
        pool_address="0x" + "4" * 40,
        token_address="0x" + "3" * 40,
        suggested_side=OrderSide.SELL,
        trigger_swap=swap_event,
        pool_state=pool_state,
        security_report=security_report,
        strength=SignalStrength.WEAK,
        alpha_score=0.2,
    )

    # 1. Weak signal fails when MODERATE or STRONG is required
    assert not generator.validate_signal_strength(weak_signal, min_strength=SignalStrength.MODERATE)
    assert not engine.validate_signal_strength(weak_signal, min_strength=SignalStrength.MODERATE)

    # 2. Strong signal passes MODERATE and STRONG thresholds
    assert generator.validate_signal_strength(strong_signal, min_strength=SignalStrength.MODERATE)
    assert engine.validate_signal_strength(strong_signal, min_strength=SignalStrength.MODERATE)
    assert generator.validate_signal_strength(strong_signal, min_strength=SignalStrength.STRONG)
    assert engine.validate_signal_strength(strong_signal, min_strength=SignalStrength.STRONG)

    # 3. Sell signals always pass for risk management / staged exits
    assert generator.validate_signal_strength(sell_signal, min_strength=SignalStrength.STRONG)
    assert engine.validate_signal_strength(sell_signal, min_strength=SignalStrength.STRONG)


def test_svm_system_program_filtering_and_rugcheck_hardening():
    """Verify system programs are rejected from Pump.fun log parsing and RugCheck null responses handled safely."""
    from alpha_engine.ingestion.decoders import _parse_pump_fun_logs
    from alpha_engine.security.constants import SOLANA_SYSTEM_PROGRAM_IDS
    from alpha_engine.security.rugcheck import _parse_rugcheck_report

    # 1. Reject logs from unrelated transactions that only invoke ComputeBudget / ATA / System Program
    unrelated_logs = [
        "Program ComputeBudget111111111111111111111111111111 invoke [1]",
        "Program log: SetComputeUnitLimit: 200000",
        "Program ComputeBudget111111111111111111111111111111 success",
        "Program ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL invoke [1]",
        "Program log: Create",
        "Program 11111111111111111111111111111111 invoke [2]",
        "Program log: CreateAccount",
        "Program 11111111111111111111111111111111 success",
        "Program ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL success",
    ]
    assert _parse_pump_fun_logs(unrelated_logs) is None

    # 2. Reject extracting system programs even if pump.fun is mentioned
    sys_logs = [
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
        "Program log: Instruction: Create",
        "Program ComputeBudget111111111111111111111111111111 invoke [2]",
        "Program ComputeBudget111111111111111111111111111111 success",
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
    ]
    assert _parse_pump_fun_logs(sys_logs) is None

    # 3. RugCheck null topHolders or null fields (e.g. Wrapped SOL) does not raise TypeError
    null_holders_raw = {
        "mintAuthority": None,
        "freezeAuthority": None,
        "markets": [],
        "topHolders": None,
        "risks": None,
        "tokenMeta": None,
    }
    rep = _parse_rugcheck_report("So11111111111111111111111111111111111111112", null_holders_raw)
    assert rep.token_address == "So11111111111111111111111111111111111111112"
    assert rep.top10_concentration == 0.0

    # 4. RugCheck invalid mint payload triggers TIER1_REJECTED
    rep_invalid = _parse_rugcheck_report("ComputeBudget111111111111111111111111111111", {"invalid_mint": True})
    assert rep_invalid.tier == SecurityTier.TIER1_REJECTED
    assert rep_invalid.passes_hard_gates is False
    assert "ComputeBudget111111111111111111111111111111" in SOLANA_SYSTEM_PROGRAM_IDS



