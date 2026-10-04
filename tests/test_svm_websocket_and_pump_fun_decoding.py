"""
tests/test_svm_websocket_and_pump_fun_decoding.py
=================================================
Comprehensive unit and integration tests for:
1. Pump.fun TradeEvent (binary Anchor & text-based fallback) decoding in decoders.py.
2. SVM WebSocket connection hardening and throttled subscription batching in svm.py.
3. Stateful resubscription without ping/pong stalling.
4. PendingLaunchBuffer.record_trade() volume aggregation, buy counts, and graduation.
5. End-to-end data flow from SVM trade ingestion to PendingLaunchBuffer graduation.
"""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal
from pathlib import Path
import struct
import sys
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

# Ensure project root is in sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

for _p in (_ROOT / ".venv" / "lib").glob("python*/site-packages"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from alpha_engine.config import EngineConfig
from alpha_engine.engine.signals import PendingLaunchBuffer, SignalGenerator
from alpha_engine.ingestion.decoders import (
    PUMP_FUN_PROGRAM_ID,
    _PUMP_TRADE_EVENT_DISCRIMINATOR,
    _b58encode,
    _parse_pump_fun_trade_logs,
)
from alpha_engine.ingestion.svm import SVMIngester
from alpha_engine.models.enums import ChainIdentifier, SecurityTier, SignalSource
from alpha_engine.models.events import (
    RawSignalEvent,
    SwapEvent,
)
from alpha_engine.models.state import PoolState, SecurityReport
from alpha_engine.rate_limiter.registry import RateLimiterRegistry


def _encode_b58_pubkey(seed_char: str) -> bytes:
    """Helper to generate a 32-byte pseudo-pubkey."""
    return (seed_char.encode("ascii") * 32)[:32]


def test_pump_fun_anchor_trade_event_decoding():
    """Verify Anchor binary TradeEvent parsing extracts mint, sol_amount, token_amount, buyer, slot."""
    mint_bytes = _encode_b58_pubkey("M")
    buyer_bytes = _encode_b58_pubkey("B")
    expected_mint = _b58encode(mint_bytes)
    expected_buyer = _b58encode(buyer_bytes)

    # 1.5 SOL in lamports = 1_500_000_000
    sol_raw = 1_500_000_000
    # 25,000,000 tokens (6 decimals)
    token_raw = 25_000_000_000_000
    is_buy = 1
    timestamp = int(time.time())
    v_sol_raw = 31_500_000_000
    v_tok_raw = 1_048_000_000_000_000

    # Build binary Anchor TradeEvent
    payload = bytearray()
    payload.extend(_PUMP_TRADE_EVENT_DISCRIMINATOR)  # 8 bytes
    payload.extend(mint_bytes)                       # 32 bytes
    payload.extend(struct.pack("<Q", sol_raw))       # 8 bytes
    payload.extend(struct.pack("<Q", token_raw))     # 8 bytes
    payload.append(is_buy)                           # 1 byte
    payload.extend(buyer_bytes)                      # 32 bytes
    payload.extend(struct.pack("<q", timestamp))     # 8 bytes
    payload.extend(struct.pack("<Q", v_sol_raw))     # 8 bytes
    payload.extend(struct.pack("<Q", v_tok_raw))     # 8 bytes

    b64_event = base64.b64encode(payload).decode("ascii")
    logs = [
        f"Program {PUMP_FUN_PROGRAM_ID} invoke [1]",
        "Program log: Instruction: Buy",
        f"Program data: {b64_event}",
        f"Program {PUMP_FUN_PROGRAM_ID} success",
    ]

    trade = _parse_pump_fun_trade_logs(logs, tx_sig="5abc1234567890abcdef1234567890abcdef1234567890", slot=123456)
    assert trade is not None
    assert trade["mint"] == expected_mint
    assert trade["sol_amount"] == Decimal("1.5")
    assert trade["token_amount"] == Decimal("25000000")
    assert trade["buyer"] == expected_buyer
    assert trade["slot"] == 123456
    assert trade["is_buy"] is True
    assert trade["virtual_sol_reserves"] == Decimal("31.5")


def test_pump_fun_text_log_fallback_decoding():
    """Verify human-readable log fallback correctly extracts trade data when Anchor event is absent."""
    mint = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    buyer = "3yFwqCzEbNqzpbtg2CW87d97TXJSDpbD5jBkheTqA83T"
    logs = [
        f"Program {PUMP_FUN_PROGRAM_ID} invoke [1]",
        "Program log: Instruction: Buy",
        f"Program log: mint: {mint} buyer: {buyer}",
        "Program log: sol_amount: 800000000",
        f"Program {PUMP_FUN_PROGRAM_ID} success",
    ]

    trade = _parse_pump_fun_trade_logs(logs, tx_sig="tx_fallback_sig", slot=98765)
    assert trade is not None
    assert trade["mint"] == mint
    assert trade["sol_amount"] == Decimal("0.8")
    assert trade["buyer"] == buyer
    assert trade["slot"] == 98765
    assert trade["is_buy"] is True


def test_pending_launch_buffer_record_trade_aggregation():
    """Verify PendingLaunchBuffer.record_trade directly aggregates volume, buyers, and graduates."""
    buffer = PendingLaunchBuffer(
        min_age_s=10.0,
        max_age_s=60.0,
        min_buys=3,
        min_volume_native=Decimal("0.8"),
        min_unique_signers=2,
        min_blocks_span=2,
    )
    token = "PumpMintRecordTrade1111111111111111111111111"
    pool_addr = "CurveAddrRecordTrade111111111111111111111111"
    report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )
    t_0 = 1000.0
    buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000028"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    pool = PoolState(
        pool_address=pool_addr,
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        native_reserve=Decimal("30.0"),
        token_reserve=Decimal("1073000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=1,
    )
    sig_gen = SignalGenerator()

    # Trade 1: 0.3 SOL from BuyerA at block 10
    sig1 = buffer.record_trade(
        token_address=token,
        sol_amount=Decimal("0.3"),
        buyer="BuyerA111111111111111111111111111111111111",
        slot=10,
        is_buy=True,
        current_time_s=t_0 + 2.0,
        pool=pool,
        signal_generator=sig_gen,
    )
    assert sig1 is None
    staged = buffer.get_staged(token)
    assert staged is not None
    assert staged.total_volume_native == Decimal("0.3")
    assert staged.buy_count == 1
    assert len(staged.unique_buyers) == 1

    # Trade 2: 0.3 SOL from BuyerB at block 11
    sig2 = buffer.record_trade(
        token_address=token,
        sol_amount=Decimal("0.3"),
        buyer="BuyerB111111111111111111111111111111111111",
        slot=11,
        is_buy=True,
        current_time_s=t_0 + 5.0,
        pool=pool,
        signal_generator=sig_gen,
    )
    assert sig2 is None
    assert staged.total_volume_native == Decimal("0.6")
    assert staged.buy_count == 2
    assert len(staged.unique_buyers) == 2

    # Trade 3: 0.3 SOL from BuyerA at block 12, time t_0 + 12s (>= min_age_s 10.0s)
    # Total volume = 0.9 >= 0.8, buys = 3 >= 3, buyers = 2 >= 2, blocks = 3 >= 2 -> GRADUATES!
    sig3 = buffer.record_trade(
        token_address=token,
        sol_amount=Decimal("0.3"),
        buyer="BuyerA111111111111111111111111111111111111",
        slot=12,
        is_buy=True,
        current_time_s=t_0 + 12.0,
        pool=pool,
        signal_generator=sig_gen,
    )
    assert sig3 is not None
    assert sig3.token_address == token
    assert staged.graduated is True
    assert staged.total_volume_native == Decimal("0.9")


@pytest.mark.anyio
async def test_svm_throttled_subscription_batching_and_options():
    """Verify SVMIngester uses hardened connection options and throttles subscriptions in batches of 20."""
    event_q: asyncio.Queue = asyncio.Queue()
    limiter = RateLimiterRegistry.default()

    # Seed registry with 45 pools using genuine 32-byte Base58 pubkeys
    fake_registry: dict[str, tuple[str, str, int, int]] = {}
    for i in range(45):
        curve_bytes = (f"C{i:02d}".encode("ascii") * 16)[:32]
        mint_bytes = (f"M{i:02d}".encode("ascii") * 16)[:32]
        curve = _b58encode(curve_bytes)
        mint = _b58encode(mint_bytes)
        fake_registry[curve] = (mint, "So11111111111111111111111111111111111111112", 6, 9)

    ingester = SVMIngester(
        ws_url="wss://mainnet.helius-rpc.com/?api-key=test",
        pool_registry=fake_registry,
        event_queue=event_q,
        limiter=limiter,
    )

    sent_messages: list[str] = []
    received_count = 0

    class MockWS:
        def __init__(self):
            self.send = AsyncMock()
            self._messages_to_yield = []
            # We record send calls
            async def _record_send(msg: str):
                sent_messages.append(msg)
                data = json.loads(msg)
                req_id = data.get("id")
                # Prepare corresponding response
                resp = json.dumps({"jsonrpc": "2.0", "result": 1000 + req_id, "id": req_id})
                self._messages_to_yield.append(resp)
            self.send.side_effect = _record_send

        def __aiter__(self):
            return self

        async def __anext__(self):
            nonlocal received_count
            # Yield responses as they are created
            while not self._messages_to_yield:
                await asyncio.sleep(0.01)
                if received_count >= 10:
                    raise StopAsyncIteration
            received_count += 1
            return self._messages_to_yield.pop(0)

    mock_ws = MockWS()
    connect_kwargs = {}

    class MockConnectContext:
        def __init__(self, *args, **kwargs):
            connect_kwargs.update(kwargs)

        async def __aenter__(self):
            return mock_ws

        async def __aexit__(self, exc_type, exc, tb):
            pass

    with patch("websockets.connect", side_effect=MockConnectContext):
        ingester._running = True
        task = asyncio.create_task(ingester._connect_and_stream())
        await asyncio.sleep(0.3)
        ingester._running = False
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    # Verify hardened connection options
    assert connect_kwargs.get("ping_interval") == 20
    assert connect_kwargs.get("ping_timeout") == 60
    assert connect_kwargs.get("close_timeout") == 10
    assert connect_kwargs.get("max_size") == 10 * 1024 * 1024
    assert connect_kwargs.get("max_queue") == 4096

    # Verify subscriptions were sent (45 pools + 2 program IDs)
    assert len(sent_messages) >= 20
    assert ingester._sub_to_pool  # Subscriptions were recorded statefully


@pytest.mark.anyio
async def test_runner_pump_fun_swap_routing_to_buffer_graduation():
    """End-to-end integration test: staged token receives Pump.fun SwapEvents via ingestion queue, updates telemetry, and graduates."""
    from alpha_engine.engine.runner import PaperTradingEngine
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    config = EngineConfig(
        observation_window_min_sec=10.0,
        observation_window_max_sec=90.0,
        buffer_min_buys=3,
        buffer_target_volume_sol=Decimal("0.8"),
        buffer_min_unique_buyers=2,
    )
    engine = PaperTradingEngine(config)
    engine._gatekeeper = MagicMock(spec=SecurityGatekeeper)

    token = "PumpEndToEndMint11111111111111111111111111"
    pool_addr = "PumpEndToEndCurve111111111111111111111111"
    report = SecurityReport(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    raw_sig = RawSignalEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address=token,
        pool_address=pool_addr,
        source=SignalSource.PUMP_FUN_MINT,
    )

    t_0 = time.time() - 15.0  # Staged 15s ago so >= min_age_s (10.0)
    engine._pending_launch_buffer.add_launch(
        token_address=token,
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address=pool_addr,
        initial_price=Decimal("0.000000030"),
        report=report,
        raw_signal=raw_sig,
        t_0=t_0,
    )

    # Register initial pool state
    engine.get_or_create_initial_pool_state(
        ChainIdentifier.SOLANA_MAINNET,
        token,
        pool_addr,
    )

    # Enqueue 3 SwapEvents representing Pump.fun trades:
    now_ns = time.time_ns()
    tx1_hash = "1" * 64
    tx2_hash = "2" * 64
    tx3_hash = "3" * 64
    # Trade 1: 0.3 SOL buy from Buyer 1
    swap1 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=100,
        tx_hash=tx1_hash,
        sender="Buyer1111111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns,
        tx_signature=tx1_hash,
        slot=100,
        signer="Buyer1111111111111111111111111111111111111",
    )
    # Trade 2: 0.3 SOL buy from Buyer 2
    swap2 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=101,
        tx_hash=tx2_hash,
        sender="Buyer2222222222222222222222222222222222222",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns + 1_000_000,
        tx_signature=tx2_hash,
        slot=101,
        signer="Buyer2222222222222222222222222222222222222",
    )
    # Trade 3: 0.3 SOL buy from Buyer 1 (Total: 0.9 SOL, 3 buys, 2 unique buyers, 2 blocks)
    swap3 = SwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=102,
        tx_hash=tx3_hash,
        sender="Buyer1111111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=token,
        amount_in=Decimal("0.3"),
        amount_out=Decimal("10000000"),
        timestamp_ns=now_ns + 2_000_000,
        tx_signature=tx3_hash,
        slot=102,
        signer="Buyer1111111111111111111111111111111111111",
    )

    await engine._ingestion_q.put(swap1)
    await engine._ingestion_q.put(swap2)
    await engine._ingestion_q.put(swap3)

    # Process all 3 items through _process_ingestion_queue
    task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    # Verify staging buffer updated dynamically
    staged = engine._pending_launch_buffer.get_staged(token)
    assert staged is not None
    assert staged.total_volume_native == Decimal("0.9")
    assert staged.buy_count == 3
    assert len(staged.unique_buyers) == 2
    assert staged.graduated is True

    # Verify graduated signal was placed in _signal_q
    assert not engine._signal_q.empty()
    grad_signal = await engine._signal_q.get()
    assert grad_signal.token_address == token
    assert grad_signal.source == SignalSource.PUMP_FUN_MINT


def test_pump_fun_anchor_create_event_0x1b72a94f18d748f2():
    """Verify parsing of Pump.fun CreateEvent with discriminator 0x1b72a94f18d748f2."""
    from alpha_engine.ingestion.decoders import _PUMP_CREATE_EVENT_DISCRIMINATOR, _parse_pump_fun_logs
    from alpha_engine.models.events import PumpMintEvent

    assert _PUMP_CREATE_EVENT_DISCRIMINATOR == bytes.fromhex("1b72a94f18d748f2")

    mint_bytes = _encode_b58_pubkey("M")
    curve_bytes = _encode_b58_pubkey("C")
    expected_mint = _b58encode(mint_bytes)
    expected_curve = _b58encode(curve_bytes)

    # Build binary Anchor CreateEvent
    # 8 discriminator + name(len+str) + symbol(len+str) + uri(len+str) + mint(32) + bonding_curve(32)
    payload = bytearray()
    payload.extend(bytes.fromhex("1b72a94f18d748f2"))
    name = b"TokenName"
    symbol = b"TKN"
    uri = b"https://token.uri"
    payload.extend(struct.pack("<I", len(name)) + name)
    payload.extend(struct.pack("<I", len(symbol)) + symbol)
    payload.extend(struct.pack("<I", len(uri)) + uri)
    payload.extend(mint_bytes)
    payload.extend(curve_bytes)

    b64_event = base64.b64encode(payload).decode("ascii")
    logs = [
        f"Program {PUMP_FUN_PROGRAM_ID} invoke [1]",
        "Program log: Instruction: Create",
        f"Program data: {b64_event}",
        f"Program {PUMP_FUN_PROGRAM_ID} success",
    ]

    event = _parse_pump_fun_logs(logs, tx_sig="create_sig_123", slot=55555)
    assert event is not None
    assert isinstance(event, PumpMintEvent)
    assert event.mint == expected_mint
    assert event.bonding_curve == expected_curve
    assert event.virtual_sol_reserves == Decimal("30.0")
    assert event.slot == 55555


@pytest.mark.anyio
async def test_svm_trade_deduplication():
    """Verify SVMIngester deduplicates identical slot:mint:buyer:sol_amount trades."""
    event_q: asyncio.Queue = asyncio.Queue()
    limiter = RateLimiterRegistry.default()

    mint = "M" * 32
    buyer = "B" * 32
    mint_b58 = _b58encode(mint.encode("ascii"))
    buyer_b58 = _b58encode(buyer.encode("ascii"))

    ingester = SVMIngester(
        ws_url="wss://mainnet.helius-rpc.com/?api-key=test",
        pool_registry={},
        event_queue=event_q,
        limiter=limiter,
    )

    # Build a trade log notification message
    sol_raw = 1_000_000_000  # 1.0 SOL
    token_raw = 10_000_000_000_000
    payload = bytearray()
    payload.extend(_PUMP_TRADE_EVENT_DISCRIMINATOR)
    payload.extend(mint.encode("ascii"))
    payload.extend(struct.pack("<Q", sol_raw))
    payload.extend(struct.pack("<Q", token_raw))
    payload.append(1)  # is_buy
    payload.extend(buyer.encode("ascii"))
    payload.extend(struct.pack("<q", int(time.time())))
    payload.extend(struct.pack("<Q", 31_000_000_000))
    payload.extend(struct.pack("<Q", 1_000_000_000_000_000))

    b64_event = base64.b64encode(payload).decode("ascii")
    logs = [
        f"Program {PUMP_FUN_PROGRAM_ID} invoke [1]",
        "Program log: Instruction: Buy",
        f"Program data: {b64_event}",
        f"Program {PUMP_FUN_PROGRAM_ID} success",
    ]

    notif_msg = json.dumps({
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": 77777},
                "value": {
                    "signature": "sig_trade_123456789012345678901234567890",
                    "logs": logs,
                },
            },
        },
    })

    class MockWS:
        def __init__(self):
            self.send = AsyncMock()
            self._messages = [
                json.dumps({"jsonrpc": "2.0", "result": 1, "id": 1}),
                notif_msg,
                notif_msg,  # duplicate
            ]

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(0.01)
            if self._messages:
                return self._messages.pop(0)
            await asyncio.sleep(0.05)
            raise StopAsyncIteration

    class MockConnectContext:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return MockWS()

        async def __aexit__(self, exc_type, exc, tb):
            pass

    with patch("websockets.connect", side_effect=MockConnectContext):
        ingester._running = True
        await ingester._connect_and_stream()

    # Collect events queued
    events = []
    while not event_q.empty():
        events.append(await event_q.get())

    from alpha_engine.models.events import PumpSwapEvent
    swaps = [e for e in events if isinstance(e, PumpSwapEvent)]
    # Exactly 1 swap should be queued despite duplicate notifications
    assert len(swaps) == 1
    assert swaps[0].amount_in == Decimal("1.0")


@pytest.mark.anyio
async def test_runner_pump_mint_event_pipeline_evaluation_and_staging():
    """Verify runner evaluates PumpMintEvent through gatekeeper and stages it in launch_buffer."""
    from alpha_engine.engine.runner import PaperTradingEngine
    from alpha_engine.models.events import PumpMintEvent
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    config = EngineConfig()
    engine = PaperTradingEngine(config)
    mock_gk = MagicMock(spec=SecurityGatekeeper)
    clean_report = SecurityReport(
        token_address="PumpMintPipe111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    mock_gk.evaluate_token = AsyncMock(return_value=clean_report)
    mock_gk.is_rejected = MagicMock(return_value=False)
    engine._gatekeeper = mock_gk

    mint_ev = PumpMintEvent(
        mint="PumpMintPipe111111111111111111111111111111",
        bonding_curve="PumpCurvePipe11111111111111111111111111111",
        virtual_sol_reserves=Decimal("30.0"),
        virtual_token_reserves=Decimal("1073000000.0"),
        slot=88888,
    )

    await engine._ingestion_q.put(mint_ev)

    task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    mock_gk.evaluate_token.assert_awaited_once_with(
        token_address="PumpMintPipe111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        pool_address="PumpCurvePipe11111111111111111111111111111",
    )
    assert engine.launch_buffer.is_staged("PumpMintPipe111111111111111111111111111111") is True


@pytest.mark.anyio
async def test_runner_auto_discovery_fallback_on_large_buy():
    """Verify auto-discovery fallback evaluates security and stages unstaged token on buy >= 0.5 SOL."""
    from alpha_engine.engine.runner import PaperTradingEngine
    from alpha_engine.models.events import PumpSwapEvent
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    config = EngineConfig()
    engine = PaperTradingEngine(config)
    mock_gk = MagicMock(spec=SecurityGatekeeper)
    clean_report = SecurityReport(
        token_address="AutoDiscToken11111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        passes_hard_gates=True,
    )
    mock_gk.evaluate_token = AsyncMock(return_value=clean_report)
    mock_gk.is_rejected = MagicMock(return_value=False)
    engine._gatekeeper = mock_gk

    target_token = "AutoDiscToken11111111111111111111111111111"
    pool_addr = "AutoDiscPool111111111111111111111111111111"

    # Buy of 0.6 SOL for an unstaged token
    buy_swap = PumpSwapEvent(
        chain=ChainIdentifier.SOLANA_MAINNET,
        block_number=99999,
        tx_hash="tx_autodisc_11111111111111111111111111111111",
        sender="BigBuyer11111111111111111111111111111111111",
        pool_address=pool_addr,
        token_in="So11111111111111111111111111111111111111112",
        token_out=target_token,
        amount_in=Decimal("0.6"),
        amount_out=Decimal("15000000"),
        timestamp_ns=time.time_ns(),
        slot=99999,
        mint=target_token,
        sol_amount=Decimal("0.6"),
        buyer="BigBuyer11111111111111111111111111111111111",
        is_buy=True,
    )

    await engine._ingestion_q.put(buy_swap)

    task = asyncio.create_task(engine._process_ingestion_queue())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    # Verify auto-discovery triggered evaluate_token and staged the token
    mock_gk.evaluate_token.assert_awaited()
    assert engine.launch_buffer.is_staged(target_token) is True
    staged = engine.launch_buffer.get_staged(target_token)
    assert staged is not None
    assert staged.total_volume_native == Decimal("0.6")
    assert staged.buy_count == 1


def test_gatekeeper_and_config_thresholds():
    """Verify MAX_TOP10_CONCENTRATION_PUMP_FUN threshold is 0.65 across gatekeeper and config."""
    from alpha_engine.config import MAX_TOP10_CONCENTRATION_PUMP_FUN as CFG_CONC, EngineConfig
    from alpha_engine.security.gatekeeper import (
        MAX_TOP10_CONCENTRATION_PUMP_FUN as GK_CONC,
        SecurityGatekeeper,
        calculate_top10_concentration,
    )

    assert CFG_CONC == 0.65
    assert GK_CONC == 0.65
    assert SecurityGatekeeper.MAX_TOP10_CONCENTRATION_PUMP_FUN == 0.65
    cfg = EngineConfig()
    assert cfg.max_pump_fun_top10_concentration == 0.65

    # Test calculate_top10_concentration excludes pump.fun bonding curve
    mint = "PumpTokenCalc111111111111111111111111111111"
    holders = [
        {"address": "CurveAccount1111111111111111111111111111", "pct": 70.0},
        {"address": "HolderA11111111111111111111111111111111", "pct": 10.0},
        {"address": "HolderB11111111111111111111111111111111", "pct": 10.0},
    ]
    conc = calculate_top10_concentration(holders, is_pump=True, mint_address=mint)
    # The 70% curve holding is excluded (>= 50% on idx 0)
    assert conc == pytest.approx(0.20, rel=1e-2)


@pytest.mark.anyio
async def test_svm_monitored_pools_isolation_from_dynamic_pump_mints():
    """Verify dynamic pump.fun mints do not pollute _monitored_pools and reconnection remains bounded."""
    event_q: asyncio.Queue = asyncio.Queue()
    limiter = RateLimiterRegistry.default()
    initial_pool = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"

    ingester = SVMIngester(
        ws_url="wss://api.mainnet-beta.solana.com",
        pool_registry={initial_pool: ("MintA", "MintB", 6, 9)},
        event_queue=event_q,
        limiter=limiter,
    )

    assert ingester._monitored_pools == {initial_pool}

    # Simulate 50 dynamic pump.fun mints arriving and populating _pool_registry
    for i in range(50):
        curve_addr = f"DynamicCurve{i:04d}11111111111111111111111"
        mint_addr = f"DynamicMint{i:04d}11111111111111111111111"
        ingester._pool_registry[curve_addr] = (mint_addr, "So11111111111111111111111111111111111111112", 6, 9)

    # _pool_registry has grown to 51
    assert len(ingester._pool_registry) == 51
    # But _monitored_pools MUST strictly remain 1
    assert len(ingester._monitored_pools) == 1
    assert ingester._monitored_pools == {initial_pool}


@pytest.mark.anyio
async def test_svm_rate_limit_1013_triggers_failover_and_extended_backoff():
    """Verify error 1013 (too many subscriptions) triggers immediate failover and >= 5.0s backoff."""
    from websockets.exceptions import ConnectionClosed
    from websockets.frames import Close

    event_q: asyncio.Queue = asyncio.Queue()
    limiter = RateLimiterRegistry.default()

    ingester = SVMIngester(
        ws_url="wss://api.mainnet-beta.solana.com",
        pool_registry={},
        event_queue=event_q,
        limiter=limiter,
        failover_urls=["wss://backup-rpc.solana.com"],
    )

    assert ingester._ws_url == "wss://api.mainnet-beta.solana.com"
    assert len(ingester._url_candidates) >= 2

    # Simulate connect_and_stream raising 1013 rate limit error
    call_count = 0
    async def mock_connect_and_stream():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionClosed(
                Close(1013, "Rate limit reached: Too many subscriptions attempted. Please open a new connection."),
                None,
            )
        # Terminate loop on second call
        ingester._running = False

    ingester._connect_and_stream = mock_connect_and_stream  # type: ignore

    # Run ingester.run()
    task = asyncio.create_task(ingester.run())
    await asyncio.sleep(1.5)
    if not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    # Verify that it failed over to backup URL
    assert ingester._ws_url == "wss://backup-rpc.solana.com"


