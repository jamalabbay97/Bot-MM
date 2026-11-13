"""
tests/test_issue11a_wallet_history.py — Wallet History Data Layer & Trade Reconstruction
========================================================================================
Test-First suite for Issue 11 Part A:
1. Swap parsing for each supported venue fixture (Pump.fun, Raydium, Jupiter, Base Uniswap v2/v3)
2. FIFO matching with partial sells
3. Fees & historical-price conversion (using historical native prices, not current prices)
4. Transfers in/out treated explicitly and not counted as sells
5. Unrealized open positions excluded from win rate / PnL
6. SQLite cache hit avoids network
7. Rate limit and free-tier quota accounting respected
8. Provider error -> "unavailable" gracefully propagated to profiler ("insufficient data", not "not smart money")
9. End-to-end integration: profiler_agent receives non-empty real WalletTradeRecord objects
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any
import pytest

from alpha_engine.models.enums import ChainIdentifier, WalletClassification
from alpha_engine.models.profiler import (
    NormalizedSwap,
    SwapVenue,
    WalletTradeRecord,
)
from alpha_engine.profiler.history_provider import (
    ProviderQuotaExceededError,
    ProviderUnavailableError,
    WalletHistoryProvider,
)
from alpha_engine.profiler.price_oracle import HistoricalPriceOracle
from alpha_engine.profiler.reconstructor import TradeReconstructor
from alpha_engine.profiler.solana_history import HeliusSolanaHistoryProvider
from alpha_engine.profiler.base_history import AlchemyBaseHistoryProvider
from alpha_engine.profiler.history_db import WalletHistoryDatabase
from alpha_engine.profiler.profiler import SmartMoneyProfiler
from alpha_engine.ruflo.agents.profiler_agent import SmartMoneyProfilerAgent
from alpha_engine.rate_limiter.registry import RateLimiterRegistry

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "wallet_history"


def load_fixture(filename: str) -> Any:
    path = FIXTURES_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class MockPriceOracle(HistoricalPriceOracle):
    """Deterministic offline price oracle backed by test fixture."""

    def __init__(self, price_data: dict[str, dict[str, str]]) -> None:
        self.price_data = price_data

    async def get_historical_native_price(
        self, chain: ChainIdentifier, timestamp: int
    ) -> Decimal:
        chain_key = "solana" if chain == ChainIdentifier.SOLANA_MAINNET else "base"
        sub_map = self.price_data.get(chain_key, {})
        # Find exact or closest timestamp <= target
        sorted_ts = sorted(int(k) for k in sub_map.keys())
        chosen_ts = None
        for ts in sorted_ts:
            if ts <= timestamp:
                chosen_ts = ts
            else:
                break
        if chosen_ts is None and sorted_ts:
            chosen_ts = sorted_ts[0]
        if chosen_ts is not None:
            return Decimal(sub_map[str(chosen_ts)])
        # Fail-closed: fail if no price is available
        raise ValueError(f"No mock price for {chain} at {timestamp}")


# ---------------------------------------------------------------------------
# 1. Swap Parsing Tests for Supported Venues
# ---------------------------------------------------------------------------

def test_swap_parsing_solana_pump_fun_and_raydium():
    """Verify robust parsing of Pump.fun buy/sell and Raydium AMM swaps."""
    pump_data = load_fixture("solana_pump_fun_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    
    provider = HeliusSolanaHistoryProvider(api_key="test-key")
    swaps = provider.parse_enhanced_transactions(pump_data, wallet=wallet)
    
    assert len(swaps) == 2, f"Expected 2 swaps, got {len(swaps)}"
    
    buy_swap = swaps[0]
    assert isinstance(buy_swap, NormalizedSwap)
    assert buy_swap.venue == SwapVenue.PUMP_FUN.value
    assert buy_swap.is_buy is True
    assert buy_swap.token_address == "PumpToken11111111111111111111111111111111111pump"
    assert buy_swap.native_amount == Decimal("1.0")
    assert buy_swap.token_amount == Decimal("1000.0")
    assert buy_swap.fee_native == Decimal("0.000005")
    assert buy_swap.block_time == 1715000000

    sell_swap = swaps[1]
    assert isinstance(sell_swap, NormalizedSwap)
    assert sell_swap.venue == SwapVenue.PUMP_FUN.value
    assert sell_swap.is_buy is False
    assert sell_swap.token_address == "PumpToken11111111111111111111111111111111111pump"
    assert sell_swap.native_amount == Decimal("0.8")
    assert sell_swap.token_amount == Decimal("500.0")
    assert sell_swap.fee_native == Decimal("0.000005")
    assert sell_swap.block_time == 1715001200

    # Raydium parsing
    raydium_data = load_fixture("solana_raydium_swap.json")
    ray_swaps = provider.parse_enhanced_transactions(raydium_data, wallet=wallet)
    assert len(ray_swaps) == 1
    ray_swap = ray_swaps[0]
    assert isinstance(ray_swap, NormalizedSwap)
    assert ray_swap.venue == SwapVenue.RAYDIUM.value
    assert ray_swap.is_buy is False
    assert ray_swap.native_amount == Decimal("0.4")
    assert ray_swap.token_amount == Decimal("500.0")


def test_swap_parsing_solana_jupiter_multi_route():
    """Verify parsing of Jupiter multi-route transaction."""
    jup_data = load_fixture("solana_jupiter_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    
    provider = HeliusSolanaHistoryProvider(api_key="test-key")
    swaps = provider.parse_enhanced_transactions(jup_data, wallet=wallet)
    assert len(swaps) == 1
    j_swap = swaps[0]
    assert isinstance(j_swap, NormalizedSwap)
    assert j_swap.venue == SwapVenue.JUPITER.value
    assert j_swap.is_buy is True
    assert j_swap.native_amount == Decimal("2.0")
    assert j_swap.token_amount == Decimal("50.0")
    assert j_swap.token_address == "JupToken1111111111111111111111111111111111111"


def test_swap_parsing_base_uniswap_v2_and_v3():
    """Verify parsing of Base EVM swaps across Uniswap V2 and V3."""
    v2_data = load_fixture("base_uniswap_v2_swap.json")
    wallet = "0xaaaa111122223333444455556666777788889999"
    
    provider = AlchemyBaseHistoryProvider(api_key="test-key")
    v2_swaps = provider.parse_transaction_receipts(v2_data, wallet=wallet)
    assert len(v2_swaps) == 2
    
    buy_v2 = v2_swaps[0]
    assert isinstance(buy_v2, NormalizedSwap)
    assert buy_v2.venue == SwapVenue.UNISWAP_V2.value
    assert buy_v2.is_buy is True
    assert buy_v2.native_amount == Decimal("0.5")
    assert buy_v2.token_amount == Decimal("5000.0")
    assert buy_v2.fee_native == Decimal("0.00015")  # 150000 * 1e9 / 1e18 = 0.00015 ETH

    sell_v2 = v2_swaps[1]
    assert isinstance(sell_v2, NormalizedSwap)
    assert sell_v2.is_buy is False
    assert sell_v2.native_amount == Decimal("0.4")
    assert sell_v2.token_amount == Decimal("2500.0")

    v3_data = load_fixture("base_uniswap_v3_swap.json")
    v3_swaps = provider.parse_transaction_receipts(v3_data, wallet=wallet)
    assert len(v3_swaps) == 1
    buy_v3 = v3_swaps[0]
    assert isinstance(buy_v3, NormalizedSwap)
    assert buy_v3.venue == SwapVenue.UNISWAP_V3.value
    assert buy_v3.is_buy is True
    assert buy_v3.native_amount == Decimal("1.0")
    assert buy_v3.token_amount == Decimal("800.0")


# ---------------------------------------------------------------------------
# 2. TradeReconstructor: FIFO Lot Matching, Partial Sells, Fees, Historical Prices
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fifo_matching_with_partial_sells_and_fees():
    """
    Test FIFO lot matching with partial sells:
    1. Buy 1,000 tokens for 1.0 SOL at T=1715000000 (fee 0.000005 SOL, SOL=$150)
    2. Sell 500 tokens for 0.8 SOL at T=1715001200 (fee 0.000005 SOL, SOL=$160)
    3. Sell remaining 500 tokens for 0.4 SOL at T=1715005000 (fee 0.000010 SOL, SOL=$170)
    """
    prices_data = load_fixture("defillama_historical_prices.json")
    oracle = MockPriceOracle(prices_data)
    reconstructor = TradeReconstructor(price_oracle=oracle)
    
    pump_data = load_fixture("solana_pump_fun_swap.json")
    ray_data = load_fixture("solana_raydium_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    
    provider = HeliusSolanaHistoryProvider(api_key="test-key")
    swaps = provider.parse_enhanced_transactions(pump_data + ray_data, wallet=wallet)
    
    reconstruction = await reconstructor.reconstruct_trades(
        swaps=swaps,
        chain=ChainIdentifier.SOLANA_MAINNET,
        wallet_address=wallet,
    )
    
    closed_trades = reconstruction.closed_trades
    assert len(closed_trades) == 2, f"Expected 2 closed trades, got {len(closed_trades)}"
    
    # Trade 1: 500 tokens sold on Pump.fun
    trade1 = closed_trades[0]
    assert trade1.token_address == "PumpToken11111111111111111111111111111111111pump"
    assert trade1.buy_timestamp == 1715000000
    assert trade1.sell_timestamp == 1715001200
    assert trade1.holding_time_seconds == 1200.0
    
    # Cost basis for 500 tokens:
    # Original buy: 1.0 SOL + 0.000005 fee = 1.000005 SOL total cost for 1000 tokens.
    # Half of lot: 0.5000025 SOL cost basis.
    assert trade1.invested_native == Decimal("0.5000025")
    # Proceeds: 0.8 SOL - 0.000005 fee = 0.799995 SOL.
    # Realized native PnL: 0.799995 - 0.5000025 = 0.2999925 SOL.
    assert trade1.realized_native_pnl == Decimal("0.2999925")
    # Historical USD conversion:
    # Buy at SOL=$150: cost_usd = 0.5000025 * 150 = $75.000375
    # Sell at SOL=$160: proceeds_usd = 0.799995 * 160 = $127.9992
    # Realized USD PnL: $127.9992 - $75.000375 = $52.998825
    assert trade1.realized_pnl_usd == Decimal("52.998825")
    assert trade1.is_win is True
    
    # Trade 2: remaining 500 tokens sold on Raydium
    trade2 = closed_trades[1]
    assert trade2.buy_timestamp == 1715000000
    assert trade2.sell_timestamp == 1715005000
    assert trade2.holding_time_seconds == 5000.0
    # Proceeds: 0.4 SOL - 0.000010 fee = 0.399990 SOL.
    # Realized native PnL: 0.399990 - 0.5000025 = -0.1000125 SOL (loss).
    assert trade2.realized_native_pnl < Decimal(0)
    assert trade2.is_win is False


@pytest.mark.asyncio
async def test_transfers_not_counted_as_sells():
    """Verify that pure token transfers into/out of wallet are NOT counted as sells or losses."""
    prices_data = load_fixture("defillama_historical_prices.json")
    oracle = MockPriceOracle(prices_data)
    reconstructor = TradeReconstructor(price_oracle=oracle)
    
    transfer_data = load_fixture("solana_transfers.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    
    provider = HeliusSolanaHistoryProvider(api_key="test-key")
    swaps = provider.parse_enhanced_transactions(transfer_data, wallet=wallet)
    
    assert all(s.is_transfer for s in swaps)
    
    reconstruction = await reconstructor.reconstruct_trades(
        swaps=swaps,
        chain=ChainIdentifier.SOLANA_MAINNET,
        wallet_address=wallet,
    )
    
    # Outgoing transfer of 300 tokens must NOT create a closed trade or sell record
    assert len(reconstruction.closed_trades) == 0
    assert reconstruction.total_realized_pnl_usd == Decimal(0)


@pytest.mark.asyncio
async def test_unrealized_positions_excluded_from_closed_trades():
    """Verify open positions are tracked separately and excluded from win rate / PnL."""
    prices_data = load_fixture("defillama_historical_prices.json")
    oracle = MockPriceOracle(prices_data)
    reconstructor = TradeReconstructor(price_oracle=oracle)
    
    jup_data = load_fixture("solana_jupiter_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    
    provider = HeliusSolanaHistoryProvider(api_key="test-key")
    swaps = provider.parse_enhanced_transactions(jup_data, wallet=wallet)
    
    reconstruction = await reconstructor.reconstruct_trades(
        swaps=swaps,
        chain=ChainIdentifier.SOLANA_MAINNET,
        wallet_address=wallet,
    )
    
    assert len(reconstruction.closed_trades) == 0
    assert len(reconstruction.open_positions) == 1
    open_pos = reconstruction.open_positions[0]
    assert open_pos.token_address == "JupToken1111111111111111111111111111111111111"
    assert open_pos.amount_remaining == Decimal("50.0")


# ---------------------------------------------------------------------------
# 3. SQLite Caching & Network Avoidance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_hit_avoids_network(tmp_path):
    """Verify that cached swaps avoid network calls on subsequent requests."""
    db_path = str(tmp_path / "test_history.db")
    history_db = WalletHistoryDatabase(db_path=db_path)
    await history_db.connect()

    pump_data = load_fixture("solana_pump_fun_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"
    provider = HeliusSolanaHistoryProvider(api_key="test-key", history_db=history_db)

    network_call_count = 0

    async def fake_fetch(wallet: str = "", **kwargs):
        nonlocal network_call_count
        network_call_count += 1
        return pump_data

    provider._fetch_enhanced_transactions_raw = fake_fetch

    since_ts = 1714900000
    until_ts = 1715010000

    # First fetch: Cache miss -> network called once
    swaps1 = await provider.fetch_swaps(
        wallet=wallet, chain=ChainIdentifier.SOLANA_MAINNET, since=since_ts, until=until_ts
    )
    assert len(swaps1) == 2
    assert network_call_count == 1

    # Second fetch: Cache hit -> network call count remains 1!
    swaps2 = await provider.fetch_swaps(
        wallet=wallet, chain=ChainIdentifier.SOLANA_MAINNET, since=since_ts, until=until_ts
    )
    assert len(swaps2) == 2
    assert network_call_count == 1

    await history_db.close()


# ---------------------------------------------------------------------------
# 4. Rate Limiting & Quota Accounting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_and_quota_accounting(tmp_path):
    """Verify free-tier quota accounting raises ProviderQuotaExceededError when exhausted."""
    db_path = str(tmp_path / "test_quota.db")
    history_db = WalletHistoryDatabase(db_path=db_path)
    await history_db.connect()

    rate_limiters = RateLimiterRegistry.default()
    provider = HeliusSolanaHistoryProvider(
        api_key="test-key",
        history_db=history_db,
        rate_limiters=rate_limiters,
        daily_request_quota=2,  # set tiny quota of 2 requests
    )

    wallet = "TokenWallet111111111111111111111111111111111"

    async def fake_empty(wallet: str = "", **kwargs):
        return []

    provider._fetch_enhanced_transactions_raw = fake_empty

    # Request 1: OK
    await provider.fetch_swaps(wallet=f"{wallet}1", chain=ChainIdentifier.SOLANA_MAINNET)
    # Request 2: OK
    await provider.fetch_swaps(wallet=f"{wallet}2", chain=ChainIdentifier.SOLANA_MAINNET)

    # Request 3: Exceeds quota -> raises ProviderQuotaExceededError
    with pytest.raises(ProviderQuotaExceededError) as exc_info:
        await provider.fetch_swaps(wallet=f"{wallet}3", chain=ChainIdentifier.SOLANA_MAINNET)
    assert "quota exhausted" in str(exc_info.value).lower()

    await history_db.close()


# ---------------------------------------------------------------------------
# 5. Graceful Degradation: Provider Error -> "unavailable" -> "insufficient data"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_provider_error_propagates_unavailable_to_profiler(tmp_path):
    """Verify provider error propagates 'unavailable' resulting in 'insufficient data'."""
    db_path = str(tmp_path / "test_profiler.db")

    class FailingProvider(WalletHistoryProvider):
        async def _fetch_swaps_network(self, wallet, chain, since, until, limit):
            raise ProviderUnavailableError("Helius RPC endpoint unreachable (503 Service Unavailable)")

    profiler = SmartMoneyProfiler(
        db_path=db_path,
        history_provider=FailingProvider(),
    )
    await profiler.initialize()

    agent = SmartMoneyProfilerAgent(profiler=profiler)
    result = await agent.tool_evaluate_wallet(
        wallet_address="AnyWallet111111111111111111111111111111111",
        chain="solana_mainnet",
    )

    # Must report "insufficient data", NEVER "not smart money" or falsely approved/rejected
    assert result["classification"] == WalletClassification.INSUFFICIENT_DATA.value
    assert result["eligible_for_copytrade"] is False
    assert any("insufficient data" in r.lower() for r in result["rejection_reasons"])

    await profiler.close()


# ---------------------------------------------------------------------------
# 6. Full End-to-End Integration Test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_profile_wallet_receives_real_records_integration(tmp_path):
    """
    Verify profile_wallet and tool_evaluate_wallet receive non-empty real
    WalletTradeRecord objects from history provider + reconstructor.
    """
    db_path = str(tmp_path / "test_integration.db")
    history_db = WalletHistoryDatabase(db_path=db_path)
    await history_db.connect()

    prices_data = load_fixture("defillama_historical_prices.json")
    oracle = MockPriceOracle(prices_data)
    reconstructor = TradeReconstructor(price_oracle=oracle)

    pump_data = load_fixture("solana_pump_fun_swap.json")
    ray_data = load_fixture("solana_raydium_swap.json")
    wallet = "TokenWallet111111111111111111111111111111111"

    # Synthesize 16 trades to test full qualification logic
    synthetic_swaps = []
    base_swaps = HeliusSolanaHistoryProvider(api_key="test").parse_enhanced_transactions(
        pump_data + ray_data, wallet=wallet
    )
    for i in range(8):
        for s in base_swaps:
            copy_s = s.model_copy(
                update={
                    "token_address": f"TokenAddress{i:02d}11111111111111111111111111111pump",
                    "tx_hash": f"TxHash{i:02d}{s.tx_hash[8:]}",
                    "block_time": s.block_time + (i * 86400),
                }
            )
            synthetic_swaps.append(copy_s)

    class FixtureHistoryProvider(WalletHistoryProvider):
        async def _fetch_swaps_network(self, wallet, chain, since, until, limit):
            return synthetic_swaps

    profiler = SmartMoneyProfiler(
        db_path=db_path,
        history_provider=FixtureHistoryProvider(),
        reconstructor=reconstructor,
        history_db=history_db,
    )
    await profiler.initialize()

    agent = SmartMoneyProfilerAgent(profiler=profiler)

    # tool_evaluate_wallet should now fetch real trades (NOT trades=[])
    result = await agent.tool_evaluate_wallet(
        wallet_address=wallet,
        chain="solana_mainnet",
    )

    # Verify non-empty trades were evaluated
    assert result["total_trades"] == 16, f"Expected 16 closed trades, got {result['total_trades']}"
    assert result["win_rate_pct"] == 50.0  # 8 wins, 8 losses
    assert float(result["total_pnl_usd"]) > 0.0

    # Verify reconstructed trades and metrics were persisted to SQLite
    persisted_trades = await history_db.get_reconstructed_trades(
        wallet_address=wallet,
        chain=ChainIdentifier.SOLANA_MAINNET,
    )
    assert len(persisted_trades) == 16
    assert isinstance(persisted_trades[0], WalletTradeRecord)

    await profiler.close()
    await history_db.close()
