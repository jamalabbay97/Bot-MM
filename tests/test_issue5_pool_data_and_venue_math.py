"""
tests/test_issue5_pool_data_and_venue_math.py
Unit and integration tests for Issue 5:
- Elimination of fabricated pool reserves, prices, and token supply.
- PoolState provenance (source, fetched_at, data_age, ReserveType.EXACT vs DERIVED).
- Derived-reserves policy enforcement (reject vs cap_size).
- Supply fetching / unknown market cap rejection.
- Venue capabilities table, rejection of unsupported venues (e.g. CLMM), and Quoter integration.
- Pump.fun bonding curve real state vs unknown state rejection.
- Runner DexScreener and GeckoTerminal handling of empty/partial JSON.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_engine.config import EngineConfig
from alpha_engine.execution.costs import TradeCostModel
from alpha_engine.execution.executor import PaperExecutor
from alpha_engine.models.enums import ChainIdentifier, ExecutionVenue, OrderSide, SecurityTier, SignalSource, SignalStrength
from alpha_engine.models.events import SignalEvent
from alpha_engine.models.state import PoolState, ReserveType, SecurityReport
from alpha_engine.pricing.native_price_service import PriceResult, NativePriceService, NativePriceStatus
from alpha_engine.pricing.venue_capabilities import (
    IVenueQuoter,
    VenueQuote,
    VenueStatus,
    get_venue_capability,
)


def make_signal_event(
    chain: ChainIdentifier,
    token_address: str,
    pool_state: PoolState,
    suggested_side: OrderSide = OrderSide.BUY,
    source: SignalSource = SignalSource.DEX_SWAP,
    execution_venue: Optional[ExecutionVenue] = None,
    strength: SignalStrength = SignalStrength.STRONG,
    alpha_score: float = 0.85,
    market_cap_usd: Optional[Decimal] = None,
) -> SignalEvent:
    sec = SecurityReport(
        token_address=token_address,
        chain=chain,
        tier=SecurityTier.CLEAN,
        is_honeypot=False,
        buy_tax_bps=0,
        sell_tax_bps=0,
        venue=execution_venue,
    )
    return SignalEvent(
        timestamp_ns=time.time_ns(),
        chain=chain,
        pool_address=pool_state.pool_address,
        token_address=token_address,
        suggested_side=suggested_side,
        source=source,
        pool_state=pool_state,
        security_report=sec,
        strength=strength,
        alpha_score=alpha_score,
        market_cap_usd=market_cap_usd,
        execution_venue=execution_venue,
    )


class MockContext:
    def __init__(self, data: Any, status: int = 200) -> None:
        self._data = data
        self._status = status

    async def __aenter__(self) -> Any:
        resp = MagicMock()
        resp.status = self._status
        resp.json = AsyncMock(return_value=self._data)
        return resp

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class MockSession:
    def __init__(self, data: Any, status: int = 200) -> None:
        self._data = data
        self._status = status

    def get(self, url: str, **kwargs: Any) -> MockContext:
        return MockContext(self._data, self._status)


class DummyPriceService(NativePriceService):
    """Deterministic price service for test isolation."""

    def __init__(self, sol_price: Decimal = Decimal("200.0"), eth_price: Decimal = Decimal("3000.0")):
        self.sol_price = sol_price
        self.eth_price = eth_price

    def get_price(self, chain: ChainIdentifier) -> PriceResult:
        if chain == ChainIdentifier.SOLANA_MAINNET:
            return PriceResult(
                price=self.sol_price,
                age_s=0.1,
                status=NativePriceStatus.OK,
                source="test",
            )
        return PriceResult(
            price=self.eth_price,
            age_s=0.1,
            status=NativePriceStatus.OK,
            source="test",
        )


class MockVenueQuoter(IVenueQuoter):
    """Mock quoter simulating Uniswap v3 on-chain Quoter or Jupiter."""

    def __init__(self, fixed_quote: Optional[VenueQuote] = None):
        self.fixed_quote = fixed_quote
        self.call_count = 0

    async def quote_buy(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        native_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        _ = (token_address, pool_address, pool)
        self.call_count += 1
        if self.fixed_quote is not None:
            return self.fixed_quote
        # Non-linear concentrated liquidity quote distinct from x*y=k
        tokens_out = native_amount_in * Decimal("50000.0")
        eff_price = native_amount_in / tokens_out
        return VenueQuote(
            venue=venue,
            chain=chain,
            amount_in=native_amount_in,
            amount_out=tokens_out,
            effective_price=eff_price,
            price_impact_bps=12,
            fee_amount=native_amount_in * Decimal("0.003"),
            source="mock_uniswap_v3_quoter",
        )

    async def quote_sell(
        self,
        chain: ChainIdentifier,
        token_address: str,
        pool_address: str,
        token_amount_in: Decimal,
        venue: ExecutionVenue,
        pool: Optional[Any] = None,
    ) -> VenueQuote:
        _ = (token_address, pool_address, pool)
        self.call_count += 1
        if self.fixed_quote is not None:
            return self.fixed_quote
        native_out = token_amount_in / Decimal("50000.0")
        eff_price = native_out / token_amount_in
        return VenueQuote(
            venue=venue,
            chain=chain,
            amount_in=token_amount_in,
            amount_out=native_out,
            effective_price=eff_price,
            price_impact_bps=15,
            fee_amount=native_out * Decimal("0.003"),
            source="mock_uniswap_v3_quoter",
        )


@pytest.fixture
def base_config() -> EngineConfig:
    return EngineConfig()


@pytest.fixture
def price_service() -> NativePriceService:
    return DummyPriceService()


# -----------------------------------------------------------------------------
# 1. Missing liquidity/price/supply -> no fill, counter incremented, WARNING logged
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_missing_liquidity_price_supply_rejects_and_counts_telemetry(
    base_config: EngineConfig, price_service: NativePriceService, caplog: pytest.LogCaptureFixture
) -> None:
    from alpha_engine.engine.runner import PaperTradingEngine

    engine = PaperTradingEngine(config=base_config)
    engine._native_price_service = price_service

    # Test 1a: Missing price in DexScreener JSON
    partial_no_price = {
        "pairs": [
            {
                "chainId": "solana",
                "pairAddress": "PairNoPrice1111111111111111111111111111111",
                "dexId": "raydium",
                "priceUsd": "0",
                "priceNative": "0",
                "liquidity": {"usd": 50000.0, "quote": 250.0, "base": 1000000.0},
            }
        ]
    }
    session_1a = MockSession(partial_no_price)
    pool = await engine._fetch_dexscreener_state(
        session=session_1a,
        chain=ChainIdentifier.SOLANA_MAINNET,
        clean_token="TokenNoPrice111111111111111111111111111111",
        pool_address="",
    )
    assert pool is None, "PoolState must be None when price is missing (never invent 0.000000028)"
    assert engine.telemetry_counters.get("no_price", 0) > 0, "Telemetry counter 'no_price' must be incremented"
    assert any("no_price" in r.message.lower() or "missing price" in r.message.lower() for r in caplog.records)

    # Test 1b: Missing liquidity in DexScreener JSON
    partial_no_liq = {
        "pairs": [
            {
                "chainId": "solana",
                "pairAddress": "PairNoLiq11111111111111111111111111111111",
                "dexId": "raydium",
                "priceUsd": "1.50",
                "priceNative": "0.0075",
                "liquidity": {"usd": 0, "quote": 0, "base": 0},
            }
        ]
    }
    session_1b = MockSession(partial_no_liq)
    pool = await engine._fetch_dexscreener_state(
        session=session_1b,
        chain=ChainIdentifier.SOLANA_MAINNET,
        clean_token="TokenNoLiq11111111111111111111111111111111",
        pool_address="",
    )
    assert pool is None, "PoolState must be None when liquidity is missing (never invent 30 SOL / 5 ETH)"
    assert engine.telemetry_counters.get("no_liquidity_data", 0) > 0, "Telemetry counter 'no_liquidity_data' must be incremented"


# -----------------------------------------------------------------------------
# 2. Derived-reserves policy enforced (reject or size cap)
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_derived_reserves_policy_enforcement(
    base_config: EngineConfig, price_service: NativePriceService
) -> None:
    from alpha_engine.execution.book import PositionBook

    book = PositionBook()
    cost_model = TradeCostModel(config=base_config, price_service=price_service)

    # Derived pool (from API liquidity USD)
    derived_pool = PoolState(
        pool_address="DerivedPool1111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="DerivedToken11111111111111111111111111111",
        native_reserve=Decimal("20.0"),  # 20 SOL derived depth
        token_reserve=Decimal("2000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        reserve_type=ReserveType.DERIVED,
        source="DexScreener",
        fetched_at=time.time(),
        data_age=1.5,
    )
    assert derived_pool.is_derived is True

    signal = make_signal_event(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="DerivedToken11111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        source=SignalSource.DEX_SWAP,
        pool_state=derived_pool,
    )

    # 2a: Policy = "reject" (default / safer policy)
    base_config.derived_reserves_policy = "reject"
    executor_reject = PaperExecutor(
        position_book=book,
        price_service=price_service,
        cost_model=cost_model,
        derived_reserves_policy="reject",
    )
    fill_reject = await executor_reject.execute_signal(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("1.0"),
    )
    assert fill_reject is None, "Executor must reject signal when pool has derived reserves and policy='reject'"
    assert executor_reject.telemetry_counters.get("derived_reserves_rejected", 0) > 0

    # 2b: Policy = "cap_size" (cap to e.g. 2% of derived native depth = 2% of 20 SOL = 0.4 SOL)
    executor_cap = PaperExecutor(
        position_book=book,
        price_service=price_service,
        cost_model=cost_model,
        derived_reserves_policy="cap_size",
        derived_reserves_depth_cap_pct=Decimal("0.02"),
    )
    fill_cap = await executor_cap.execute_signal(
        signal=signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("1.0"),  # Requests 1.0 SOL, must be capped to 0.4 SOL
    )
    assert fill_cap is not None, "Executor must fill capped amount under 'cap_size' policy"
    assert fill_cap.simulated_native_spent == Decimal("0.4"), (
        f"Trade size must be capped to 0.4 SOL (2% of 20 SOL), got {fill_cap.simulated_native_spent}"
    )


# -----------------------------------------------------------------------------
# 3. Unsupported venue (e.g., CLMM) rejected; supported venue quoted via quoter
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unsupported_venue_rejected_and_supported_venue_quoted(
    base_config: EngineConfig, price_service: NativePriceService
) -> None:
    from alpha_engine.execution.book import PositionBook
    from alpha_engine.security.gatekeeper import SecurityGatekeeper

    # Check venue capabilities table
    assert get_venue_capability(ExecutionVenue.RAYDIUM_CLMM).status == VenueStatus.UNSUPPORTED
    assert get_venue_capability(ExecutionVenue.ORCA_WHIRLPOOL).status == VenueStatus.UNSUPPORTED
    assert get_venue_capability(ExecutionVenue.UNISWAP_V3).status == VenueStatus.SUPPORTED

    book = PositionBook()
    cost_model = TradeCostModel(config=base_config, price_service=price_service)
    executor = PaperExecutor(position_book=book, price_service=price_service, cost_model=cost_model)

    clmm_pool = PoolState(
        pool_address="ClmmPool111111111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="ClmmToken1111111111111111111111111111111",
        native_reserve=Decimal("100.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        reserve_type=ReserveType.EXACT,
    )
    clmm_signal = make_signal_event(
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="ClmmToken1111111111111111111111111111111",
        suggested_side=OrderSide.BUY,
        source=SignalSource.DEX_SWAP,
        pool_state=clmm_pool,
        execution_venue=ExecutionVenue.RAYDIUM_CLMM,
    )

    # 3a: Unsupported CLMM pool must be rejected with counted reason
    fill_unsupported = await executor.execute_signal(
        signal=clmm_signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("1.0"),
    )
    assert fill_unsupported is None, "Unsupported venue RAYDIUM_CLMM must be rejected"
    assert executor.telemetry_counters.get("unsupported_venue_raydium_clmm", 0) > 0

    # 3b: Supported concentrated liquidity venue (UNISWAP_V3) with active mock Quoter
    mock_quoter = MockVenueQuoter()
    cost_model_v3 = TradeCostModel(config=base_config, price_service=price_service, quoter=mock_quoter)
    executor_v3 = PaperExecutor(position_book=book, price_service=price_service, cost_model=cost_model_v3)

    v3_pool = PoolState(
        pool_address="0x3333333333333333333333333333333333333333",
        chain=ChainIdentifier.BASE_MAINNET,
        token_address="0x4444444444444444444444444444444444444444",
        native_reserve=Decimal("10.0"),
        token_reserve=Decimal("500000.0"),
        fee_numerator=30,
        fee_denominator=10000,
        last_updated_block=100,
        reserve_type=ReserveType.EXACT,
    )
    v3_signal = make_signal_event(
        chain=ChainIdentifier.BASE_MAINNET,
        token_address="0x4444444444444444444444444444444444444444",
        suggested_side=OrderSide.BUY,
        source=SignalSource.DEX_SWAP,
        pool_state=v3_pool,
        execution_venue=ExecutionVenue.UNISWAP_V3,
    )

    fill_v3 = await executor_v3.execute_signal(
        signal=v3_signal,
        portfolio_equity_usd=Decimal("10000.0"),
        fixed_trade_amount_native=Decimal("0.1"),
    )
    assert fill_v3 is not None, "Supported UNISWAP_V3 venue must execute via Quoter"
    assert mock_quoter.call_count > 0, "Mock quoter quote_buy must have been called"
    assert fill_v3.tokens_acquired == Decimal("0.1") * Decimal("50000.0"), (
        "Tokens acquired must come from Quoter output, not x*y=k CPMM math"
    )


# -----------------------------------------------------------------------------
# 4. Market cap unknown -> mcap-dependent filter rejects explicitly
# -----------------------------------------------------------------------------
def test_market_cap_unknown_rejects_explicitly(base_config: EngineConfig) -> None:
    pool = PoolState(
        pool_address="PoolMcapTest1111111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        token_address="TokenMcapTest111111111111111111111111111",
        native_reserve=Decimal("20.0"),
        token_reserve=Decimal("1000000.0"),
        fee_numerator=25,
        fee_denominator=10000,
        last_updated_block=100,
        total_supply=None,  # Real supply UNKNOWN
        circulating_supply=None,
    )
    # calculate_market_cap_usd must return None when supply is unknown, NEVER assume 1B
    mcap = pool.calculate_market_cap_usd(native_price_usd=Decimal("200.0"))
    assert mcap is None, "Market cap must be None when real supply is unavailable (never default to 1B)"

    # When supply is known, calculate correctly:
    pool_known = pool.model_copy(update={"total_supply": Decimal("500000000.0")})  # 500M real supply
    # Spot price = 20 SOL / 1,000,000 tokens = 0.00002 SOL/token * $200 = $0.004/token * 500M = $2,000,000
    mcap_known = pool_known.calculate_market_cap_usd(native_price_usd=Decimal("200.0"))
    assert mcap_known == Decimal("2000000.0000")


# -----------------------------------------------------------------------------
# 5. Pump.fun: unknown curve state -> reject; decoded curve state -> correct quote
# -----------------------------------------------------------------------------
def test_pump_fun_unknown_state_rejects_and_decoded_state_quotes_accurately() -> None:
    from alpha_engine.math.cpmm import (
        cpmm_out,
        derive_pump_fun_quote_from_state,
        get_initial_bonding_curve_pool,
    )

    # 5a: For a token whose current curve state is unknown, get_initial_bonding_curve_pool
    # must NOT invent virtual reserves or must return None / fail closed
    pool_unknown = get_initial_bonding_curve_pool(
        token_address="UnknownPumpToken1111111111111111111111111",
        chain=ChainIdentifier.SOLANA_MAINNET,
        require_verified_launch=True,
    )
    assert pool_unknown is None, "Must reject unknown curve state (never assume initial 30 SOL / 1.073B)"

    # 5b: Decoded real curve state:
    # virtual_sol = 32.5 SOL, virtual_token = 980,000,000 tokens, fee = 100 bps (1%)
    # BUY 1.0 SOL
    # Hand calculation:
    # 1. fee = 1.0 * 0.01 = 0.01 SOL -> native_in_after_fee = 0.99 SOL
    # 2. k = 32.5 * 980_000_000 = 31,850,000,000
    # 3. new_sol = 32.5 + 0.99 = 33.49 SOL
    # 4. new_token = 31,850,000,000 / 33.49 = 951,030,158.256195879366975216...
    # 5. tokens_out = 980_000_000 - new_token = 28,969,841.743804120633024784...
    quote = derive_pump_fun_quote_from_state(
        virtual_sol_reserves=Decimal("32.5"),
        virtual_token_reserves=Decimal("980000000.0"),
        amount_in=Decimal("1.0"),
        is_buy=True,
        fee_bps=100,
    )
    expected_tokens_out = (
        Decimal("980000000.0") - (Decimal("32.5") * Decimal("980000000.0")) / Decimal("33.49")
    )
    assert abs(quote.amount_out - expected_tokens_out) < Decimal("0.0001"), (
        f"Quote amount out {quote.amount_out} must match hand-computed {expected_tokens_out}"
    )


# -----------------------------------------------------------------------------
# 6. Integration test through runner for DexScreener/GeckoTerminal empty/partial JSON
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_integration_runner_dexscreener_and_geckoterminal_empty_partial_json(
    base_config: EngineConfig, price_service: NativePriceService
) -> None:
    from alpha_engine.engine.runner import PaperTradingEngine

    engine = PaperTradingEngine(config=base_config)
    engine._native_price_service = price_service

    # Empty DexScreener response
    session_empty = MockSession({})
    pool = await engine._fetch_dexscreener_state(
        session=session_empty,
        chain=ChainIdentifier.SOLANA_MAINNET,
        clean_token="TokenEmptyDex1111111111111111111111111111",
        pool_address="",
    )
    assert pool is None, "Empty DexScreener JSON must return None"

    # Empty GeckoTerminal response
    with patch.object(engine.market_oracle, "get_geckoterminal_pool_stats", new_callable=AsyncMock) as mock_gt:
        mock_gt.return_value = {}
        pool_gt = await engine._fetch_geckoterminal_state(
            chain=ChainIdentifier.SOLANA_MAINNET,
            clean_token="TokenEmptyGT11111111111111111111111111111",
            pool_address="",
        )
        assert pool_gt is None, "Empty GeckoTerminal stats must return None"

    # Valid DexScreener response with real data
    valid_dex_json = {
        "pairs": [
            {
                "chainId": "solana",
                "pairAddress": "ValidPair111111111111111111111111111111111",
                "dexId": "raydium",
                "priceUsd": "2.00",
                "priceNative": "0.01",  # 0.01 SOL/token (at $200 SOL = $2.00)
                "liquidity": {"usd": 40000.0, "quote": 100.0, "base": 10000.0},
            }
        ]
    }
    session_valid = MockSession(valid_dex_json)
    pool_valid = await engine._fetch_dexscreener_state(
        session=session_valid,
        chain=ChainIdentifier.SOLANA_MAINNET,
        clean_token="ValidToken11111111111111111111111111111111",
        pool_address="",
    )
    assert pool_valid is not None
    assert pool_valid.reserve_type == ReserveType.DERIVED
    assert pool_valid.source.startswith("DexScreener")
    assert pool_valid.native_reserve == Decimal("100.0")
    assert pool_valid.token_reserve == Decimal("10000.0")
    assert pool_valid.spot_price_native_per_token == Decimal("0.01")
