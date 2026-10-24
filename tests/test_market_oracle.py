"""
tests.test_market_oracle — Unit and Integration Tests for Public Market Oracle
==============================================================================
Tests for:
1. Alternative.me Crypto Fear & Greed Index parsing, risk modifier scaling, caching, and fallback.
2. DefiLlama Coin Price Oracle for Solana and Base with TTL caching.
3. DefiLlama Chain TVL fetch for Solana and Base with TTL caching.
4. GeckoTerminal DEX pool analytics (reserves, FDV, 24h volume, price).
5. Aggregated Macro Market Summary.
6. Telegram Ingester /oracle command integration.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp

from alpha_engine.ingestion.market_oracle import (
    FEAR_GREED_TTL_S,
    DEFILLAMA_PRICE_TTL_S,
    DEFILLAMA_TVL_TTL_S,
    GECKOTERMINAL_TTL_S,
    MarketOracle,
)
from alpha_engine.ingestion.telegram import TelegramIngester


class MockResponse:
    """Helper mock for aiohttp response."""

    def __init__(self, status: int, json_data: Any) -> None:
        self.status = status
        self._json_data = json_data

    async def json(self) -> Any:
        return self._json_data

    async def text(self) -> str:
        return str(self._json_data)

    async def __aenter__(self) -> MockResponse:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        pass


@pytest.mark.asyncio
async def test_fear_and_greed_risk_scaling_and_caching() -> None:
    """Verify Fear & Greed classification and dynamic risk modifier scaling."""
    oracle = MarketOracle()

    # 1. Extreme Fear (score 20 -> risk modifier 0.75)
    mock_data_ef = {"data": [{"value": "20", "value_classification": "Extreme Fear"}]}
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_data_ef)) as mock_get:
        fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fng["value"] == 20
        assert fng["classification"] == "Extreme Fear"
        assert fng["risk_modifier"] == 0.75
        assert mock_get.call_count == 1

        # Test cache hit (timestamp should not trigger second get)
        cached_fng = await oracle.get_fear_and_greed(force_refresh=False)
        assert cached_fng["value"] == 20
        assert mock_get.call_count == 1  # No additional HTTP request

    # 2. Greed (score 70 -> risk modifier 1.10)
    mock_data_g = {"data": [{"value": "70", "value_classification": "Greed"}]}
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_data_g)):
        fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fng["value"] == 70
        assert fng["classification"] == "Greed"
        assert fng["risk_modifier"] == 1.10

    # 3. Extreme Greed (score 85 -> risk modifier 1.20)
    mock_data_eg = {"data": [{"value": "85", "value_classification": "Extreme Greed"}]}
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_data_eg)):
        fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fng["value"] == 85
        assert fng["risk_modifier"] == 1.20

    # 4. Fear (score 35 -> risk modifier 0.85)
    mock_data_f = {"data": [{"value": "35", "value_classification": "Fear"}]}
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_data_f)):
        fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fng["value"] == 35
        assert fng["risk_modifier"] == 0.85

    # 5. Neutral (score 50 -> risk modifier 1.00)
    mock_data_n = {"data": [{"value": "50", "value_classification": "Neutral"}]}
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_data_n)):
        fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fng["value"] == 50
        assert fng["risk_modifier"] == 1.00

    # 6. Fallback on network error (should return cached or default 50 Neutral)
    with patch.object(aiohttp.ClientSession, "get", side_effect=aiohttp.ClientError("Network error")):
        oracle._fng_cached = None  # clear cache
        fallback_fng = await oracle.get_fear_and_greed(force_refresh=True)
        assert fallback_fng["value"] == 50
        assert fallback_fng["classification"] == "Neutral"
        assert fallback_fng["risk_modifier"] == 1.00

    await oracle.close()


@pytest.mark.asyncio
async def test_defillama_coin_price_oracle() -> None:
    """Verify DefiLlama multi-token price fetching and TTL caching."""
    oracle = MarketOracle()

    mock_resp = {
        "coins": {
            "solana:So11111111111111111111111111111111111111112": {
                "price": 182.50,
                "symbol": "SOL",
                "timestamp": 1729000000,
                "confidence": 0.99,
            },
            "coingecko:ethereum": {
                "price": 2650.00,
                "symbol": "ETH",
                "timestamp": 1729000000,
                "confidence": 0.99,
            },
        }
    }

    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_resp)) as mock_get:
        prices = await oracle.get_defillama_coin_prices(force_refresh=True)
        assert prices["solana"] == 182.50
        assert prices["base_eth"] == 2650.00
        assert mock_get.call_count == 1

        # Cache check
        cached_prices = await oracle.get_defillama_coin_prices(force_refresh=False)
        assert cached_prices["solana"] == 182.50
        assert mock_get.call_count == 1

    # Network error fallback
    with patch.object(aiohttp.ClientSession, "get", side_effect=Exception("Timeout")):
        oracle._prices_cached.clear()
        err_prices = await oracle.get_defillama_coin_prices(force_refresh=True)
        assert err_prices == {}

    await oracle.close()


@pytest.mark.asyncio
async def test_defillama_chain_tvl_oracle() -> None:
    """Verify DefiLlama chain TVL fetching for Solana and Base."""
    oracle = MarketOracle()

    mock_tvl_data = [
        {"name": "Ethereum", "tvl": 55000000000.0, "tokenSymbol": "ETH"},
        {"name": "Solana", "tvl": 8250000000.0, "tokenSymbol": "SOL"},
        {"name": "Base", "tvl": 3100000000.0, "tokenSymbol": "ETH"},
        {"name": "Arbitrum", "tvl": 2900000000.0, "tokenSymbol": "ARB"},
    ]

    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_tvl_data)) as mock_get:
        tvls = await oracle.get_defillama_chain_tvl(force_refresh=True)
        assert tvls["solana"] == 8250000000.0
        assert tvls["base"] == 3100000000.0
        assert mock_get.call_count == 1

        # Cache hit
        cached_tvls = await oracle.get_defillama_chain_tvl(force_refresh=False)
        assert cached_tvls["solana"] == 8250000000.0
        assert mock_get.call_count == 1

    # Network error fallback
    with patch.object(aiohttp.ClientSession, "get", side_effect=Exception("API Down")):
        oracle._tvl_cached.clear()
        err_tvl = await oracle.get_defillama_chain_tvl(force_refresh=True)
        assert err_tvl == {}

    await oracle.close()


@pytest.mark.asyncio
async def test_geckoterminal_pool_stats() -> None:
    """Verify GeckoTerminal public DEX analytics parsing."""
    oracle = MarketOracle()

    token_address = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    mock_gt_data = {
        "data": [
            {
                "id": "solana_pool_123",
                "type": "pool",
                "attributes": {
                    "name": "BONK / SOL",
                    "base_token_price_usd": "0.00002145",
                    "reserve_in_usd": "4250000.50",
                    "fdv_usd": "1800000000.00",
                    "volume_usd": {"h24": "12500000.00"},
                },
                "relationships": {
                    "dex": {"data": {"id": "raydium"}}
                },
            }
        ]
    }

    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, mock_gt_data)):
        pool_stats = await oracle.get_geckoterminal_pool_stats("solana", token_address, force_refresh=True)
        assert pool_stats is not None
        assert pool_stats["pool_name"] == "BONK / SOL"
        assert pool_stats["base_token_price_usd"] == 0.00002145
        assert pool_stats["reserve_in_usd"] == 4250000.50
        assert pool_stats["fdv_usd"] == 1800000000.00
        assert pool_stats["volume_usd_24h"] == 12500000.00
        assert pool_stats["dex"] == "raydium"

    # Empty pools handling
    with patch.object(aiohttp.ClientSession, "get", return_value=MockResponse(200, {"data": []})):
        empty_stats = await oracle.get_geckoterminal_pool_stats("solana", "NonExistentTokenAddress", force_refresh=True)
        assert empty_stats is None

    await oracle.close()


@pytest.mark.asyncio
async def test_macro_market_summary_aggregation() -> None:
    """Verify aggregated get_macro_market_summary returns all sub-oracles."""
    oracle = MarketOracle()

    with (
        patch.object(oracle, "get_fear_and_greed", return_value={"value": 65, "classification": "Greed", "risk_modifier": 1.10}),
        patch.object(oracle, "get_defillama_coin_prices", return_value={"solana": 180.0, "base_eth": 2600.0}),
        patch.object(oracle, "get_defillama_chain_tvl", return_value={"solana": 8.0e9, "base": 3.0e9}),
    ):
        summary = await oracle.get_macro_market_summary()
        assert "fear_and_greed" in summary
        assert "prices" in summary
        assert "tvl" in summary
        assert summary["fear_and_greed"]["value"] == 65
        assert summary["prices"]["solana"] == 180.0
        assert summary["tvl"]["solana"] == 8.0e9

    await oracle.close()


@pytest.mark.asyncio
async def test_telegram_oracle_command() -> None:
    """Verify Telegram /oracle command executes and formats responses accurately."""
    event_q: asyncio.Queue[Any] = asyncio.Queue()
    ingester = TelegramIngester(
        event_queue=event_q,
        api_id=12345,
        api_hash="mock_hash",
        admin_ids=[111222333],
    )

    mock_oracle = MarketOracle()
    with (
        patch.object(mock_oracle, "get_fear_and_greed", return_value={"value": 72, "classification": "Greed", "risk_modifier": 1.10}),
        patch.object(mock_oracle, "get_defillama_coin_prices", return_value={"solana": 195.50, "base_eth": 2720.00}),
        patch.object(mock_oracle, "get_defillama_chain_tvl", return_value={"solana": 9.1e9, "base": 3.4e9}),
    ):
        ingester.set_market_oracle(mock_oracle)

        # Mock Telegram Event
        mock_event = MagicMock()
        mock_event.is_private = True
        mock_event.out = False
        mock_event.sender_id = 111222333
        mock_event.text = "/oracle"
        mock_event.raw_text = "/oracle"

        replies: list[str] = []

        async def capture_reply(event: Any, text: str, **kwargs: Any) -> None:
            replies.append(text)

        with patch.object(ingester, "_safe_reply", side_effect=capture_reply):
            await ingester._handle_dm_message(mock_event)

        assert len(replies) >= 1
        final_reply = replies[-1]
        assert "Fear & Greed Index" in final_reply
        assert "72/100" in final_reply
        assert "Greed" in final_reply
        assert "1.10x" in final_reply
        assert "SOL:" in final_reply
        assert "$195.50" in final_reply
        assert "Solana TVL:" in final_reply
        assert "$9.10B" in final_reply

    await mock_oracle.close()
