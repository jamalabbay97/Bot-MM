"""
alpha_engine.config — Engine Configuration & Environment Loading
================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+
"""

from __future__ import annotations

import os
from decimal import Decimal


def _require_env(key: str) -> str:
    """Read a required environment variable; raise clearly if missing."""
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            "Set it in your shell or a .env file before starting the engine."
        )
    return val


def _optional_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


class EngineConfig:
    """
    Central configuration object built from environment variables.
    """

    def __init__(self) -> None:
        self.alchemy_ws_url: str = _require_env("ALCHEMY_WS_URL")
        self.alchemy_http_url: str = _require_env("ALCHEMY_HTTP_URL")
        self.helius_ws_url: str = _require_env("HELIUS_WS_URL")
        self.aerodrome_router: str = _require_env("AERODROME_ROUTER")
        self.weth_address: str = _require_env("WETH_ADDRESS")

        self.db_path: str = _optional_env("DB_PATH", "paper_trading.db")
        self.initial_sol: Decimal = Decimal(_optional_env("INITIAL_SOL_BALANCE", "10.0"))
        self.initial_eth: Decimal = Decimal(_optional_env("INITIAL_ETH_BALANCE", "2.0"))
        self.sol_price_usd: Decimal = Decimal(_optional_env("SOL_PRICE_USD", "150.0"))
        self.eth_price_usd: Decimal = Decimal(_optional_env("ETH_PRICE_USD", "3400.0"))
        self.snapshot_interval_s: float = float(_optional_env("SNAPSHOT_INTERVAL_S", "60"))
        self.log_level: str = _optional_env("LOG_LEVEL", "INFO")

    @property
    def initial_equity_usd(self) -> Decimal:
        """Total starting portfolio equity in USD."""
        return (
            self.initial_sol * self.sol_price_usd
            + self.initial_eth * self.eth_price_usd
        )
