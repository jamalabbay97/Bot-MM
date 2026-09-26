"""
alpha_engine.config — Centralized Configuration & Secret Management
===================================================================
Multi-Chain Paper Trading & Alpha Analytics Engine (Bot-MM)
Python 3.11+ | pydantic-settings | zero-leak architecture
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Optional, Sequence

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineConfig(BaseSettings):
    """
    Centralized, strongly typed configuration for the Bot-MM trading engine.
    Automatically loads secrets and environment variables from a local `.env` file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # -------------------------------------------------------------------------
    # 1. RPC Endpoints (EVM / Base & SVM / Solana)
    # -------------------------------------------------------------------------
    base_rpc_http: str = Field(
        default="https://base-mainnet.g.alchemy.com/v2/demo",
        description="HTTP JSON-RPC endpoint for Base (EVM)",
    )
    base_rpc_ws: str = Field(
        default="wss://base-mainnet.g.alchemy.com/v2/demo",
        description="WebSocket RPC endpoint for Base (EVM) log subscriptions",
    )
    solana_rpc_http: str = Field(
        default="https://mainnet.helius-rpc.com/?api-key=demo",
        description="HTTP JSON-RPC endpoint for Solana (SVM)",
    )
    solana_rpc_ws: str = Field(
        default="wss://mainnet.helius-rpc.com/?api-key=demo",
        description="WebSocket RPC endpoint for Solana (SVM) log subscriptions",
    )

    # -------------------------------------------------------------------------
    # 2. DEX Protocol Addresses
    # -------------------------------------------------------------------------
    aerodrome_router: str = Field(
        default="0xcF77a3Ba9A5CA399B7c97c749566343833341fdC",
        description="Aerodrome router address on Base",
    )
    weth_address: str = Field(
        default="0x4200000000000000000000000000000000000006",
        description="Canonical WETH contract on Base",
    )

    # -------------------------------------------------------------------------
    # 3. Telegram Scraper Credentials
    # -------------------------------------------------------------------------
    telegram_api_id: Optional[int] = Field(
        default=None,
        description="Telegram API ID from my.telegram.org",
    )
    telegram_api_hash: Optional[str] = Field(
        default=None,
        description="Telegram API Hash from my.telegram.org",
    )
    telegram_session_name: str = Field(
        default="bot_mm_session",
        description="Telethon session identifier file name",
    )
    telegram_bot_token: Optional[str] = Field(
        default=None,
        description="Optional Telegram Bot Token (from @BotFather) for bot-mode authentication",
    )
    telegram_channels: list[str] | str = Field(
        default_factory=list,
        description="Target alpha channels/groups to scrape (comma-separated or JSON array)",
    )

    # -------------------------------------------------------------------------
    # 4. Engine Operations & Logging
    # -------------------------------------------------------------------------
    log_level: str = Field(
        default="INFO",
        description="Application logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    max_queue_size: int = Field(
        default=100,
        ge=10,
        le=10000,
        description="Bounded queue limit for event bus to enforce drop_oldest backpressure",
    )
    db_path: str = Field(
        default="paper_trading.db",
        description="Path to SQLite persistence ledger file",
    )
    snapshot_interval_s: float = Field(
        default=60.0,
        gt=0.0,
        description="Seconds between periodic portfolio snapshots and telemetry heartbeats",
    )

    # -------------------------------------------------------------------------
    # 5. Quantitative Risk & Sizing Parameters
    # -------------------------------------------------------------------------
    max_portfolio_risk_pct: float = Field(
        default=0.01,
        ge=0.0001,
        le=0.10,
        description="Maximum risk allocation per trade (Half-Kelly cap, default: 1%)",
    )
    stop_loss_pct: float = Field(
        default=-0.15,
        le=-0.01,
        ge=-0.50,
        description="Initial hard stop-loss threshold (default: -15%)",
    )

    # -------------------------------------------------------------------------
    # 6. Starting Balances & Price Baseline
    # -------------------------------------------------------------------------
    initial_sol: Decimal = Field(
        default=Decimal("10.0"),
        gt=Decimal(0),
        description="Simulated starting capital in native SOL",
    )
    initial_eth: Decimal = Field(
        default=Decimal("2.0"),
        gt=Decimal(0),
        description="Simulated starting capital in native ETH",
    )
    sol_price_usd: Decimal = Field(
        default=Decimal("150.0"),
        gt=Decimal(0),
        description="Baseline valuation for SOL in USD",
    )
    eth_price_usd: Decimal = Field(
        default=Decimal("3400.0"),
        gt=Decimal(0),
        description="Baseline valuation for ETH in USD",
    )

    # -------------------------------------------------------------------------
    # Coercion & Normalization Validators
    # -------------------------------------------------------------------------
    @field_validator("telegram_channels", mode="before")
    @classmethod
    def parse_telegram_channels(cls, v: Any) -> list[str]:
        """Convert comma-separated strings or empty strings into list[str]."""
        if v is None:
            return []
        if isinstance(v, str):
            clean = v.strip()
            if not clean:
                return []
            return [ch.strip() for ch in clean.split(",") if ch.strip()]
        if isinstance(v, (list, tuple, set)):
            return [str(ch).strip() for ch in v if str(ch).strip()]
        return []

    @field_validator("telegram_api_id", mode="before")
    @classmethod
    def parse_telegram_api_id(cls, v: Any) -> Optional[int]:
        """Coerce raw string integer to int, or None if empty."""
        if v is None or v == "":
            return None
        return int(v)

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_env_fallbacks(cls, data: Any) -> Any:
        """Map legacy environment variable names to new unified names if present."""
        if isinstance(data, dict):
            # Base / Alchemy mappings
            if "alchemy_http_url" in data and "base_rpc_http" not in data:
                data["base_rpc_http"] = data["alchemy_http_url"]
            elif os.getenv("ALCHEMY_HTTP_URL") and "base_rpc_http" not in data:
                data["base_rpc_http"] = os.environ["ALCHEMY_HTTP_URL"]

            if "alchemy_ws_url" in data and "base_rpc_ws" not in data:
                data["base_rpc_ws"] = data["alchemy_ws_url"]
            elif os.getenv("ALCHEMY_WS_URL") and "base_rpc_ws" not in data:
                data["base_rpc_ws"] = os.environ["ALCHEMY_WS_URL"]

            # Solana / Helius mappings
            if "helius_ws_url" in data and "solana_rpc_ws" not in data:
                data["solana_rpc_ws"] = data["helius_ws_url"]
            elif os.getenv("HELIUS_WS_URL") and "solana_rpc_ws" not in data:
                data["solana_rpc_ws"] = os.environ["HELIUS_WS_URL"]

            # Initial Balances
            if os.getenv("INITIAL_SOL_BALANCE") and "initial_sol" not in data:
                data["initial_sol"] = os.environ["INITIAL_SOL_BALANCE"]
            if os.getenv("INITIAL_ETH_BALANCE") and "initial_eth" not in data:
                data["initial_eth"] = os.environ["INITIAL_ETH_BALANCE"]

            # Bot token detection if inadvertently placed in session name
            sess = data.get("telegram_session_name") or os.getenv("TELEGRAM_SESSION_NAME")
            if sess and ":" in str(sess) and not data.get("telegram_bot_token"):
                data["telegram_bot_token"] = str(sess).strip()
                data["telegram_session_name"] = "bot_mm_session"
        return data

    # -------------------------------------------------------------------------
    # Backward Compatibility Facades & Helpers
    # -------------------------------------------------------------------------
    @property
    def alchemy_ws_url(self) -> str:
        return self.base_rpc_ws

    @property
    def alchemy_http_url(self) -> str:
        return self.base_rpc_http

    @property
    def helius_ws_url(self) -> str:
        return self.solana_rpc_ws

    @property
    def initial_equity_usd(self) -> Decimal:
        """Total starting portfolio equity in USD."""
        return (
            self.initial_sol * self.sol_price_usd
            + self.initial_eth * self.eth_price_usd
        )


def _require_env(key: str, default: str | None = None) -> str:
    """Read an environment variable or raise an error if not present and no default provided."""
    val = os.getenv(key, default)
    if val is None:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return val


def _optional_env(key: str, default: str | None = None) -> str | None:
    """Read an optional environment variable returning default if unset."""
    return os.getenv(key, default)

