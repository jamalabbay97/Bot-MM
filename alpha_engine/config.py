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

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Risk and Portfolio State Machine Constants
MAX_ACTIVE_POSITIONS: int = 4
MAX_PORTFOLIO_EXPOSURE_PCT: float = 0.05
MAX_PER_TOKEN_RISK_PCT: float = 0.015
MAX_TOP10_CONCENTRATION: float = 0.20
MAX_TOP10_CONCENTRATION_PUMP_FUN: float = 0.65


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

    base_fallback_rpcs: list[str] = Field(
        default_factory=lambda: ["https://mainnet.base.org", "https://base.llamarpc.com"],
        description="Fallback HTTP RPC endpoints for Base (EVM)",
    )
    solana_fallback_rpcs: list[str] = Field(
        default_factory=lambda: ["https://api.mainnet-beta.solana.com"],
        description="Fallback HTTP RPC endpoints for Solana (SVM)",
    )

    # -------------------------------------------------------------------------
    # 2. Private Builder & MEV Protection Endpoints
    # -------------------------------------------------------------------------
    flashbots_rpc: str = Field(
        default="https://rpc.flashbots.net",
        description="Flashbots private EVM builder RPC",
    )
    titan_builder_rpc: str = Field(
        default="https://rpc.titanbuilder.xyz",
        description="Titan private builder RPC",
    )
    mev_blocker_rpc: str = Field(
        default="https://rpc.mevblocker.io",
        description="MEV Blocker private RPC",
    )
    jito_tip_floor_url: str = Field(
        default="https://bundles.jito.wtf/api/v1/bundles/tip_floor",
        description="Jito Bundles dynamic tip floor API endpoint",
    )

    # -------------------------------------------------------------------------
    # 3. DEX Protocol Addresses
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
    # 4. Telegram & X (Twitter) Scraper Credentials
    # -------------------------------------------------------------------------
    x_bearer_token: Optional[str] = Field(
        default=None,
        description="Twitter/X API v2 Bearer Token for sentiment streaming",
    )
    x_api_key: Optional[str] = Field(
        default=None,
        description="Twitter/X API Consumer Key",
    )
    x_api_secret: Optional[str] = Field(
        default=None,
        description="Twitter/X API Consumer Secret",
    )
    telegram_api_id: Optional[int] = Field(
        default=None,
        description="Telegram API ID from my.telegram.org",
    )
    telegram_api_hash: Optional[str] = Field(
        default=None,
        description="Telegram API Hash from my.telegram.org",
    )
    telegram_session_name: str = Field(
        default="alpha_engine_listener",
        description="Telethon session identifier file name",
    )
    telegram_session_string: Optional[str] = Field(
        default=None,
        description="Telethon StringSession for persistent authentication without session file",
    )
    telegram_bot_token: Optional[str] = Field(
        default=None,
        description="Optional Telegram Bot Token (from @BotFather) for bot-mode authentication",
    )
    telegram_channels: list[str] | str = Field(
        default_factory=lambda: [
            "insiderpaper",
            "disclosetv",
            "TreeNewsFeed",
            "lookonchain",
            "WatcherGuru",
        ],
        description="Target alpha channels/groups to scrape (comma-separated or JSON array)",
    )
    telegram_admin_ids: list[int] = Field(
        default_factory=list,
        description="Authorized Telegram user IDs for interactive admin bot controls (comma-separated)",
    )

    # -------------------------------------------------------------------------
    # 5. AlphaSupervisor-AI Autonomous Risk Engine
    # -------------------------------------------------------------------------
    ai_supervisor_enabled: bool = Field(
        default=True,
        description="Enable AlphaSupervisor-AI autonomous risk engine and signal vetting",
    )
    ai_provider: str = Field(
        default="gemini",
        description="AI provider for AlphaSupervisor-AI (gemini, openai, openrouter)",
    )
    gemini_api_key: Optional[str] = Field(
        default=None,
        description="Google Gemini API key for AlphaSupervisor-AI",
    )
    ai_api_key: Optional[str] = Field(
        default=None,
        description="Generic AI API key for supervisor engine",
    )
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI / OpenRouter API key for supervisor engine",
    )
    ai_model: str = Field(
        default="gemini-2.5-flash",
        description="AI model to query for AlphaSupervisor-AI supervision",
    )
    ai_timeout_s: float = Field(
        default=4.0,
        ge=0.5,
        le=30.0,
        description="Max latency timeout in seconds for AI supervisor response before falling back to local deterministic engine",
    )
    ai_strict_veto: bool = Field(
        default=True,
        description="Enforce strict veto if AI supervisor decision is PASS",
    )

    # -------------------------------------------------------------------------
    # 6. Engine Operations & Logging
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
    # 4b. Staging Buffer & Wave-2 Dip-Reversal Engine Configuration
    # -------------------------------------------------------------------------
    wave2_staging_enabled: bool = Field(
        default=True,
        description="Enable Wave-2 Dip-Reversal and accumulation breakout monitoring",
    )
    wave2_ttl_seconds: float = Field(
        default=10800.0,
        ge=3600.0,
        le=43200.0,
        description="Adjustable TTL (1 to 6+ hours) for staged tokens in Wave-2 buffer",
    )
    wave2_volume_surge_multiplier: float = Field(
        default=2.5,
        ge=1.5,
        le=5.0,
        description="Volume surge multiplier threshold k: Volume_5m > k * SMA_15m",
    )
    wave2_min_buy_delta_pct: float = Field(
        default=65.0,
        ge=50.0,
        le=90.0,
        description="Net buy volume percentage threshold (>65%) in recent window",
    )
    wave2_max_top10_concentration_pct: float = Field(
        default=25.0,
        ge=5.0,
        le=50.0,
        description="Maximum top-10 non-pool holder concentration percentage (<25%)",
    )
    wave2_min_consolidation_duration_s: float = Field(
        default=900.0,
        ge=300.0,
        le=7200.0,
        description="Minimum consolidation duration in seconds (default: 15 mins)",
    )

    # -------------------------------------------------------------------------
    # 5. Quantitative Risk & Sizing Parameters
    # -------------------------------------------------------------------------
    max_active_positions: int = Field(
        default=3,
        ge=1,
        le=50,
        description="Hard cap on concurrent active positions across all chains (default: 3)",
    )
    max_portfolio_exposure_pct: float = Field(
        default=0.05,
        ge=0.001,
        le=1.0,
        description="Maximum total portfolio exposure across all open lots (5%)",
    )
    max_per_token_risk_pct: float = Field(
        default=0.015,
        ge=0.001,
        le=0.20,
        description="Maximum capital allocated per signal/token (1.5%)",
    )
    circuit_breaker_max_losses: int = Field(
        default=3,
        ge=1,
        description="Consecutive losses triggering circuit breaker freeze (default: 3)",
    )
    circuit_breaker_drawdown_pct: float = Field(
        default=0.08,
        ge=0.01,
        le=0.50,
        description="Rolling daily drawdown triggering circuit breaker (default: 8%)",
    )
    circuit_breaker_freeze_duration_s: float = Field(
        default=3600.0,
        ge=60.0,
        description="Duration in seconds to freeze new buys when circuit breaker trips (60 mins)",
    )
    max_portfolio_risk_pct: float = Field(
        default=0.01,
        ge=0.0001,
        le=0.10,
        description="Maximum risk allocation per trade (Half-Kelly cap, default: 1%)",
    )
    max_autonomous_risk_pct: float = Field(
        default=0.05,
        ge=0.0001,
        le=0.10,
        description="Maximum autonomous portfolio risk allocation per trade (2% to 5%)",
    )
    stop_loss_pct: float = Field(
        default=-0.08,
        le=-0.01,
        ge=-0.50,
        description="Initial hard stop-loss threshold (default: -8%)",
    )
    emergency_stop_pct: float = Field(
        default=-0.25,
        le=-0.05,
        ge=-0.90,
        description="Emergency hard stop threshold during grace period (default: -25%)",
    )
    exit_grace_period_sec: float = Field(
        default=15.0,
        ge=0.0,
        description="Grace period in seconds after entry to suppress micro-tick noise stop-outs (default: 15s)",
    )
    dns_doh_servers: list[str] | str = Field(
        default_factory=lambda: ["1.1.1.1", "8.8.8.8"],
        description="Trusted upstream DoH/DNS servers to bypass local sinkholes",
    )
    observation_window_min_sec: float = Field(
        default=30.0,
        ge=1.0,
        description="Minimum observation window in seconds before graduating staged launch in PendingLaunchBuffer (default: 30s)",
    )
    observation_window_max_sec: float = Field(
        default=90.0,
        ge=5.0,
        validation_alias=AliasChoices("observation_window_max_sec", "observation_window_sec"),
        description="Maximum observation window in seconds before dropping staged launch in PendingLaunchBuffer (default: 90s)",
    )
    buffer_target_volume_sol: Decimal = Field(
        default=Decimal("0.8"),
        gt=Decimal(0),
        validation_alias=AliasChoices("buffer_target_volume_sol", "target_vol_sol"),
        description="Target cumulative native volume in SOL to graduate staged launch (default: 0.8 SOL)",
    )
    buffer_min_buys: int = Field(
        default=3,
        ge=1,
        validation_alias=AliasChoices("buffer_min_buys", "min_buys"),
        description="Minimum buy transactions required to graduate staged launch in PendingLaunchBuffer (default: 3)",
    )
    buffer_min_unique_buyers: int = Field(
        default=2,
        ge=1,
        validation_alias=AliasChoices("buffer_min_unique_buyers", "min_unique_buyers", "min_buyers"),
        description="Minimum distinct buyers required to graduate a staged launch in PendingLaunchBuffer (default: 2)",
    )
    slot_bundle_threshold: float = Field(
        default=0.60,
        ge=0.0,
        le=1.0,
        description="Max allowed fraction of initial buys in the same slot before flagging DEV_BUNDLED",
    )

    # -------------------------------------------------------------------------
    # 5b. Security Gatekeeper & Negative Cache Settings
    # -------------------------------------------------------------------------
    MAX_TOP10_CONCENTRATION: float = 0.20
    MAX_TOP10_CONCENTRATION_PUMP_FUN: float = 0.65

    max_top10_concentration: float = Field(
        default=0.20,
        ge=0.0,
        le=1.0,
        description="Max Top 10 holder concentration for standard tokens (default: 20%)",
    )
    max_pump_fun_top10_concentration: float = Field(
        default=0.65,
        ge=0.0,
        le=1.0,
        description="Max Top 10 holder concentration for Pump.fun tokens excluding bonding curve (default: 65%)",
    )
    gatekeeper_negative_cache_ttl_s: float = Field(
        default=300.0,
        gt=0.0,
        description="TTL for gatekeeper negative rejection cache in seconds (default: 300s)",
    )
    gatekeeper_negative_cache_maxsize: int = Field(
        default=5000,
        ge=100,
        description="Max capacity for gatekeeper negative cache (default: 5000 entries)",
    )

    # -------------------------------------------------------------------------
    # 5c. Trade Frequency & Pacing Governor Settings
    # -------------------------------------------------------------------------
    pacing_max_initial_trades: int = Field(
        default=10,
        ge=1,
        description="Maximum initial consecutive trades before activating pacing cooldown",
    )
    pacing_cooldown_hours: float = Field(
        default=4.0,
        ge=0.0,
        description="Mandatory cooldown in hours after completing initial trades (PACING_COOLDOWN_HOURS)",
    )
    pacing_interval_hours: float = Field(
        default=3.0,
        ge=0.0,
        description="Cooldown in hours between subsequent paced trades (PACING_INTERVAL_HOURS)",
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
        default_channels = [
            "insiderpaper",
            "disclosetv",
            "TreeNewsFeed",
            "lookonchain",
            "WatcherGuru",
        ]
        if v is None:
            return list(default_channels)
        if isinstance(v, str):
            clean = v.strip()
            if not clean:
                return list(default_channels)
            return [ch.strip() for ch in clean.split(",") if ch.strip()]
        if isinstance(v, (list, tuple, set, Sequence)):
            cleaned = [str(ch).strip() for ch in v if str(ch).strip()]
            return cleaned if cleaned else list(default_channels)
        return list(default_channels)

    @field_validator("base_fallback_rpcs", "solana_fallback_rpcs", mode="before")
    @classmethod
    def parse_fallback_rpcs(cls, v: Any) -> list[str]:
        """Convert comma-separated strings or list into list[str]."""
        if v is None:
            return []
        if isinstance(v, str):
            clean = v.strip()
            if not clean:
                return []
            return [rpc.strip() for rpc in clean.split(",") if rpc.strip()]
        if isinstance(v, (list, tuple, set, Sequence)):
            cleaned = [str(rpc).strip() for rpc in v if str(rpc).strip()]
            return cleaned
        return []

    @field_validator("dns_doh_servers", mode="before")
    @classmethod
    def parse_dns_doh_servers(cls, v: Any) -> list[str]:
        """Convert comma-separated strings or list into list[str]."""
        default_servers = ["1.1.1.1", "8.8.8.8"]
        if v is None:
            return list(default_servers)
        if isinstance(v, str):
            clean = v.strip()
            if not clean:
                return list(default_servers)
            return [s.strip() for s in clean.split(",") if s.strip()]
        if isinstance(v, (list, tuple, set, Sequence)):
            cleaned = [str(s).strip() for s in v if str(s).strip()]
            return cleaned if cleaned else list(default_servers)
        return list(default_servers)

    @field_validator("telegram_admin_ids", mode="before")
    @classmethod
    def parse_telegram_admin_ids(cls, v: Any) -> list[int]:
        """Convert comma-separated strings or list of int/str to list[int]."""
        if v is None:
            return []
        if isinstance(v, str):
            clean = v.strip()
            if not clean:
                return []
            ids: list[int] = []
            for item in clean.split(","):
                item_s = item.strip()
                if item_s.lstrip("-").isdigit():
                    ids.append(int(item_s))
            return ids
        if isinstance(v, (list, tuple, set, Sequence)):
            res: list[int] = []
            for item in v:
                if isinstance(item, int):
                    res.append(item)
                elif isinstance(item, str) and item.strip().lstrip("-").isdigit():
                    res.append(int(item.strip()))
            return res
        if isinstance(v, int):
            return [v]
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

            sol_ws = data.get("solana_rpc_ws") or os.getenv("SOLANA_RPC_WS", "")
            if isinstance(sol_ws, str) and "solana-mainnet.g.alchemy.com" in sol_ws:
                data["solana_rpc_ws"] = sol_ws.replace(
                    "solana-mainnet.g.alchemy.com", "solana-mainnet.streaming.alchemy.com"
                )

            # Failover RPC list mappings
            if "base_rpc_failover_urls" in data and "base_fallback_rpcs" not in data:
                data["base_fallback_rpcs"] = data["base_rpc_failover_urls"]
            elif os.getenv("BASE_RPC_FAILOVER_URLS") and "base_fallback_rpcs" not in data:
                data["base_fallback_rpcs"] = os.environ["BASE_RPC_FAILOVER_URLS"]

            if "solana_rpc_failover_urls" in data and "solana_fallback_rpcs" not in data:
                data["solana_fallback_rpcs"] = data["solana_rpc_failover_urls"]
            elif os.getenv("SOLANA_RPC_FAILOVER_URLS") and "solana_fallback_rpcs" not in data:
                data["solana_fallback_rpcs"] = os.environ["SOLANA_RPC_FAILOVER_URLS"]

            # Initial Balances
            if os.getenv("INITIAL_SOL_BALANCE") and "initial_sol" not in data:
                data["initial_sol"] = os.environ["INITIAL_SOL_BALANCE"]
            if os.getenv("INITIAL_ETH_BALANCE") and "initial_eth" not in data:
                data["initial_eth"] = os.environ["INITIAL_ETH_BALANCE"]

            # Bot token detection if inadvertently placed in session name
            sess = data.get("telegram_session_name") or os.getenv("TELEGRAM_SESSION_NAME")
            if sess and ":" in str(sess):
                if not data.get("telegram_bot_token"):
                    data["telegram_bot_token"] = str(sess).strip()
                data["telegram_session_name"] = "alpha_engine_listener"
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
    def min_unique_buyers(self) -> int:
        """Backward compatibility alias for buffer_min_unique_buyers."""
        return self.buffer_min_unique_buyers

    @property
    def min_buyers(self) -> int:
        """Alias for buffer_min_unique_buyers."""
        return self.buffer_min_unique_buyers

    @property
    def min_buys(self) -> int:
        """Alias for buffer_min_buys."""
        return self.buffer_min_buys

    @property
    def target_vol_sol(self) -> Decimal:
        """Alias for buffer_target_volume_sol."""
        return self.buffer_target_volume_sol

    @property
    def observation_window_sec(self) -> float:
        """Alias for observation_window_max_sec."""
        return self.observation_window_max_sec

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

