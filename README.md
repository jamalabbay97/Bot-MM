# Bot-MM: Multi-Chain Paper Trading & Alpha Analytics Engine

A production-grade, event-driven, multi-chain Paper Trading and Alpha Analytics Engine engineered in Python 3.11+. The engine operates in a zero-capital simulation environment with local state persistence (SQLite in WAL mode), strictly adhering to the free-tier quotas of public/developer RPC providers for Solana (Helius) and Base (Alchemy).

---

## 🏛️ Modular Package Architecture

The engine is structured as a standard Python package (`alpha_engine`) with strict separation of concerns, high cohesion, and low coupling:

```text
Bot-MM/
├── pyproject.toml               # Package build configuration & CLI registration
├── README.md                    # System architecture documentation
│
├── alpha_engine/                # Core modular package
│   ├── __init__.py              # Unified public API export
│   ├── __main__.py              # Entry point for python -m alpha_engine
│   ├── cli.py                   # Command-line interface launcher
│   ├── config.py                # Environment configuration & parameter parsing
│   │
│   ├── models/                  # Domain Types & Event Schemas
│   │   ├── __init__.py          # Model re-exports
│   │   ├── base.py              # Strict Pydantic v2 configuration
│   │   ├── enums.py             # ChainIdentifier, OrderSide, SecurityTier, SignalStrength
│   │   ├── events.py            # SwapEvent, SignalEvent, PoolStateUpdateEvent, ShutdownSentinel
│   │   └── state.py             # PoolState, SecurityReport, PaperFill, TradeRecord, PortfolioSnapshot
│   │
│   ├── rate_limiter/            # RPC Token-Bucket Quota Management
│   │   ├── __init__.py          # Rate limiter re-exports
│   │   ├── bucket.py            # AsyncTokenBucket with continuous leak & adaptive sleep
│   │   └── registry.py          # RateLimiterRegistry (Alchemy Base 25 CU/s & Helius 10 req/s)
│   │
│   ├── math/                    # Quantitative Math & Risk Mechanics
│   │   ├── __init__.py          # Math re-exports
│   │   ├── cpmm.py              # Constant-product AMM mechanics, quotes & clamped impact
│   │   ├── mev.py               # Stochastic latency sampling & LogNormal MEV burst model
│   │   └── sizing.py            # Half-Kelly position sizing, gas-drag gate & alpha classification
│   │
│   ├── security/                # Dual-Tier Security Gatekeeper
│   │   ├── __init__.py          # Security re-exports
│   │   ├── constants.py         # Gate thresholds, API URLs, Router & ERC-20 ABIs
│   │   ├── goplus.py            # Base/EVM GoPlus security API parser (LP burn, taxes, mint)
│   │   ├── rugcheck.py          # Solana/SVM RugCheck API parser
│   │   ├── preflight.py         # Tier-2 local eth_call honeypot simulation
│   │   └── gatekeeper.py        # SecurityGatekeeper pipeline orchestrator
│   │
│   ├── ingestion/               # Resilient Dual-Chain WebSocket Ingestion
│   │   ├── __init__.py          # Ingestion re-exports
│   │   ├── decoders.py          # Uniswap/Aerodrome Swap/Sync decoders & Raydium parser
│   │   ├── evm.py               # Base EVM WebSocket ingester (topics: Swap, Sync)
│   │   ├── svm.py               # Solana SVM WebSocket ingester (targeted pool subscriptions)
│   │   └── coordinator.py       # IngestionCoordinator with jittered exponential backoff
│   │
│   ├── execution/               # Paper Execution & Persistence
│   │   ├── __init__.py          # Execution re-exports
│   │   ├── book.py              # In-memory PositionBook (FIFO lots) & RunningMetrics
│   │   ├── executor.py          # PaperExecutor (fills with latency and impact)
│   │   └── ledger.py            # SQLiteLedger (WAL mode, indexed trade & snapshot tables)
│   │
│   └── engine/                  # System Coordination & Orchestration
│       ├── __init__.py          # Engine re-exports
│       ├── registry.py          # In-memory dynamic PoolRegistry
│       ├── signals.py           # Alpha SignalGenerator
│       └── runner.py            # PaperTradingEngine asynchronous orchestrator
│
├── tests/                       # Automated Test Suite
│   └── test_modular_architecture.py
│
└── Backward-Compatibility Facades (Root Level):
    ├── models.py                # Re-exports alpha_engine.models
    ├── rate_limiter.py          # Re-exports alpha_engine.rate_limiter
    ├── quant_math.py            # Re-exports alpha_engine.math
    ├── security.py              # Re-exports alpha_engine.security
    ├── ingestion.py             # Re-exports alpha_engine.ingestion
    ├── execution.py             # Re-exports alpha_engine.execution
    └── engine.py                # Re-exports alpha_engine.engine
```

---

## ⚡ Installation & Setup

1. **Activate Virtual Environment:**
   ```bash
   source .venv/bin/activate
   ```

2. **Install in Editable Mode:**
   ```bash
   pip install -e .
   ```

3. **Configure Environment Variables (`.env` or shell):**
   ```bash
   export ALCHEMY_WS_URL="wss://base-mainnet.g.alchemy.com/v2/<KEY>"
   export ALCHEMY_HTTP_URL="https://base-mainnet.g.alchemy.com/v2/<KEY>"
   export HELIUS_WS_URL="wss://mainnet.helius-rpc.com/?api-key=<KEY>"
   export AERODROME_ROUTER="0xcF77a3Ba9A5CA399B7c97c74884691038574C017"
   export WETH_ADDRESS="0x4200000000000000000000000000000000000006"
   ```

---

## 🚀 Running the Engine

### Via Command-Line Interface:
```bash
alpha-engine --help
alpha-engine --demo
```

### Via Python Module:
```bash
python -m alpha_engine
```

### Programmatic Usage:
```python
from decimal import Decimal
from alpha_engine import (
    ChainIdentifier,
    OrderSide,
    PoolState,
    cpmm_buy_quote,
    simulate_latency,
    compute_position_size,
)

pool = PoolState(
    pool_address="0x6c561b446416e1a00e8e93e221854d6ea4171372",
    chain=ChainIdentifier.BASE_MAINNET,
    native_reserve=Decimal("1500"),
    token_reserve=Decimal("5000000"),
    fee_numerator=3,
    fee_denominator=1000,
    last_updated_block=20_000_000,
    token_decimals=6,
    native_decimals=18,
)

# 1. Deterministic CPMM Quote in NATIVE_PER_TOKEN
quote = cpmm_buy_quote(pool, Decimal("1.0"))
print(f"Acquired: {quote.amount_out} tokens @ {quote.execution_price} ETH/token | Impact: {quote.price_impact_bps} bps")

# 2. Stochastic MEV Burst Model
latency = simulate_latency(pool, signal_timestamp_ns=1_000_000_000)
print(f"Sampled Latency: {latency.latency_ms} ms | MEV Front-Run Fraction: {latency.frontrun_fraction * 100:.3f}%")
```

---

## 🧪 Running Validation Tests

Execute the automated test suite verifying all 8 architectural and mathematical test cases:
```bash
python tests/test_modular_architecture.py
```
