# Baseline Test Suite & Static Audit Report

**Date:** 2026-11-08  
**Project:** Bot-MM (alpha_engine)  
**Python Version:** Python 3.14.4  
**Test Framework:** pytest 9.1.1 (with pytest-asyncio 1.4.0, anyio 4.15.1, pytest-timeout 2.4.0)  

---

## 1. Environment & Dependency Status

- **Virtual Environment:** Configured at `.venv`
- **Network Access:** Operational.
- **Dependency Status:**
  - All production dependencies from `requirements.txt` and development dependencies from `requirements-dev.txt` installed and verified cleanly via `pip check`.
  - Added `pytest-timeout 2.4.0` to prevent indefinite test runner hanging caused by pre-existing database lock deadlocks.

---

## 2. Test Suite Execution Summary

Executed the complete pytest test suite across all 53 test modules under `tests/`:

```
=========================== short test summary info ============================
FAILED tests/test_autonomous_engine.py::test_adaptive_feedback_wallet_pruning
FAILED tests/test_concurrency_and_correlation.py::test_50_concurrent_async_sqlite_tasks[asyncio]
FAILED tests/test_observability_dashboard.py::test_smart_wallet_endpoints
FAILED tests/test_profiler_and_rate_limiter.py::test_wallet_profiler_wash_trading_3_hop_bfs
FAILED tests/test_profiler_and_rate_limiter.py::test_wallet_profiler_approved_whitelist
============= 5 failed, 425 passed, 1 warning in 161.22s (0:02:41) =============
```

- **Total Collected Tests:** 430
- **Passed:** 425 (98.8%)
- **Failed:** 5 (1.2%)
- **Errors:** 0
- **Warnings:** 1 (`StarletteDeprecationWarning` regarding `httpx` with `starlette.testclient`)

---

## 3. Pre-Existing Failures & Root Cause Analysis

All 5 pre-existing test failures were diagnosed with exact root causes:

### 1. `tests/test_autonomous_engine.py::test_adaptive_feedback_wallet_pruning`
- **Failure Type:** Timeout / Hang (>10.0s)
- **Root Cause:** Re-entrancy deadlock in `WhitelistDatabase` (`alpha_engine/profiler/whitelist_db.py`). The method `update_status` is decorated with `@sqlite_retry` (from `alpha_engine/execution/ledger.py`), which acquires `self._lock`. Inside `update_status`, it calls `await self.connect()`, which is **also** decorated with `@sqlite_retry`. Because `self._lock = asyncio.Lock()` is a non-reentrant lock, the second attempt to acquire `self._lock` deadlocks forever. Furthermore, the test assertion attempts `updated["status"]`, whereas `get_wallet` returns a `WhitelistRecord` pydantic model object, not a dictionary.

### 2. `tests/test_concurrency_and_correlation.py::test_50_concurrent_async_sqlite_tasks[asyncio]`
- **Failure Type:** Timeout / Hang (>10.0s)
- **Root Cause:** Re-entrancy deadlock in `WhitelistDatabase.upsert_wallet`. `upsert_wallet` is decorated with `@sqlite_retry` (which acquires `self._lock`) and immediately calls `await self.connect()`, which is also decorated with `@sqlite_retry` and attempts to re-acquire `self._lock`. With 50 concurrent tasks, all tasks hitting `upsert_wallet` deadlock on the non-reentrant `asyncio.Lock`.

### 3. `tests/test_observability_dashboard.py::test_smart_wallet_endpoints`
- **Failure Type:** Timeout / Hang (>10.0s)
- **Root Cause:** Event loop deadlock between Starlette synchronous `TestClient` and `async def` pytest function. The test function is declared as `async def test_smart_wallet_endpoints()` with `@pytest.mark.asyncio`, but instantiates Starlette's synchronous `TestClient(app)`. `TestClient` attempts to create an AnyIO blocking portal (`run_blocking_portal`) within the running asyncio event loop, causing AnyIO to block indefinitely on thread synchronization.

### 4. `tests/test_profiler_and_rate_limiter.py::test_wallet_profiler_wash_trading_3_hop_bfs`
- **Failure Type:** Timeout / Hang (>10.0s)
- **Root Cause:** Re-entrancy deadlock in `WhitelistDatabase.ban_wallet` -> `update_status` -> `connect()`. Both `update_status` and `connect` attempt to acquire `self._lock = asyncio.Lock()`, deadlocking the event loop.

### 5. `tests/test_profiler_and_rate_limiter.py::test_wallet_profiler_approved_whitelist`
- **Failure Type:** Timeout / Hang (>10.0s)
- **Root Cause:** Re-entrancy deadlock in `SmartMoneyProfiler.evaluate_and_whitelist` -> `WhitelistDatabase.upsert_wallet` -> `connect()`. Both methods are wrapped with `@sqlite_retry`, which blocks on non-reentrant `self._lock`.

---

## 4. Static Code Audit Baseline (`run_all.py`)

All 7 audit scripts in `tools/audit/` were executed against the codebase (excluding tests and external environments) and output was saved to `audit_baseline.txt`:

| Audit Script | Description | Finding Count |
| :--- | :--- | :---: |
| `native_price_literals.py` | Hardcoded numeric literals, Decimal prices, and getattr defaults for native prices | 33 |
| `task_awaitables.py` | Synchronous functions passed to `create_task`/`gather` or awaited | 2 |
| `model_getattr_defaults.py` | `getattr(obj, "field", <default>)` calls and existence against pydantic models | 896 |
| `conditional_precedence.py` | Conditional expressions (`IfExp`) used as condition/compare operand or precedence bug | 51 |
| `swallowed_exceptions.py` | `except Exception` blocks that only `pass`, `continue`, or `logger.debug` | 102 |
| `invented_data.py` | Hardcoded fallback reserves (30.0 SOL, 5.0 ETH), token supplies (1B), or prices (0.000000028) | 150 |
| `unused_config.py` | Unused Settings fields and dead `extra_params` keys | 34 |
| **Total Findings** | **Combined across all audit scripts** | **1,268** |

### Key Audit Script Highlights:
- **`task_awaitables.py` (2 critical hits):**
  1. `alpha_engine/engine/runner.py:1083`: `asyncio.create_task(self.self_learning_engine.on_trade_closed(...))` where `on_trade_closed` is a synchronous `def` returning a `dict`, causing a runtime `TypeError`.
  2. `alpha_engine/security/preflight.py:144`: `await inspect_bytecode_for_delayed_taxes(...)` where `inspect_bytecode_for_delayed_taxes` is defined as a synchronous `def`.
- **`conditional_precedence.py` (51 hits):**
  - Identified the exact promotion gate operator precedence flaw in `alpha_engine/engine/experimentation.py:808`:
    `if m.max_drawdown_pct >= 12.0 if is_champ_underperforming else 8.0:`
    When `is_champ_underperforming` is False, the `if` test evaluates to `8.0` (truthy), making the gate always pass unconditionally.
- **`unused_config.py` (34 hits):**
  - Discovered dead `extra_params` keys in `alpha_engine/engine/experimentation.py` (`min_surge_k`, `min_dormancy_hours`, `min_surge_multiplier`, `max_slippage_bps`, `min_wallet_sortino`, `organic_conviction_min`) that are created by the strategy generator but never read by any downstream logic.

