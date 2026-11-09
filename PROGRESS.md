# Remediation Progress

Track progress across all identified defects and phases. All items must satisfy the Anti-Shallow Contract and Fixing Rules.

---

## Issue 1 Native prices hardcoded
- [x] reproduced
  - `config.py:316`: `sol_price_usd: Decimal = Decimal("150.0")`, `eth_price_usd: Decimal = Decimal("3400.0")` hardcoded defaults
  - `runner.py:__init__`: copied once into `self._sol_price` and never updated
  - `tools/audit/native_price_literals.py`: 33 baseline literal occurrences across codebase
- [x] red test
  - `tests/test_native_price_service.py`: 6 tests created covering mid-run price changes, fail-closed stale pricing, divergence tolerance, secondary source failover, AST literal audit, and runner entry path integration
- [x] fix
  - `alpha_engine/pricing/native_price_service.py`: Single source of truth `NativePriceService` with `DefiLlamaPriceSource`, `CoinbasePriceSource`, `BinancePriceSource`, TTL cache, configurable max age and divergence tolerance
  - `alpha_engine/engine/runner.py`: Lifecycle background refresh loop, fail-closed gating on BUY entry, degraded exit flag, dynamic USD conversions
  - `alpha_engine/execution/book.py`: Added `entry_native_price_usd` and `price_degraded` to `OpenLot`
  - Replaced all non-test static fallbacks in `signals.py`, `screener_priority.py`, `telemetry.py`, `chat_interface.py`, `telegram.py`, `dex_metrics.py`, `execution_agent.py`, `wallet_tracker.py`
- [x] green test
  - `.venv/bin/pytest tests/test_native_price_service.py` -> 6 passed in 8.02s
- [x] mutation check
  - Inverted gating check in `runner.py:4263` (`if False and price_res.status != NativePriceStatus.OK`); verified `test_stale_price_blocks_entries_allows_exits_flagged_degraded` and `test_source_disagreement_beyond_tolerance_blocks_entries` failed; restored source fix and verified green
- [x] sweep done
  - `tools/audit/native_price_literals.py`: 0 findings (`[native_price_literals] Total findings: 0`)
- [x] wiring proof
  - `runner.py:_run_single_cycle` starts `self._native_price_service.start()`
  - Runner teardown calls `self._native_price_service.stop()`
  - Runner entry gate at `_process_signal_queue:4263` guards buys using `self._native_price_service.get_price(signal_item.chain)`
  - `test_integration_runner_entry_path_uses_live_native_price` exercises `runner.manual_enter_position` with live SOL price $200
- [x] README/docs updated
  - `.env.example`: Documented `ALLOW_STATIC_NATIVE_PRICES`, `SOL_PRICE_USD`, `ETH_PRICE_USD`, `NATIVE_PRICE_REFRESH_INTERVAL_S`, `NATIVE_PRICE_CACHE_TTL_S`, `NATIVE_PRICE_MAX_AGE_S`, `NATIVE_PRICE_DIVERGENCE_TOLERANCE_PCT`
  - `README.md`: Documented Section 5 Live Native Price Service & Multi-Source Gating

---

## Issue 2 SOL sized with ETH price + double sizing + Kelly defaults
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 3 Asymmetric/inconsistent cost model
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 4 Latency/MEV does not model price movement
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 5 Fabricated pool data + wrong venue math
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 6 Broken post-trade hook (create_task on sync fn)
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 7 Slippage fed to tuner always 50
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 8 Promotion gate precedence bug + weak statistics
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 9 Strategy generation not real
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 10 Council LLM never called, no real shared memory, model-name rewriting
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Issue 11 Wallet profiling has no data and is not wired
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

---

## Phase 3 Tick recorder, replay, walk-forward, model-vs-reality report, README truthfulness
- [ ] reproduced
- [ ] red test
- [ ] fix
- [ ] green test
- [ ] mutation check
- [ ] sweep done
- [ ] wiring proof
- [ ] README/docs updated

