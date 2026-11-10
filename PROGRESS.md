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
- [x] reproduced
  - `runner.py`: Constructed `PaperExecutor(native_price_usd=cfg.eth_price_usd)` and passed `fixed_trade_amount_native=None` for autonomous buys.
  - Sizing occurred twice: runner computed `preliminary_cost` with per-chain price, but executor silently re-sized via Kelly using `self._native_price_usd` ($3,400 ETH price).
  - With default config (equity = $8,300, SOL = $150, ETH = $3,400), runner balance-checked 1% on SOL ($83 / $150 = 0.5533 SOL), but executor bought 1% of $8300 / $3400 = 0.0244 SOL (22.667x discrepancy).
  - `PaperExecutor` defaulted `win_rate=0.5, avg_win=0.1, avg_loss=0.05`, fabricating positive Kelly edge before trades occurred.
- [x] red test
  - `tests/test_issue2_execution_sizing.py`: 6 tests created covering:
    1. `test_paper_executor_requires_price_service_and_no_update_native_price`: constructing without `price_service` fails; `native_price_usd` and `update_native_price()` removed.
    2. `test_sol_and_eth_sizes_inversely_proportional_to_prices`: SOL and ETH trades with equal USD risk produce native sizes inversely proportional to prices.
    3. `test_kelly_calibration_and_shrinkage`: conservative uncalibrated fraction (<30 closed trades), Kelly with Bayesian shrinkage toward 0.5 (>=30 closed trades), negative edge rejection.
    4. `test_sized_equals_balance_checked_equals_filled_all_modes`: exact equality of sized, balance-checked, and filled amounts across autonomous, manual-USD, and manual-native modes.
    5. `test_integration_runner_signal_processing_per_chain_pricing`: end-to-end runner signal processing with SOL ($200) vs Base/ETH ($4,000).
    6. `test_regression_22_7x_discrepancy_eliminated`: verifies 22.7x audit discrepancy is resolved.
- [x] fix
  - `alpha_engine/pricing/native_price_service.py`: Dynamic per-chain pricing injection.
  - `alpha_engine/execution/executor.py`: Removed `native_price_usd` and `update_native_price()`; injected `price_service`; deleted duplicate Kelly sizing from `execute_signal()`; enforced explicit `fixed_trade_amount_native` on BUY; updated fill `kelly_fraction` to reflect real equity fraction.
  - `alpha_engine/engine/strategy/risk_manager.py`: Established `calculate_order_size()` as single sizing authority; added `update_ledger_stats()`; implemented uncalibrated fixed fraction gating (<30 closed trades) and Bayesian shrinkage of win rate toward 0.5.
  - `alpha_engine/math/sizing.py`: Added `shrink_win_rate()`; updated `half_kelly_fraction()` to use explicit 0.5 multiplier.
  - `alpha_engine/engine/runner.py`: Balance-check exact sized amount from `risk_engine.calculate_order_size()`, pass explicit size and equity fraction to executor, update open exposure USD per-chain.
  - `alpha_engine/config.py`: Added `kelly_min_closed_trades` (30), `kelly_uncalibrated_fraction` (0.01), `kelly_multiplier` (0.5), `kelly_shrinkage_trades` (20).
- [x] green test
  - `.venv/bin/pytest tests/test_issue2_execution_sizing.py` -> 6 passed in 22.53s.
  - `.venv/bin/pytest` -> 442 passed in 118.82s.
- [x] mutation check
  - Mutation 1: Inverted calibration check in `risk_manager.py` (`if total_closed >= min_closed`); verified `test_kelly_calibration_and_shrinkage` failed immediately (`AssertionError: assert not True`); restored and verified green.
  - Mutation 2: Inverted per-chain pricing in `runner.py` to always size with ETH price; verified `test_integration_runner_signal_processing_per_chain_pricing` failed with `assert Decimal('0.05') == Decimal('1.0')`; restored and verified green.
- [x] sweep done
  - Swept all `PaperExecutor` constructor usages across test suite (`test_manual_trade_amount_and_balance.py`, `test_platform_identification.py`, `test_strict_realtime_pricing.py`, `test_hft_risk_and_exits.py`, `test_audit_remediation_and_concurrency.py`, `test_strategy_v2_integration.py`, `test_pending_launch_buffer_telemetry_and_paper_exec.py`, `test_enterprise_engine_hardening.py`, `test_dex_trading_integration.py`, `test_execution_pricing_and_dedup.py`).
  - Audited `alpha_engine/ruflo/swarm.py` and replaced static fallbacks with price service.
  - Ran `tools/audit/native_price_literals.py` -> 0 findings across repository.
- [x] wiring proof
  - `runner.py`: `PaperTradingEngine` instantiates `PaperExecutor(position_book=self._position_book, price_service=self._native_price_service)` and passes `self._executor` to `RufloSwarmCoordinator`.
  - Signal processing loop calls `self.risk_engine.calculate_order_size(...)` with live per-chain `native_p` from `self._native_price_service.get_price(signal_item.chain)`, balance-checks `sizing_res.native_trade_size`, and forwards `fixed_trade_amount_native=sizing_res.native_trade_size` to `executor.execute_signal(...)`.
- [x] README/docs updated
  - `README.md`: Reconciled Half-Kelly equation to $f^* = \frac{p \cdot b - q}{b} \times 0.5$, documented uncalibrated phase (<30 trades) and Bayesian shrinkage.
  - `.env.example`: Added `KELLY_MIN_CLOSED_TRADES`, `KELLY_UNCALIBRATED_FRACTION`, `KELLY_MULTIPLIER`, `KELLY_SHRINKAGE_TRADES`.

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

