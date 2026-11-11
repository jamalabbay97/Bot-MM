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
- [x] reproduced
  - `runner.py`: All 3 entry calls (`execute_signal`) omitted `apply_drag` (default `False`, free entry) while all 4 exit calls (`execute_exit`) passed `apply_drag=True`.
  - `sizing.py`: Hardcoded `ASSUMED_SLIPPAGE_PCT = Decimal("0.01")` was added on top of CPMM pool math (double-counting price impact).
  - Docstring/constant mismatch: docstring claimed "5% slippage" and "0.005 gas" while constants defined 1% and 0.0005.
  - `runner.py`: Hardcoded `simulated_fee = Decimal("0.002") if BASE else Decimal("0.005")` in 3 balance check sites.
  - `experimentation.py`: Shadow lots used static `gas_usd = Decimal("0.05")`, fixed 0.10 size, and static 0.05 exit gas.
- [x] red test
  - Created `tests/test_issue3_cost_model.py` with 6 tests:
    1. `test_ast_audit_no_apply_drag_in_codebase`: AST scan across repository ensuring `apply_drag` is completely removed.
    2. `test_symmetric_round_trip_at_zero_price_change`: Verifies a zero-price-change round-trip loses exactly modeled swap fees + CPMM price impact + network fees symmetrically.
    3. `test_entry_and_exit_costs_identical_functions_of_inputs`: Verifies fee schedules and gas formulas are identical functions for BUY and SELL.
    4. `test_each_venue_fee_schedule_applied_and_unknown_venue_fails_closed`: Verifies all venues apply exact fees from config and unknown venues fail closed (`UnknownVenueError`).
    5. `test_shadow_lots_parity_with_live_paper_fills`: Verifies parity between live-paper fills and shadow tournament lots for identical inputs.
    6. `test_balance_check_uses_same_estimate_as_fill`: Verifies balance checks in runner use `cost_model.get_network_fee_native` rather than hardcoded 0.005 / 0.002.
- [x] fix
  - `alpha_engine/config.py`: Added Section 5.1 strongly typed fee schedule (`fee_pump_fun_bps=100`, `fee_pumpswap_bps=30`, `fee_raydium_amm_bps=25`, `fee_raydium_cpmm_bps=25`, `fee_raydium_clmm_bps=25`, `fee_uniswap_v2_bps=30`, `fee_uniswap_v3_bps=30`, `fee_aerodrome_bps=30`), `extra_slippage_bps=0`, and Solana/Base network fee parameters with official URLs and `verified_on: 2026-11-11`.
  - `alpha_engine/models/enums.py`: Added `PUMPSWAP` and `AERODROME` to `ExecutionVenue`. Implemented `resolve_trade_venue(...)` failing closed on unknown venues, avoiding false-positive substrings.
  - `alpha_engine/execution/costs.py`: Created `TradeCostModel`, `TradeCostBreakdown`, `UnknownVenueError`. Calculates CPMM quotes using venue-specific fee rates, price impact from pool math only, extra slippage allowance, and dynamic network fees in native and USD.
  - `alpha_engine/execution/executor.py`: Injected `cost_model: Optional[TradeCostModel]`. Removed `apply_drag` from `execute_signal`, `execute_exit`, `execute_scalp_pipeline`, and `execute_swing_dca_pipeline`. Applied unified cost calculation to both BUY and SELL.
  - `alpha_engine/engine/runner.py`: Initialized `self._cost_model` in `__init__` and wired into `PaperExecutor`. Replaced hardcoded `0.002`/`0.005` in all 3 balance check sites with `self._cost_model.get_network_fee_native()`. Removed `apply_drag=True` from all 4 `execute_exit` calls.
  - `alpha_engine/engine/experimentation.py`: Injected `cost_model` in `StrategyTournamentRegistry`. Replaced hardcoded `gas_usd=0.05` with `cost_model.calculate_trade_costs`. Replaced hardcoded exit gas with symmetric `lot.gas_drag_usd`.
  - `alpha_engine/engine/backtester.py`: Injected `cost_model` in `WalkForwardBacktester` and replaced hardcoded `$0.10` drag with dynamic venue and network fees.
  - `alpha_engine/math/sizing.py`: Fixed constant `ASSUMED_SLIPPAGE_PCT = Decimal("0.0")` to prevent double counting. Reconciled docstrings and constants ($0.02 Solana, 0.0005 gas, 0% extra slippage).
- [x] green test
  - `tests/test_issue3_cost_model.py`: 6/6 passed.
  - Full test suite: 448 passed, 0 failed in 114s.
- [x] mutation check
  - Mutated `alpha_engine/execution/costs.py` line 295 (`if side == OrderSide.BUY: fee_bps = 0`); ran `test_symmetric_round_trip_at_zero_price_change`; verified test failed with `AssertionError: assert Decimal('24.508...') < Decimal('0.000001')`. Restored code and confirmed green.
- [x] sweep done
  - Repo-wide search for `apply_drag` -> 0 occurrences remaining.
  - Repo-wide audit `tools/audit/native_price_literals.py` -> 0 hardcoded native price literals.
  - All test files updated (`tests/test_dex_trading_integration.py`, `tests/test_hft_risk_and_exits.py`, `tests/test_strategy_experimentation_and_swarm.py`).
- [x] wiring proof
  - `runner.py`: `PaperTradingEngine.__init__` creates `self._cost_model = TradeCostModel(config=self.config, price_service=self._native_price_service)` and passes `cost_model=self._cost_model` to `PaperExecutor`.
  - Runner pre-trade balance check uses `self._cost_model.get_network_fee_native(signal_item.chain)` at all 3 balance check sites.
  - `executor.execute_signal` and `executor.execute_exit` call `self._cost_model.calculate_trade_costs(...)` symmetrically.
- [x] README/docs updated
  - `.env.example`: Added Section 9 documenting all venue fee BPS, extra slippage BPS, and network fee parameters.
  - `alpha_engine/math/sizing.py`: Reconciled docstrings and constants.

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
- [x] reproduced
  - `runner.py`: DexScreener/GeckoTerminal pool builders substituted fabricated reserves (`native_reserve = Decimal("30.0")` SOL / `Decimal("5.0")` ETH, `token_reserve = Decimal("1073000000.0")`, `spot_price = Decimal("0.000000028")`) when APIs returned empty/partial JSON.
  - `models/state.py`: `calculate_market_cap_usd` assumed a 1B token supply when `total_supply` was `None`.
  - `math/cpmm.py`: `get_initial_bonding_curve_pool` assumed initial virtual reserves for arbitrary tokens whose curve state was unknown.
  - Wrong pricing math: Concentrated liquidity pools (Uniswap v3, Raydium CLMM, Orca Whirlpool) silently used CPMM $x \cdot y = k$.
- [x] red test
  - Created `tests/test_issue5_pool_data_and_venue_math.py` with 6 exhaustive tests:
    1. `test_missing_liquidity_price_supply_rejects_and_counts_telemetry`: missing liquidity/price returns `None`, logs rate-limited WARNING, increments telemetry counters (`no_liquidity_data`, `no_price`, `stale_state`).
    2. `test_derived_reserves_policy_enforcement`: verifies `ReserveType.DERIVED` provenance and tests both `"reject"` (default) and `"cap_size"` policies.
    3. `test_unsupported_venue_rejected_and_supported_venue_quoted`: verifies `VENUE_CAPABILITIES` table; unsupported venues (Raydium CLMM, Orca Whirlpool) reject fail-closed; supported venues (Uniswap v3) quote via `IVenueQuoter`.
    4. `test_market_cap_unknown_rejects_explicitly`: verifies unknown supply produces `market_cap_usd = None` and mcap-dependent filters reject explicitly with `mcap_unknown_rejected`.
    5. `test_pump_fun_unknown_state_rejects_and_decoded_state_quotes_accurately`: unverified/unknown curves reject; decoded curve states quote accurately against hand-computed values.
    6. `test_integration_runner_dexscreener_and_geckoterminal_empty_partial_json`: runner DexScreener and GeckoTerminal paths return `None` on empty or partial API responses without inventing data.
- [x] fix
  - `alpha_engine/models/state.py`: Added `ReserveType` enum (`EXACT`, `DERIVED`) and provenance fields (`reserve_type`, `source`, `fetched_at`, `data_age`, `is_derived`, `is_verified`) to `PoolState`. Updated `calculate_market_cap_usd` to return `None` when supply is unknown (except for verified Solana Pump.fun tokens per official Anchor IDL).
  - `alpha_engine/engine/runner.py`: Removed all invented fallback reserves, prices, and supplies. When API data is missing/incomplete, returns `None`, logs rate-limited `WARNING`, and increments counted telemetry reasons.
  - `alpha_engine/pricing/venue_capabilities.py`: Created `VENUE_CAPABILITIES` table mapping venues to `SUPPORTED`, `UNSUPPORTED`, `DERIVED_ONLY`. Created `IVenueQuoter` interface with `EvmUniswapV3Quoter` computing single-tick concentrated liquidity ($L = \sqrt{x \cdot y}$) in simulation, while rejecting unsupported CLMMs (`UnsupportedVenueError`).
  - `alpha_engine/execution/costs.py`: Wired `TradeCostModel.get_venue_quote` to `IVenueQuoter` and enforced venue support validation.
  - `alpha_engine/execution/executor.py`: Implemented configurable `derived_reserves_policy` (`"reject"` default or `"cap_size"`).
  - `alpha_engine/security/gatekeeper.py`: Enforced venue support checks and explicit rejection of tokens with unknown market cap when filters depend on it (`mcap_unknown_rejected`).
  - `alpha_engine/math/cpmm.py`: Derived Pump.fun state strictly from real on-chain curve account data or verified launch events; rejected unknown curves (`create_verified_pump_launch_pool`). Defined verified IDL constants (`PUMP_FUN_INITIAL_VIRTUAL_SOL_RESERVE`, `PUMP_FUN_INITIAL_VIRTUAL_TOKEN_RESERVE`, `PUMP_FUN_TOTAL_TOKEN_SUPPLY`, `PUMP_FUN_FEE_BASIS_POINTS`, `PUMP_FUN_INITIAL_SPOT_PRICE`).
- [x] green test
  - `.venv/bin/pytest tests/test_issue5_pool_data_and_venue_math.py` -> 6 passed in 5.03s.
  - `.venv/bin/pytest -q` -> 454 passed, 0 failed in 248.21s.
- [x] mutation check
  - Mutation 1: Bypassed `derived_reserves_policy == "reject"` in `executor.py`; verified `test_derived_reserves_policy_enforcement` failed immediately (`AssertionError: Executor must reject signal when pool has derived reserves and policy='reject'`). Restored and verified green.
  - Mutation 2: Mutated `VENUE_CAPABILITIES[ExecutionVenue.RAYDIUM_CLMM]` to `SUPPORTED`; verified `test_unsupported_venue_rejected_and_supported_venue_quoted` failed immediately (`AssertionError: assert <VenueStatus.SUPPORTED> == <VenueStatus.UNSUPPORTED>`). Restored and verified green.
- [x] sweep done
  - Ran `tools/audit/invented_data.py`: Total findings dropped from 152 to 66 (20 are in the audit script itself; remaining 46 are verified Anchor IDL constants, volume thresholds, or unit conversions).
  - Eliminated all occurrences of hardcoded price fallback `Decimal("0.000000028")` and hardcoded reserve `Decimal("30")` across the entire codebase.
- [x] wiring proof
  - DexScreener and GeckoTerminal ingestion in `runner.py`: Missing price, liquidity, or supply returns `None` and increments counted telemetry (`no_liquidity_data`, `no_price`, `stale_state`, `mcap_unknown_rejected`).
  - `PaperExecutor.execute_signal` enforces `derived_reserves_policy` ("reject" by default, or "cap_size" with `derived_reserves_depth_cap_pct=0.02`).
  - `SecurityGatekeeper` and `TradeCostModel` check `is_venue_supported` against `VENUE_CAPABILITIES`. Unsupported venues (Raydium CLMM, Orca Whirlpool) are rejected fail-closed with telemetry reason `unsupported_venue_rejected`. Supported concentrated liquidity venues (Uniswap v3) use `IVenueQuoter` (`EvmUniswapV3Quoter`).
  - Market cap: `calculate_market_cap_usd` in `models/state.py` returns `None` if supply is unknown. `SecurityGatekeeper` rejects tokens with unknown market cap when filters depend on it, logging a warning and incrementing `mcap_unknown_rejected`.
- [x] README/docs updated
  - `.env.example`: Added Section 10 documenting `DERIVED_RESERVES_POLICY` and `DERIVED_RESERVES_DEPTH_CAP_PCT`.

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

