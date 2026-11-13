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
- [x] reproduced
  - `alpha_engine/math/mev.py`: `simulate_latency()` sampled latency uniformly in [1200, 3500] ms, but only shifted `fill_timestamp_ns`; pool state was never re-read at fill time.
  - `simulate_frontrun_volume()` drew an unconditional adverse LogNormal fraction (0.1%-2.5% of native reserve) regardless of venue, route exposure (private vs public), priority fee, or trade size.
  - README claimed 45-320 ms while code executed 1200-3500 ms.
  - Shadow tournament and backtester used inconsistent or detached execution models.
- [x] red test
  - `tests/test_issue4_latency_and_mev.py`: 6 exhaustive tests:
    1. `test_moving_market_price_reflected_at_fill`: deterministic seed + price series moving up (+21%) and down (-19%) during latency window confirms fill price reflects market move at fill time.
    2. `test_stale_pool_at_fill_rejected_with_counter`: if no market state fresher than `fill_max_stale_ms` exists at fill time, rejects fail-closed with `stale_pool_at_fill_rejected`.
    3. `test_private_route_zero_sandwich_impact_and_public_route_scales`: private routes (`is_private_route=True`) have zero MEV sandwich frontrun impact; public routes scale with trade size / depth and slippage tolerance, bounded by `mev_max_sandwich_impact_bps`.
    4. `test_seeded_rng_reproducibility`: identical config seed produces identical latencies and MEV impacts; different seeds produce different draws.
    5. `test_parity_shadow_tournament_and_live_paper_fills`: live `PaperExecutor` and `StrategyTournamentRegistry` invoke the exact same `alpha_engine.math.mev` module and pending-fill logic with identical outputs.
    6. `test_integration_runner_entry_path_scripted_feed`: end-to-end runner test with a scripted price feed demonstrating pending order resolution and fill-time pricing.
- [x] fix
  - `alpha_engine/config.py`: Added Section 5.3 (`execution_seed`, `latency_min_ms_solana=45`, `latency_max_ms_solana=320`, `latency_min_ms_base=150`, `latency_max_ms_base=2000`, `latency_distribution="lognormal"`, `fill_max_stale_ms=4000`, `fill_timeout_ms=10000`, `default_is_private_route=False`, `mev_sandwich_depth_threshold=0.005`, `mev_sandwich_slippage_threshold_bps=50`, `mev_max_sandwich_impact_bps=500`).
  - `alpha_engine/math/mev.py`: Completely revamped: `sample_fill_latency()` with chain-specific defaults and seeded RNG; conditional `simulate_frontrun_volume()` requiring public mempool, depth > 0.5%, slippage > 50 bps; `determine_market_state_at_fill()` resolving freshest pool state and failing closed on staleness; unified `simulate_latency()`.
  - `alpha_engine/execution/executor.py`: Injected `pool_resolver` and `is_private_route`; added seeded RNG and `telemetry_counters`; in paper mode, awaits pending fill delay up to `fill_timeout_ms`, resolves freshest pool state at fill time, rejects stale states fail-closed, and calculates execution quotes on `adjusted_pool`.
  - `alpha_engine/engine/experimentation.py`: Wired `config` and seeded RNG into `StrategyTournamentRegistry`; updated `evaluate_signal_and_record_shadow_lots` to call unified `simulate_latency()` with `market_state_resolver`.
  - `alpha_engine/engine/backtester.py`: Wired `config` and seeded RNG into `WalkForwardBacktester`; updated backtest simulation to sample latency and apply conditional MEV.
  - `alpha_engine/engine/runner.py`: Passed `config` and pool resolvers (`_resolve_pool_for_executor`, `_resolve_pool_for_tournament`) to `PaperExecutor` and `StrategyTournamentRegistry`.
- [x] green test
  - `tests/test_issue4_latency_and_mev.py` -> 6/6 passed in 8.91s.
  - `tests/test_native_price_service.py`, `tests/test_issue2_execution_sizing.py`, `tests/test_issue3_cost_model.py`, `tests/test_issue4_latency_and_mev.py`, `tests/test_issue5_pool_data_and_venue_math.py` -> 30/30 passed.
  - All modular architecture and swarm tests pass cleanly.
- [x] mutation check
  - Mutated `executor.py` to bypass fill-time pool resolution and use signal-time pool: `test_moving_market_price_reflected_at_fill` failed (`AssertionError: assert Decimal('9.90099...') != Decimal('9.90099...')`).
  - Mutated `mev.py` to apply frontrunning even on private routes: `test_private_route_zero_sandwich_impact_and_public_route_scales` failed (`AssertionError: assert Decimal('0.05') == Decimal('0.0')`).
- [x] sweep done
  - Repo-wide search for unseeded `random.` in execution paths -> None.
  - Verified tournament registry and backtester use the unified `mev.py` module.
  - Ran `tools/audit/native_price_literals.py` -> 0 findings.
- [x] wiring proof
  - `runner.py`: `PaperTradingEngine` creates `PaperExecutor` and `StrategyTournamentRegistry` with `config=self.config` and pool resolvers wired to `_pool_registry`.
  - `executor.py`: `execute_signal` and `execute_exit` await fill delay in paper mode, call `determine_market_state_at_fill` via `simulate_latency`, quote against `adjusted_pool`.
  - `experimentation.py`: `evaluate_signal_and_record_shadow_lots` calls `simulate_latency` with `market_state_resolver` resolving from pool registry.
- [x] README/docs updated
  - `README.md`: Section 3 updated with real latency ranges (Solana: 45–320ms, Base: 150–2000ms), official documentation citations, conditional MEV rules, and pending-fill mechanics.
  - `.env.example`: Added Section 11 documenting all latency, timeout, and MEV configuration parameters.

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
- [x] reproduced
  - `runner.py:5707`: `asyncio.create_task(self._self_learning_engine.on_trade_closed(self_learning_payload))` called on synchronous `SelfLearningEngine.on_trade_closed`, raising `TypeError: a coroutine was expected`.
  - Swallowed by `except Exception` logged at DEBUG (`logger.debug("Self-learning hook failed: %s", e)`).
  - First statement in shared try block skipped `swarm_coordinator.on_trade_closed` and `strategy_tournament.record_live_trade_outcome`.
  - `record_live_trade_outcome` fabricated fake lot (entry 1.0, basis $100, token "LIVE_SETTLED_TRADE", chain forced Solana) and inverted PnL sign to match `is_win`.
- [x] red test
  - `tests/test_issue6_post_trade_learning_hook.py`:
    1. `test_post_trade_hooks_invoked_exactly_once`: Confirms all 3 consumers invoked exactly once per closed trade.
    2. `test_failure_in_one_consumer_does_not_skip_others_and_logged_at_error`: Confirms failure in one consumer logs ERROR with traceback, increments telemetry `post_trade_hook_failure_total`, and allows remaining consumers to execute.
    3. `test_record_live_trade_outcome_records_real_trade_data`: Verifies tournament records real lot data (token, chain, basis, prices, fees, hold time).
    4. `test_record_live_trade_outcome_never_rewrites_pnl_sign_and_trusts_pnl`: Verifies conflicting `is_win` logs ERROR and trusts PnL without rewriting sign.
    5. `test_task_awaitables_audit_zero_findings`: Audits repo for any `create_task`/`await` on non-coroutines.
    6. `test_integration_runner_real_close_path`: Full runner cycle closing a position exercises all hooks.
- [x] fix
  - `alpha_engine/engine/self_learning/self_learning_engine.py`: Converted `on_trade_closed` to an honest `async def`, added `on_trade_closed_sync` for sync callers, awaited `_record_to_ruflo_memory_async`.
  - `alpha_engine/engine/experimentation.py`: Overhauled `record_live_trade_outcome` to accept genuine trade attributes, construct real `ShadowLot`, log ERROR on `is_win`/`realized_pnl_usd` contradiction, and trust PnL.
  - `alpha_engine/security/preflight.py`: Added `inspect_bytecode_from_rpc` async function, resolving await on sync function.
  - `alpha_engine/engine/runner.py`: Wrapped consumers in isolated try/except blocks with ERROR logging, exc_info=True, and `self.telemetry.increment("post_trade_hook_failure_total")`.
- [x] green test
  - `.venv/bin/pytest tests/test_issue6_post_trade_learning_hook.py` -> 6 passed.
- [x] mutation check
  - Inverted PnL sign rewrite in `record_live_trade_outcome`: `realized_pnl_usd = -abs(realized_pnl_usd) if not is_win else abs(realized_pnl_usd)`.
  - Verified `test_record_live_trade_outcome_never_rewrites_pnl_sign_and_trusts_pnl` failed (`AssertionError: 20.0 == -20.0`); restored fix.
- [x] sweep done
  - `tools/audit/task_awaitables.py`: Baseline 2 findings -> 0 findings (`[task_awaitables] Total findings: 0`).
- [x] wiring proof
  - `runner.py:_close_lot_and_record`: Invokes `self._self_learning_engine.on_trade_closed`, `self._swarm_coordinator.on_trade_closed`, and `self._strategy_tournament.record_live_trade_outcome`.
  - Exercised by `test_integration_runner_real_close_path`.
- [x] README/docs updated
  - Documented post-trade hook architecture and telemetry counter `post_trade_hook_failure_total`.

---

## Issue 7 Slippage fed to tuner always 50
- [x] reproduced
  - `runner.py:1184`: `actual_slip = int(getattr(exit_fill, "slippage_bps", 50))` fell back to 50 because `PaperFill` had no `slippage_bps` attribute.
  - `AutonomousHyperparameterTuner._recalibrate_hyperparameters` always saw constant 50, calibrating `max_slippage_bps` to the 75 bps floor with no empirical variance.
  - `fill_latency_ms` and other model attributes similarly used loose `getattr` defaults.
- [x] red test
  - `tests/test_issue7_slippage_and_tuner.py`:
    1. `test_paper_fill_has_typed_slippage_bps_field`: Validates `PaperFill` has typed `slippage_bps` field.
    2. `test_paper_executor_computes_realized_slippage_buy`: Verifies realized BUY slippage vs spot reference price.
    3. `test_paper_executor_computes_realized_slippage_sell`: Verifies realized SELL slippage vs spot reference price.
    4. `test_varying_trade_sizes_produce_varying_realized_slippage`: Verifies price impact causes varying realized slippage across small and large trades.
    5. `test_tuner_keeps_defaults_below_min_samples`: Verifies tuner preserves defaults when observations < `min_slippage_samples` (10).
    6. `test_tuner_calibrates_when_samples_reach_threshold`: Verifies tuner calibrates 75th percentile + 20% margin, bounded within `[75, 300]` bps.
    7. `test_runner_feeds_real_slippage_to_tuner`: Integration test verifying runner passes real slippage into tuner.
    8. `test_no_getattr_with_literal_default_targeting_fill_slippage`: AST audit ensuring no `getattr` defaults target fill slippage in execution/closed-trade paths.
- [x] fix
  - `alpha_engine/models/state.py`: Added typed `slippage_bps: Annotated[int, Field(ge=0)] = 0` to `PaperFill`, added `slippage_bps` and `actual_slippage_bps` alias to `TradeRecord`, updated `TradeRecord.from_fill` to copy typed fields directly.
  - `alpha_engine/execution/executor.py`: Realized slippage computed in `execute_signal`, `execute_exit`, and `execute_swing_dca_pipeline` as effective fill price vs reference pool price:
    - BUY: `max(0, int(round((effective_price - ref_price) / ref_price * 10000)))`
    - SELL: `max(0, int(round((ref_price - effective_price) / ref_price * 10000)))`
  - `alpha_engine/engine/runner.py`: Replaced `getattr(exit_fill, "slippage_bps", 50)` with `exit_fill.slippage_bps`, `fill_latency_ms`, `simulated_gas_cost_usd`, `order_id` with direct typed access.
  - `alpha_engine/ruflo/memory.py`: Replaced hardcoded fallback 50 with typed attribute access.
  - `alpha_engine/engine/self_learning/hyperparameter_tuner.py`: Added `min_slippage_samples: int = 10` parameter, required minimum sample size before recalibrating `max_slippage_bps`, documented bounds `[75, 300]` bps.
- [x] green test
  - `.venv/bin/pytest tests/test_issue7_slippage_and_tuner.py` -> 8 passed.
- [x] mutation check
  - Mutation 1 (Executor): Forcing `realized_slippage_bps = 50` failed `test_paper_executor_computes_realized_slippage_sell` and `test_varying_trade_sizes_produce_varying_realized_slippage` (`AssertionError: 50 != 50`).
  - Mutation 2 (Tuner): Removing `min_slippage_samples` check failed `test_tuner_keeps_defaults_below_min_samples` (`AssertionError: Expected default 150, got 264`). Both restored.
- [x] sweep done
  - `tools/audit/model_getattr_defaults.py`: Decreased from 995 to 987 findings; all literal defaults on fill slippage/latency in runner/executor eliminated.
- [x] wiring proof
  - `PaperExecutor.execute_signal` and `execute_exit` compute `slippage_bps` and assign to `PaperFill`.
  - `runner.py:_close_lot_and_record` reads `exit_fill.slippage_bps` directly and passes it to `SelfLearningEngine.on_trade_closed`.
  - `SelfLearningEngine` passes `actual_slippage_bps` into `AutonomousHyperparameterTuner.record_trade_outcome`.
  - Exercised end-to-end by `test_runner_feeds_real_slippage_to_tuner`.
- [x] README/docs updated
  - Documented realized slippage computation formula, `min_slippage_samples` threshold, and `[75, 300]` bps calibration range.

---

## Issue 8 Promotion gate precedence bug + weak statistics
- [x] reproduced
  - `alpha_engine/engine/experimentation.py:943` (now line 1128): `if m.max_drawdown_pct >= 12.0 if is_champ_underperforming else 8.0: continue` parsed in Python as `((mdd >= 12.0) if is_champ_underperforming else 8.0)`. When the champion is healthy (`is_champ_underperforming=False`), it evaluated to `8.0` (truthy), rejecting 100% of challengers regardless of metrics.
  - Inadequate sample size: `min_promotion_trades` was 8 (healthy) and 15 (underperforming), where standard error of the mean is large and random noise easily passes.
  - No statistical confidence criterion: lacked bootstrap lower bound or hypothesis test on expectancy.
  - No out-of-sample validation: curve-fit strategies failing out-of-sample were eligible for promotion.
  - Ignored live exit parameters: `PositionBook.evaluate_lot_exit` restricted trailing stops and take profit to a hardcoded 5-strategy whitelist; newly promoted champions were ignored by the live exit engine.
  - Challenger retirement leak: `prune_underperforming_challengers` set status to `RETIRED` but left variants in `self.challengers`, allowing them to accumulate shadow trades and be evaluated for promotion.
  - Sortino calculation bug: in `recompute_metrics`, 0 losing trades evaluated `sortino_ratio` to 0.0 instead of 99.0, causing 100% win rate challengers to be rejected.
- [x] red test
  - `tests/test_issue8_promotion_gate.py`: 12 comprehensive tests:
    1. `test_reproduce_precedence_bug_healthy_champ_blocks_superior_challenger`: Superior challenger rejected when champion is healthy under bug; passes with fix.
    2. `test_inferior_challenger_is_not_promoted`: Inferior challenger (PF < 1.35 or Sortino <= champ + 0.1) rejected.
    3. `test_high_drawdown_challenger_rejected`: Challenger with MDD >= threshold rejected.
    4. `test_drawdown_boundary_thresholds_healthy_and_underperforming`: Validates exact boundary conditions (8.0% healthy, 12.0% underperforming).
    5. `test_insufficient_sample_size_rejected`: Challenger with N < min_trades rejected.
    6. `test_retired_challengers_cannot_be_promoted_and_do_not_leak`: Pruned challengers moved to `retired_challengers` and cannot be promoted.
    7. `test_outlier_win_dependency_rejected`: Challenger with > 60% profit from single trade rejected.
    8. `test_confidence_criterion_bootstrap_rejects_noisy_edge`: Challenger with superficial edge but 95% bootstrap lower bound of expectancy <= 0 rejected.
    9. `test_superior_in_sample_inferior_out_of_sample_rejected`: Time-split gate catches in-sample curve fitting and rejects out-of-sample bleeding.
    10. `test_cooldown_prevents_immediate_subsequent_promotion`: Immediate back-to-back promotion blocked during cooldown; succeeds after cooldown expiry.
    11. `test_rollback_demotes_failing_new_champion`: Newly promoted champion losing >= $20 in first 10 trades rolled back to previous champion.
    12. `test_promotion_changes_live_exit_decisions_integration`: Full integration test proving promotion callback atomically updates runner parameters and changes live exit engine behavior.
- [x] fix
  - `alpha_engine/config.py`: Added Section 5d-3 with typed configuration: `tournament_min_promotion_trades=50`, `tournament_min_promotion_trades_underperforming=30`, `tournament_train_fraction=0.70`, `tournament_bootstrap_resamples=1000`, `tournament_bootstrap_confidence_level=0.95`, `tournament_promotion_cooldown_trades=10`, `tournament_promotion_cooldown_seconds=300.0`, `tournament_rollback_max_trades=10`, `tournament_rollback_max_loss_usd=-20.0`, `tournament_rollback_min_win_rate=25.0`.
  - `alpha_engine/engine/experimentation.py`:
    - Rewrote conditional with explicit local variable: `mdd_threshold = 12.0 if is_champ_underperforming else 8.0; if m.max_drawdown_pct >= mdd_threshold: continue`.
    - Added `StrategyVariant.compute_metrics_from_lots`, `split_closed_lots_by_time`, and `compute_bootstrap_expectancy`.
    - Fixed Sortino calculation in `recompute_metrics` when downside deviation is zero to evaluate to 99.0 when net return is positive.
    - Fixed `prune_underperforming_challengers` to pop retired variants from `self.challengers` into `self.retired_challengers`.
    - Implemented promotion cooldown check (`is_in_promotion_cooldown`), 95% bootstrap lower bound check (`boot_lower > 0.0`), and time-split out-of-sample validation gate.
    - Implemented automated rollback rule (`evaluate_rollback`) in `record_live_trade_outcome`.
  - `alpha_engine/execution/book.py`:
    - Updated `evaluate_lot_exit` to accept `custom_take_profit_pct` and `trailing_stop_from_peak_pct` arguments and removed restrictive strategy name whitelist.
    - Ensured passed `hard_stop_loss_pct` overrides lot default.
  - `alpha_engine/engine/runner.py`:
    - Overhauled `_on_champion_promoted` to atomically synchronize `active_strategy`, `hard_stop_loss_pct`, `custom_take_profit_pct`, `trailing_stop_from_peak_pct`, `time_exit_s`, `min_entry_score`, and open lot parameters.
    - Forwarded updated parameters from `_monitor_and_manage_positions` directly to `evaluate_lot_exit`.
- [x] green test
  - `tests/test_issue8_promotion_gate.py`: 12/12 passed.
  - `tests/test_strategy_experimentation_and_swarm.py` & `tests/test_autonomous_strategy_evolution_loop.py`: 13/13 passed.
- [x] mutation check
  - Mutation 1 (Precedence bug restored): `test_reproduce_precedence_bug_healthy_champ_blocks_superior_challenger` failed (`AssertionError: None is not None`).
  - Mutation 2 (OOS gate bypassed): `test_superior_in_sample_inferior_out_of_sample_rejected` failed (`AssertionError: Challenger bleeding on out-of-sample segment must be rejected`).
  - Mutation 3 (Bootstrap gate bypassed): `test_confidence_criterion_bootstrap_rejects_noisy_edge` failed (`AssertionError: Noisy challenger failing bootstrap confidence bound must be rejected`).
  - Mutation 4 (Cooldown bypassed): `test_cooldown_prevents_immediate_subsequent_promotion` failed (`AssertionError: Promotion during cooldown period must be blocked`).
  - Mutation 5 (Rollback bypassed): `test_rollback_demotes_failing_new_champion` failed (`AssertionError: Failing champion must be rolled back to previous champion`).
- [x] sweep done
  - `tools/audit/conditional_precedence.py`: Findings reduced from 54 to 52; `ast.IfExp used directly as test of 'if'` eliminated completely repo-wide (0 findings).
  - All 52 remaining findings audited and verified as safe parenthesized fallback chains or standard ternaries.
- [x] wiring proof
  - `evaluate_promotions` promotes challenger -> invokes `_on_champion_promoted` in `runner.py`.
  - `_on_champion_promoted` updates `runner.hard_stop_loss_pct`, `runner.trailing_stop_from_peak_pct`, `runner.custom_take_profit_pct`, `runner.time_exit_s`, `runner.min_entry_score`, and existing open lots in `PositionBook`.
  - `runner._monitor_and_manage_positions` passes updated parameters to `evaluate_lot_exit`.
  - Verified by `test_promotion_changes_live_exit_decisions_integration`.
- [x] README/docs updated
  - Documented promotion criteria, cooldown, rollback rules, bootstrap expectancy bounds, and OOS time-split validation.

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

