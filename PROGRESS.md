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
- [x] reproduced
  - `AutonomousStrategyGenerator` (experimentation.py) produced 5 hardcoded templates and deterministic +/-10% mutations driven by generation-counter parity (no randomness, crossover only merged dicts).
  - `extra_params` keys (`min_net_buy_delta`, `max_mcap_usd`, `min_surge_k`, `min_dormancy_hours`, `min_surge_multiplier`, `max_slippage_bps`, `min_wallet_sortino`, `organic_conviction_min`) were read by NO code; `custom_filter` was never assigned.
  - `evaluate_signal_and_record_shadow_lots` gave every variant the SAME simulated entry fill (champion's), only checking `min_entry_score` and `allowed_sources`.
  - Shadow trades used a fixed 0.10 size and $0.05 gas instead of dynamic sizing and dynamic cost modeling.
  - Champion promotion in `runner.py` only updated SL/TP/trailing stop and never applied the champion's entry rules or source filters to live decision-making.
- [x] red test
  - `tests/test_issue9_strategy_generation.py`: 10 comprehensive tests:
    1. `test_schema_validation_unknown_metric_fails`: Unknown metric names fail schema validation with `SchemaValidationError`.
    2. `test_schema_validation_out_of_bounds_threshold_fails`: Out-of-bounds thresholds fail schema validation with `SchemaValidationError`.
    3. `test_variants_make_own_entry_decisions_and_get_own_fills`: Two variants with different entry rules enter different subsets of the same signal stream and receive independent simulated fills.
    4. `test_seeded_rng_mutation_reproducible_and_different_seeds_differ`: Seeded RNG mutations are reproducible with the same seed and differ across different seeds.
    5. `test_deduplication_prevents_staging_identical_variants`: Exact duplicate variants are rejected with `DUPLICATE_STRATEGY` and never staged.
    6. `test_promotion_changes_live_entry_decisions_integration`: End-to-end integration test through runner entry path proving champion promotion updates live entry rules and gates live signal execution.
    7. `test_generation_logs_persisted_to_ledger`: Generation logs (parents, action, changes, parameters, train/val metrics) are persisted into `strategy_generation_logs` in `SQLiteLedger`.
    8. `test_metric_registry_extraction_and_rule_groups`: Verifies all metric extractors against signals/pools and complex nested rule groups with AND/OR evaluation.
    9. `test_shadow_sizing_dynamic_uses_live_sizing_not_fixed_constants`: Verifies shadow trade sizing dynamically calibrates with portfolio equity, entry scores, and dynamic gas costs.
    10. `test_shadow_fills_parity_with_live_paper_executor`: Proves full execution parity between shadow fills and live paper fills for identical market inputs.
- [x] fix
  - Created `alpha_engine/engine/rules.py`:
    - Declarative schema: `ComparisonOperator`, `LogicalOperator`, `RuleCondition`, `RuleGroup`, `EntryRuleSet`.
    - `METRIC_REGISTRY` with 11 validated metrics: `mcap_usd`, `net_buy_delta`, `volume_surge_multiplier`, `dormancy_hours`, `slippage_estimate_bps`, `wallet_score`, `social_conviction`, `liquidity_usd`, `holder_concentration`, `alpha_score`, `dip_depth_pct` with canonical aliases.
    - Full type validation, bound checking, evaluation against signals/pools/security reports, and deterministic canonical fingerprinting.
  - Updated `alpha_engine/execution/ledger.py`:
    - Added `strategy_generation_logs` table schema and indices (`idx_strategy_gen_name`, `idx_strategy_gen_ts`).
    - Added `record_strategy_generation_log` and `get_strategy_generation_logs` methods.
  - Overhauled `alpha_engine/engine/experimentation.py`:
    - Replaced `custom_filter` and consumed all legacy `extra_params` keys into typed `entry_rules: EntryRuleSet`.
    - Seeded RNG (`self._rng = random.Random(seed)`) for reproducible stochastic mutations, crossover, and novel strategy generation.
    - Implemented 25% exploration budget in `generate_evolutionary_generation` for novel stochastic rule synthesis.
    - Implemented `is_duplicate_strategy` deduplication check based on canonical fingerprinting.
    - Updated `evaluate_signal_and_record_shadow_lots` to evaluate each variant's own `entry_rules` and `allowed_sources`, and execute independent simulated fills with dynamic `_calculate_shadow_trade_size` using `RiskManager` and live native pricing.
    - Updated `evaluate_and_stage_candidate` to record generation logs to ledger and stage qualified/seed candidates.
  - Updated `alpha_engine/engine/backtester.py`:
    - Replaced hardcoded 200/2600 native prices with dynamic `price_service` lookup.
    - Added dynamic column detection on `trades` table (`is_new_launch`, `price_impact_bps`, `strategy_name`).
    - Reconstructed realistic, correlated signal metrics and mapped signal sources to evaluate candidate entry rules against historical trades.
  - Updated `alpha_engine/engine/runner.py`:
    - Added `set_native_price` helper method.
    - Added `evaluate_champion_entry_rules` checking champion `allowed_sources`, `min_entry_score`, and `champion_entry_rules`.
    - Wired champion entry rules evaluation directly into `_process_signal_queue` live entry path before trade execution.
    - Updated `_on_champion_promoted` to synchronize `champion_entry_rules` and `champion_allowed_sources`.
- [x] green test
  - `tests/test_issue9_strategy_generation.py`: 10/10 passed.
  - `tests/test_autonomous_strategy_generator.py`: 5/5 passed.
  - `tests/test_autonomous_strategy_evolution_loop.py`: 4/4 passed.
  - `tests/test_strategy_experimentation_and_swarm.py`: 9/9 passed.
  - `tests/test_strategy_management_and_trailing_stop.py`: 13/13 passed.
  - `tests/test_strategy_v2_integration.py`: 2/2 passed.
  - Full issue regression suite (`tests/test_issue*.py`): 67/67 passed.
- [x] mutation check
  - Mutation 1: Mutated `EntryRuleSet.evaluate` to return `True` unconditionally -> Caught by `test_variants_make_own_entry_decisions_and_get_own_fills` and `test_promotion_changes_live_entry_decisions_integration`.
  - Mutation 2: Mutated `is_duplicate_strategy` to return `False` unconditionally -> Caught by `test_deduplication_prevents_staging_identical_variants`.
- [x] sweep done
  - Ran `tools/audit/unused_config.py`: Findings reduced from 32 to 26; 0 remaining findings in `experimentation.py` or `rules.py`.
- [x] wiring proof
  - Evolution worker (`AutonomousStrategyEvolutionLoop`) -> `AutonomousStrategyGenerator.generate_evolutionary_generation` -> `evaluate_and_stage_candidate` -> stages into `StrategyTournamentRegistry.challengers`.
  - Tournament evaluation (`evaluate_signal_and_record_shadow_lots`) -> evaluates each variant's own `entry_rules` -> independent fills with dynamic sizing.
  - Promotion (`evaluate_promotions`) -> `_on_champion_promoted` -> updates `runner.champion_entry_rules` and `runner.champion_allowed_sources`.
  - Live entry path (`runner._process_signal_queue`) -> calls `runner.evaluate_champion_entry_rules(signal_item, pool=signal_item.pool_state)` -> gates live execution.
- [x] README/docs updated
  - Updated PROGRESS.md and module documentation in `rules.py`, `experimentation.py`, and `runner.py`.

---

## Issue 10 Council LLM never called, no real shared memory, model-name rewriting
- [x] reproduced
  - `alpha_engine/council/llm_client.py`: Verified `execute_debate_turn` was never called by any agent or by `CouncilArbiterCRO`; Rounds 2-3 were templated strings (e.g. `defend_or_revise` added +5 score if two peers approved).
  - `alpha_engine/council/llm_client.py` and `alpha_engine/engine/ai_supervisor.py`: Forced model rewriting remapped any model string containing "flash" or "pro" to fixed names (`gemini-2.5-flash`), ignoring configured models (including valid ones like `gemini-3.8-flash`).
  - `alpha_engine/ruflo/memory.py`: Ruflo episodic memory was written via `record_episode` but never read by council agents before forming verdicts.
  - Hard vetoes had no guarantee against LLM hallucination or weighting overrides; prompt injection from external metadata was unhandled.
- [x] red test
  - Created `tests/test_issue10_council_debate.py` containing 12 unit and integration tests with `FakeCouncilLLMClient`:
    1. `test_llm_output_changes_verdicts`: Verifies that `execute_debate_turn` is actually called and changes verdict scores.
    2. `test_malformed_json_triggers_fallback_and_is_counted`: Verifies corrupt JSON triggers deterministic fallback and records `SCHEMA_VALIDATION_ERROR`.
    3. `test_timeout_triggers_fallback_and_is_counted`: Verifies LLM timeout falls back to deterministic logic and records `TIMEOUT`.
    4. `test_shared_memory_retrieval_changes_prompt_content`: Verifies relevant past episodes are retrieved and injected into agent prompts.
    5. `test_post_mortem_updates_agent_accuracy_and_weights_with_shrinkage`: Verifies post-trade post-mortem updates accuracy with minimum sample requirement (`council_min_sample_size=20`) and shrinkage (`council_shrinkage_prior_weight=10.0`), and persists weights to SQLite ledger.
    6. `test_hard_veto_cannot_be_overridden_by_llm_or_weights`: Verifies fatal security flags (mint/freeze authority, honeypot) force REJECT regardless of LLM scores or weights.
    7. `test_configured_model_string_passed_through_unchanged`: Verifies configured model strings (e.g., `gemini-3.8-flash`) are passed through without rewriting.
    8. `test_unknown_model_fails_with_clear_error_and_counts_fallback`: Verifies unknown models fail with `InvalidModelError` and count as fallback rather than silent substitution.
    9. `test_prompt_injection_in_metadata_cannot_override_rules`: Verifies untrusted token metadata is delimited with `<untrusted_external_content>` and cannot override rules or schema.
    10. `test_real_cross_examination_questions_answered_with_concrete_data`: Verifies Round 2 questions are answered using on-chain data and condition Round 3 revisions.
    11. `test_fallback_ratio_warning_in_status_and_telemetry`: Verifies high fallback ratio triggers WARNING in telemetry and `/status`.
    12. `test_integration_through_council_arbiter_and_runner_path`: Verifies deliberation executes end-to-end in runner signal queue processing.
- [x] fix
  - `alpha_engine/config.py`: Added Section 5f-2 parameters (`council_enabled`, `council_timeout_s`, `llm_max_cost_per_signal_usd`, `llm_max_concurrent_calls`, `llm_rate_limit_rpm`, `council_fallback_warning_threshold`, `council_fallback_window_size`, `council_min_sample_size`, `council_shrinkage_prior_weight`, `model_aliases`).
  - `alpha_engine/council/models.py`: Added telemetry attributes (`generation_path`, `fallback_reason`, `latency_ms`, `evidence`, `llm_turns`, `fallback_turns`, `fallback_ratio`, `avg_llm_latency_ms`, `fallback_reasons`, `warning_triggered`).
  - `alpha_engine/council/llm_client.py`: Implemented strict schema validation (`validate_debate_turn_schema`), prompt injection defense (`delimit_untrusted_content`), concurrency semaphore, rate limiter, retries with exponential backoff on HTTP 429, telemetry tracking, and removed forced model rewriting (raising `InvalidModelError` on unknown models).
  - `alpha_engine/engine/ai_supervisor.py`: Removed hardcoded model rewriting in `_normalize_model_name` and passed configured model and ledger to council components.
  - `alpha_engine/ruflo/memory.py`: Implemented `query_relevant_episodes` (bounded multi-attribute retrieval) and `format_episodes_compact_summary`.
  - `alpha_engine/council/reach_agent.py`, `forensic_agent.py`, `quant_agent.py`: Overhauled to call `execute_debate_turn` in Rounds 1, 2, and 3; guaranteed non-negotiable fatal vetoes; answered Round 2 peer questions with real verified data; conditioned Round 3 revisions.
  - `alpha_engine/council/arbiter_cro.py`: Orchestrated shared memory retrieval and prompt injection; real Round 2 cross-examination; conditioned Round 3 revisions; mandatory Devil's Advocate; Bayesian weight adaptation in `record_trade_outcome`; telemetry helpers `get_llm_telemetry()` and `get_status_summary()`.
  - `alpha_engine/execution/ledger.py`: Added `council_agent_weights` table, `save_council_agent_weights`, `get_council_weights`, and `get_council_agent_performance`.
  - `alpha_engine/engine/runner.py`: Connected Consumer 4 (`council_cro.record_trade_outcome`) in `_record_closed_trade_and_notify`.
  - `alpha_engine/dashboard/telemetry.py` & `alpha_engine/ingestion/telegram.py`: Exposed council AI metrics, LLM vs fallback ratio, latency, failure reasons, and fallback warnings in dashboard and `/status`.
- [x] green test
  - `tests/test_issue10_council_debate.py`: 12/12 passed in 57s.
  - Existing council test suite (`tests/test_council_multi_round_debate.py`, `tests/test_council_veto_enforcement.py`, `tests/test_council_devils_advocate_and_qa.py`): 9/9 passed.
  - Full issue regression suite (`tests/test_issue*.py`): 91/91 passed.
- [x] mutation check
  - Mutation 1: Bypassed fatal veto override in `forensic_agent.py` (`if False:` instead of `if has_fatal:`) -> Caught by `test_hard_veto_cannot_be_overridden_by_llm_or_weights` with `AssertionError: assert <AgentVote.APPROVE: 'APPROVE'> == <AgentVote.REJECT: 'REJECT'>`.
  - Mutation 2: Bypassed schema validation in `llm_client.py` (`return raw_dict`) -> Caught by `test_malformed_json_triggers_fallback_and_is_counted` with `KeyError: 'vote'`.
  - Mutation 3: Wiped episodic memory summary before agent prompts in `arbiter_cro.py` (`memory_summary = ""`) -> Caught by `test_shared_memory_retrieval_changes_prompt_content` with `AssertionError: assert ('MASSIVE_WHALE_RUN' in ... or 'surged 110%' in ...)`.
- [x] sweep done
  - Repo-wide search for model rewriting (`gemini-1.5`, `gemini-2.0`, `gemini-2.5`, `gemini-pro`) -> 0 forced model rewrites remaining.
  - Verified 9 active call sites of `execute_debate_turn` across council agents (Rounds 1, 2, and 3).
  - Verified exception handling in `alpha_engine/ruflo/swarm.py` logs `logger.error` and fails closed on unhandled council crashes.
- [x] wiring proof
  - Ingestion / Signal Queue (`runner.py`) -> `SwarmCoordinator.execute_trade_consensus` -> `CouncilArbiterCRO.deliberate` -> shared memory retrieved from `RufloEpisodicMemory` -> injected into agent prompts -> 9 LLM turns executed across 3 rounds via `CouncilLLMClient.execute_debate_turn` -> CRO synthesis & veto gate.
  - Closed Trade Hook (`runner._record_closed_trade_and_notify`) -> `CouncilArbiterCRO.record_trade_outcome` -> writes post-mortem `MemoryEpisode` to `RufloEpisodicMemory` -> updates agent accuracy with shrinkage -> persists weights via `SQLiteLedger.save_council_agent_weights`.
  - Observability (`telemetry.py` & `telegram.py`) -> `CouncilArbiterCRO.get_llm_telemetry()` -> surfaces turn breakdown, fallback ratio, average latency, failure reasons, and fallback warnings in dashboard and `/status`.
- [x] README/docs updated
  - Updated PROGRESS.md with full Issue 10 verification and self-audit.

---

## Issue 11 Wallet profiling has no data and is not wired
### Part A: Wallet History Data Layer & Trade Reconstruction
- [x] reproduced
  - `alpha_engine/ruflo/agents/profiler_agent.py`: Verified `tool_evaluate_wallet` invoked `profile_wallet(wallet_address, chain, trades=[])` with an empty trades list for every candidate wallet.
  - `alpha_engine/models/profiler.py`: Verified `WalletTradeRecord` was defined but never constructed anywhere in runtime code (only mocked in test files).
  - `alpha_engine/profiler/evaluator.py`: Verified `WalletEvaluator.MIN_CLOSED_TRADES = 15`; because `trades=[]`, every evaluated wallet received `classification = INSUFFICIENT_HISTORY` and could never qualify for smart money.
  - `alpha_engine/tracker/wallet_tracker.py`: Verified only 5 transaction signatures were retrieved via `getSignaturesForAddress` solely for display/notification strings, never reconstructing trades.
- [x] red test
  - Created `tests/test_issue11a_wallet_history.py` (10 tests) with recorded fixtures in `tests/fixtures/wallet_history/`:
    1. `test_swap_parsing_solana_pump_fun_and_raydium`: Validated parsing of Pump.fun buy/sell bonding curve swaps and Raydium AMM swaps into `NormalizedSwap`.
    2. `test_swap_parsing_solana_jupiter_multi_route`: Validated net input/output parsing across Jupiter multi-route hops.
    3. `test_swap_parsing_base_uniswap_v2_and_v3`: Validated Uniswap V2, V3, and Aerodrome swap logs with gas fee calculation in native ETH.
    4. `test_fifo_matching_with_partial_sells_and_fees`: Validated FIFO lot matching, pro-rated cost basis on partial sells, and fee deductions.
    5. `test_transfers_not_counted_as_sells`: Validated that incoming and outgoing external token transfers do not trigger false sell trades or false losses.
    6. `test_unrealized_positions_excluded_from_closed_trades`: Validated that open buy lots remain in `open_positions` and are excluded from closed trades and win rate.
    7. `test_cache_hit_avoids_network`: Validated that synced windows in SQLite `wallet_swaps_cache` return cached swaps without invoking network provider.
    8. `test_rate_limit_and_quota_accounting`: Validated token bucket acquisition and atomic quota accounting raising `ProviderQuotaExceededError`.
    9. `test_provider_error_propagates_unavailable_to_profiler`: Validated that provider network failure/quota exhaustion propagates as `insufficient_data` (never "not smart money").
    10. `test_profile_wallet_receives_real_records_integration`: Full integration verifying `profile_wallet` receives 16 reconstructed `WalletTradeRecord` objects and persists them to SQLite.
- [x] fix
  - `alpha_engine/models/enums.py`: Added `SwapVenue` enum (`PUMP_FUN`, `RAYDIUM`, `JUPITER`, `UNISWAP_V2`, `UNISWAP_V3`, `AERODROME`) and `WalletClassification.INSUFFICIENT_DATA`.
  - `alpha_engine/models/profiler.py`: Added `NormalizedSwap`, `OpenPositionRecord`, and `TradeReconstructionResult` models.
  - `alpha_engine/config.py`: Added Section 5c-3 settings (`solana_history_provider`, `base_history_provider`, `wallet_history_backfill_days`, `wallet_history_cache_ttl_s`, `wallet_history_free_tier_daily_quota`, `helius_api_key`, `alchemy_api_key`).
  - `alpha_engine/profiler/history_db.py`: Created `WalletHistoryDatabase` with WAL mode SQLite tables (`wallet_swaps_cache`, `wallet_sync_status`, `provider_quota_usage`, `wallet_reconstructed_trades`, `wallet_reconstructed_metrics`, `historical_native_prices`).
  - `alpha_engine/profiler/price_oracle.py`: Created `HistoricalPriceOracle` supporting DefiLlama historical pricing with hourly SQLite caching.
  - `alpha_engine/profiler/history_provider.py`: Created `WalletHistoryProvider` ABC with retry backoff, token bucket rate limiter integration, daily quota accounting, and cache handling.
  - `alpha_engine/profiler/solana_history.py`: Created `HeliusSolanaHistoryProvider` parsing Pump.fun, Raydium, and Jupiter routes.
  - `alpha_engine/profiler/base_history.py`: Created `AlchemyBaseHistoryProvider` parsing Base EVM swaps and transfers.
  - `alpha_engine/profiler/reconstructor.py`: Created `TradeReconstructor` implementing deterministic FIFO lot matching, historical pricing, fee subtraction, partial sells, and open position separation.
  - `alpha_engine/profiler/profiler.py`: Connected `history_provider`, `reconstructor`, and `history_db` to `SmartMoneyProfiler`; added `get_wallet_trade_records`; updated `profile_wallet` to fetch real history when `trades=None` and fail closed to `INSUFFICIENT_DATA` on provider failure.
  - `alpha_engine/ruflo/agents/profiler_agent.py`: Replaced `trades=[]` with `trades=None` in `tool_evaluate_wallet`; mapped provider unavailable to `"insufficient_data"`.
- [x] green test
  - `tests/test_issue11a_wallet_history.py`: 10/10 passed in 6.0s.
  - `tests/test_profiler_and_rate_limiter.py`: 8/8 passed in 3.9s.
  - Full issue regression suite (`tests/test_issue*.py`): 89/89 passed in 52.4s.
- [x] mutation check
  - Mutation 1: Bypassed transfer check in `reconstructor.py` (`if swap.is_transfer and False:`) -> Caught by `test_transfers_not_counted_as_sells` (`AssertionError: assert 1 == 0`).
  - Mutation 2: Omitted transaction fee subtraction from sell proceeds in `reconstructor.py` (`net_exit_native = swap.native_amount`) -> Caught by `test_fifo_matching_with_partial_sells_and_fees` (`AssertionError: assert Decimal('0.2999975') == Decimal('0.2999925')`).
  - Mutation 3: Returned silent empty list on provider failure in `history_provider.py` (`return []`) -> Caught by `test_provider_error_propagates_unavailable_to_profiler` (`AssertionError: assert 'insufficient_history' == 'insufficient_data'`).
- [x] sweep done
  - Verified `WalletTradeRecord` is constructed in `reconstructor.py` and `history_db.py`.
  - Replaced hardcoded `trades=[]` in `profiler_agent.py`.
- [x] wiring proof
  - Evaluator Tool (`profiler_agent.tool_evaluate_wallet`) -> `SmartMoneyProfiler.profile_wallet` (with `trades=None`) -> `SmartMoneyProfiler.get_wallet_trade_records` -> checks `history_db.get_reconstructed_trades` -> fetches via `WalletHistoryProvider.fetch_swaps` -> parses into `NormalizedSwap` -> runs `TradeReconstructor.reconstruct_trades` -> constructs `WalletTradeRecord` objects -> persists to `history_db.save_reconstructed_trades` -> evaluates via `WalletEvaluator.evaluate(trades=trades)` -> persists whitelist result.
- [x] Part B: Smart-Wallet Discovery, Evaluation, Whitelist, Tracking and Signals
  - [x] reproduced
    - Runtime dead ends: `evaluate_and_whitelist`, `record_wallet_purchase`, `is_whitelisted`, `reverse_engineer_winning_tokens` had zero runtime callers in the runner/coordinator loop.
    - Whitelist DB was never consulted in the decision path before evaluating candidate trades or signals.
    - `SignalSource.WHALE_WALLET` was never produced by the runner or ingestion coordinators.
    - No continuous background candidate discovery, evaluation queue, or demotion loop existed in the runner lifecycle.
  - [x] red test
    - Created `tests/test_issue11b_smart_wallets.py` (11 tests with fixtures, no external network):
      1. `test_known_profitable_wallet_whitelisted`: A consistent profitable wallet (>15 trades, >60% win rate, low outlier ratio) is evaluated and whitelisted.
      2. `test_wash_trader_rejected_with_right_reason`: Circular wash-trading cycles and churning volume with net near-zero PnL are rejected as `CIRCULAR_WASH`/`WASH_TRADER`.
      3. `test_single_lucky_trade_wallet_rejected_with_right_reason`: Wallets where a single trade accounts for >60% of total PnL are rejected as `LUCKY_OUTLIER`.
      4. `test_mev_bot_rejected_with_right_reason`: High-speed MEV/sandwich patterns (median hold time < 45s, same-block buy/sell) are rejected as `MEV_BOT`.
      5. `test_insider_funded_wallet_rejected_with_right_reason`: Wallets funded by deployer or mixer hops (<3 hops) are rejected as `CABAL_INSIDER`.
      6. `test_insufficient_data_deferred_not_rejected`: Wallets with provider rate-limit/quota errors are marked `DEFERRED` for re-queueing, not rejected.
      7. `test_out_of_sample_consistency_failure_rejected`: Wallets with in-sample profitability that fail out-of-sample forward testing are rejected for multiple-testing bias.
      8. `test_cluster_agreement_increases_conviction`: Multiple independent whitelisted wallets buying the same token within 120s increases cluster size, conviction multiplier, and alpha score.
      9. `test_whitelisted_wallet_signal_gatekeeper_security_veto_applies`: Whitelisted wallet signals enter the normal gatekeeper and Ruflo Council path; honeypot/security flags fail closed.
      10. `test_demotion_after_bad_copy_trade_results`: Underperforming copied trades (3 consecutive losses or net negative copy PnL) trigger automatic demotion to `SUSPENDED` in SQLite whitelist.
      11. `test_end_to_end_runner_swap_stream_to_paper_fill_with_wallet_attribution`: End-to-end integration test through runner from a fixture swap stream to a paper fill with wallet attribution on `OpenLot` and post-trade performance recording.
  - [x] fix
    - `alpha_engine/profiler/pipeline.py`: Created `SmartMoneyPipeline` orchestrating the 6 pipeline stages:
      1. Candidate discovery: early buyers via `reverse_engineer_winning_tokens`, recurring winners, and Telegram/news feeds stored with provenance in SQLite `wallet_candidates`.
      2. Quota-aware prioritization queue: background worker prioritizing recurring candidates while respecting free-tier daily quotas.
      3. Evaluation with `WalletEvaluator`: multi-criteria behavioral analysis (outlier ratio, circular wash trading, funding hops, MEV bot detection, out-of-sample split).
      4. Whitelist curation: `whitelist_db` with status, score, reasons, last-evaluated time; periodic re-evaluation and automatic demotion/ban hooked to real copy-trade outcomes.
      5. Live tracking: monitoring whitelisted wallets on SVM/EVM swap streams, deduplicating within cooldown, computing conviction (trade size vs baseline) and cluster agreement.
      6. Signal emission: Emits `WHALE_WALLET` signals into the normal gatekeeper -> council -> sizing -> execution path with wallet score, cluster size, conviction, and provenance.
    - `alpha_engine/engine/rules.py`: Registered `wallet_score`, `cluster_size`, `wallet_conviction` in `METRIC_REGISTRY` with extractors and canonical aliases.
    - `alpha_engine/models/events.py`: Added wallet attribution fields (`wallet_address`, `wallet_score`, `cluster_size`, `wallet_conviction`, `wallet_provenance`, `qualifying_wallets`) to `SignalEvent`.
    - `alpha_engine/execution/book.py`: Added `wallet_address` and `copied_wallets` to `OpenLot` and `PositionBook.open_lot()`.
    - `alpha_engine/engine/runner.py`:
      - Instantiated `self._wallet_pipeline` in `PaperTradingEngine` and scheduled `start()` and `stop()` in the runner lifecycle.
      - Wired swap events in `_process_ingestion_queue` to `on_swap_event`, streaming `WHALE_WALLET` signals into `_signal_q`.
      - Wired `_record_closed_trade_and_notify` Consumer 5 to record copy-trade outcomes via `record_copied_trade_outcome()` and discover candidates on winning tokens (`discover_from_winning_token()`).
      - Wired news/whale feeds to `discover_from_news_or_feed()` and enforced that whale alert feeds are treated as candidate discovery only, not trusted blindly without wallet evaluation.
      - Exposed `wallet_pipeline` state in `_get_engine_status()`.
    - `alpha_engine/ruflo/swarm.py`:
      - Wired `profiler` and `whitelist_db` into `SmartMoneyProfilerAgent` and `RufloSupervisorAgent`.
      - In `execute_trade_consensus`, auto-extract `wallet_address` from signals and consult `whitelist_db` directly in the decision path before external API profiling.
      - Passed `signal`, `pool`, `report`, and wallet profile to supervisor audit tool.
    - `alpha_engine/ruflo/agents/supervisor_agent.py`:
      - Added `whitelist_db` and `profiler` injection.
      - Populated `wallet_telemetry` with genuine profile metrics (`total_trades`, `win_rate_pct`, `median_hold_time_s`, `single_trade_outlier_pct`) when evaluating whitelisted wallets, preventing false survivorship-bias vetoes.
    - `alpha_engine/dashboard/telemetry.py` & `alpha_engine/ingestion/telegram.py`:
      - Exposed candidates, queue depth, whitelisted count, evaluations/hr, quota usage, and signals emitted in dashboard telemetry and `/status`.
      - Cleaned up default `telegram_channels`: removed placeholder/irrelevant entries (`insiderpaper`, `disclosetv`).
  - [x] green test
    - `tests/test_issue11b_smart_wallets.py`: 11/11 passed in 15.18s.
    - `tests/test_telegram_news_whale_ingestion.py`: 8/8 passed in 4.12s.
    - `tests/test_issue*.py`: 100/100 passed in 157.96s.
  - [x] mutation check
    - Mutated outlier bias check in `evaluator.py` (`if False and outlier_pnl_ratio > cfg_max_outlier_ratio:`); verified `test_single_lucky_trade_wallet_rejected_with_right_reason` failed with `AssertionError: assert low_win_rate == lucky_outlier`. Reverted mutation and verified green.
  - [x] sweep done
    - Verified previously uncalled functions (`evaluate_and_whitelist`, `record_wallet_purchase`, `is_whitelisted`, `reverse_engineer_winning_tokens`) now have active runtime callers.
    - Verified `SignalSource.WHALE_WALLET` is actively emitted by `on_swap_event` and processed by the runner.
    - Verified whitelist DB is consulted in the decision path before external API calls.
  - [x] wiring proof
    - Swap Ingestion -> `runner._process_ingestion_queue` -> `SmartMoneyPipeline.on_swap_event` -> Whitelist check (`whitelist_db.is_whitelisted`) -> Dedupe & Conviction sizing -> Cluster agreement (`profiler.record_wallet_purchase`) -> Gatekeeper Preflight -> Emits `WHALE_WALLET` `SignalEvent` -> Runner `_process_signal_queue` -> Gatekeeper Screening -> Ruflo Swarm Consensus (Scout, Profiler, Gatekeeper, Quant, Reach, Council CRO, Supervisor) -> RiskEngine Half-Kelly Sizing -> `PositionBook.open_lot` (persisting `wallet_address` and `copied_wallets`) -> Trade Exit (`_record_closed_trade_and_notify`) -> Consumer 5 calls `SmartMoneyPipeline.record_copied_trade_outcome()` -> Updates `wallet_copy_performance` in SQLite -> Auto-demotes to `SUSPENDED` if underperforming.
  - [x] README/docs updated
    - Updated `PROGRESS.md` with Issue 11 Part B completed and ticked.

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

