# Fixing Rules

The following rules govern every prompt and change in this project:

1. **VERIFY BEFORE FIXING**: reproduce every finding yourself; if a finding is wrong or partly right, say so with evidence. Line numbers may have shifted.
2. **TEST-FIRST**: write a test that FAILS on current code for the right reason, then fix, then show it passes. Never edit/weaken/delete existing tests to get green; if an existing test encodes buggy behavior, explain why and replace it with a correct one.
3. **FIX THE CLASS OF BUG**: after each fix, search the whole repo (grep + AST script) for the same pattern and fix or justify every other occurrence.
4. **NO SILENT FALLBACKS**: never replace a failure with an invented number. If real data is unavailable/stale, REJECT the action (fail closed), log WARNING/ERROR, count it in telemetry. Never swallow exceptions at DEBUG level in trading or learning paths.
5. **NO HARDCODED MARKET VALUES** in logic (native prices, reserves, supply, guessed fees). Fees/constants come from config with a comment citing the official source, looked up from current official docs if web access exists; otherwise mark UNVERIFIED and make the setting explicit and required.
6. **Do not game acceptance criteria**: no loosened thresholds or widened tolerances; mocks only for network I/O.
7. **Keep Decimal for money/price math**. PAPER trading only: no live order submission, no secrets, no real API keys needed for tests.
8. **One commit per issue**: "fix(<area>): <what> [issue N]". Run the full suite before and after each issue; report regressions.
9. **Ambiguity**: document the assumption and choose the safest (fail-closed) behavior.
10. **WIRING PROOF**: every new component must be invoked from the real runtime path (alpha_engine/engine/runner.py startup/entry/exit flow). Show the call chain and add an integration test that exercises it through that path.
11. **MUTATION CHECK**: after the fix, temporarily revert only the source change and show the new tests fail again; then restore.
12. **NO SKELETONS**: a feature that only has the structure (classes, config keys, schemas) but is not consumed by runtime code counts as NOT DONE.

