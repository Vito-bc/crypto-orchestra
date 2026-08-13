# Execution Task — Retire V3 and Harden the Research Evidence Pipeline

Status: **READY FOR IMPLEMENTATION**

Priority: **NEXT TASK**

Owner: next implementation agent/developer

Source review: `docs/research/2026-08-professional-review-addendum.md`

## Objective

Turn the 2026-08 research verdict into a safe, reproducible project state:

1. preserve the confirmed live NO-GO decision;
2. retire V3 as an activation candidate without changing live behavior;
3. correct the replay, journal, equity, and data-provenance defects found in
   professional review;
4. leave the repository ready to pre-register a separate slow-trend experiment.

This task does **not** develop or deploy a new trading strategy.

## Mandatory starting point

Do not implement from `research/2026-08-regime-edge-pass` as the deployment
base. That branch predates later SQLite and exit-safety fixes.

1. Start a new branch from the latest
   `safety/exit-metadata-preflight-hardening` (or its merged successor on
   `main`).
2. Bring over the research-only changes from commit `cf71578` and the
   professional-review documentation.
3. Preserve all newer execution, preflight, product metadata, ledger, and exit
   safety behavior.
4. Resolve documentation conflicts in favor of `DRY_RUN=true`, live NO-GO,
   and the professional review.

Before editing, record:

- starting commit;
- merge base;
- test count and Ruff status;
- whether the worktree already contains user changes.

## Non-negotiable safety constraints

- Keep `DRY_RUN=true`.
- Keep `v3_enforcement_enabled=False`.
- Do not change position sizing, ATR stop/target values, order placement,
  preflight, exits, product parsing, ledger, or reconciliation behavior.
- Do not run authenticated trading calls or place/cancel orders.
- Do not tune ER thresholds or inspect new slow-trend variants.
- Do not claim that execution safety implies positive expectancy.
- Preserve all user-owned and unrelated worktree changes.

If any required change reaches the live execution path rather than research or
shadow accounting, stop and request review before continuing.

## Phase 1 — Record V3 retirement

Update `docs/trial_registry.md` and `CLAUDE.md`:

1. Mark V3 ER-30 as **RETIRED / REJECTED FOR ACTIVATION**.
2. Record the reason: integrated continuous PF 0.69 versus 0.86 without V3;
   the original positive case was caused by selected windows.
3. Preserve threshold 0.20 only as historical trial metadata.
4. State that future V3 observations are diagnostic and cannot reactivate the
   candidate without a new pre-registered trial ID.
5. Remove any remaining instruction to wait for `n>=20` before deciding whether
   V3 may go live.

Acceptance criteria:

- no document says V3 is pending activation;
- no document recommends setting enforcement to true;
- runtime configuration remains enforcement-off.

## Phase 2 — Make historical replay path-explicit

Files expected to change:

- `backtesting/signal_scanner.py`
- `backtesting/oos_replay.py`
- new or existing tests under `tests/`

Requirements:

1. Give historical `scan_asset()` an explicit, immutable V3-enforcement
   argument. Do not mutate `ASSET_CONFIG` globals in a test or CLI.
2. Run two distinct paths in `oos_replay.py`:
   - unfiltered V2 path for counterfactual comparison;
   - integrated V3 path in which blocked signals affect `skip_until` and stop
     history exactly as they would during enforcement.
3. Label both paths clearly. Never derive the formal candidate cohort by
   post-filtering the unfiltered path.
4. Add `PENDING`/`resolved=false` handling for signals whose stop/target/max-hold
   horizon is not fully observable at the requested end boundary.
5. Exclude pending outcomes from PF, bootstrap, expectancy, episode
   concentration, and closed-trade counts.
6. Validate CLI dates and fail clearly when `end < OOS_FREEZE` or when requested
   candle coverage is incomplete.

Required tests:

- a blocked trade exposes a later signal only on the integrated path;
- unfiltered and integrated paths cannot be silently substituted;
- a signal inside the final 36 hours stays pending unless stop/target is
  actually observed;
- pending signals do not enter activation statistics;
- the fixed 2026-07-12 to 2026-08-09 audit still reports the two integrated
  accepted losses, while also exposing the correct integrated block count.

## Phase 3 — Repair shadow-journal cohort and outcome semantics

Files expected to change:

- `pipeline/v3_journal.py`
- `pipeline/runner.py` only where research metadata is logged
- `tests/test_v3_properties.py`

Store three separate concepts:

- `candidate_accepted = not v3_would_block`;
- `enforcement_accepted = not v3_blocked`;
- actual disposition: whether an order/trade was placed, blocked elsewhere, or
  remained counterfactual.

Requirements:

1. Build candidate statistics from `candidate_accepted`.
2. Maintain backward-compatible reading of existing journal rows.
3. Resolve every pending shadow signal counterfactually when sufficient candles
   exist, including candidate-accepted signals that were not traded.
4. If actual fills/exits are joined, use a tested stable signal ID and retain
   provenance (`actual` versus `counterfactual`).
5. Do not let duplicate outcome events change the result nondeterministically.
6. Count all-win bootstrap samples as PF greater than one; do not discard
   infinity from the probability denominator.
7. Use the same `<=50%` episode-concentration boundary everywhere.

Required end-to-end test:

1. write one candidate-accepted shadow signal and one candidate-blocked signal;
2. resolve both from deterministic candles;
3. summarize the journal;
4. prove that only the candidate-accepted record enters candidate PF and
   expectancy;
5. prove that rerunning reconciliation is idempotent.

Because V3 is retired in Phase 1, this repaired journal is a trustworthy
diagnostic record and reusable infrastructure—not an activation mechanism.

## Phase 4 — Correct equity and drawdown accounting

Files expected to change:

- `backtesting/equity_report.py`
- `tests/test_equity_report.py`

Requirements:

1. Require or accept explicit evaluation start/end boundaries.
2. Include initial equity `1.0` before the first realized trade.
3. Compute max drawdown from the initial capital baseline.
4. Compute calendar-time underwater by weighting elapsed intervals, not by
   counting post-trade observations.
5. For an unrecovered final drawdown, measure duration through the evaluation
   end—not merely through the last trade.
6. Keep observation-weighted underwater only if it is separately named.
7. Define CAGR, exposure, and trades/year over the requested evaluation window.
8. Handle zero-loss/all-win Sortino and PF cases without NumPy warnings.

Required tests:

- the first trade is a loss;
- irregular multi-month gaps between trades;
- unrecovered drawdown through evaluation end;
- exact recovery to peak and new high;
- no trades, one trade, all wins, all losses;
- timezone-aware and naive inputs have a documented deterministic contract.

Expected audit reference for the existing ZEC sequence:

- max DD remains approximately `-71.04%`;
- elapsed calendar time underwater is approximately `97.15%` for
  2021-03-01 through 2026-07-12;
- longest boundary-aware episode is approximately `1,564.3 days`.

## Phase 5 — Add immutable research provenance

Add a deterministic research runner and machine-readable artifacts for:

- continuous and registry-window ZEC results;
- integrated V3 comparison;
- asset-by-regime matrix;
- mean-reversion probe;
- slow-trend probe;
- agent vote IC;
- drawdown-buying probe.

The runner must use frozen configuration—not CLI-selected thresholds—and emit:

- code commit;
- config/trial ID;
- asset and exact evaluation boundaries;
- candle row counts, min/max timestamps, and detected gaps;
- SHA-256 for every input dataset;
- fee and slippage assumptions;
- result rows in JSON or another diffable machine-readable format.

Add a committed manifest. Fail closed when input coverage is incomplete. Do not
silently fall back to a different data provider during a registered run.

The prose review and registry must either be generated from the artifact or
checked against it in a test.

## Phase 6 — Verification and handoff

Run, at minimum:

```powershell
venv\Scripts\python.exe -m pytest -q
venv\Scripts\python.exe -m ruff check .
venv\Scripts\python.exe backtesting\oos_replay.py --end 2026-08-09
venv\Scripts\python.exe backtesting\equity_report.py ZEC-USD 2021-03-01 2026-07-12
```

Also run the new deterministic research runner twice and prove byte-identical
machine-readable output for the same code/config/data manifest.

Final handoff must report:

- files changed;
- tests added and total test result;
- exact reproduced headline metrics;
- any metrics that changed and why;
- confirmation that live-path behavior and V3 enforcement did not change;
- remaining limitations;
- diff against the latest safety base, not only against the old research
  branch.

## Definition of done

This task is complete only when all of the following are true:

- V3 is formally retired as an activation candidate;
- `DRY_RUN=true` and V3 enforcement remains off;
- replay uses a real integrated path and handles right-censoring;
- journal cohort/outcome semantics are correct end-to-end;
- equity duration metrics use calendar time and explicit boundaries;
- headline research results have committed reproduction code and immutable
  input/result manifests;
- tests and Ruff pass without new warnings;
- newer execution-safety changes remain intact;
- no slow-trend parameter search has begun.

## Follow-on task — do not start yet

After this task is accepted, create a separate pre-registration task for slow
trend following. Freeze the 55-day entry / 20-day exit definition, universe,
portfolio construction, cost stress, OOS boundary, breadth requirement,
leave-one-episode-out rule, and drawdown limits **before** running another trend
backtest.
