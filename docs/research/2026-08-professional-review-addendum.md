# Professional Review Addendum — 2026-08-09 Research Pass

Review target: commit `cf71578` (`research/2026-08-regime-edge-pass`).

Review scope: reproducibility of the headline ZEC result, methodology, the two
new research tools, V3 journal semantics, data provenance, and whether any
finding justifies a live-path change. No production trading logic was changed
as part of this review.

Implementation handoff: `docs/tasks/2026-08-research-evidence-hardening.md`.

## Executive decision

| Area | Review decision |
|---|---|
| Core research verdict | **ACCEPT** |
| Evidence/tooling package | **REQUEST CHANGES** before treating it as a formal OOS record |
| Current momentum/V3 family | **NO-GO for live deployment** |
| `DRY_RUN` / shadow posture | **KEEP** |
| Slow trend following | **PRE-REGISTERED RESEARCH CANDIDATE ONLY** |

The central conclusion is sound and was independently reproduced from the
cached Coinbase candles:

| Replay | n | Avg/trade | PF |
|---|---:|---:|---:|
| Four registry windows | 101 | -0.08% | ~1.00 |
| Continuous ZEC, V3 off | 133 | -0.366% | 0.855 |
| Continuous ZEC, integrated V3 on | 80 | -0.895% | 0.691 |

The four individual registry windows also reproduced exactly. This is strong
falsification evidence: filling the omitted calendar interval turns an already
marginal strategy clearly negative, and the locked ER-30 filter makes it worse.
The review findings below do not rescue V2 or V3. They limit how much weight can
be placed on the new OOS, equity-duration, cross-family, and agent-analysis
claims until the evidence pipeline is tightened.

## What was done well

1. The work attacked window-selection bias instead of searching for another
   favorable parameter. Replaying the continuous history was the highest-value
   test available.
2. The scanner reproduced the registered windows before being used for the
   falsification. That is an important harness-parity check.
3. Conclusions are appropriately negative where the evidence is negative.
   No live parameter, sizing, or execution change was smuggled into a research
   pass.
4. Removing the stale “profitable / ready to go live” instruction from
   `CLAUDE.md` closed a genuine operational safety hazard.
5. The slow-trend result was described as hypothesis-generating and its episode
   concentration was disclosed rather than optimized away.

## Findings requiring changes

### P1 — the research branch is not a deployment base

`research/2026-08-regime-edge-pass` diverges from `main` at `055edc4`. It does
not contain the later SQLite connection-race fix on `main` or the Coinbase
preflight/product-metadata/exit-safety hardening on
`safety/exit-metadata-preflight-hardening`. Its 432-test baseline is therefore
not the latest operational safety baseline.

Required change: integrate the research documentation and research-only tools
on top of the current safety history; do not deploy or promote the research
branch itself as the runtime branch. Resolve any documentation conflicts in
favor of the current NO-GO strategy verdict and the newer execution safeguards.

### P1 — `oos_replay.py` is not an integrated V3 replay

`oos_replay.py` runs `scan_asset()` with V3 enforcement disabled and then
defines the candidate cohort by post-filtering `v3_would_block`. That is not
equivalent to running the filter inside the scanner. A blocked trade changes
`skip_until` and stop-history state, so later signals can differ. The registry
already documents this interaction as the reason earlier post-filtered results
were misleading.

For the 2026-07-12 to 2026-08-09 slice, the two accepted losses happen to remain
the same under an integrated audit replay, but the paths are already different:
the integrated run counts three V3 blocks while the shadow/post-filter report
shows two. Therefore `n=2, both losses` is useful diagnostic evidence, but the
current tool must not be called the formal reconstructed V3 record.

Required change: make V3 enforcement an explicit immutable argument to the
historical scanner and run a separate integrated candidate path. Preserve the
unfiltered path only for counterfactual comparison.

### P1 — the shadow journal cannot currently evaluate the registered cohort

The runner logs `accepted=not v3_blocked`. While enforcement is off,
`v3_blocked` is always false, so every shadow signal is recorded as accepted,
including signals with `v3_would_block=true`. `summarise_journal()` then uses
that field as the V3-accepted cohort.

There is a second break in the chain: `reconcile_pending()` resolves only
records whose `accepted` field is false, while shadow mode writes them all as
true. No other production call to `log_outcome()` exists. Restarting the
scheduler therefore accumulates signal rows, but does not by itself produce
closed accepted outcomes for the five activation criteria.

Required change:

1. Store separate fields for `candidate_accepted = not v3_would_block`,
   `enforcement_accepted = not v3_blocked`, and actual order/trade disposition.
2. Build the research cohort from `candidate_accepted`, not the enforcement
   field.
3. Resolve all pending shadow signals counterfactually, or join actual fills and
   exits through a tested signal ID.
4. Add an end-to-end test that starts with a shadow signal and ends with the
   correct candidate-cohort summary.

Until this is corrected, the V3 `n>=20` gate is not operationally measurable
from `logs/v3_journal.jsonl` as documented.

### P1 — most headline research claims have no committed reproduction path

The commit contains prose results for the asset-by-regime matrix, mean
reversion, slow trend following, agent vote IC, drawdown buying, and trial count,
but it does not contain the scripts, frozen configs, or result artifacts that
generated those tables. Only the ZEC scanner replays, OOS helper, and equity
helper can be rerun from the commit.

Required change: commit one deterministic research runner (or a small set of
focused runners), a machine-readable result artifact, the exact configuration,
and a data manifest. The prose document should be generated from or checked
against that artifact. Until then, the non-ZEC family findings are credible
research notes, not independently auditable evidence.

### P2 — “time underwater” is observation-weighted, not calendar-time weighted

`equity_report.summary()` computes `time_underwater` as the fraction of
post-trade equity observations below their running peak. With irregular gaps
between trades, this is not the fraction of elapsed time underwater. The
drawdown duration also begins at the first losing trade and ends at the last
trade for an unrecovered episode, rather than using the evaluation boundaries.

For the same ZEC replay:

- current reported value: 92% of post-trade observations underwater;
- calendar-weighted audit from 2021-03-01 through 2026-07-12: approximately
  **97.15% of elapsed time underwater**;
- current longest duration: 1,562 days;
- boundary-aware duration: approximately **1,564.3 days**.

The implementation also omits an explicit initial-equity row. If the first
trade is a loss, that loss becomes the initial peak and is excluded from max
drawdown. The first ZEC trade in this particular replay is a winner, so the
reported ZEC max drawdown of about -71.04% is unaffected; the generic utility
is still incorrect for other sequences.

Required change: accept explicit evaluation start/end timestamps, include
initial equity 1.0, calculate elapsed-time-weighted underwater duration, and
label per-trade observation metrics separately if they are retained.

### P2 — the replay can prematurely close right-censored trades

`_simulate_trade()` reports `MAX_HOLD` with the configured hold duration even
when the requested dataset ends before that duration is observable. A signal
within 36 hours of `--end` can therefore enter the OOS criteria as a completed
trade using a partial horizon.

Required change: return a `PENDING`/`resolved=false` outcome when the full
horizon is unavailable, and exclude it from closed-trade criteria.

### P2 — data integrity exists locally but is not attached to the result

All nine local parquet files checked during this review matched their SHA-256
sidecars, and the daily series had no missing calendar days. The 1h series do
contain several gaps longer than one hour (three to six per asset, maximum six
hours); ZEC includes a two-hour gap during the forward-OOS window on 2026-07-17.
There is no documented gap policy or sensitivity check.

Because `data/` and the sidecars are git-ignored, another checkout cannot prove
that it used the same snapshot. In addition, the downloader appends after the
latest cached timestamp but does not backfill a missing prefix when an existing
cache starts after the requested start.

Required change: write a committed manifest containing dataset hashes, row
counts, min/max timestamps, detected gaps, fetch time, and the policy used for
each gap. Fail closed when requested coverage is incomplete.

### P3 — the all-wins metric edge case emits warnings

The full suite passes (`432 passed, 1 deselected`) and Ruff is clean, but the
all-wins equity test emits three NumPy warnings because Sortino calculates a
standard deviation over an empty downside sample. This does not alter the main
verdict, but research reports should be warning-clean so missing-data and
undefined-metric cases cannot hide in routine output.

Required change: explicitly return `NaN` (or a documented infinity convention)
when the downside sample is empty, without calling `std()` on it, and test the
chosen contract.

## Strategy conclusions

1. **Retire V3 as an activation candidate.** Its in-sample rationale is
   falsified on the continuous record, and the current forward journal cannot
   evaluate its registered cohort correctly. Keeping a cheap diagnostic shadow
   series is reasonable; waiting six months for an activation decision is not.
2. **Do not add regime routing, mean reversion, compression gating, drawdown
   buying, or agent weighting to the current mechanism.** None has demonstrated
   incremental expectancy, and each adds degrees of freedom.
3. **Keep LLM agents out of the alpha claim.** Until a controlled ablation shows
   improvement after costs, run them only on scanner events or use them for
   explanation/monitoring. Hourly calls are not supported by measured value.
4. **Treat slow trend following as a new strategy family, not a patch to V2.**
   The positive result is worth a disciplined trial, but 57 correlated trades
   and one or two dominant secular episodes are insufficient for deployment.
5. **Execution hardening and strategy expectancy are separate gates.** Safe
   order handling is necessary, but it cannot turn a negative signal family
   into a live-ready strategy.

## Recommended next experiment: slow trend following

Before any additional scan, register one immutable specification:

- signal: close above the prior 55 fully closed daily highs;
- entry: next available open, with an explicit gap/slippage rule;
- exit: close below the prior 20 fully closed daily lows, executed next open;
- universe: fixed before the run, with listing/delisting rules and no survivor
  substitution;
- portfolio: one sizing rule, maximum aggregate crypto exposure, and a rule for
  simultaneous correlated signals;
- costs: actual Coinbase tier plus predeclared adverse slippage stress;
- evidence boundary: one untouched forward period or anchored walk-forward
  schedule; no parameter changes after the boundary.

Pre-register acceptance criteria that test breadth and concentration, not just
pooled PF:

1. positive net expectancy and PF above a fixed hurdle after stressed costs;
2. positive median asset result and a predeclared minimum share of profitable
   assets;
3. positive leave-one-episode-out result;
4. no asset or market episode contributing more than a fixed share of gross
   profit;
5. portfolio max drawdown and calendar time-underwater below fixed limits;
6. performance reported against buy-and-hold and cash on both return and
   drawdown, without claiming independence across crypto assets.

The exact numeric hurdles belong in the trial registry before the first new
result is inspected. If these cannot be chosen without looking at another
backtest, stop: that would create another selected window/configuration.

## Operational priority order

1. Keep `DRY_RUN=true`; make no live-path strategy change.
2. Integrate the research-only changes on top of the latest safety branch; do
   not use the research branch as a deployment base.
3. Fix the journal cohort/outcome chain and integrated replay semantics.
4. Correct equity duration/baseline accounting and add boundary tests.
5. Add immutable data/result manifests and committed reproduction scripts.
6. Formally retire V3 or explicitly record why the diagnostic shadow series is
   worth maintaining.
7. Only then pre-register and run the slow-trend trial.
8. Run LLM analysis on signal events rather than hourly while its incremental
   contribution remains unmeasured.

## Final assessment

This was a valuable research pass because it prevented a false live launch and
found a stronger negative result. Its most important product is the corrected
decision, not a new strategy. The decision is production-relevant and should be
kept; several supporting tools and evidence claims need the changes above
before they can serve as the formal validation record for the next candidate.
