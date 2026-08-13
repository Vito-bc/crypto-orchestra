# Strategy Review — Independent Research Pass (2026-08-09)

Professional follow-up: `docs/research/2026-08-professional-review-addendum.md`.
The follow-up accepts the central V2/V3 falsification and the continued live
NO-GO posture, but identifies evidence-pipeline issues that must be fixed before
the OOS replay, V3 journal, or equity-duration metrics are treated as formal
validation records. This research branch also predates the latest main/safety
hardening and must not be used as a deployment base.

> ## ⚠️ Reproducibility status (added 2026-08-09)
>
> Only part of this document can be regenerated from committed code. The
> deterministic runner `backtesting/research_runner.py` reproduces, and pins in
> `docs/research/artifacts/results.json`:
>
> - the continuous-window ZEC result (PF 0.86) and the integrated V3 comparison
>   (PF 0.69);
> - the four registry windows (25 / 12 / 27 / 37 closed trades);
> - the zero-tuning transfer test under the frozen ZEC mechanism;
> - ER-30 and VM-30 regime cells;
> - the equity/drawdown metrics.
>
> **LEGACY / UNVERIFIED — no implementation exists in this repository:**
> mean reversion, slow/trend following, LLM agent-vote IC, drawdown-conditional
> allocation, and the realised-vol / Bollinger-bandwidth regime cells. Their
> numbers below cannot be regenerated, are recorded under `non_reproducible` in
> the artifact, and **must not be cited in a decision**. Do not re-derive them
> from memory — recover the original implementation or run a new, explicitly
> pre-registered replication trial with one frozen configuration.
>
> Where this document calls a probe "positive" (e.g. slow trend following), read
> that as an unverified historical note, not as evidence of an edge.

Scope: full re-validation of the frozen V2/V3 momentum strategy, an
asset × regime edge matrix, structural hypothesis tests (regime routing,
strategy families, agent ensemble value, drawdown-conditional allocation,
volatility compression), and a forward-OOS reconstruction. All heavy numbers
are also logged in `docs/trial_registry.md` (2026-08-09 section).

Data: Coinbase exchange candles (1h + 1d, parquet cache) — ZEC from 2020-12,
BTC/ETH from 2020-01, SOL from 2021-06, all through 2026-08-09. Fee model:
maker 0.4% / taker 0.6% (V2 model). No parameters were tuned in this pass;
the frozen ZEC config was applied unmodified everywhere.

## 1. Diagnosis

The system's honest state before this pass: V2 momentum judged ~zero-edge on
four validation windows (PF ≈ 1.00 combined), downgraded to paper/shadow;
V3 (ER-30 ≥ 0.20 regime filter) pre-registered and awaiting ≥20 forward-OOS
trades; live execution DRY_RUN; scheduler down since 2026-07-23.

This pass adds one decisive methodological correction: **the four validation
windows were themselves a form of selection.** Scanning the continuous window
(2021-03 → 2026-07-12, no gaps) with the identical mechanism:

| Configuration | n | WR | Avg | PF |
|---|---|---|---|---|
| Four registry windows combined | 101 | 47% | −0.08% | ~1.00 |
| Continuous window | 133 | 42% | −0.37% | 0.86 |
| … gap 2023-01→2024-08 alone | 24 | 17% | −2.35% | 0.23 |
| Continuous + V3 integrated enforcement | 80 | 39% | −0.90% | 0.69 |

The harness reproduces all four registry-window results exactly (n, PF, avg),
so this is data, not implementation drift.

Conclusions:

1. **V2 is negative-edge, more clearly than previously recorded.** The
   never-scanned 2023→mid-2024 stretch is the strategy's worst regime and was
   silently absent from the "cross-cycle" estimate.
2. **V3's in-sample case is falsified.** The +0.31%/trade improvement was an
   artifact of the window layout; integrated enforcement on the continuous
   window *reduces* PF from 0.86 to 0.69. The pre-registered OOS trial can run
   to completion (enforcement stays off either way), but the research
   expectation is now failure.
3. The seemingly positive complement cell (er<0.20: +0.43%, PF 1.22) collapses
   to PF 1.02 when its single best month (2022-03) is removed. ER-30 carries no
   robust information for this entry in either direction.

## 2. Asset × regime edge matrix

Frozen mechanism transferred with zero tuning (clean mechanism test):

| Asset | n | Avg | PF | Best regime cell (any bucketing) |
|---|---|---|---|---|
| ZEC | 133 | −0.37% | 0.86 | none robust (see above) |
| BTC | 203 | −0.91% | 0.38 | none (best: RV-high, n=14, PF 1.04) |
| ETH | 181 | −0.89% | 0.50 | none (best: BBW-mid, PF 0.73) |
| SOL | 97 | −0.56% | 0.72 | none robust (RV-low PF 1.11, n=49, episode-driven) |

Bucketings tested: ER-30 (3 buckets), vm_30 sign, realized-vol-30 expanding
percentile terciles, Bollinger-width expanding percentile terciles. All
look-ahead-safe (previous closed UTC day only).

**The answer to "does strategy X have edge on asset Y in regime Z" is: this
strategy family (1h EMA50 breakout momentum, long-only, ATR 2.0/3.5, 36h) has
no demonstrated positive cell for any asset in any tested regime.** The
correct action per the project's own standard is DO NOT TRADE it live — which
is the current posture; keep it.

Volatility compression specifically: compressed-BBW cells are among the
*worst* (ZEC 0.50, BTC 0.27, ETH 0.45 PF). "Quiet market → imminent profitable
breakout" is rejected for this entry mechanism.

## 3. Strategy families (single pre-declared config each; no sweeps)

- **Mean reversion** (1h RSI<30 + below lower BB, 2×ATR stop/target, 36h):
  PF 0.30–0.62 on all four assets; *worse* in ER<0.20 "range" regimes.
  Rejected — including the idea of routing MR into range regimes.
- **Slow trend following** (daily close > 55d high entry, close < 20d low
  exit, next-open execution, taker fees): positive on all four assets
  (pooled n = 57; BTC PF 4.4, ETH 7.4, SOL 1.8, ZEC 3.2), the only family
  probe that isn't structurally negative after fees. Caveats that keep it
  hypothesis-only: profit is dominated by 1–2 secular episodes per asset
  (BTC +236% 2020-10→2021-05; ZEC +610% 2025-09→12 — without it ZEC TF is
  −7% total); 6-year long-only window with strong upward drift; 12–17 trades
  per asset. Notable genuine property: 2022 bear damage was small vs
  buy-and-hold (BTC −17% vs −65%; ETH −38% vs −70%).
- **Event/momentum family**: not testable from current data (no event
  timestamps with sufficient history). Not pursued.
- **No-trade**: currently the correct "strategy" for the momentum family.

## 4. Agent ensemble

From `logs/agent_decisions.jsonl` (2026-04-15 → 2026-07-11, 1,995 decisions
with real votes, subsampled to 158 per-asset-per-day observations; daily-block
bootstrap CIs): no agent's signed-confidence IC against 24h/72h forward
returns is distinguishable from zero. One marginal cell (whale, IC24 = 0.17,
CI [+0.01, +0.32]) out of 14 tests is what noise produces, and that agent's
BUY votes preceded *negative* forward returns (−2.0% mean vs −0.2% baseline).

Verdict: no evidential basis for a weighted/dynamic ensemble. The current
architecture (deterministic scanner gates entries; agents veto-only) is the
right shape; if anything the hourly agent calls are a cost line with no
measured alpha contribution.

## 5. Drawdown analysis

`backtesting/equity_report.py` now computes the full suite (max/avg DD,
episode durations, time underwater, recovery factor, Calmar, Sharpe, Sortino,
PF, expectancy, exposure, turnover). On the continuous ZEC curve: max DD −71%,
92% of post-trade observations underwater, longest observed episode 4.3 years.
A boundary-aware audit in the professional addendum measures approximately
97.15% of elapsed calendar time underwater and 1,564.3 days for the longest
episode; the utility needs explicit evaluation boundaries before its duration
metrics are considered final.

**"Buy the strategy drawdown"**: tested at −3/−5/−8/−10% equity-ATH thresholds
with 5/10/20-trade forward horizons on four asset curves. 1–3 qualifying
episodes per curve — far below any evidentiary bar — and every cell's forward
mean was *below* the unconditional mean. Rejected; no dynamic allocation.

## 6. Forward OOS (2026-07-12 freeze → 2026-08-09)

The scheduler outage (2026-07-23 → 2026-08-09) meant the live shadow journal
missed the window. `backtesting/oos_replay.py` provides a provisional
counterfactual reconstruction: 4 reported shadow signals,
2 V3-accepted (er ≥ 0.20), both stopped out (−3.70%, −3.61%); the 2 V3-blocked
signals netted −1.09%. n = 2 of the ≥20 required — no criteria verdict, but
nothing so far contradicts the continuous-window expectation of failure.

Professional-review qualification: the tool post-filters a non-enforced scan
rather than running a separate integrated V3 path. The two accepted losses are
unchanged in an integrated audit of this slice, but the path/block count differs.
Treat this output as diagnostic, not as the formal OOS journal.

## 7. Changes implemented

Evidence-justified only; zero live-path changes:

1. `docs/trial_registry.md` — this pass logged (~30 trials), decisions recorded.
2. `CLAUDE.md` — removed superseded "profitable / ready to go live" claims that
   contradicted the registry (a live-safety hazard for any future session).
3. `backtesting/oos_replay.py` — scheduler-independent OOS reconstruction +
   pre-registered criteria evaluation.
4. `backtesting/equity_report.py` + `tests/test_equity_report.py` — drawdown
   accounting suite.

Explicitly rejected (tested, failed): regime-routing layer, agent weighting,
MR family, compression gating, drawdown-buying allocation, any parameter or
risk-engine changes.

## 8. Remaining weaknesses & next priorities

1. **No live strategy has positive expected value.** Nothing here changes
   that; the honest posture is shadow mode and research.
2. Do not rely on restarting the scheduler alone to complete the V3 trial. The
   journal currently conflates enforcement acceptance with candidate acceptance
   in shadow mode and has no working accepted-outcome chain. Fix those semantics
   first. Given the falsified IS case, the professional recommendation is to
   retire V3 as an activation candidate and retain only cheap diagnostic shadow
   data if useful.
3. If new edge is pursued, the trend-following family is the only lead with
   support in this data. Before ANY further scanning: pre-register one config,
   acceptance criteria, and cost model in the trial registry; prefer breadth
   (many assets, small size) since the P&L profile is 1–2 winners per years.
4. Consider cutting or down-scoping the hourly LLM agent spend until a
   strategy exists that their vetoes measurably improve.
