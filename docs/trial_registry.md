# Trial Registry — ZEC Momentum Strategy

All hyperparameter searches, asset tests, and model variants explored before the V2 freeze.
Any positive result must be interpreted in light of this registry (multiple testing).

OOS start: **2026-07-12** (git tag `v2-adx25-frozen`, commit `f06881d`)
Frozen config: ZEC-USD only, ADX=25, atr_stop=2.0, atr_target=3.5, btc_regime_filter=False

---

## Assets tested

| Asset    | Period        | Result        | Decision   |
|----------|---------------|---------------|------------|
| ZEC-USD  | recent_year   | PF=1.00 (Coinbase) | Paper/Shadow only — see V2 verdict |
| ETH-USD  | recent_year   | not run (disabled after ATR bug) | Disabled |
| LINK-USD | recent_year   | PF=0.25       | Rejected   |
| ATOM-USD | recent_year   | PF=0.81       | Rejected   |
| AVAX-USD | recent_year   | PF=0.40       | Rejected   |
| DOT-USD  | recent_year   | PF=0.61       | Rejected   |

Note: earlier yfinance result for ZEC recent_year was PF=1.32. After migration to Coinbase data, PF=1.00.

## V2 verdict — mathematically negative

Cross-cycle validation (4 historical regimes of ZEC-USD, Coinbase data — not 4 independent tests):

| Period         | n  | WR  | PF   | Avg P&L |
|----------------|----|-----|------|---------|
| bull_2021      | 25 | 60% | 1.42 | +0.87%  |
| bear_2022      | 12 | 50% | 1.41 | +0.97%  |
| mid_year_hold  | 27 | 37% | 0.52 | -1.29%  |
| recent_year    | 37 | 46% | 1.00 | +0.01%  |
| **Combined**   | 101| 47% | **~1.00** | **-0.08%** |

- Leave-one-event-out (Sep-Nov 2025 ZEC rally): remaining 19 signals avg -0.45%, PF=0.795
- Single-event dependence: entire positive edge from ONE ZEC rally episode
- After spread/slippage/missed fills (~25bps): definitively negative
- **Decision 2026-07-13**: V2 downgraded to paper/shadow mode only

## V3 regime filter (ER-30) — research findings

Hypothesis: Kaufman Efficiency Ratio (ER-30 = |net_move_30d| / sum(|daily_moves|)) predicts whether a momentum signal fires in a trending vs choppy regime.

**Integrated filter results** (scan with `v3_enforcement_enabled=True` — what live trading experiences):

| Period       | No filter | er>=0.20 | er>=0.25 | er>=0.30 | er>=0.35 |
|--------------|-----------|----------|----------|----------|----------|
| bull_2021    | PF=1.42 n=25 | PF=1.41 n=16 | PF=0.77 n=11 | PF=0.75 n=10 | PF=1.16 n=8 |
| bear_2022    | PF=1.41 n=12 | PF=1.03 n=9  | PF=5.32 n=5  | PF=3.26 n=4  | PF=inf  n=2 |
| mid_year     | PF=0.52 n=27 | PF=1.00 n=18 | PF=0.77 n=16 | PF=0.78 n=14 | PF=1.14 n=10|
| recent_year  | PF=1.00 n=37 | PF=1.11 n=25 | PF=1.16 n=24 | PF=1.11 n=21 | PF=0.99 n=15|

Combined avg per trade:
- No filter:  -0.08% (4-period weighted, 101 signals)
- er >= 0.20: +0.31% (68 signals)
- er >= 0.25: unstable (n=5 in bear_2022 inflates to PF=5.32)
- er >= 0.35: recent_year drops to PF=0.99 (marginal)

> **These PF>1 results are descriptive outcomes from selected, non-independent
> historical windows.** They omit the losing 2023–mid-2024 interval and are
> superseded by the continuous PF 0.86 result. They provide no evidence for
> routing, activation, or live deployment.

The words "best stable threshold" and "most robust integrated threshold"
previously appeared here. They were removed: on the continuous window integrated
enforcement is *worse* (PF 0.69 vs 0.86), so no threshold in this table is
"best" or "robust" in any forward sense. 0.20 is retained only as historical
trial metadata recording what was tested.

Key insight: the er >= 0.25+ post-hoc result was misleading. The integrated
filter changes signal generation via skip_until interactions, so post-hoc
filtering of an unfiltered run does not reproduce the enforced cohort.

Cost stress test (additional friction on top of Coinbase fees already in P&L):
- er >= 0.20: survives +20bps of friction before avg goes negative
- er >= 0.40: survives +100bps friction (PF=1.133, n=17 — too few)

### STATUS: RETIRED / REJECTED FOR ACTIVATION (2026-08-09)

V3 ER-30 is **no longer an activation candidate.** This is a terminal decision
for this trial ID, not a pause pending more data.

**Reason.** On the continuous 2021→2026 window, integrated V3 enforcement makes
results *worse*, not better: **PF 0.69 with V3 versus PF 0.86 without it.** The
original positive case was an artifact of period-selected windows — the four
registry windows omitted the 2023→mid-2024 stretch, and the filter's apparent
edge did not survive evaluation on continuous data. Note that PF 0.86 is itself
below 1.0, so the baseline this was measured against is also unprofitable.

**Consequences.**

- `v3_enforcement_enabled = False` and stays false. Nothing in this document
  authorises turning it on.
- `v3_candidate_threshold = 0.20` is retained **only as historical trial
  metadata**, recording what was tested. It is not a pending configuration.
- The former activation criteria (n >= 20 closed trades, PF > 1.20, bootstrap,
  friction stress, episode concentration) are **withdrawn**. There is no trade
  count at which V3 activates. Do not resume "check every 5 closed trades".
- Future `v3_would_block` observations in `logs/v3_journal.jsonl` are
  **diagnostic only**. They measure filter behaviour; they are not evidence for
  reactivation and cannot trigger it.
- Reactivating V3 in any form requires a **new pre-registered trial ID** with
  its own hypothesis, data boundaries, and acceptance rule, registered before
  any new evaluation is run. It may not inherit this trial's threshold or
  criteria.

The repaired shadow journal is retained as reusable research infrastructure and
as an honest diagnostic record — not as an activation mechanism.

## ADX threshold

| ADX   | Period       | WR   | Avg P&L | Notes             |
|-------|-------------|------|---------|-------------------|
| 20    | full_year   | ~40% | -0.56%  | V1 baseline       |
| 25    | full_year   | ~40% | -0.55%  | Marginal improvement |
| 25    | live_period | 75%  | +4.42%  | 4 trades — not OOS |

ADX=25 was selected after observing the live_period. That period is now IS, not OOS.

## Stop / target multipliers

| atr_stop | atr_target | R:R   | Tested on | Result                    |
|----------|-----------|-------|-----------|---------------------------|
| 2.0      | 3.5       | 1.75  | ZEC full_year | -0.77% avg           |
| 2.5      | 4.5       | 1.80  | ETH (disabled) | n/a                 |

## BTC regime filter

| Config                  | Period      | Impact                        |
|------------------------|-------------|-------------------------------|
| btc_regime_filter=True  | live_period | Blocked all Jun 2026 ZEC signals (ZEC +30% while BTC below EMA) |
| btc_regime_filter=False | live_period | Passed all 30 scanner signals |

Decision: ZEC is decorrelated from BTC during its breakout regimes. Filter disabled for ZEC.

## Fee model iterations

| Maker  | Taker  | Notes                                      |
|--------|--------|--------------------------------------------|
| 0.2%   | 0.4%   | V1 — wrong, inflated backtest P&L          |
| 0.4%   | 0.6%   | V2 — correct Coinbase Advanced base tier   |

## Data source

| Source       | Status  | Notes                                              |
|--------------|---------|---------------------------------------------------|
| yfinance     | Limited | 730-day window for 1h data; inflates P&L vs exchange data |
| Coinbase API | Active  | Exact exchange data (ZEC from 2020-12-08), paginated, parquet cache |

## Periods used

| Period name       | Warmup       | Test window         | Used for              |
|-------------------|-------------|---------------------|-----------------------|
| bull_2021         | 2020-12-08  | Mar – Nov 2021      | Cross-cycle validation (V3 IS research) |
| bear_2022         | 2021-10-01  | Jan – Dec 2022      | Cross-cycle validation (V3 IS research) |
| mid_year_holdout  | 2024-07-14  | Aug 2024 – May 2025 | Cross-cycle validation — NOT clean OOS (overlaps full_year used for ADX/asset selection) |
| recent_year       | 2025-04-01  | Jul 2025 – Jul 2026 | ADX comparison, asset selection — IS |
| live_period       | 2026-04-01  | Jun – Jul 2026      | ADX=25 selection — IS |
| **forward_oos**   | **2026-07-12** | **2026-07-12+**  | **True clean OOS — no parameter selected on this data** |

## Approximate trial count

Counting distinct (asset, ADX, stop/target, fee_model, data_source, period) combinations tested: ~25-30.
V3 ER-30 threshold tested at 0.20, 0.25, 0.30, 0.35, 0.40 on all 4 IS periods → add ~20 more trials.
For proper Deflated Sharpe Ratio correction, log new trials as they occur.

## V3 status summary (2026-07-14)

Infrastructure complete (all IS research done — no further parameter selection allowed):
- `_compute_regime_metrics()`: computes er_30, vm_30, ema50_slope; look-ahead-safe (uses `< day_boundary`); UTC-asserted
- `ASSET_CONFIG["ZEC-USD"]`: `v3_candidate_threshold=0.20` (locked), `v3_enforcement_enabled=False` (shadow only)
- `scan_latest()`: returns `er_30`, `v3_would_block`, `v3_blocked`, `ema200_valid`, `n_daily_bars` in live signal dict
- `runner.py`: SQLite idempotency (`_claim_signal` / `_complete_signal`), shadow-logs `v3_would_block` without blocking
- `v3_journal.py`: append-only JSONL; `V3_SIGNAL` + `V3_OUTCOME` schema; episode grouping (30d gap); `reconcile_pending()` counterfactual resolver; `summarise_journal()` with 5-point criteria check
- `bootstrap_analysis.py`: block bootstrap (b=4, N=10,000) + leave-one-event-out analysis
- `er_threshold_analysis.py`, `cost_stress_test.py`, `v3_integrated_test.py`: IS research scripts (frozen — do not re-run to select thresholds)
- `tests/test_v3_properties.py`: 6 property tests covering look-ahead, UTC, concurrency, crash recovery, resolver idempotency, episode grouping

~~Pending for V3 activation~~ — **WITHDRAWN 2026-08-09.** V3 is retired as an
activation candidate (see STATUS above). There is no OOS trade count that
activates it, no remaining criteria checklist, and no path from this trial ID to
`v3_enforcement_enabled = True`. Continued logging is diagnostic only.

## Independent research pass — 2026-08-09 (continuous-window analysis)

Full write-up: `docs/research/2026-08-strategy-review.md`. All results below are
IS unless marked OOS. Data: Coinbase parquet (ZEC/BTC/ETH/SOL, 1h+1d, through
2026-08-09). Frozen V2 mechanism throughout — no parameter selection performed.

### Harness validation

Stock scanner on the four registry periods reproduces the registry exactly
(bull_2021 n=25 PF=1.42 avg=+0.87%; bear_2022 n=12 PF=1.41 avg=+0.97%;
mid_year n=27 PF=0.52 avg=-1.29%; recent_year n=37 PF=1.00 avg=+0.01%).
Discrepancies below are therefore data, not harness drift.

### Continuous-window results (removes period-selection bias)

| Trial | n | WR | Avg P&L | PF |
|-------|---|----|---------|----|
| ZEC continuous 2021-03 → 2026-07-12, no filter | 133 | 42% | -0.37% | 0.86 |
| ZEC continuous, V3 ER>=0.20 **integrated enforcement** | 80 | 39% | **-0.90%** | **0.69** |
| ZEC gap 2023-01 → 2024-08 (never scanned before) | 24 | 17% | -2.35% | 0.23 |
| ZEC continuous, LOEO (drop Sep–Nov 2025 episode) | 113 | 40% | -0.62% | 0.75 |

Key finding: the V3 IS case (+0.31%/trade across the 4 windows) **does not
survive removal of period windowing** — on the continuous window the filter
makes results worse. The 4-period "cross-cycle" estimate accidentally excluded
~2 years of data (2023 → mid-2024) in which the strategy loses -2.35%/trade.

The apparent inverse cell (er<0.20: +0.43%, PF 1.22, n=53) collapses to
PF 1.02 after removing its single best month (2022-03) — episode concentration,
not signal. Conclusion: ER-30 carries no robust information for this entry
mechanism in either direction.

### Cross-asset transfer (frozen ZEC mechanism, zero per-asset tuning — clean)

> Superseded by the 2026-08-13 warm-up correction below. The figures that stood
> here (BTC n=203 PF 0.38 from 2020-09; ETH n=181 PF 0.50; SOL n=97 PF 0.72 from
> 2022-02) came from an un-provenanced run whose windows do not match any
> committed artifact. Current values are in
> `docs/research/artifacts/results.json` and are reproduced in the table below.

| Asset | n | Avg P&L | PF | Window |
|-------|---|---------|----|--------|
| BTC-USD | 174 | -0.90% | 0.359 | 2021-03-01 → 2026-07-12 |
| ETH-USD | 150 | -0.92% | 0.476 | 2021-03-01 → 2026-07-12 |
| SOL-USD | 97 | -0.56% | 0.718 | 2022-01-03 → 2026-07-12 |
| ZEC-USD | 114 | -0.62% | 0.761 | 2021-06-26 → 2026-07-12 |

The "ER>=0.20 kept" column is removed: those figures have no counterpart in any
committed artifact and cannot be regenerated from this repository.

No asset, no ER bucket, no realized-vol tercile, no BB-width (compression)
tercile, and no vm_30 direction produces a robustly positive cell for the
breakout mechanism. Volatility-compression states do NOT predict profitable
breakout entries here (compressed-BBW cells: ZEC PF 0.50, BTC 0.27, ETH 0.45).

### Strategy-family probes (single pre-declared config each, no sweeps)

> ⚠️ **LEGACY / UNVERIFIED — not reproducible from committed code (2026-08-09).**
> The implementations that produced the numbers in this section are **not in
> this repository**. The deterministic research runner
> (`backtesting/research_runner.py`) therefore cannot regenerate them, and they
> are listed in `docs/research/artifacts/results.json` under
> `non_reproducible`. Treat every figure below as an unverified historical note,
> not as evidence. Do not cite them in a decision, and do not re-derive them from
> memory — either recover the original implementation or run a new, explicitly
> pre-registered replication trial with one frozen configuration.
>
> This applies to: mean reversion, slow/trend following, "buy the strategy
> drawdown", LLM agent-vote IC, and the realised-vol / Bollinger-bandwidth
> regime cells referenced above.

- Mean reversion (1h RSI<30 + lower-BB, 2.0 ATR stop/target): PF 0.30–0.62 on
  all four assets; WORSE in ER<0.20 "range" regimes. **Rejected**, including the
  "route MR to range regimes" idea.
- Trend following (daily 55d-high entry / 20d-low exit, taker fees): positive
  on all four assets (pooled n=57), but profit is concentrated in 1–2 secular
  episodes per asset (BTC +236% Oct-2020→May-2021; ZEC +610% Sep→Dec-2025;
  without it ZEC TF is -7% total). 2022 bear: BTC TF -17% vs B&H -65%.
  **Hypothesis-generating only** — would need its own pre-registered trial.

### Other hypotheses tested and rejected

- "Buy the strategy drawdown": 1–3 qualifying episodes per equity curve
  (insufficient), and forward returns after drawdown thresholds were BELOW the
  unconditional mean in every cell. Rejected on current evidence.
- LLM agent votes as alpha (Apr–Jul 2026 logs, 158 daily-subsampled obs,
  daily-block bootstrap): no agent IC90 excludes zero except one marginal cell
  out of 14 tests (expected under pure noise; that agent's BUY votes preceded
  negative returns). Veto-only role remains the ceiling; no weighting layer.

### Forward OOS observations (deterministic replay 2026-07-12 → 2026-08-09)

Scheduler was down 2026-07-23 → 2026-08-09, so the live shadow journal missed
this window; the scanner is deterministic, so the record is reconstructed from
exchange candles (`backtesting/oos_replay.py`):

| Time (UTC) | ER-30 | v3_would_block | Outcome |
|------------|-------|----------------|---------|
| 2026-07-16 13:00 | 0.098 | True | STOP -4.01% |
| 2026-07-18 12:00 | 0.160 | True | TP +2.92% |
| 2026-07-21 00:00 | 0.230 | False | STOP -3.70% |
| 2026-07-21 13:00 | 0.230 | False | STOP -3.61% |

V3-accepted so far: n=2, both losses. **Diagnostic only.** There is no trade
count that activates V3 — the "≥20 required" target and the criteria checklist
were withdrawn on 2026-08-09 (see STATUS above). Threshold remains locked;
enforcement remains off; no criteria decision is pending because there are no
criteria.

### Trial count update

This pass adds ~30 trials (continuous/gap windows ×2 filter states, 4 assets ×
5 regime bucketings, 2 family probes × 4 assets, DD-buying grid, agent ICs).
Interpret any future marginal positive accordingly.

### Decisions (2026-08-09)

1. V2/V3 momentum family: research artifact, not a path to live. Enforcement
   stays off; shadow journaling continues; the pre-registered OOS trial may run
   to completion but the continuous-window evidence predicts failure.
2. No regime-routing layer, no agent-weighting layer, no drawdown-based
   allocation, no compression gating — all unsupported by data.
3. Slow long-only trend following is **LEGACY / UNVERIFIED**, not a lead. Its
   implementation is not in this repository, it is recorded under
   `non_reproducible` in `docs/research/artifacts/results.json`, and its own
   write-up notes that the profit is concentrated in 1–2 secular episodes per
   asset — the single-episode dependence that killed V2. Pursuing it means
   pre-registering a *replication* trial with a single frozen config, an
   acceptance rule, and leave-one-episode-out as a primary criterion, BEFORE
   any further scans. It is not evidence of an edge.

## Trial `2026-08-warmup-semantics.v1` — warm-up correction (2026-08-13)

Supersedes `2026-08-evidence-hardening.v1`. Not a new strategy and not a
parameter change: a defect correction in how the *existing* mechanism was
evaluated. Superseded artifacts are kept at
`docs/research/artifacts/superseded/2026-08-evidence-hardening.v1/`.

### The defect

`_detect_breakout_signal` failed **open** on a missing indicator. A hard gate
whose operand was NaN was skipped (`x is not None and x < y`), and scored inputs
were coerced to neutral defaults (`_safe(col) or 1.0`). While an indicator was
warming up, the bar was therefore judged by a **weaker mechanism than the config
declares**, and no counter recorded it. The previous config compounded this by
starting each asset's evaluation at its first candle — months before the frozen
mechanism's 200-day daily EMA exists for ZEC and SOL.

### Registered boundaries

Effective start = first bar on the merged 1h grid at which every declared gate
is evaluable. Declared in `RESEARCH_CONFIG["asset_effective_start"]` and
drift-checked each run; ~1 day later than the daily frame's own first-valid bar
because daily stamps are shifted +1d against look-ahead.

| Asset | First cached candle | Effective start | Binding gate |
|-------|--------------------|-----------------|--------------|
| ZEC-USD | 2020-12-08 | **2021-06-26** | daily EMA200 |
| BTC-USD | 2020-01-02 | 2020-07-20 | daily EMA200 (predates window) |
| ETH-USD | 2020-01-02 | 2020-07-20 | daily EMA200 (predates window) |
| SOL-USD | 2021-06-17 | **2022-01-03** | daily EMA200 |

Excluded warm-up: ZEC 2,809 candles / 327 signals refused; SOL 4,736 / 534.
BTC and ETH are unaffected.

### Effect on headline results

| Trial | Superseded | Corrected |
|-------|-----------|-----------|
| ZEC continuous, no filter | n=133, PF 0.855, −0.366%/trade | **n=114, PF 0.761, −0.623%/trade** |
| ZEC continuous, integrated V3 | n=80, PF 0.691, −0.895% | **n=71, PF 0.706, −0.832%** |
| bull_2021 | n=25, PF 1.419, +0.871% | **n=6, PF 0.960, −0.083%** |
| bear_2022 | n=12, PF 1.413 | unchanged |
| mid_year_holdout | n=27, PF 0.523 | unchanged |
| recent_year | n=37, PF 1.004 | unchanged |
| Transfer BTC / ETH | n=174 PF 0.359 / n=150 PF 0.476 | unchanged |
| Transfer SOL | n=118, PF 0.698 | **n=97, PF 0.718, −0.564%** |
| ZEC max DD / longest DD | −71.04% / 1564.3 d | −71.04% / 1564.3 d (unchanged) |

The 19 excluded ZEC trades contributed **+22.28%** between them. The 6 trades in
bull_2021 that the declared mechanism actually judges are net negative — **the
entire apparent 2021 bull-window edge was produced by a span in which the
declared daily-EMA veto could not be computed.** The "period selection"
explanation for that window is therefore incomplete: the more direct cause is
instrumental.

SOL's corrected row (n=97, −0.56%) reproduces the pre-hardening registry figure
exactly, confirming that the original 2026-08 pass had effectively started SOL
after its warm-up and that the evidence-hardening config regressed this.

### Conclusions — unchanged in direction, stronger in degree

- V2 momentum remains unprofitable; the correction makes it **worse**
  (−0.62%/trade vs −0.37%).
- Integrated V3 (PF 0.706) remains worse than no filter (PF 0.761). Retirement
  stands.
- No asset reaches PF 1.0 under the frozen mechanism.
- `DRY_RUN=true`, `v3_enforcement_enabled=False`, LIVE **NO-GO**.

### Live-path change (deliberate)

`scan_latest()` shares `_detect_breakout_signal` with the research scanner, so
fail-closed applies live. A failed daily-candle download previously dropped the
daily trend veto and could emit a BUY during a data outage; it now refuses and
logs the missing inputs. Covered by `tests/test_gate_availability.py`.

### Recorded, not fixed in this trial

1. **`backtesting/walk_forward.py` never attached the daily frame at all**, so
   the declared daily-EMA gate was absent for every signal on every asset — it
   has always validated a weaker mechanism than it reports. Its loop also
   enumerated only three blocked reasons, so `daily_trend` and `btc_regime`
   blocks fell through and were traded. It now raises rather than reporting a
   meaningless result. Any previously recorded walk-forward number is void.
2. **`agents/breakout_agent.py`** carries the same `or 1.0` / `or 0.0` fallback
   idiom. It feeds an advisory vote, not the entry gate, so it is out of scope
   here.
3. `requirements.txt` pins no versions, so `ta`/`pandas`/`numpy` upgrades can
   move every number above with no artifact change.
4. `research_runner.py --verify` is not run in CI.

## Methodology repair — `2026-08-walkforward-repair.v1` (2026-08-18)

**Registered BEFORE any result was produced.** Phase 6.10. This is a tool
repair, not a search for edge, and it is pre-registered precisely so that the
numbers it eventually emits cannot be presented as a discovery.

### Status of everything this tool ever produced

**VOID.** `backtesting/walk_forward.py` has been disabled since the warm-up
trial. Its defects were not marginal:

1. `_load_asset` never attached the daily frame, so `close_1d` / `ema*_1d` were
   absent from every row and the declared daily-EMA trend gate was skipped for
   every signal on every asset.
2. The scan loop enumerated only three blocked reasons, so `daily_trend` and
   `btc_regime` blocks fell through and were **traded**.
3. It used `FEE_RATE = 0.006` for entry *and* exit, while the registered
   mechanism is maker 0.4% entry / 0.4% take-profit / taker 0.6% stop. Entry was
   overcharged and take-profit exits were overcharged by 50%.
4. An unfilled max-hold horizon at the right edge of the data was reported as a
   completed `MAX_HOLD` trade — an invented outcome.
5. Unknown assets silently fell back to the ETH strategy config and an empty
   `ASSET_CONFIG`, i.e. a different mechanism under the asset's own name.

The README table that claimed "ZEC-USD +0.30% avg OOS — ✅ EDGE / the only asset
with genuine out-of-sample edge" came from this tool. It is withdrawn and must
not be revived.

### What the repair may and may not conclude

The windows below were **inspected repeatedly during the original development**
and are recorded in this registry's trial count. Re-running them on a corrected
tool produces **historical diagnostics only**:

- it may establish that the tool now measures the mechanism it declares;
- it may **not** be called clean OOS;
- it may **not** be cited as evidence of edge, in either direction, for
  activation purposes.

A genuine out-of-sample claim requires a new pre-registered hypothesis and
forward data that has never been examined. That is Phase 7 and is not
authorised by this entry.

### Frozen protocol

| Item | Value |
|---|---|
| Windows | 3 rolling, unchanged from the original tool (2024-09-01 → 2025-06-28) |
| Train / test split | half-open `[train_start, test_start)` and `[test_start, test_end)`, non-overlapping within a window |
| Stop candidates | `[1.5, 2.0, 2.5, 3.0]` — **frozen, not extended** |
| R:R | 1.75, fixed |
| Selection rule | highest train avg P&L with n >= 3 resolved trades |
| Fees | the registered model: entry 0.4%, take-profit 0.4%, stop/max-hold 0.6% |
| Censoring | an unobservable horizon is PENDING and excluded from every statistic |
| Gates | all declared gates enforced; any blocked reason means no trade |

No new sweep parameter, asset, window or threshold is introduced. Adding one
would make this a search rather than a repair.

### Provenance

Output is a deterministic artifact under
`docs/research/artifacts/walk_forward/`, carrying content hashes of the
result-determining code, the pinned environment, and the window-scoped logical
hash of every input — the same scheme as the main research runner
(`ohlcv-logical-v1`). It is verifiable with
`python backtesting/walk_forward.py --verify`.

Live status is unchanged and cannot be changed by this work: **LIVE NO-GO**,
`DRY_RUN=true`, V3 off.

## Infrastructure change — Phase 6.9 reproducibility (2026-08-15)

Not a trial: no scan was run, no parameter changed, and `results.json` is
**byte-identical** before and after. Registered here because it changes what the
provenance artifact asserts.

### What changed

1. **Computational environment pinned and recorded.** `requirements.txt` listed
   twelve bare package names with no versions. `numpy`/`pandas`/`ta` compute
   every indicator, and `pyarrow` — the parquet engine pandas selects implicitly
   — was not even listed, so it was a silent unpinned dependency of every
   number. All direct dependencies are now pinned exactly; the four
   result-determining ones plus the canonical interpreter (Python 3.13.5 exact)
   are recorded in `manifest.environment`, while the exact declared pins for
   the whole COMPUTATIONAL CLOSURE are separately content-addressed in
   `manifest.dependencies`. Pinning only the four roots was not enough:
   `python-dateutil`, `six` and `tzdata` sit underneath pandas, decide how
   timestamps parse, and were resolved by whatever pip happened to pick, so a
   fresh install could move the numbers while `--verify-code` stayed green.
   A change to either declaration or installed environment now invalidates
   verification loudly, and a test recomputes the closure so a newly introduced
   transitive dependency cannot slip in unpinned. `write_artifacts` refuses to
   run on a non-canonical Python.

   Note: CI ran Python 3.11 while the artifacts were produced on 3.13. Any
   research check added to CI before this would have compared numbers computed
   under a different interpreter. CI is now on 3.13.

2. **Provenance identity is content-addressed.** SHA-256 per `_CODE_PATHS` file
   plus aggregate `code_sha256`, exact numerical dependency pins and the actual
   environment are bound by `provenance_sha256`. Verification compares the
   exact file list as well as the aggregates and fails closed on missing,
   escaping or symlinked declarations. `code_commit` is informational only —
   it went stale twice already, each time needing a follow-up commit
   purely to repoint it, and a squash merge no longer requires one. Artifact
   generation now refuses a dirty working tree.

3. **`--verify-code`**: exact source/dependency/environment provenance, in
   milliseconds, with no candle cache and no git history. It is a required CI
   check and its real CLI boundary is tested in rewritten shallow and
   history-free checkouts.

### Input identity — RESOLVED 2026-08-15, option (a)

Adopted: **window-scoped canonical logical hash**, `ohlcv-logical-v1`.

An input's identity is the OHLCV a registered scan can actually read:

| Field | Value |
|---|---|
| `scope_start_inclusive` | `2020-01-01T00:00:00+00:00` (`_DAILY_HISTORY_START`) |
| `scope_end_inclusive` | `2026-07-12T00:00:00+00:00` |
| Encoding | schema `time, open, high, low, close, volume`; int64 UTC epoch ns big-endian; OHLCV IEEE-754 float64 big-endian; sorted by time |
| Fails closed on | duplicate timestamps, NaN/±Inf, non-numeric, missing column, no rows in scope |

The start is the daily history start, **not** `asset_effective_start`: warm-up
rows determine the EMAs and therefore gate availability itself, so they are
input, not context. The end is **inclusive** because
`coinbase_candles.download` slices `time >= start & time <= end`, so the
boundary bar is read.

The physical parquet SHA-256 is retained as `physical_sha256`, informational
only. Gap budgets were re-measured inside the scope: ZEC 1h drops 16 → 15,
because one of its gaps lay entirely in the tail and had been consuming budget
for data the research never reads.

Consequence: the full `research_runner.py --verify` is now a **required CI
check**, fed by credential-free public hydration.

### Superseded analysis — why whole-file hashing failed

> Historical. This records the measurement that motivated the decision above.
> The full `--verify` replay **is** now a required check; at the time of this
> analysis it was not, and the reason was a methodology question rather than a
> plumbing one.

`backtesting/hydrate_research_data.py` was written and works: it materialises
the candle cache from Coinbase's **public** endpoint with no credentials
(`RESTClient()` + `get_public_candles`), so CI needs no exchange secrets. A local
run from an empty `data/candles/` was measured:

- **`results.json` regenerated BYTE-IDENTICALLY.** The research is reproducible
  from public data — measured, not assumed.
- **Zero** differing rows fall inside the evaluation window.
- Yet **all eight input SHA-256s mismatch.**

| Dataset | Rows | Differing / extra | Inside window |
|---|---:|---:|---:|
| BTC/ETH/SOL 1d | 2412 / 2412 / 1880 | 1 each (2026-08-09) | 0 |
| BTC/ETH/SOL 1h | 57888 / 57888 / 45082 | 1 each (2026-08-09 16:00) | 0 |
| ZEC 1d | 2071 | 4 (2026-07-13/15/17, 08-09) | 0 |
| ZEC 1h | 49664 | 3, incl. one extra bar 2026-07-17 01:00 | 0 |

Every difference is at or after the 2026-07-12 freeze: the last cached candle
was incomplete when the cache was built and the exchange has since completed it,
plus a few late-July ZEC revisions.

The defect is in the manifest scheme, not the data: **an input hash covers the
whole parquet file, including rows outside the evaluation window that the
exchange keeps revising.** The hash therefore asserts more than what determines
the results, and a required job built on it would fail on data the research
never reads.

Option **(a)** was chosen and is implemented above. (b) release-pinned inputs and
(c) deliberate re-baselining were rejected: the first adds a hosting dependency,
the second makes every stale tail a manual step and invites exactly the
"regenerate until green" habit this work exists to prevent.

### Professional review addendum (2026-08-09)

See `docs/research/2026-08-professional-review-addendum.md`.

- The central continuous-window falsification is independently reproduced and
  accepted: V2 remains negative and integrated V3 remains worse. Live stays
  NO-GO; `DRY_RUN=true`.
- ~~The current `oos_replay.py` result is diagnostic rather than the formal OOS
  record because it post-filters a non-enforced path. The V3 journal also
  conflates shadow enforcement acceptance with candidate acceptance and does
  not currently close the registered accepted cohort end-to-end.~~
  **RESOLVED 2026-08-09** (PR #4): `oos_replay.py` runs a real integrated path
  with right-censoring, and `v3_journal.py` separates `candidate_accepted`,
  `enforcement_accepted` and disposition. Recorded here as a review finding
  that was acted on, not as a live defect.
- The 92% underwater value is observation-weighted. A boundary-aware calendar
  audit gives approximately 97.15% through 2026-07-12; max DD remains about
  -71% for this ZEC sequence.
- **Recommendation (not an activation decision):** retire V3 as a deployable
  candidate, fix the research evidence pipeline, and pre-register slow trend
  following as a separate strategy family before any new scan.
- This research branch predates the latest main/safety hardening. Integrate the
  research-only changes onto the current safety history; never deploy from the
  research branch itself.

## Trial `2026-09-cost-sensitivity.v1` — prospective cost sensitivity (2026-09-17)

**PROSPECTIVE COST SENSITIVITY study — NOT a trial of a mechanism, NOT an edge
test, NOT a parameter search.** Full write-up:
[`2026-09-cost-sensitivity.md`](2026-09-cost-sensitivity.md). Its numbers must
never be quoted as an edge result or as a restatement of any historical
artifact, and it authorizes nothing.

**Question.** PR #18 adopted a measured prospective operational fee schedule
(`pipeline/fees.py` `CURRENT_SCHEDULE`: maker 0.6% / taker 1.2%, always taker
on exit — see CLAUDE.md's "Fees" section) that is higher than the frozen
historical research assumption (`_ENTRY_FEE`/`_TP_FEE`/`_SL_FEE`: 0.4% maker
entry, 0.4% maker take-profit, 0.6% taker stop/max-hold). What gross move must
a trade clear to break even at the measured tier, and where does the frozen
V2 ZEC mechanism (trial `2026-08-warmup-semantics.v1`, n=114, PF 0.761,
-0.62%/trade) sit against that threshold?

**Two scenarios reported, per standing decision.** Coinbase changed the
account's tier again on 2026-09-16 — one day after the 4-reading cohort
`CURRENT_SCHEDULE` was itself confirmed from. As of 2026-09-17 there is one
probe reading at the new tier (maker 0.5% / taker 0.9%, `pricing_tier
"Intro"`), not yet the same 4-consecutive-reading bar. Memory
`fee-tier-change-2026-09-16` records the decision the same day: do not adopt
on one reading, but *"any cost-sensitivity analysis must report both 1.8%
and 1.4% scenarios."* This trial reports both: **ADOPTED**
(`pipeline/fees.py` `CURRENT_SCHEDULE`, 0.6%/1.2%) and **CANDIDATE, not
adopted** (single 2026-09-17 reading, 0.5%/0.9%, hardcoded as local script
constants — not written into `pipeline/fees.py`).

**Method.** `backtesting/cost_sensitivity.py` re-runs the exact scan behind
the `results.json` row `{asset: ZEC-USD, trial: V2-continuous}`, asserts it
reproduces `n=114, PF=0.76141, expectancy=-0.006227` byte-for-byte, then
re-prices the same entry/exit prices at each schedule. No frozen research
constant was changed; no existing artifact was regenerated, overwritten, or
superseded; no parameter was searched or swept.

**Headline numbers** (arithmetic and full breakdown in the write-up):

| | PF | Expectancy |
|---|---:|---:|
| Frozen artifact (historical fee model) | 0.761 | -0.62%/trade |
| PROSPECTIVE SENSITIVITY — ADOPTED (0.6%/1.2%, same 114 trades) | 0.517 | -1.49%/trade |
| PROSPECTIVE SENSITIVITY — CANDIDATE, not adopted (0.5%/0.9%, same 114 trades) | 0.617 | -1.09%/trade |

- Break-even gross move: **+1.82%** at the ADOPTED schedule, **+1.41%** at
  the CANDIDATE (unconfirmed) schedule, uniformly across
  STOP_LOSS/MAX_HOLD/TAKE_PROFIT under both — `close_position()` always
  prices the exit leg at TAKER, unlike the frozen research model's cheaper
  maker-priced TAKE_PROFIT assumption.
- At the median historical ATR, the ADOPTED break-even consumes ~57% of the
  frozen mechanism's own 2.0x-ATR stop distance and ~33% of its 3.5x-ATR
  target distance; the CANDIDATE break-even consumes ~44% and ~25%
  respectively.
- Converting TAKE_PROFIT exits to maker orders (cost side only, no non-fill
  rate assumed — that needs its own pre-registered study) recovers only part
  of the gap under either schedule: PF 0.582/-1.29%/trade (ADOPTED) or PF
  0.664/-0.96%/trade (CANDIDATE). Still clearly negative both ways.

**Decision.** This strategy family is not viable at either the measured
(adopted) or the candidate (unconfirmed) operational cost structure; cost
alone is sufficient to reject it independent of further mechanism research.
Does not change `DRY_RUN`, `LIVE_BALANCE_USD`, `ASSET_CONFIG`, V3 status, or
Phase 7B status — none of which this trial touches or authorizes. Does not
resolve whether the candidate tier should be formally adopted — that decision
rests on the 4-reading cohort (2026-09-17 → 2026-09-20) per
`fee-tier-change-2026-09-16`, independent of this trial's conclusion.
**LIVE NO-GO stands.**

---

## Standing policy — evidence requirements for every trial registered after this commit (2026-09-18)

**Pre-registered for future trials; post-hoc for the two closures recorded
below, which are its first applications.** This distinction is load-bearing:
applying a rule after the data is in is not the same act as declaring it before,
and the two closures do not get to claim the strength of a pre-registration they
did not have. Every trial registered *after* this commit does, and there is no
grandfather clause.

Origin: `docs/research/literature/2026-09-17-review/`, in particular report 05
§7b (equivalence framing, Lakens 2017), §1c (DSR needs `V[{SR_n}]`, PBO needs a
synchronous `(T × N)` P&L matrix — neither reconstructible from PF and n alone),
and §7c (Bailey et al.: "the counter of trials cannot be turned back"). Filing a
review authorizes nothing; this section is the one thing that review changes
about how trials are run.

### 1. Declare a SESOI and a kill rule, before the trial runs

Each trial's registry entry states, **before any data contact**:

- a **SESOI** — the smallest economically meaningful per-trade edge — derived
  from the **operational fee schedule in force at registration**
  (`pipeline/fees.py` `CURRENT_SCHEDULE`, not the frozen research constants, and
  not a candidate tier that has not cleared its reading cohort); and
- a **kill rule**, stated exactly as: *the hypothesis of an edge ≥ SESOI is
  rejected when the one-sided 95% upper bound on the per-trade mean falls below
  SESOI.*

The bound is `mean + 1.645 · SD / √n` on the trial's own measured per-trade net
return series, with a percentile bootstrap on the mean reported alongside it as
a shape check (fixed seed, declared at registration, not searched afterwards).

A SESOI chosen after seeing the result is p-hacking with extra steps. A kill
rule that cannot fire is not a kill rule.

### 2. Declare the expected decidable-edge floor, and do not start a trial that fails it

Each trial also states, before it runs, its **decidable-edge floor**:

```
floor = 1.645 · SD / sqrt(n_effective)
```

where `SD` is the best available estimate of per-trade net return dispersion
(measured if one exists for a comparable mechanism, borrowed and explicitly
labelled as borrowed otherwise) and `n_effective` is the number of
**independent** trade events the trial expects to observe — not the raw trade
count. Where assets are correlated, `n_effective` is reduced accordingly, and
the estimator used must be named in the entry.

**If that floor exceeds the trial's own SESOI, the trial is not started.** A
trial that cannot distinguish the smallest edge worth having from zero cannot
decide its own question, and running it produces a number that was never going
to mean anything. This is a gate on starting, not a caveat to add afterwards.

### 3. Retain per-trial return SERIES in the artifacts

Summary statistics are not enough. Each trial's artifacts carry the per-trade
return series itself, so that multiple-testing corrections — DSR's `V[{SR_n}]`,
CSCV's `(T × N)` matrix — remain computable later. This costs nothing at the
time and is the only thing that makes them computable at all afterwards.
Retention is not conditional on a trial succeeding; failed trials are exactly
the ones those corrections need.

### 4. The trial counter is monotone

Retiring a line and later reviving it **increments N**. A revived line is a new
trial ID with its own hypothesis, boundaries and acceptance rule, and it may not
inherit the retired line's thresholds, criteria or multiple-testing budget. The
count of trials attempted in this repository never decreases, and every future
claim is measured against the running total, not against the number of trials
currently considered live.

### What this policy does not do

It does not authorize anything. It does not alter `DRY_RUN`,
`LIVE_BALANCE_USD`, `ASSET_CONFIG`, V3's retirement, Phase 7B's unauthorized
status or the standing decision that 7R3b is not run, and it does not license a
new trial. It is a constraint on how a future trial must be registered if one is
ever opened.

---

## Closure 1 — V2 / ZEC momentum: RETIRED AS AN ACTIVATION CANDIDATE, on EDGE (2026-09-18)

**No new trial ID.** This is a verdict on the existing line
`2026-08-warmup-semantics.v1` (ZEC-USD, continuous window 2021-06-26 →
2026-07-12, n=114, PF 0.76141, −0.6227%/trade). No scan was re-run, no artifact
regenerated, no frozen constant touched. It is the first application of the
standing policy above — **post-hoc**, by construction.

### The measurements

Per-trade net return series, n=114, from
[`research/2026-09-cost-sensitivity.md`](research/2026-09-cost-sensitivity.md)
§6 (sample SD, n−1; `SE = SD/√114`; bound = `mean + 1.645·SE`):

| Scenario | Mean | **Measured SD** | SE | One-sided 95% upper bound | Bootstrap bound (10^5 resamples, seed 20260917) |
|---|---:|---:|---:|---:|---:|
| Frozen 1.0% research model | −0.6227% | **5.1095%** | 0.4786% | **+0.1645%** | +0.1675% |
| ADOPTED, measured (0.6% maker / 1.2% taker) | −1.4918% | **4.9969%** | 0.4680% | **−0.7220%** | −0.7194% |
| CANDIDATE, not adopted (0.5% / 0.9%) | −1.0909% | **5.0121%** | 0.4694% | **−0.3187%** | −0.3161% |

**Bootstrap agreement.** The per-trade distribution is visibly non-normal at the
level of individual trades (skewness ≈ +0.62, excess kurtosis ≈ −0.32; the
STOP_LOSS and TAKE_PROFIT clusters make it two-humped), so the normal-theory
bound is checked rather than assumed. The percentile bootstrap on the *mean*
lands within 0.003–0.004 percentage points of the normal-theory bound in every
scenario and falls on the same side of zero in every scenario. The conclusion
does not rest on the normal approximation.

The measured SD is **larger** than the +1.75R/−1R two-point approximation the
literature review worked from (4.70%), so that review's bound of +0.105%/trade
was too narrow. The measured numbers above supersede it. The review's own caveat
predicted the direction of its error correctly.

### The SESOI, and its arithmetic

SESOI is set from the operational fee schedule's **break-even gross move** — the
gross move a trade must clear before it earns anything — at **one tenth** of it:

| Schedule | entry / exit rate | Break-even gross move | **SESOI = 10% of break-even** |
|---|---|---:|---:|
| Frozen 1.0% research model (taker-priced exit) | 0.4% / 0.6% | +1.0060% | **+0.1006%/trade** |
| ADOPTED (`pipeline/fees.py` `CURRENT_SCHEDULE`) | 0.6% / 1.2% | +1.8219% | **+0.1822%/trade** |
| CANDIDATE, not adopted | 0.5% / 0.9% | +1.4127% | **+0.1413%/trade** |

The reasoning behind the one-tenth coefficient: an edge smaller than a tenth of
what the venue takes per round trip means more than 90% of the mechanism's gross
output is fee, and the venue — not the account — is the party the mechanism
earns for. **The coefficient is not load-bearing here.** At both operational
schedules the bound is below **zero**, so the kill rule fires for *any*
non-negative SESOI whatsoever; the coefficient matters only for the frozen-model
row, where it does not change that row's outcome either
(+0.1645% > +0.1006%, so the frozen model does not reject).

### Applying the kill rule

| Schedule | Bound | SESOI | Bound − SESOI | Fires? |
|---|---:|---:|---:|---|
| Frozen 1.0% research model | +0.1645% | +0.1006% | +0.0639% (+0.13 SE) | **No** |
| ADOPTED, measured | −0.7220% | +0.1822% | −0.9042% (−1.93 SE) | **Yes — REJECT** |
| CANDIDATE, not adopted | −0.3187% | +0.1413% | −0.4600% (−0.98 SE) | **Yes — REJECT** |

Measured against zero rather than against SESOI, those two bounds sit 1.54 SE
(adopted) and 0.68 SE (candidate) below it. The adopted-cost rejection is the
robust one; the candidate-cost rejection is nearer the boundary but holds under
both the normal-theory and the bootstrap bound at this sample size.

### Verdict — RETIRED AS AN ACTIVATION CANDIDATE

The sample does **not** establish that this mechanism loses money — at n=114 the
conventional test is uninformative (t = −1.41, p = 0.159; 95% PF interval
roughly [0.49, 1.13]), and anyone citing PF 0.761 as proof of a negative edge is
over-reading it — and the sample **does** bound any true edge below zero at
operational cost, because the one-sided 95% upper bound on the per-trade mean is
−0.7220% at the adopted schedule and −0.3187% at the candidate schedule, so a
profitable version of this mechanism is ruled out at the costs we face. Both
statements are true at once, they are not in tension, and neither may be quoted
without the other.

The precise reading is this: **the edge is not demonstrably absent, it is
demonstrably smaller than the cost of trading it on this venue.** At the frozen
1.0% research model the one-sided 95% upper bound is **+0.1645%, above zero** —
the sample there does not rule out a small positive edge at all. The rejection
comes from the **cost gap**, not from the signal being disproved. Nothing in
this closure shows the mechanism has no edge; it shows that whatever edge it may
have is smaller than what Coinbase charges to collect it.

(The PF interval is an approximation from the two-point +1.75R/−1R payoff model,
quoted as [0.50, 1.13] in `literature/2026-09-17-review/04-momentum-breakout-evidence.md`
§5a and [0.49, 1.11] in that review's `00-synthesis.md` §1. It is not measured,
and the measured SD above implies the true interval is wider than either.)

Two consequences follow, and no others:

- V2 / ZEC is **retired as an activation candidate**, terminally for this trial
  ID, on the same footing as V3: there is no trade count, no shadow cohort and
  no forward window that reactivates it. Reviving this line means a new
  pre-registered trial ID under the standing policy above, which increments N.
- Because the rejection is a cost verdict rather than a signal verdict,
  **execution optimisation cannot rescue it.** Perfect maker-both-legs execution
  at the current rates reproduces a ~1.0% round trip — the exact cost assumption
  under which this mechanism already measures PF 0.761. The ceiling on execution
  work is the losing baseline, not break-even.

Unchanged by this closure: `DRY_RUN=true`, **LIVE NO-GO**, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, V3's retirement, Phase 7B's unauthorized status, and the
standing decision that 7R3b is not run. Shadow/paper journaling continues as
research infrastructure.

### Recorded alongside, not part of the verdict

The cross-asset ordering among BTC / ETH / SOL / ZEC is **not established**. The
BTC (t = −6.79) and ETH (t = −4.47) losses are real; but no pairwise gap among
the four survives Bonferroni across six comparisons, and SOL vs ZEC is a coin
flip (t = −0.19). "ZEC is the least bad" is the top of an unresolved ranking of
degrees of losing, not a fact about ZEC — ZEC's selection as the shadow asset
rests on noise. These t-statistics come from the review's two-point payoff
approximation applied to the artifact's PF/n values
(`literature/2026-09-17-review/04-momentum-breakout-evidence.md` §4b), which
understates dispersion, so the true |t| are smaller and the ordering is if
anything *less* resolved than shown. This does not change the verdict above; it
removes a premise that was never load-bearing and should stop being repeated.

---

## Closure 2 — broad-universe Coinbase spot trend: NOT STARTED, on FEASIBILITY (2026-09-18)

**No new trial ID, and no trial.** This closes a line that was never opened. It
is the first application of the standing policy's rule that *a trial whose
decidable-edge floor exceeds its own SESOI is not run* — the gate fires at
registration, before any data contact, which is the point of having it.

**This closure is not comparable to Closure 1 and is deliberately not worded
like it.** Closure 1 measured a mechanism and bounded its edge. This one
measures nothing about returns at all.

### What was measured

From [`research/data/universe_inventory_2026-09-17.md`](research/data/universe_inventory_2026-09-17.md)
— a read-only inventory in which no strategy return, PF, Sharpe, expectancy,
equity curve or asset performance ranking is computed anywhere, in the document
or in its source script:

| Candidate rule | Pairs (N) | rho_bar | N_eff | Common overlap | Decidable-edge floor @95% |
|---|---:|---:|---:|---|---|
| A: ≥3y history, median 30d vol ≥ $1M | 31 | 0.5801 | 1.685 | 2023-07-13 → 2026-09-18 (3.19 y) | **2.02–2.97%** |
| B: ≥2y history, median 30d vol ≥ $250k | 61 | 0.5695 | 1.734 | 2024-09-04 → 2026-09-18 (2.04 y) | **2.48–2.97%** |
| C: ≥1y history, median 30d vol ≥ $5M | 20 | 0.5793 | 1.666 | 2025-08-20 → 2026-09-18 (1.08 y) | **3.11–3.76%** |

`rho_bar` is the mean pairwise Pearson correlation of daily log returns over each
rule's common overlap window; `N_eff = N / (1 + (N−1)·rho_bar)`. Each floor is a
range because it is bracketed by two non-nested bases — a saturating
portfolio-wide cluster count and a correlation-adjusted pooled count — and
neither end dominates.

**20 to 61 Coinbase USD pairs carry the information of fewer than two
independent assets.** All three candidate rules land in the same narrow band,
rho_bar 0.5695–0.5801 and N_eff 1.666–1.734. No rule escapes it.

### The gate, and why it fires

Against a break-even gross move of **1.41%** at the candidate schedule, every
floor above is larger than the entire round-trip cost — 1.4× to 2.7× it — before
any SESOI is applied at all. Under the standing policy's SESOI construction
(one tenth of break-even, +0.1413%/trade at that schedule) the floors exceed the
SESOI by a factor of roughly 14 to 27.

The horizon arithmetic is the part worth recording. At rule A's pooled event
rate — `N_eff 1.685 × 3.094 entries/asset/yr` = **5.21 entries/yr** — and the
measured per-trade SD of 4.9969% (the adopted-schedule series from Closure 1,
**borrowed** and labelled as borrowed, since nothing about this universe's own
dispersion has been measured):

| Floor target | n required | Years at 5.21 entries/yr |
|---|---:|---:|
| 1.0%/trade | 67.6 | **~13 years** |
| 0.5%/trade | 270.3 | **~52 years** |

A trial that needs thirteen years before it can detect a 1%/trade edge, on a
venue whose fee schedule changed twice in the month this was written, is not a
trial. It is a commitment to wait.

### Verdict — NOT STARTED

**A broad-universe trend trial on Coinbase spot is not started, because it
cannot decide its own question within a usable horizon.** The decidable-edge
floor for every candidate rule exceeds that rule's own SESOI by more than an
order of magnitude, which is precisely the condition under which the standing
policy above says a trial is not run.

**This closes the line on FEASIBILITY.** No return hypothesis was evaluated, no
P&L was computed, no strategy was simulated and no asset was ranked. **This is
therefore NOT evidence that such a program would be unprofitable** — it is not
evidence about profitability in either direction, and it must never be cited as
if it were. The line is closed because the question is undecidable here at this
event rate, not because it was answered.

### Why breadth does not help

The binding constraint is **cross-asset correlation, not trade count.** Going
from 20 pairs to 61 moves `N_eff` from 1.666 to 1.734 — the information content
of the book barely moves, while each added pair pays the full round-trip cost on
every one of its own entries. **Adding correlated pairs multiplies cost without
adding information.** Rule B is the direct demonstration: it admits roughly
twice rule A's pairs and ends with the *smaller* pooled n, because admitting
2-year-old listings shortens the overlap window every member must share. "Trade
more names" is not an escape from this; it is the same trial with a larger fee
bill.

### These floors are the optimistic end

The correlation-adjusted pooled floor treats `N_eff` independent assets as each
firing at its measured event rate for the whole overlap window. That ignores two
things, and both cut the same way: **one asset's entry events cluster in time**,
and **a single trend regime moves many names together beyond what a daily-return
`rho_bar` captures.** `rho_bar` is itself a full-window average of a
regime-dependent quantity — report 03 of the review records BTC-alt R² swinging
from 0.89 to roughly zero inside 24 months — so it is not a guarantee for any
sub-period. The true floors are therefore **higher** than the table, by an
unquantified amount, and the horizon arithmetic above is correspondingly
optimistic.

### What this closure does not do

It does not evaluate, authorize, rank or reject any strategy family. It does not
touch the LEGACY / UNVERIFIED slow-trend note (Project State item 6), which
remains exactly what it was. It does not bear on Phase 7B, whose separate
coverage contract is still open and still unmet; nor on 7R3b, which stands not
run on its own separate grounds. It changes nothing operational. A future
broad-universe test is not forbidden by this entry — it is required to clear the
floor gate first, with a genuinely different universe, venue or event rate that
makes its own question decidable.

---

## Closure 3 — 4h BTC/ETH perpetuals: NOT STARTED, on FEASIBILITY (2026-09-19)

**No new trial ID, and no trial.** This closes a line that was never opened,
using the same rule as Closure 2: a trial whose decidable-edge floor exceeds its
own SESOI is not run. Source:
[`../research/2026-09-19-perps-gate.md`](../research/2026-09-19-perps-gate.md) —
a blind gate that computed no P&L, profit factor, expectancy, Sharpe, drawdown
or equity curve, and ranked neither mechanism nor asset by outcome.

**This closure is not comparable to Closures 1 or 2 and is deliberately not
worded like either.** Closure 1 measured a mechanism and bounded its edge.
Closure 2 and this one measure nothing about returns at all.

### What was measured

Two mechanisms, declared before the gate touched data, fixed with no parameter
searched, tuned or compared on outcome: **M1**, a Donchian 55/20 breakout on 4h
bars (the frozen STF-CLOSE-55-20 rule on a faster clock — its lookbacks are
imported from the frozen protocol, not chosen for this gate); and **M2**, a
30-bar time-series-momentum sign rule, always in a position, both assets. They
bracket the event-rate axis: the rarest plausible rule and the densest one.

BTC and ETH USDT-margined perps (Binance, proxy history for Coinbase CFM,
venue mismatch declared) carry **rho_bar 0.8385 on 4h log returns, N_eff
1.088** — two perps hold less independent information than the 20-61 spot
pairs of Closure 2 (N_eff 1.666-1.734). Breadth is worse here, not better.

| Mechanism | Basis | Decidable-edge floor @ 6.7y | SESOI | Gate |
|---|---|---:|---:|---|
| M1 (Donchian 55/20, 4h) | BTCUSDT / ETHUSDT | **22.1-28.8%/yr** | 10%/yr | **FAIL** |
| M2 (30-bar TSMOM, every bar) | BTCUSDT / ETHUSDT | **37.8-49.4%/yr** | 10%/yr | **FAIL** |

Both fail by 2.2x to 4.9x. Horizons to reach the 10%/yr floor at each
mechanism's own BTC event rate: **33 years** for M1 (34% of bars held) and
**96 years** for M2 (always in a position). A trial that needs a human
lifetime before it can detect the smallest edge worth having is not a trial;
it is a commitment to wait — the same reading Closure 2 gave its own 13-52
year horizons.

### The identity, stated as the reusable result

Substituting the gate's declared dispersion construction (`sigma_trade =
SD_bar · sqrt(mean hold in bars)`) into the floor gives:

```
floor_annual = 1.645 · SD_bar · sqrt(bars in position per year / Y)
```

Because trades-per-year times mean-hold-in-bars **is** bars-in-position-per-year,
**the trade count cancels out of the floor exactly.** Trading the same exposure
more often cannot lower the annual floor — it depends only on per-bar
dispersion and time in market — and it only multiplies the fee bill, which
scales with the trade count and not its square root. M2's worse floor than
M1's comes entirely from spending 2.9x the time in market, not from its 10x
trade count; the trade count bought it nothing statistically and cost it
34%/yr in fees. There is no trading frequency that escapes this identity, on
these assets, at this cost.

**Consequence.** At BTC's measured ~60% annualised volatility, the floor
identity's one-sided 95% bound is `1.645/sqrt(Y)` on a per-unit-of-volatility
basis: at the 6.7-year proxy history that is **0.64**, and even a full 20-year
history only reaches **0.37**. A directional program on an asset at this
volatility therefore needs an after-cost Sharpe above roughly 0.6-0.4,
depending on how much history is available, before this venue's history could
tell it apart from zero. No documented retail crypto directional result in the
literature this project has reviewed is measured net of a round-trip cost this
high (the review's own synthesis: "No study models a 1.5-2% round trip
explicitly," and "No peer-reviewed result at >=1.5% round-trip cost" —
`literature/2026-09-17-review/02-strategy-families-cost-tolerance.md`), so this
is an absence of evidence rather than evidence that no such result exists —
but it is the honest state of the record.

### Verdict — NOT STARTED

**A directional trial on 4h BTC/ETH perpetuals is not started, because neither
declared mechanism can decide its own question within a usable horizon.** This
closes the **CLASS** of directional programs on these two assets at this cost
and volatility level — not one venue, not one mechanism. A different asset, a
materially lower-volatility instrument, or a materially cheaper venue changes
the inputs to the identity and would have to be re-evaluated on its own terms;
this closure does not pre-judge that case. **No P&L, profit factor, Sharpe,
drawdown or equity curve was computed anywhere in the gate document or its
source code**, and no return hypothesis was evaluated.

### The correction this closure records

The 2026-09-18 CFM scoping document ([`../research/2026-09-18-perps-scoping.md`](../research/2026-09-18-perps-scoping.md)
§7) reported a 4h decidable-edge floor of **0.0286%/trade at three years** and
called it "the first time in this repository a floor has come in below the
cost of trading." **That number was correct and the reading was wrong.** A
mechanism trading at that frequency must clear it roughly 2,387 times a year;
expressed annually the same floor is **~68%/yr** — worse than every spot
figure Closure 2 recorded. The per-trade framing divided the requirement by
the very trade count that generates it. **State every future decidable-edge
floor annually, not per-trade**, unless the annual figure is given alongside
it. This does not reopen or alter Closure 2; it corrects how a floor from any
future feasibility gate must be read.

### What this closure does not do

It does not evaluate, authorize, rank or reject any strategy family, and it
does not bear on carry (below), whose income source is documented and
non-directional and which this identity does not bind. It does not touch V3's
retirement, Phase 7B's status (now moot — see CLAUDE.md), or the standing
decision that 7R3b is not run. It changes nothing operational. A future
directional trial on this asset class is not forbidden — it is required to
clear the floor gate first, with a genuinely different asset, venue or cost
structure that makes its own question decidable.

---

## Carry (long spot / short perpetual) — PRICED, NOT STARTED (2026-09-19)

**Not a closure, and not a trial.** Nothing here is retired, rejected, or ruled
out on edge or on feasibility. Source:
[`../research/2026-09-19-carry-scoping.md`](../research/2026-09-19-carry-scoping.md)
— read-only pricing of long-spot / short-perpetual funding carry on this
account's venue. No strategy was backtested, no parameter was searched, and no
trial was registered; the document's own conclusion section is titled
"numbers," not "verdict."

### Why this is the one class the four closures above do not cover

Closures 1-3 and the floor-gate identity bound **directional** programs, whose
per-trade variance is the underlying's own price variance. A hedged carry
position (long spot, short a contract-equivalent perp) cancels that price
exposure; its income is the funding series this project has so far measured
only as a long's **cost** (Closure 3's source document, and
`2026-09-18-perps-scoping.md` §6). Hedged means low variance, and low variance
means a given edge is decidable in a much shorter window than any directional
one — the floor-gate identity works **for** a program like this, not against
it. Carry is not closed on edge or on feasibility; it has simply not been
started, because the measured premium has not cleared this venue's cost.

### The numbers

At the operational spot schedule (`pipeline/fees.py` `CURRENT_SCHEDULE`, 0.6%
maker / 1.2% taker) plus CFM's perp rate (0.095%/0.100%): the spot leg's
1.80% round trip is **~9x** the perp leg's 0.195% and is what sets the
break-even. Break-even funding — the realised cycle rate, the conservative
reading being the higher median-cycle figure — is **14.78%/yr (BTC), 14.39%/yr
(ETH)** on the realised cycle rate, or **38-46%/yr** on a typical single cycle.

Full-window (6.67 years) net on committed capital: **+0.04%/yr BTC,
+3.1%/yr ETH** — and essentially all of ETH's full-window income was earned in
2020-2021, not since. **2025 and 2026-to-August are net negative on both
assets**, at both the adopted and the uncleared candidate spot schedule.

Trailing 7-day funding has cleared the median-cycle break-even only rarely
since 2022: **0% of readings in 2022 and 2025 (both assets), briefly in 2023
(ETH, 0.18% of readings) and 2024 (BTC 3.37%, ETH 5.56%), and 0% of readings
in 2026 to date on either asset.** 2026 median trailing funding is 3.0%/yr
(BTC) and 1.9%/yr (ETH) — a fifth of the ~15%/yr bar.

### Verdict — NOT STARTED, condition for re-evaluation recorded

**Carry is not closed on edge — its income source is documented and
non-directional, and the floor identity does not bind it. It is NOT STARTED
because the premium has been below this venue's break-even for five years. A
funding monitor with thresholds 14.8%/14.4% (BTC/ETH) is the recorded
condition for re-evaluation**, designed but not built in
`2026-09-19-carry-scoping.md` §5 (public product record, no credential
required, hourly poll against CFM's own `funding_rate` field).

Two structural risks are recorded alongside, not part of the verdict:
committed capital exceeds the $100 `LIVE_BALANCE_USD` cap for one hedged unit
of either product (a hedged BTC unit costs $1,118, ETH $367), and a
non-overlapping full-margin excursion on the short leg occurred 10 times (BTC)
and 21 times (ETH) over 6.7 years on a 7-day view — concentrated in the same
years that produced the income.

### What this entry does not do

It does not authorize a trial, a monitor build, or a transfer of funds into
the CFM wallet. It does not evaluate whether the Binance proxy history
transfers to CFM's own (undocumented) funding levels. It changes nothing
operational: `DRY_RUN=true`, **LIVE NO-GO**, `LIVE_BALANCE_USD`,
`ASSET_CONFIG` and every closure above are untouched.
