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
- er >= 0.20: +0.31% (68 signals) — **best stable threshold**
- er >= 0.25: unstable (n=5 in bear_2022 inflates to PF=5.32)
- er >= 0.35: recent_year drops to PF=0.99 (marginal)

Key insight: the er >= 0.25+ post-hoc result was misleading. Integrated filter changes signal generation via skip_until interactions. er >= 0.20 is the most robust integrated threshold.

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

| Asset | n | Avg P&L | PF | ER>=0.20 kept |
|-------|---|---------|----|----|
| BTC-USD (2020-09→) | 203 | -0.91% | 0.38 | PF 0.43 |
| ETH-USD (2020-09→) | 181 | -0.89% | 0.50 | PF 0.47 |
| SOL-USD (2022-02→) | 97 | -0.56% | 0.72 | PF 0.74 |

No asset, no ER bucket, no realized-vol tercile, no BB-width (compression)
tercile, and no vm_30 direction produces a robustly positive cell for the
breakout mechanism. Volatility-compression states do NOT predict profitable
breakout entries here (compressed-BBW cells: ZEC PF 0.50, BTC 0.27, ETH 0.45).

### Strategy-family probes (single pre-declared config each, no sweeps)

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

V3-accepted so far: n=2 (of ≥20 required), both losses. No criteria decision
yet; threshold remains locked; enforcement remains off.

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
3. Only durable positive lead: slow long-only trend following. If pursued,
   pre-register a single config + acceptance rule BEFORE any further scans.

### Professional review addendum (2026-08-09)

See `docs/research/2026-08-professional-review-addendum.md`.

- The central continuous-window falsification is independently reproduced and
  accepted: V2 remains negative and integrated V3 remains worse. Live stays
  NO-GO; `DRY_RUN=true`.
- The current `oos_replay.py` result is diagnostic rather than the formal OOS
  record because it post-filters a non-enforced path. The V3 journal also
  conflates shadow enforcement acceptance with candidate acceptance and does
  not currently close the registered accepted cohort end-to-end.
- The 92% underwater value is observation-weighted. A boundary-aware calendar
  audit gives approximately 97.15% through 2026-07-12; max DD remains about
  -71% for this ZEC sequence.
- **Recommendation (not an activation decision):** retire V3 as a deployable
  candidate, fix the research evidence pipeline, and pre-register slow trend
  following as a separate strategy family before any new scan.
- This research branch predates the latest main/safety hardening. Integrate the
  research-only changes onto the current safety history; never deploy from the
  research branch itself.
