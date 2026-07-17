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

**Candidate for V3 live**: `v3_candidate_threshold = 0.20` (pre-registered, LOCKED)
**Current config**: `v3_enforcement_enabled = False` — shadow/research mode only; `v3_would_block` logged to `logs/v3_journal.jsonl` but trades are NOT blocked until OOS criteria met.

**V3 activation criteria (pre-registered 2026-07-13, threshold LOCKED — cannot be changed after first OOS signal):**
1. n >= 20 closed V3-accepted trades
2. PF > 1.20 (gross wins / gross losses, from v3_journal.jsonl)
3. P_bootstrap(PF > 1) > 90% (block bootstrap b=4, N=10,000)
4. Avg P&L > 0% after adding +0.25% per-trade friction (stress test)
5. No single market episode accounts for > 50% of gross profit

Check every 5 closed trades. At first check (n=5), PF can be infinite with 5 wins — criteria 1 is the binding gate. Threshold 0.20 is fixed; do NOT re-select on OOS data.

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

Pending for V3 activation (check every 5 closed accepted trades):
- [ ] Accumulate ≥20 forward OOS accepted signals (2026-07-12+)
- [ ] All 5 activation criteria pass (see above)
- [ ] Only then: set `v3_enforcement_enabled = True` in ASSET_CONFIG (threshold stays 0.20)
