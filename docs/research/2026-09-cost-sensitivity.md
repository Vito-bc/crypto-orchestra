# Prospective Cost Sensitivity — 2026-09-17

Trial `2026-09-cost-sensitivity.v1`. Registered in
[`../trial_registry.md`](../trial_registry.md).

> **This is a PROSPECTIVE COST SENSITIVITY study, not a trial of a mechanism
> and not a new edge test.** It answers one question: given a measured
> prospective operational fee schedule, what gross move must a trade clear to
> break even, and where does the frozen V2 ZEC mechanism sit against that
> threshold? Its numbers must never be quoted as an edge result, must never be
> treated as a restatement of the historical artifact
> (`docs/research/artifacts/results.json`, trial
> `2026-08-warmup-semantics.v1`), and say nothing about what any account paid
> during 2020-2026. **Nothing in this document authorizes anything.** No
> parameter was searched, swept, or changed; no frozen research constant was
> modified; no existing artifact was regenerated or overwritten.

**Two scenarios, per standing decision.** Coinbase changed the account's fee
tier on 2026-09-16, one day after the 4-reading cohort that
`pipeline/fees.py` `CURRENT_SCHEDULE` was adopted from. As of this writing
there is exactly one probe reading (2026-09-17) at the new tier, not the
same 4-consecutive-reading bar the adopted schedule was confirmed under. The
project decision recorded the same day (memory `fee-tier-change-2026-09-16`)
is explicit: *"Do not adopt the new schedule on one reading... Any
cost-sensitivity analysis must report both 1.8% and 1.4% scenarios."* This
document reports both:

- **ADOPTED** — `pipeline/fees.py` `CURRENT_SCHEDULE` (`coinbase-intro-1-2026-09`,
  maker 0.6% / taker 1.2%). This is what the operational code actually
  charges today; `active_schedule()` returns nothing else.
- **CANDIDATE** — the single 2026-09-17 probe reading
  (`logs/stf_cost_probe.jsonl`, gitignored, `observed_at`
  `2026-09-17T00:05:04.823090+00:00`, `fee_tier.pricing_tier="Intro"`, maker
  0.5% / taker 0.9%). **Not adopted anywhere in code.** It exists in this
  document only because the standing decision requires it to be reported;
  formal adoption (a new `FeeSchedule` entry) waits for the 4-reading cohort
  (2026-09-17 → 2026-09-20) per that decision.

Script: [`../../backtesting/cost_sensitivity.py`](../../backtesting/cost_sensitivity.py)
(deterministic, no network — reads the same local candle cache
`research_runner.py --verify-code` already validates against the committed
code and environment). The candidate schedule's rates are hardcoded as local
constants in the script, sourced from the probe log quoted above — the script
does not read them from `pipeline/fees.py`, which does not contain them. Run
it yourself to reproduce every number below:

```powershell
venv\Scripts\python.exe backtesting/cost_sensitivity.py
```

The script's first act is to re-run the exact scan behind the artifact's
`{asset: ZEC-USD, trial: V2-continuous}` row and assert that it reproduces
`n=114`, `PF=0.76141`, `expectancy=-0.006227` **before** doing any re-pricing.
It does, byte-for-byte:

```
Reproduced closed trades: n=114 (artifact: n=114)
Reproduced frozen-model PF=0.76141 expectancy=-0.006227 (artifact: PF=0.76141 expectancy=-0.006227)
```

## The fee models compared

| | Entry | Exit |
|---|---|---|
| **Historical research (frozen)** — `backtesting/signal_scanner.py` | 0.4% maker (`_ENTRY_FEE`) | **TAKE_PROFIT: 0.4% maker** (`_TP_FEE`)  •  **STOP_LOSS / MAX_HOLD: 0.6% taker** (`_SL_FEE`) |
| **Prospective, ADOPTED** — `pipeline/fees.py` `CURRENT_SCHEDULE` | 0.6% maker | **1.2% taker, on every exit path** |
| **Prospective, CANDIDATE (not adopted)** — single 2026-09-17 reading | 0.5% maker | **0.9% taker, on every exit path** |

The frozen research model is *not* a flat 0.4%/0.6% split — it charges
TAKE_PROFIT exits as if they were maker limit fills. The operational code
does not do this: `pipeline/position_tracker.close_position()` always calls
`place_market_sell()` regardless of `reason`, so `exit_rate =
active_schedule().rate_for(TAKER)` unconditionally, for both prospective
schedules. This document's "prospective" numbers follow `close_position()`,
not the frozen model's TAKE_PROFIT treatment — see "What this changes about
the motivating +0.8pp estimate" below.

## 1. Prospective round-trip cost per trade, by exit path

Every leg's notional base matches `close_position()` exactly:

- **Entry fee** = `qty_usd * entry_rate` — always the schedule's maker rate,
  of entry USD notional, on every exit path, because it is stamped on the
  order at placement, before the exit reason is known.
- **Exit fee** = `gross_proceeds * exit_rate` — always the **taker** rate,
  applied to `qty_coins * exit_price` (gross proceeds), on every exit path.
  The rate never varies by exit reason; the resulting cost *as a percentage
  of entry notional* does, because gross proceeds differ by how far price
  moved.

Measured over the frozen mechanism's actual n=114 ZEC trades (entry/exit
prices from the reproduced scan):

| Exit path | n | Mean round-trip cost — ADOPTED (0.6%/1.2%) | Mean round-trip cost — CANDIDATE (0.5%/0.9%) |
|---|---:|---:|---:|
| STOP_LOSS | 64 | 1.7587% | 1.3691% |
| MAX_HOLD | 14 | 1.8231% | 1.4174% |
| TAKE_PROFIT | 36 | 1.8762% | 1.4571% |

All three cluster near the schedule's entry+exit sum because the entry fee is
fixed and the exit-price ratio is close to 1.0 for most trades; TAKE_PROFIT
is slightly higher in both scenarios because gross proceeds are largest there
(exit price is furthest above entry), so the same taker rate bites on a
larger dollar amount.

## 2. Break-even gross move

Solving `net_pnl_pct = 0` for `(exit_price*(1-exit_rate) - entry_price*(1+entry_rate))/entry_price*100`:

| Schedule | entry_rate | exit_rate | Break-even gross move |
|---|---:|---:|---:|
| **ADOPTED (all exit paths — matches `close_position()`)** | 0.6% | 1.2% | **+1.8219%** |
| **CANDIDATE, not adopted (all exit paths)** | 0.5% | 0.9% | **+1.4127%** |
| Legacy, taker-priced exit (STOP_LOSS / MAX_HOLD in the frozen model) | 0.4% | 0.6% | +1.0060% |
| Legacy, maker-priced exit (TAKE_PROFIT in the frozen model) | 0.4% | 0.4% | +0.8032% |

The task brief that motivated this study described the historical assumption
as a flat "1.0% round trip (0.4% entry + 0.6% exit)." That figure is a close
approximation of the **legacy taker-exit break-even** (1.0060%, not exactly
1.0% — the fee legs compound rather than add, because the exit fee is charged
on gross proceeds, which are `entry_price * (1 + gross_move)`, not on entry
notional). It does **not** describe TAKE_PROFIT exits under the frozen model,
which break even at a materially lower 0.8032% because that model prices
them as maker on both legs. See the note below on what this means for the
motivating cost-gap estimate.

**Any trade in this repository — under any exit path — must clear +1.82%
gross at the ADOPTED tier, or +1.41% at the CANDIDATE (unconfirmed) tier,
before it earns anything.**

## 3. The frozen V2 ZEC trade sequence, re-priced (PROSPECTIVE SENSITIVITY)

> ⚠️ The numbers in this section are **not** a restatement of
> `docs/research/artifacts/results.json`. They take the *same* n=114 entry/exit
> prices the frozen mechanism produced over 2021-06-26 → 2026-07-12 and apply
> a *different, current (or candidate)* fee assumption to them. They do not
> supersede the artifact, and they say nothing about what any account paid in
> 2020-2026.

| | n | PF | Expectancy |
|---|---:|---:|---:|
| Frozen artifact (historical fee model) | 114 | 0.76141 | -0.6227%/trade |
| **PROSPECTIVE SENSITIVITY — ADOPTED (0.6%/1.2%)** | 114 | **0.51720** | **-1.4918%/trade** |
| **PROSPECTIVE SENSITIVITY — CANDIDATE, not adopted (0.5%/0.9%)** | 114 | **0.61718** | **-1.0909%/trade** |

By exit path, mean per-trade P&L:

| Exit path | n | ADOPTED | CANDIDATE (not adopted) |
|---|---:|---:|---:|
| STOP_LOSS | 64 | -5.1963% | -4.8066% |
| MAX_HOLD | 14 | +0.1055% | +0.5113% |
| TAKE_PROFIT | 36 | +4.4727% | +4.8918% |

Re-pricing at either prospective tier does not change which trades won or
lost — it changes the magnitude. At the ADOPTED tier, PF drops from 0.761 to
0.517 and per-trade expectancy roughly **doubles in the negative direction**,
from -0.62% to -1.49%. At the CANDIDATE tier the drop is smaller but still
substantial: PF 0.617, expectancy -1.09%/trade. **Both are clearly negative.**

## 4. What fraction of the strategy's own stop/target the cost consumes

ATR-as-%-of-entry-price at the actual n=114 entries (the 1h ATR(14) value
`_simulate_trade` reads for sizing — see the note on "4h ATR" below). This
distribution is a property of the trade sequence, not of any fee schedule, so
it is the same input for both scenarios:

| | Value |
|---|---:|
| Mean | 1.8583% |
| Median | 1.5958% |
| Q1 | 1.2332% |
| Q3 | 2.1082% |

The frozen ZEC config sets `atr_stop=2.0`, `atr_target=3.5`. At the median
ATR%, stop distance = 3.1915% of entry price, target distance = 5.5852%:

| | Break-even as a fraction of stop | Break-even as a fraction of target |
|---|---:|---:|
| ADOPTED (break-even +1.8219%) | **57.08%** | **32.62%** |
| CANDIDATE, not adopted (break-even +1.4127%) | **44.26%** | **25.29%** |

At the median trade's own volatility, the round-trip cost alone consumes
**more than half the stop distance** at the adopted tier, or **just under
half** at the unconfirmed candidate tier — and roughly a quarter to a third
of the target distance either way — before any consideration of whether
price actually moves favorably. The frozen mechanism's designed reward:risk
of 1.75 (3.5/2.0) does not translate to a 1.75 payoff-to-cost ratio under
either fee assumption examined here.

**Note on "4h ATR":** there is no separate 4h-timeframe ATR column anywhere
in this codebase. `attach_higher_timeframe_context()` in `backtesting/backtest.py`
merges only `close`/`ema50`/`ema200`/`trend` from the 4h frame (as
`close_4h`, `ema50_4h`, etc.) onto the 1h grid; the `atr` column
`_simulate_trade` actually reads for stop/target sizing is computed on the 1h
frame (`AverageTrueRange(..., window=14)` in `_download_and_compute`, called
with `interval="1h"`). The figures above use that 1h ATR — the value the
mechanism actually sizes against — rather than inventing a 4h ATR series that
does not exist in the code.

## 5. Maker-exit sensitivity (TAKE_PROFIT only) — cost side only

If TAKE_PROFIT exits were placed as maker limit orders instead of market
sells, their exit leg would price at the schedule's maker rate instead of its
taker rate. STOP_LOSS and MAX_HOLD exits cannot be converted this way — a
stop or a timed exit needs guaranteed execution, which is what a market order
buys; pricing it as a resting maker order would trade fill-certainty for a
fee saving on exactly the exits where certainty matters most.

This section quantifies the cost side only, on the n=36 TAKE_PROFIT trades in
the sample, under both schedules:

| | ADOPTED (0.6% maker / 1.2% taker) | CANDIDATE, not adopted (0.5% maker / 0.9% taker) |
|---|---:|---:|
| Mean cost reduction per TAKE_PROFIT trade | 0.6381 pp | 0.4254 pp |
| Mean P&L, actual (taker exit) | +4.4727% | +4.8918% |
| Mean P&L, hypothetical (maker exit) | +5.1108% | +5.3172% |

Applied to the whole n=114 sample (only the 36 TAKE_PROFIT trades repriced;
STOP_LOSS and MAX_HOLD unchanged, still taker):

| | PF | Expectancy |
|---|---:|---:|
| ADOPTED, all taker | 0.51720 | -1.4918%/trade |
| **ADOPTED, hypothetical TAKE_PROFIT at maker** | 0.58241 | **-1.2903%/trade** |
| CANDIDATE (not adopted), all taker | 0.61718 | -1.0909%/trade |
| **CANDIDATE (not adopted), hypothetical TAKE_PROFIT at maker** | 0.66432 | **-0.9566%/trade** |

This is the **cost effect only**, under both schedules. It does **not**
estimate what fraction of TAKE_PROFIT signals would actually fill as a
resting maker order instead of running through to stop or max-hold — that is
a distinct, unmeasured question that would need its own pre-registered
study. Quoting a fill rate here would manufacture exactly the kind of number
this document is structured to avoid: an unmeasured input dressed as a
result.

## 6. Conclusion

At the ADOPTED prospective operational cost (1.8% round trip, uniformly
taker-priced on exit regardless of reason), a trade must clear +1.82% gross
before it earns anything — roughly 57% of the frozen ZEC mechanism's own stop
distance and a third of its target distance at the median historical ATR —
and the same n=114 trade sequence that was already unprofitable at the
modeled historical rate (PF 0.761, -0.62%/trade) is roughly twice as
unprofitable at this measured rate (PF 0.517, -1.49%/trade). At the
CANDIDATE, not-yet-adopted tier (1.4% round trip) the picture is directionally
identical and only quantitatively softer: break-even +1.41%, PF 0.617,
-1.09%/trade — still clearly negative. Even the best available fee-side lever
examined here — converting TAKE_PROFIT exits to maker orders, with zero
assumed non-fill cost — only claws back part of the gap under either schedule
(to -1.29%/trade adopted, -0.96%/trade candidate), leaving the family
negative under every fee assumption examined here, confirmed or not.
**This strategy family is not viable at either the measured (adopted) or the
candidate (unconfirmed) operational cost structure**, and cost alone is
sufficient to reject it independent of any further mechanism research; this
finding does not by itself change `DRY_RUN`, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, V3 status, or Phase 7B status, none of which this study
touches or authorizes. It also does not resolve, and is not evidence toward,
whether the candidate tier reading should be formally adopted — that decision
rests on the 4-reading cohort per the standing decision, not on this study's
conclusion being convenient either way.

## What this changes about the motivating +0.8pp estimate

The task that produced this document estimated the cost gap as a flat "+0.8pp
of drag" (measured 1.8% round trip minus modeled 1.0%), before the candidate
tier reading existed. That is a reasonable single-number approximation of the
**blended** effect at the ADOPTED tier (-0.6227% legacy expectancy vs
-1.4918% measured expectancy is a **+0.8691pp** swing on this sample), but it
is not uniform across exit paths, and it does not yet account for the
candidate tier:

- STOP_LOSS / MAX_HOLD-type trades, ADOPTED tier: legacy break-even 1.006% →
  measured break-even 1.822%, a **+0.82pp** shift — close to the motivating
  estimate.
- TAKE_PROFIT-type trades, ADOPTED tier: legacy break-even 0.803% → measured
  break-even 1.822%, a **+1.02pp** shift — noticeably larger than +0.8pp,
  because the frozen research model under-costs TAKE_PROFIT exits relative to
  what the operational code actually charges (it assumes a maker fill that
  `close_position()` never attempts).
- If the CANDIDATE tier is eventually confirmed and adopted, the same
  blended swing shrinks to roughly **+0.47pp** (-0.6227% legacy vs -1.0909%
  candidate) — smaller than the motivating +0.8pp estimate, though still
  strongly negative in absolute terms.

Net effect: the +0.8pp framing understates the true cost increase on
TAKE_PROFIT exits and slightly overstates it on STOP_LOSS/MAX_HOLD exits at
the adopted tier; it would overstate the aggregate effect further if the
candidate tier is later confirmed. The direction (higher measured cost, worse
expectancy) holds under every schedule examined here regardless.
