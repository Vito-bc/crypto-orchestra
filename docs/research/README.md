# Research Index

Everything quantitative about this project lives here. The root `README.md`
carries only the headline verdict; this page carries the detail, and
[`../trial_registry.md`](../trial_registry.md) is the authoritative record.

**Read the registry before believing any number anywhere in this repository**,
including the numbers on this page.

## Verdict

No positive expectancy has been demonstrated. The frozen V2 momentum mechanism
is unprofitable on the continuous evaluation window.

| Result | Value |
|---|---|
| ZEC continuous (2021-06-26 → 2026-07-12), no filter | **PF 0.761**, −0.62%/trade, n=114 |
| ZEC continuous, integrated V3 ER-30 filter | **PF 0.706**, −0.83%/trade, n=71 |
| Frozen mechanism transferred to BTC | PF 0.359, n=174 |
| Frozen mechanism transferred to ETH | PF 0.476, n=150 |
| Frozen mechanism transferred to SOL | PF 0.718, n=97 |

Every figure above is asserted against
[`artifacts/results.json`](artifacts/results.json) by
`tests/test_research_provenance.py`, so it cannot drift from the artifact
without turning the suite red.

## What was retired, and why

### V3 ER-30 filter — RETIRED (2026-08-09)

Not "pending", not "awaiting more data". Integrated enforcement on the
continuous window makes results **worse** (PF 0.706 with, PF 0.761 without), and
there is no trade count that activates it. The earlier positive case came from
period-selected windows.

The former "n ≥ 20 closed trades" activation criteria are withdrawn. Further
`v3_would_block` logging is diagnostic only; reactivating the filter would
require a new pre-registered trial ID.

### Warm-up semantics — corrected (2026-08-13)

The scanner used to fail **open** when an indicator was still warming up: a
declared gate that could not yet be computed was silently skipped. 19 ZEC trades
worth +22.28% had run without the 200-day daily EMA veto.

Correcting it moved the continuous window from PF 0.855 / −0.37% to
PF 0.761 / −0.62%, and collapsed `bull_2021` from n=25 / PF 1.42 to
**n=6 / PF 0.96** — the entire apparent 2021 bull-window edge came from the
ungated span.

Superseded artifacts are kept under [`artifacts/superseded/`](artifacts/superseded/).
Their numbers are **not** comparable to current ones.

### Walk-forward validation — withdrawn, then repaired

This section once reported out-of-sample results across three market regimes and
concluded that "ZEC is the only asset with genuine out-of-sample edge"
(+0.30% avg OOS). **Those numbers are void.**

`backtesting/walk_forward.py` never attached the daily frame, so `close_1d` and
`ema*_1d` were absent from every row and the declared daily-EMA trend gate was
skipped for every signal on every asset. Its scan loop enumerated only three
blocked reasons, so `daily_trend` and `btc_regime` blocks fell through and were
traded; it charged the wrong fees; and it reported an unfinished holding period
as a completed trade.

It was repaired in trial `2026-08-walkforward-repair.v1` and now shares the
scanner's own frame assembly and trade simulator, so it cannot drift from the
mechanism it validates. Enforcing the gates it used to skip removed **475
`daily_trend`** and **118 `btc_regime`** signals it previously traded.

Its windows remain **historical diagnostics, not out-of-sample evidence**: they
were inspected repeatedly during development and are counted in the registry's
multiple-testing budget. The repair establishes that the tool now measures the
mechanism it declares — nothing more. It cannot support activation.

Output: [`artifacts/walk_forward/`](artifacts/walk_forward/), verifiable with
`walk_forward.py --verify`.

## Per-asset ATR parameters

Tuned in-sample from the full-year signal scanner across 371 trades. These are
**selected** parameters, not validated ones — the registry counts ~25–30 trials
before the freeze, so any single positive cell must be read against that
multiple-testing budget.

| Asset | Stop | Target | R:R | Min conds | Daily EMA | Enabled | Rationale |
|-------|------|--------|-----|-----------|-----------|---------|-----------|
| BTC-USD | 2.0x ATR | 3.5x ATR | 1.75 | 4 | 50d | No | Tighter stop — cleaner structure |
| ETH-USD | 2.5x ATR | 4.5x ATR | 1.80 | 4 | 50d | No | Wider stop — absorbs intraday wicks |
| SOL-USD | 2.5x ATR | 4.5x ATR | 1.80 | 4 | 200d | No | Wider stop — high volatility |
| ZEC-USD | 2.0x ATR | 3.5x ATR | 1.75 | 4 | 200d | Yes (shadow) | Frozen at `v2-adx25-frozen` |

ZEC parameters are frozen at git tag `v2-adx25-frozen`. Any change creates a new
trial and must be logged in [`../trial_registry.md`](../trial_registry.md)
**first**.

## Fee model

Frozen conservative assumptions applied by `backtesting/signal_scanner.py`:

| Leg | Rate | Constant |
|-----|------|----------|
| Entry (maker, limit at support) | 0.4% | `_ENTRY_FEE` |
| Take-profit (maker, limit at target) | 0.4% | `_TP_FEE` |
| Stop-loss / max-hold (taker) | 0.6% | `_SL_FEE` |

An earlier 0.2% / 0.4% model understated fees and inflated backtest P&L. Results
computed under it are superseded. Actual Coinbase Advanced fees vary by account
tier; these constants are research assumptions, not a live fee quote.

## Reproducibility

Artifacts are byte-identical regenerations, not saved outputs.

```bash
# Rebuild the candle cache from PUBLIC Coinbase data (no credentials)
python backtesting/hydrate_research_data.py

# Regenerate and verify
python backtesting/research_runner.py
python backtesting/research_runner.py --verify

# Cheap check: code identity + environment only, no candles, no git history
python backtesting/research_runner.py --verify-code
```

What is pinned:

- **Interpreter:** Python 3.13.5 exactly. `write_artifacts` and both verify
  paths refuse any other version, because the manifest records the interpreter
  the numbers were actually computed on.
- **Libraries:** `numpy`, `pandas`, `ta`, `pyarrow` are result-determining and
  recorded in [`artifacts/manifest.json`](artifacts/manifest.json); changing one
  invalidates verification loudly instead of quietly re-deriving the numbers.
- **Code identity:** content-addressed by SHA-256 over the result-determining
  files. `code_commit` is an informational label only.
- **Input identity:** the window-scoped logical OHLCV hash (`ohlcv-logical-v1`,
  scope 2020-01-01 → 2026-07-12, both inclusive), so the recent tail the
  exchange keeps revising cannot break verification.

Both `--verify-code` and the full `--verify` run in CI, the latter fed by
credential-free public hydration.

> The verify log prints `Downloading …` while reading the local parquet cache —
> the wording is inherited from the shared loader. No network call is made; CI
> proves it by running the same command in a job whose only network step is the
> explicit public hydration.

## Documents

| Document | What it is |
|---|---|
| [`../trial_registry.md`](../trial_registry.md) | Authoritative trial record — read first |
| [`2026-08-strategy-review.md`](2026-08-strategy-review.md) | Full strategy review |
| [`2026-08-professional-review-addendum.md`](2026-08-professional-review-addendum.md) | Addendum |
| [`artifacts/results.json`](artifacts/results.json) | Machine-checked headline numbers |
| [`artifacts/manifest.json`](artifacts/manifest.json) | Code, environment and input identity |
| [`artifacts/superseded/`](artifacts/superseded/) | Retracted results, kept for audit |

## Open leads

A slow trend-following trial (55d/20d daily Donchian) would have to be
pre-registered from scratch. The earlier "positive on all four assets" result is
**legacy / unverified** — its implementation is not in this repository, so it
cannot be regenerated, and it is recorded under `non_reproducible` in
[`artifacts/results.json`](artifacts/results.json). It is not evidence that the
family is promising; it is an unverified note.
