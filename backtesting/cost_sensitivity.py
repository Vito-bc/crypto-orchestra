"""
PROSPECTIVE COST SENSITIVITY — trial `2026-09-cost-sensitivity.v1`.

NOT a strategy trial, NOT an edge test, NOT a parameter search. See
docs/research/2026-09-cost-sensitivity.md for the write-up and
docs/trial_registry.md for the registration entry. Every number this script
prints appears, and is explained, in that document.

Question this answers: docs/research/artifacts/results.json (trial
`2026-08-warmup-semantics.v1`, row asset=ZEC-USD trial=V2-continuous, n=114)
was priced at the FROZEN historical research fee assumption
(backtesting/signal_scanner.py: entry 0.4% maker, take-profit 0.4% maker,
stop/max-hold 0.6% taker). What would the SAME trade sequence look like priced
at a MEASURED prospective operational schedule instead (entry maker, exit
taker on EVERY exit path, because pipeline/position_tracker.close_position()
always sends a market sell)?

Two scenarios are reported, per project decision 2026-09-17 recorded in
memory `fee-tier-change-2026-09-16` ("Any cost-sensitivity analysis must
report both 1.8% and 1.4% scenarios"):

  ADOPTED   pipeline/fees.py CURRENT_SCHEDULE (coinbase-intro-1-2026-09):
            maker 0.6% / taker 1.2%. This is what the code actually charges
            today — the only schedule `active_schedule()` returns.
  CANDIDATE a single probe reading, NOT yet adopted in pipeline/fees.py:
            logs/stf_cost_probe.jsonl, observed_at 2026-09-17T00:05:04Z,
            fee_tier.pricing_tier "Intro", maker 0.5% / taker 0.9%. Coinbase
            changed the account's tier on 2026-09-16, one reading does not
            meet the same 4-consecutive-reading bar the adopted schedule was
            confirmed under (2026-09-11..09-15) — see the memory for the
            decision not to adopt on one reading. This script hardcodes that
            single reading's rates as local constants for sensitivity
            purposes ONLY; it does not read pipeline/fees.py for them and
            does not change pipeline/fees.py.

What this script does NOT do:
  - it does not modify backtesting/signal_scanner.py's _ENTRY_FEE / _TP_FEE /
    _SL_FEE or any other frozen research constant
  - it does not write to docs/research/artifacts/results.json or any other
    committed artifact
  - it does not search, sweep, or select any parameter — atr_stop, atr_target,
    min_conditions, daily_ema_period etc. are read from the frozen
    ASSET_CONFIG["ZEC-USD"] exactly as the artifact used them
  - it captures the ATR value `_simulate_trade` already computes internally
    (via a call-observing wrapper) rather than re-deriving trade outcomes with
    different logic; the wrapper changes nothing about which trades occur or
    how they are priced by the frozen mechanism

Reproducibility: reads the same local, already-hydrated candle cache
(data/candles/*.parquet) that `research_runner.py --verify-code` already
confirmed matches the committed code and environment. No network access.
Running this script twice against the same cache produces the same output.

Run:
    venv\\Scripts\\python.exe backtesting/cost_sensitivity.py
"""

from __future__ import annotations

import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The artifact figures this script's reproduction must match before any
# re-pricing is trusted. Copied from docs/research/artifacts/results.json,
# trial "2026-08-warmup-semantics.v1", row {asset: ZEC-USD, trial:
# V2-continuous} — not re-derived, so a drift here means the reproduction
# disagrees with the committed artifact, not that the artifact is wrong.
_ARTIFACT_N_CLOSED = 114
_ARTIFACT_PF = 0.76141
_ARTIFACT_EXPECTANCY_PCT = -0.006227

# CANDIDATE / UNCONFIRMED schedule. Sourced directly from
# logs/stf_cost_probe.jsonl (gitignored; not a committed artifact), the
# single reading at observed_at="2026-09-17T00:05:04.823090+00:00":
#   {"fee_tier": {"pricing_tier": "Intro", "maker_fee_rate": 0.005,
#                 "taker_fee_rate": 0.009, "measured": true, ...}}
# Every reading before it (2026-09-11..09-16) still shows "Intro 1" at
# 0.006/0.012 (pipeline.fees.CURRENT_SCHEDULE). One reading is not the
# 4-consecutive-reading bar the adopted schedule was confirmed under, so this
# is NOT written into pipeline/fees.py and CURRENT_SCHEDULE is not read for
# it — see memory `fee-tier-change-2026-09-16` for the decision. These
# constants exist ONLY so this sensitivity study can report the scenario the
# project decision requires; do not import them as if they were adopted.
_CANDIDATE_MAKER_RATE = 0.005
_CANDIDATE_TAKER_RATE = 0.009


def _reproduce_v2_continuous_zec() -> tuple[list[dict], list[float]]:
    """
    Re-run the exact scan behind the V2-continuous / ZEC-USD row, capturing
    the ATR value at every entry alongside each closed trade.

    Returns (closed_trades, atr_at_entry) — parallel lists, same order as the
    scan emitted them.
    """
    import backtesting.signal_scanner as scanner
    from backtesting.research_runner import RESEARCH_CONFIG, effective_start

    asset = "ZEC-USD"
    cw = RESEARCH_CONFIG["continuous_window"]
    start = effective_start(asset, cw["start"])
    end = cw["end"]
    warmup = (pd.Timestamp(start) - pd.Timedelta(days=120)).date().isoformat()
    period = {
        "label": f"{asset} cost-sensitivity", "btc_move": "",
        "warmup": warmup, "start": start, "end": end,
    }

    atr_at_entry: list[float] = []
    _orig_simulate_trade = scanner._simulate_trade

    def _observing_simulate_trade(df, entry_i, entry_price, max_hold_hours,
                                   atr_stop, atr_target):
        # Read-only: records the ATR the real function is about to use for
        # sizing, then calls it unchanged. Does not alter which trades occur,
        # their outcomes, or their frozen-model pricing.
        atr_at_entry.append(float(df.iloc[entry_i]["atr"]))
        return _orig_simulate_trade(df, entry_i, entry_price, max_hold_hours,
                                     atr_stop, atr_target)

    prev_strict = scanner.STRICT_COINBASE_ONLY
    scanner.STRICT_COINBASE_ONLY = True
    scanner._simulate_trade = _observing_simulate_trade
    try:
        res = scanner.scan_asset(asset, period, v3_enforcement=False)
    finally:
        scanner.STRICT_COINBASE_ONLY = prev_strict
        scanner._simulate_trade = _orig_simulate_trade

    signals = res.get("signals", [])
    # Every signal that fires calls _simulate_trade exactly once, in order, so
    # atr_at_entry[k] corresponds to signals[k].
    assert len(atr_at_entry) == len(signals), (
        f"ATR capture ({len(atr_at_entry)}) and signal count ({len(signals)}) "
        "disagree — the observing wrapper is not aligned with the scan loop")

    closed_idx = [k for k, s in enumerate(signals)
                  if s["trade"].get("resolved", True)]
    closed = [signals[k] for k in closed_idx]
    closed_atr = [atr_at_entry[k] for k in closed_idx]
    return closed, closed_atr


def _measured_pnl_pct(entry_price: float, exit_price: float,
                       entry_rate: float, exit_rate: float) -> float:
    """
    Same closed form as pipeline.position_tracker.close_position():
      qty_coins      = qty_usd / entry_price
      gross_proceeds = qty_coins * exit_price
      entry_fee      = qty_usd * entry_rate
      exit_fee       = gross_proceeds * exit_rate
      net_pnl        = gross_proceeds - qty_usd - entry_fee - exit_fee
      pnl_pct        = net_pnl / qty_usd * 100
    qty_usd cancels algebraically, so it is never needed here — see the
    write-up (docs/research/2026-09-cost-sensitivity.md, item 1) for the
    reduction. exit_rate is ALWAYS the taker rate in the live/operational
    code (every exit path is a market sell), never a function of exit
    reason — unlike the frozen research model.
    """
    return (exit_price * (1 - exit_rate) - entry_price * (1 + entry_rate)) / entry_price * 100


def _pf_and_expectancy(pnl_pcts: list[float]) -> tuple[float, float]:
    """
    Same definitions as backtesting.equity_report.summary(): PF is the simple
    (uncompounded) sum of winning per-trade returns divided by the absolute
    sum of losing ones; expectancy is the mean per-trade return. A trade with
    pnl_pct == 0 counts as a loss (r <= 0), matching equity_report.
    """
    r = [p / 100 for p in pnl_pcts]
    wins = [x for x in r if x > 0]
    losses = [x for x in r if x <= 0]
    loss_sum = sum(losses)
    if loss_sum != 0:
        pf = sum(wins) / abs(loss_sum)
    else:
        pf = float("inf") if sum(wins) > 0 else 0.0
    expectancy = sum(r) / len(r)
    return pf, expectancy


# ADDENDUM 3: closing the normal-approximation question the one-sided bound
# in Item 6 otherwise leaves implicit. Fixed, dated seed — chosen before any
# resample was run, not searched for a favorable result — so the bootstrap is
# byte-reproducible like everything else in this script.
_BOOTSTRAP_SEED = 20260917
_BOOTSTRAP_N = 100_000


def _skew_kurtosis(pnl_pcts: list[float]) -> tuple[float, float]:
    """
    Adjusted Fisher-Pearson sample skewness (G1) and excess kurtosis (G2) —
    the convention behind Excel's SKEW/KURT and most statistics packages'
    bias-corrected "sample" estimators. Implemented directly rather than via
    scipy.stats: this project has no other scipy dependency, and Phase 6.9
    (research_runner.py) pins numpy/pandas/ta/pyarrow exactly for research
    provenance — adding a new pinned dependency for one script is out of
    proportion to what it is used for here.
    """
    arr = np.asarray(pnl_pcts, dtype=float)
    n = len(arr)
    mean = arr.mean()
    s = arr.std(ddof=1)   # sample SD, matches statistics.stdev used elsewhere
    z = (arr - mean) / s
    skew = (n / ((n - 1) * (n - 2))) * float(np.sum(z ** 3))
    exkurt = (
        (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * float(np.sum(z ** 4))
        - (3 * (n - 1) ** 2) / ((n - 2) * (n - 3))
    )
    return skew, exkurt


def _bootstrap_ucb(pnl_pcts: list[float]) -> float:
    """
    Percentile bootstrap one-sided 95% upper bound on the mean: resample the
    n=114 trades WITH REPLACEMENT _BOOTSTRAP_N times, take the mean of each
    resample, and report the 95th percentile of that distribution of means.
    Makes no normality assumption about the per-trade return distribution —
    the whole point of running it alongside the normal-theory bound in Item 6.
    """
    arr = np.asarray(pnl_pcts, dtype=float)
    n = len(arr)
    rng = np.random.default_rng(_BOOTSTRAP_SEED)
    resample_idx = rng.integers(0, n, size=(_BOOTSTRAP_N, n))
    resample_means = arr[resample_idx].mean(axis=1)
    return float(np.percentile(resample_means, 95))


def _report_scenario(label: str, status: str, entry_rate: float, exit_rate: float,
                      closed: list[dict], entries: list[float], exits: list[float],
                      reasons: list[str], breakeven_ref_stop: float,
                      breakeven_ref_target: float) -> list[float]:
    """
    Print items 1/2/3/5 for one fee schedule scenario. `breakeven_ref_stop`
    and `breakeven_ref_target` are this scenario's own median-ATR stop/target
    distances (% of entry price) — shared across scenarios since ATR is a
    property of the trade sequence, not of the fee schedule.

    Returns `pnls` (this scenario's 114 per-trade net returns, % of position)
    so the caller can compute Item 6's sample bounds without re-deriving them.
    """
    n = len(closed)
    print(f"\n{'=' * 70}\nSCENARIO: {label} [{status}]\n"
          f"  entry(maker)={entry_rate:.3%}  exit(taker)={exit_rate:.3%}\n"
          f"{'=' * 70}")

    # Item 1 — cost per trade by exit path
    print("Item 1 - round-trip cost per trade, by exit path (entry fee "
          "always on entry USD notional; exit fee always on gross proceeds, "
          "at the TAKER rate, matching close_position() exactly - no exit "
          "path gets a maker rate in the live/operational code):")
    print(f"  entry fee: {entry_rate:.3%} of entry notional (ALL paths)")
    by_reason_cost: dict[str, list[float]] = {}
    for e, x, r in zip(entries, exits, reasons):
        exit_fee_pct_of_entry = exit_rate * (x / e) * 100
        total_cost_pct = entry_rate * 100 + exit_fee_pct_of_entry
        by_reason_cost.setdefault(r, []).append(total_cost_pct)
    for reason, costs in sorted(by_reason_cost.items()):
        print(f"  {reason:12s} n={len(costs):3d}  "
              f"mean round-trip cost = {statistics.mean(costs):.4f}% of entry notional")

    # Item 2 — break-even gross move
    breakeven = ((1 + entry_rate) / (1 - exit_rate) - 1) * 100
    print(f"Break-even gross move (all exit paths): {breakeven:.4f}%")
    print(f"  break-even / stop distance   (median ATR) = "
          f"{breakeven / breakeven_ref_stop:.4f}")
    print(f"  break-even / target distance (median ATR) = "
          f"{breakeven / breakeven_ref_target:.4f}")

    # Item 3 — re-price the n=114 trade sequence
    pnls = [_measured_pnl_pct(e, x, entry_rate, exit_rate)
            for e, x in zip(entries, exits)]
    pf, expectancy = _pf_and_expectancy(pnls)
    print(f"PROSPECTIVE SENSITIVITY (not a restatement of the artifact): "
          f"n={n} PF={pf:.5f} expectancy={expectancy:.6f} "
          f"({expectancy*100:.4f}%/trade)")
    by_reason: dict[str, list[float]] = {}
    for r, p in zip(reasons, pnls):
        by_reason.setdefault(r, []).append(p)
    for reason, rpnls in sorted(by_reason.items()):
        print(f"  {reason:12s} n={len(rpnls):3d}  "
              f"mean_pnl_pct={statistics.mean(rpnls):.4f}")

    # Item 5 — maker-exit cost quantification (TAKE_PROFIT only, cost side only)
    tp_idx = [k for k, r in enumerate(reasons) if r == "TAKE_PROFIT"]
    tp_pnls_taker = [pnls[k] for k in tp_idx]
    tp_pnls_hyp_maker = [
        _measured_pnl_pct(entries[k], exits[k], entry_rate, entry_rate)  # exit at MAKER rate
        for k in tp_idx
    ]
    cost_delta_pp = [h - t for h, t in zip(tp_pnls_hyp_maker, tp_pnls_taker)]
    n_tp = len(tp_idx)
    print(f"Maker-exit sensitivity, TAKE_PROFIT trades only (n={n_tp}), "
          f"cost side ONLY (no fill-rate assumption):")
    if n_tp:
        print(f"  mean cost reduction per TAKE_PROFIT trade if exit were maker: "
              f"{statistics.mean(cost_delta_pp):.4f} pp")
        print(f"  mean pnl_pct (actual, taker exit): "
              f"{statistics.mean(tp_pnls_taker):.4f}%")
        print(f"  mean pnl_pct (hypothetical, maker exit): "
              f"{statistics.mean(tp_pnls_hyp_maker):.4f}%")
    hyp_by_index = dict(zip(tp_idx, tp_pnls_hyp_maker))
    whole_sample_hyp = [hyp_by_index.get(k, pnls[k]) for k in range(n)]
    hyp_pf, hyp_expectancy = _pf_and_expectancy(whole_sample_hyp)
    print(f"  whole-sample (n={n}) PF if TAKE_PROFIT exits were maker: "
          f"{hyp_pf:.5f}  expectancy={hyp_expectancy:.6f} "
          f"({hyp_expectancy*100:.4f}%/trade)")
    return pnls


def main() -> None:
    from pipeline.fees import CURRENT_SCHEDULE, LEGACY_SCHEDULE, MAKER, TAKER

    adopted_entry_rate = CURRENT_SCHEDULE.rate_for(MAKER)
    adopted_exit_rate = CURRENT_SCHEDULE.rate_for(TAKER)
    legacy_entry_rate = LEGACY_SCHEDULE.rate_for(MAKER)   # == _ENTRY_FEE == _TP_FEE
    legacy_exit_rate = LEGACY_SCHEDULE.rate_for(TAKER)    # == _SL_FEE

    print(f"Adopted schedule   (pipeline.fees.CURRENT_SCHEDULE): "
          f"entry(maker)={adopted_entry_rate:.3%}  exit(taker)={adopted_exit_rate:.3%}")
    print(f"Candidate schedule (single reading, NOT adopted)   : "
          f"entry(maker)={_CANDIDATE_MAKER_RATE:.3%}  exit(taker)={_CANDIDATE_TAKER_RATE:.3%}")
    print(f"Legacy schedule    (frozen research assumption)    : "
          f"entry(maker)={legacy_entry_rate:.3%}  exit(taker)={legacy_exit_rate:.3%}")

    closed, atr_at_entry = _reproduce_v2_continuous_zec()
    n = len(closed)
    print(f"\nReproduced closed trades: n={n} "
          f"(artifact: n={_ARTIFACT_N_CLOSED})")
    assert n == _ARTIFACT_N_CLOSED, (
        f"Reproduction produced n={n}, artifact says n={_ARTIFACT_N_CLOSED}. "
        "Do not trust the re-pricing below until this is reconciled.")

    frozen_pnls = [s["trade"]["pnl_pct"] for s in closed]
    frozen_pf, frozen_expectancy = _pf_and_expectancy(frozen_pnls)
    print(f"Reproduced frozen-model PF={frozen_pf:.5f} "
          f"expectancy={frozen_expectancy:.6f} "
          f"(artifact: PF={_ARTIFACT_PF} expectancy={_ARTIFACT_EXPECTANCY_PCT})")
    assert abs(frozen_pf - _ARTIFACT_PF) < 1e-3, "PF reproduction mismatch"
    assert abs(frozen_expectancy - _ARTIFACT_EXPECTANCY_PCT) < 1e-5, \
        "Expectancy reproduction mismatch"

    entries = [(s["price"]) for s in closed]
    exits = [(s["trade"]["exit_price"]) for s in closed]
    reasons = [s["trade"]["reason"] for s in closed]

    print(f"\nBreak-even gross move, legacy schedule, taker-priced exit "
          f"(STOP/MAX_HOLD): {((1 + legacy_entry_rate) / (1 - legacy_exit_rate) - 1) * 100:.4f}%")
    print(f"Break-even gross move, legacy schedule, maker-priced exit "
          f"(TAKE_PROFIT only): {((1 + legacy_entry_rate) / (1 - legacy_entry_rate) - 1) * 100:.4f}%")

    # ── Item 4: ATR-as-%-of-price distribution (shared — a property of the
    # trade sequence, not of any fee schedule) ───────────────────────────────
    atr_pct = [a / e * 100 for a, e in zip(atr_at_entry, entries)]
    atr_pct_sorted = sorted(atr_pct)
    median_atr_pct = statistics.median(atr_pct_sorted)
    mean_atr_pct = statistics.mean(atr_pct_sorted)
    q1_atr_pct = statistics.quantiles(atr_pct_sorted, n=4)[0]
    q3_atr_pct = statistics.quantiles(atr_pct_sorted, n=4)[2]

    atr_stop_mult = 2.0    # frozen ASSET_CONFIG["ZEC-USD"]["atr_stop"]
    atr_target_mult = 3.5  # frozen ASSET_CONFIG["ZEC-USD"]["atr_target"]

    print(f"\nATR-as-%-of-entry-price over n={n} entries "
          f"(1h ATR(14), the value _simulate_trade actually reads):")
    print(f"  mean={mean_atr_pct:.4f}%  median={median_atr_pct:.4f}%  "
          f"Q1={q1_atr_pct:.4f}%  Q3={q3_atr_pct:.4f}%")

    stop_dist_median = atr_stop_mult * median_atr_pct
    target_dist_median = atr_target_mult * median_atr_pct
    print(f"  stop distance   ({atr_stop_mult}x ATR) at median ATR%: "
          f"{stop_dist_median:.4f}% of entry price")
    print(f"  target distance ({atr_target_mult}x ATR) at median ATR%: "
          f"{target_dist_median:.4f}% of entry price")

    # ── Items 1/2/3/5, once per fee schedule scenario ────────────────────────
    adopted_pnls = _report_scenario(
        "ADOPTED (Intro 1, 2026-09)", "pipeline.fees.CURRENT_SCHEDULE",
        adopted_entry_rate, adopted_exit_rate,
        closed, entries, exits, reasons, stop_dist_median, target_dist_median)
    candidate_pnls = _report_scenario(
        "CANDIDATE (Intro, single reading 2026-09-17)",
        "NOT adopted - pending 4-reading cohort 2026-09-17..2026-09-20",
        _CANDIDATE_MAKER_RATE, _CANDIDATE_TAKER_RATE,
        closed, entries, exits, reasons, stop_dist_median, target_dist_median)

    # ── Item 6: what the sample bounds ────────────────────────────────────────
    # ADDENDUM 2. EQUIVALENCE-STYLE BOUND — not a hypothesis test, not a
    # p-value, no claim of "significance". It asks: given the spread actually
    # observed over these n=114 trades, how good could the true per-trade mean
    # plausibly be, at one-sided 95% confidence? Each scenario uses its OWN
    # sample SD (not a shared one) — the three fee schedules price the same
    # entries/exits slightly differently, so their return distributions are
    # not identical, only close.
    Z_ONE_SIDED_95 = 1.645
    _PRIOR_SD_PCT = 4.70          # prior estimate, binary +1.75R/-1R model — external to this script
    _PRIOR_UCB_AT_1PCT_PCT = 0.105

    print("\nItem 6 - what the sample bounds (equivalence-style bound; NOT a "
          "hypothesis test, NOT a p-value, no claim of 'significance'):")
    frozen_sd = statistics.stdev(frozen_pnls)
    print(f"  prior estimate (binary +1.75R/-1R model): SD ~= {_PRIOR_SD_PCT:.2f}% of "
          f"position, one-sided 95% upper bound = +{_PRIOR_UCB_AT_1PCT_PCT:.3f}%/trade "
          f"at the frozen 1.0% level")
    print(f"  measured SD at the frozen 1.0% level ({frozen_sd:.4f}%) is "
          f"{'LARGER' if frozen_sd > _PRIOR_SD_PCT else 'not larger'} than the prior's "
          f"{_PRIOR_SD_PCT:.2f}%, so the measured bound is correspondingly "
          f"{'WIDER' if frozen_sd > _PRIOR_SD_PCT else 'not wider'} than the prior's "
          f"+{_PRIOR_UCB_AT_1PCT_PCT:.3f}%/trade")

    bounds: dict[str, float] = {}
    for label, pnls in [
        ("frozen 1.0% model", frozen_pnls),
        ("ADOPTED 0.6%/1.2%", adopted_pnls),
        ("CANDIDATE 0.5%/0.9% (not adopted)", candidate_pnls),
    ]:
        mean_pct = statistics.mean(pnls)
        sd = statistics.stdev(pnls)
        se = sd / (n ** 0.5)
        ucb = mean_pct + Z_ONE_SIDED_95 * se
        bounds[label] = ucb
        sign = "ABOVE zero" if ucb > 0 else "BELOW zero"
        print(f"  {label}: mean={mean_pct:+.4f}%  SD={sd:.4f}%  SE={se:.4f}%  "
              f"one-sided 95% upper bound on true per-trade edge = {ucb:+.4f}%/trade "
              f"-- {sign}")

    # ── ADDENDUM 3: does the normal approximation hold? ──────────────────────
    # mean + 1.645*SE assumes the SAMPLE MEAN is approximately normal (by the
    # CLT, not that individual trades are). The per-trade distribution itself
    # is visibly bimodal (STOP_LOSS clusters ~-4.8%, TAKE_PROFIT ~+4.9%,
    # MAX_HOLD ~+0.5%), so this is checked rather than assumed: skewness and
    # excess kurtosis of the per-trade distribution, and a percentile
    # bootstrap bound on the MEAN (n=114, which is what the CLT needs to have
    # kicked in for, not the per-trade shape) run alongside the normal-theory
    # one to see whether they agree.
    print(f"\n  Distributional check (skew/kurtosis of the per-trade returns; "
          f"bootstrap bound on the MEAN, {_BOOTSTRAP_N:,} resamples, "
          f"seed={_BOOTSTRAP_SEED}):")
    boot_bounds: dict[str, float] = {}
    for label, pnls in [
        ("frozen 1.0% model", frozen_pnls),
        ("ADOPTED 0.6%/1.2%", adopted_pnls),
        ("CANDIDATE 0.5%/0.9% (not adopted)", candidate_pnls),
    ]:
        skew, exkurt = _skew_kurtosis(pnls)
        ucb_boot = _bootstrap_ucb(pnls)
        boot_bounds[label] = ucb_boot
        ucb_normal = bounds[label]
        side_normal = "ABOVE zero" if ucb_normal > 0 else "BELOW zero"
        side_boot = "ABOVE zero" if ucb_boot > 0 else "BELOW zero"
        agreement = "AGREES with" if side_normal == side_boot else "DISAGREES with"
        print(f"    {label}: skewness={skew:+.4f}  excess kurtosis={exkurt:+.4f}  "
              f"bootstrap 95% upper bound={ucb_boot:+.4f}%/trade "
              f"-- {side_boot}  ({agreement} the normal-theory bound)")

    for label in ("ADOPTED 0.6%/1.2%", "CANDIDATE 0.5%/0.9% (not adopted)"):
        se_label = statistics.stdev(
            adopted_pnls if label.startswith("ADOPTED") else candidate_pnls
        ) / (n ** 0.5)
        margin_se = -bounds[label] / se_label
        print(f"  {label}: normal-theory upper bound sits {margin_se:.2f} SE below zero")


if __name__ == "__main__":
    main()
