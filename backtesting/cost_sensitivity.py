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
at the MEASURED prospective operational schedule instead
(pipeline/fees.py CURRENT_SCHEDULE: entry 0.6% maker, exit 1.2% taker on
EVERY exit path, because pipeline/position_tracker.close_position() always
sends a market sell)?

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


def main() -> None:
    from pipeline.fees import CURRENT_SCHEDULE, LEGACY_SCHEDULE, MAKER, TAKER

    measured_entry_rate = CURRENT_SCHEDULE.rate_for(MAKER)
    measured_exit_rate = CURRENT_SCHEDULE.rate_for(TAKER)
    legacy_entry_rate = LEGACY_SCHEDULE.rate_for(MAKER)   # == _ENTRY_FEE == _TP_FEE
    legacy_exit_rate = LEGACY_SCHEDULE.rate_for(TAKER)    # == _SL_FEE

    print(f"Measured schedule : entry(maker)={measured_entry_rate:.3%}  "
          f"exit(taker)={measured_exit_rate:.3%}")
    print(f"Legacy schedule   : entry(maker)={legacy_entry_rate:.3%}  "
          f"exit(taker)={legacy_exit_rate:.3%}")

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

    # ── Item 1: prospective round-trip cost per trade, by exit path ─────────
    # entry_fee is always qty_usd * entry_rate -> exactly measured_entry_rate
    # (0.6%) of entry notional, on EVERY exit path, because it is stamped at
    # placement before the exit reason is known.
    # exit_fee is always gross_proceeds * exit_rate (close_position() reads
    # TAKER unconditionally) -> measured_exit_rate (1.2%) of gross proceeds,
    # i.e. measured_exit_rate * (exit_price/entry_price) of entry notional.
    # The RATE never varies by exit path; the % of ENTRY NOTIONAL it works out
    # to does, because gross proceeds differ by exit path.
    print("\nItem 1 - prospective round-trip cost per trade, by exit path "
          "(entry fee always on entry USD notional; exit fee always on gross "
          "proceeds, at the TAKER rate, matching close_position() exactly - "
          "no exit path gets a maker rate in the live/operational code):")
    print(f"  entry fee: {measured_entry_rate:.3%} of entry notional (ALL paths)")
    by_reason_cost: dict[str, list[float]] = {}
    for e, x, r in zip(entries, exits, reasons):
        exit_fee_pct_of_entry = measured_exit_rate * (x / e) * 100
        total_cost_pct = measured_entry_rate * 100 + exit_fee_pct_of_entry
        by_reason_cost.setdefault(r, []).append(total_cost_pct)
    for reason, costs in sorted(by_reason_cost.items()):
        print(f"  {reason:12s} n={len(costs):3d}  "
              f"mean round-trip cost = {statistics.mean(costs):.4f}% of entry notional")

    # ── Item 2: break-even gross move, measured schedule ─────────────────────
    breakeven_measured = ((1 + measured_entry_rate) / (1 - measured_exit_rate) - 1) * 100
    breakeven_legacy_taker_exit = ((1 + legacy_entry_rate) / (1 - legacy_exit_rate) - 1) * 100
    breakeven_legacy_maker_exit = ((1 + legacy_entry_rate) / (1 - legacy_entry_rate) - 1) * 100
    print(f"\nBreak-even gross move, measured schedule (all exit paths): "
          f"{breakeven_measured:.4f}%")
    print(f"Break-even gross move, legacy schedule, taker-priced exit "
          f"(STOP/MAX_HOLD): {breakeven_legacy_taker_exit:.4f}%")
    print(f"Break-even gross move, legacy schedule, maker-priced exit "
          f"(TAKE_PROFIT only): {breakeven_legacy_maker_exit:.4f}%")

    # ── Item 3: re-price the n=114 trade sequence ────────────────────────────
    measured_pnls = [
        _measured_pnl_pct(e, x, measured_entry_rate, measured_exit_rate)
        for e, x in zip(entries, exits)
    ]
    measured_pf, measured_expectancy = _pf_and_expectancy(measured_pnls)
    print(f"\nPROSPECTIVE SENSITIVITY (not a restatement of the artifact): "
          f"n={n} PF={measured_pf:.5f} expectancy={measured_expectancy:.6f} "
          f"({measured_expectancy*100:.4f}%/trade)")

    by_reason: dict[str, list[float]] = {}
    for s, mp in zip(closed, measured_pnls):
        by_reason.setdefault(s["trade"]["reason"], []).append(mp)
    for reason, pnls in sorted(by_reason.items()):
        print(f"  {reason:12s} n={len(pnls):3d}  "
              f"mean_measured_pnl_pct={statistics.mean(pnls):.4f}")

    # ── Item 4: ATR-as-%-of-price distribution, fraction of stop/target consumed ─
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
    print(f"  break-even ({breakeven_measured:.4f}%) / stop distance   "
          f"= {breakeven_measured / stop_dist_median:.4f}")
    print(f"  break-even ({breakeven_measured:.4f}%) / target distance "
          f"= {breakeven_measured / target_dist_median:.4f}")

    # ── Item 5: maker-exit cost quantification (TAKE_PROFIT only, cost side only) ─
    tp_pnls_taker = by_reason.get("TAKE_PROFIT", [])
    tp_entries_exits = [
        (s["price"], s["trade"]["exit_price"]) for s in closed
        if s["trade"]["reason"] == "TAKE_PROFIT"
    ]
    hypothetical_maker_exit_rate = measured_entry_rate  # today's MAKER rate
    tp_pnls_hypothetical_maker_exit = [
        _measured_pnl_pct(e, x, measured_entry_rate, hypothetical_maker_exit_rate)
        for e, x in tp_entries_exits
    ]
    # cost delta per TAKE_PROFIT trade, in percentage points of entry price:
    # exit fee is charged on gross proceeds, not entry notional, so the delta
    # is not exactly (1.2%-0.6%) — compute it exactly per trade instead of
    # assuming the additive approximation.
    cost_delta_pp = [
        hyp - taker for hyp, taker in zip(tp_pnls_hypothetical_maker_exit, tp_pnls_taker)
    ]
    n_tp = len(tp_pnls_taker)
    print(f"\nMaker-exit sensitivity, TAKE_PROFIT trades only (n={n_tp}), "
          f"cost side ONLY (no fill-rate assumption):")
    if n_tp:
        print(f"  mean cost reduction per TAKE_PROFIT trade if exit were maker: "
              f"{statistics.mean(cost_delta_pp):.4f} pp")
        print(f"  mean measured pnl_pct (actual, taker exit): "
              f"{statistics.mean(tp_pnls_taker):.4f}%")
        print(f"  mean pnl_pct (hypothetical, maker exit):     "
              f"{statistics.mean(tp_pnls_hypothetical_maker_exit):.4f}%")
    # Whole-sample effect of hypothetically maker-pricing every TAKE_PROFIT exit:
    hyp_by_index = dict(zip(
        [k for k, s in enumerate(closed) if s["trade"]["reason"] == "TAKE_PROFIT"],
        tp_pnls_hypothetical_maker_exit))
    whole_sample_hyp = [
        hyp_by_index.get(k, measured_pnls[k]) for k in range(n)
    ]
    hyp_pf, hyp_expectancy = _pf_and_expectancy(whole_sample_hyp)
    print(f"  whole-sample (n={n}) PF if TAKE_PROFIT exits were maker: "
          f"{hyp_pf:.5f}  expectancy={hyp_expectancy:.6f} "
          f"({hyp_expectancy*100:.4f}%/trade)")


if __name__ == "__main__":
    main()
