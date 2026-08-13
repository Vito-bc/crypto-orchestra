"""
Forward-OOS replay — deterministic reconstruction of the V3 shadow record.

The scanner is fully deterministic on closed candles, so the signals the live
scheduler WOULD have journaled since the pre-registered freeze (2026-07-12) can
be reconstructed from exchange candles even when the scheduler was down.

Output is counterfactual research evidence. It does NOT write to
logs/v3_journal.jsonl — the journal stays the record of what the live system
actually observed.

V3 ER-30 IS RETIRED as an activation candidate (docs/trial_registry.md,
2026-08-09). Nothing this script prints can reactivate it. It runs as a
diagnostic: it measures how the filter behaves, not whether to switch it on.

Two distinct paths are reported, and they are NOT interchangeable:

  UNFILTERED (V2)  — every signal is simulated; `v3_would_block` is recorded for
                     shadow accounting only. This is the counterfactual baseline.

  INTEGRATED (V3)  — a blocked signal is skipped BEFORE its trade is simulated,
                     so it does not advance `skip_until` and does not contribute
                     to stop history. Later signals therefore genuinely differ.
                     This is what live enforcement would have produced.

The integrated cohort must be produced by actually running the integrated path.
Post-filtering the unfiltered path by `v3_would_block` gives a different and
wrong answer, because it keeps the skip_until/stop-history side effects of
trades that enforcement would never have opened.

Right-censoring: a signal whose stop/target/max-hold horizon is not fully
observable by the requested end boundary is PENDING (`resolved=False`). Pending
signals are excluded from PF, bootstrap, expectancy, episode concentration and
closed-trade counts. Counting a censored trade as a completed MAX_HOLD would
silently invent outcomes at the right edge of the data.

Usage:
    python backtesting/oos_replay.py                 # freeze -> today
    python backtesting/oos_replay.py --end 2026-08-01
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.signal_scanner import scan_asset  # noqa: E402

OOS_FREEZE = "2026-07-12"          # pre-registered; do not move
ASSET      = "ZEC-USD"
WARMUP_D   = 120                   # enough for 4h EMA200 + whipsaw context


class ReplayInputError(ValueError):
    """CLI/date/coverage problem — fail loudly rather than replay a bad window."""


def profit_factor(pnls: np.ndarray) -> float:
    wins, losses = pnls[pnls > 0], pnls[pnls <= 0]
    if losses.sum() == 0:
        return float("inf") if wins.sum() > 0 else 0.0
    return float(wins.sum() / abs(losses.sum()))


def block_bootstrap_p_pf_gt_1(pnls: np.ndarray, b: int = 4,
                              n_boot: int = 10_000, seed: int = 42) -> float:
    """
    P(PF > 1) under a circular block bootstrap with block length b.

    An all-win resample has PF == inf, which IS greater than one and must count
    as a hit. Dropping those samples would bias the probability downward exactly
    in the favourable tail.
    """
    n = len(pnls)
    if n < b:
        return float("nan")
    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n / b))
    hits = 0
    for _ in range(n_boot):
        starts = rng.integers(0, n, size=n_blocks)
        idx = (starts[:, None] + np.arange(b)[None, :]).ravel() % n
        sample = pnls[idx[:n]]
        if profit_factor(sample) > 1.0:      # inf > 1.0 is True — counted
            hits += 1
    return hits / n_boot


def episode_concentration(signals: list[dict]) -> float:
    """
    Max share of gross profit contributed by one 30d-gap episode.

    Boundary convention: the registry criterion is "no single episode > 50%", so
    a value of exactly 0.50 passes. The same `<= 0.5` boundary is used wherever
    this is evaluated.
    """
    if not signals:
        return 0.0
    episodes: list[list[dict]] = [[signals[0]]]
    for s in signals[1:]:
        gap = (pd.Timestamp(s["timestamp"]) - pd.Timestamp(episodes[-1][-1]["timestamp"])).days
        if gap > 30:
            episodes.append([])
        episodes[-1].append(s)
    gross = [sum(x["trade"]["pnl_pct"] for x in ep if x["trade"]["pnl_pct"] > 0)
             for ep in episodes]
    total = sum(gross)
    return max(gross) / total if total > 0 else 0.0


def is_resolved(sig: dict) -> bool:
    """A signal counts as closed only if its trade horizon was fully observed."""
    return bool(sig.get("trade", {}).get("resolved", True))


def validate_window(end: str) -> str:
    """Validate the requested end boundary. Raises ReplayInputError."""
    try:
        end_ts = pd.Timestamp(end)
    except Exception as exc:
        raise ReplayInputError(f"--end {end!r} is not a valid date: {exc}") from exc
    if end_ts.tzinfo is not None:
        end_ts = end_ts.tz_localize(None)
    freeze_ts = pd.Timestamp(OOS_FREEZE)
    if end_ts < freeze_ts:
        raise ReplayInputError(
            f"--end {end} is before the pre-registered OOS freeze {OOS_FREEZE}. "
            "Replaying a window that ends before the freeze would report "
            "in-sample data as forward-OOS evidence."
        )
    today = pd.Timestamp(date.today().isoformat())
    if end_ts > today:
        raise ReplayInputError(
            f"--end {end} is in the future (today is {today.date()}). "
            "There are no candles for that window."
        )
    return end_ts.date().isoformat()


def check_coverage(res: dict, end: str) -> None:
    """Fail closed when the scanner returned no usable candle coverage."""
    if res is None:
        raise ReplayInputError(f"scan_asset returned nothing for {ASSET}")
    if not res.get("candles"):
        raise ReplayInputError(
            f"No candles for {ASSET} in {OOS_FREEZE} -> {end}. Refusing to report "
            "an empty replay as evidence of 'no signals'."
        )


def summarise(label: str, sigs: list[dict], *, show_criteria: bool) -> dict:
    """Print one path's cohort and return its resolved-only statistics."""
    resolved = [s for s in sigs if is_resolved(s)]
    pending  = [s for s in sigs if not is_resolved(s)]

    print(f"\n  {label}")
    print(f"  {'-' * len(label)}")
    if not sigs:
        print("    (no signals)")
        return {"n_resolved": 0, "n_pending": 0, "pf": float("nan")}

    for s in sigs:
        er = s.get("regime", {}).get("er_30")
        tr = s["trade"]
        tag = "BLOCKED (shadow)" if s.get("v3_would_block") else "V3-ACCEPTED"
        state = "" if is_resolved(s) else "  [PENDING — right-censored]"
        print(f"    {s['timestamp']}  er30={er}  {tag:<16}  "
              f"{tr['reason']:<11} {tr['pnl_pct']:+.2f}%  ({tr['hold_h']}h){state}")

    if pending:
        print(f"    {len(pending)} pending signal(s) excluded from all statistics.")

    pnls = np.array([s["trade"]["pnl_pct"] for s in resolved])
    stats = {"n_resolved": len(resolved), "n_pending": len(pending),
             "pf": profit_factor(pnls) if len(pnls) else float("nan")}
    if len(pnls):
        print(f"    closed n={len(pnls)}  PF={stats['pf']:.2f}  "
              f"avg={pnls.mean():+.2f}%")

    if show_criteria and len(pnls):
        # Retained as a diagnostic readout only. V3 is retired: these numbers
        # cannot activate anything, and there is no n>=20 gate any more.
        p_boot = block_bootstrap_p_pf_gt_1(pnls)
        p_str = f"{p_boot:.1%}" if not np.isnan(p_boot) else "n/a (n<4)"
        conc = episode_concentration(resolved)
        print(f"    diagnostic: P(PF>1)={p_str}  "
              f"avg-0.25%={pnls.mean() - 0.25:+.2f}%  episode_conc={conc:.0%}")
    return stats


def run(end: str | None = None) -> dict:
    end = validate_window(end or date.today().isoformat())
    warmup = (pd.Timestamp(OOS_FREEZE) - timedelta(days=WARMUP_D)).date().isoformat()
    period = {"label": f"OOS replay {OOS_FREEZE} -> {end}", "btc_move": "",
              "warmup": warmup, "start": OOS_FREEZE, "end": end}

    print("\n" + "=" * 70)
    print(f"FORWARD OOS REPLAY — {ASSET}  {OOS_FREEZE} -> {end}")
    print("Counterfactual research evidence (deterministic candle replay).")
    print("Not a substitute for logs/v3_journal.jsonl live records.")
    print("V3 is RETIRED as an activation candidate — this is diagnostic only.")
    print("=" * 70)

    # Two independent scans. The integrated cohort is PRODUCED by enforcement,
    # never derived by post-filtering the unfiltered run.
    unfiltered = scan_asset(ASSET, period, v3_enforcement=False)
    check_coverage(unfiltered, end)
    integrated = scan_asset(ASSET, period, v3_enforcement=True)
    check_coverage(integrated, end)

    assert unfiltered.get("v3_enforcement") is False
    assert integrated.get("v3_enforcement") is True

    u_stats = summarise(
        "UNFILTERED (V2 baseline) — every signal simulated, v3 shadow-tagged",
        unfiltered.get("signals", []), show_criteria=False,
    )
    i_stats = summarise(
        "INTEGRATED (V3 enforced) — blocked signals never opened",
        integrated.get("signals", []), show_criteria=True,
    )

    print("\n  Path comparison")
    print("  ---------------")
    print(f"    unfiltered: {u_stats['n_resolved']} closed, "
          f"{u_stats['n_pending']} pending")
    print(f"    integrated: {i_stats['n_resolved']} closed, "
          f"{i_stats['n_pending']} pending, "
          f"{integrated.get('blocked_v3', 0)} blocked by V3")
    print("    The two cohorts are not substitutable: enforcement changes which")
    print("    later signals exist at all, via skip_until and stop history.")

    return {"unfiltered": u_stats, "integrated": i_stats,
            "blocked_v3": integrated.get("blocked_v3", 0), "end": end}


if __name__ == "__main__":
    end_arg = None
    if "--end" in sys.argv:
        try:
            end_arg = sys.argv[sys.argv.index("--end") + 1]
        except IndexError:
            print("error: --end requires a date argument (YYYY-MM-DD)", file=sys.stderr)
            sys.exit(2)
    try:
        run(end_arg)
    except ReplayInputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
