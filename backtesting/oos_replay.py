"""
Forward-OOS replay — deterministic reconstruction of the V3 shadow record.

The scanner is fully deterministic on closed candles, so the signals the live
scheduler WOULD have journaled since the pre-registered freeze (2026-07-12)
can be reconstructed from exchange candles even when the scheduler was down.

Output is counterfactual research evidence for the pre-registered V3 trial.
It does NOT write to logs/v3_journal.jsonl — the journal stays the record of
what the live system actually observed.

Evaluates the 5 pre-registered activation criteria (docs/trial_registry.md):
  1. n >= 20 closed V3-accepted trades
  2. PF > 1.20
  3. P_bootstrap(PF > 1) > 90%   (block bootstrap, b=4, N=10,000)
  4. Avg P&L > 0% after +0.25% per-trade friction
  5. No single market episode (30d gap grouping) > 50% of gross profit

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


def profit_factor(pnls: np.ndarray) -> float:
    wins, losses = pnls[pnls > 0], pnls[pnls <= 0]
    if losses.sum() == 0:
        return float("inf") if wins.sum() > 0 else 0.0
    return float(wins.sum() / abs(losses.sum()))


def block_bootstrap_p_pf_gt_1(pnls: np.ndarray, b: int = 4,
                              n_boot: int = 10_000, seed: int = 42) -> float:
    """P(PF > 1) under a circular block bootstrap with block length b."""
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
        if profit_factor(sample) > 1.0:
            hits += 1
    return hits / n_boot


def episode_concentration(signals: list[dict]) -> float:
    """Max share of gross profit contributed by one 30d-gap episode."""
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


def run(end: str | None = None) -> None:
    end = end or date.today().isoformat()
    warmup = (pd.Timestamp(OOS_FREEZE) - timedelta(days=WARMUP_D)).date().isoformat()
    period = {"label": f"OOS replay {OOS_FREEZE} -> {end}", "btc_move": "",
              "warmup": warmup, "start": OOS_FREEZE, "end": end}

    res = scan_asset(ASSET, period)
    sigs = res.get("signals", [])

    print("\n" + "=" * 70)
    print(f"FORWARD OOS REPLAY — {ASSET}  {OOS_FREEZE} -> {end}")
    print("Counterfactual research evidence (deterministic candle replay).")
    print("Not a substitute for logs/v3_journal.jsonl live records.")
    print("=" * 70)

    if not sigs:
        print("  No scanner signals in the OOS window.")
        return

    for s in sigs:
        er = s.get("regime", {}).get("er_30")
        tr = s["trade"]
        tag = "BLOCKED (shadow)" if s.get("v3_would_block") else "V3-ACCEPTED"
        print(f"  {s['timestamp']}  er30={er}  {tag:<16}  "
              f"{tr['reason']:<11} {tr['pnl_pct']:+.2f}%  ({tr['hold_h']}h)")

    accepted = [s for s in sigs if not s.get("v3_would_block")]
    pnls = np.array([s["trade"]["pnl_pct"] for s in accepted])

    print("\n  Pre-registered activation criteria (V3-accepted trades only):")
    n_ok = len(pnls) >= 20
    print(f"    1. n >= 20:            n={len(pnls):<4} {'PASS' if n_ok else 'pending'}")
    if len(pnls) == 0:
        return
    pf = profit_factor(pnls)
    print(f"    2. PF > 1.20:          PF={pf:.2f}   "
          f"{'PASS' if pf > 1.20 else 'FAIL (interim)'}")
    p_boot = block_bootstrap_p_pf_gt_1(pnls)
    p_str = f"{p_boot:.1%}" if not np.isnan(p_boot) else "n/a (n<4)"
    print(f"    3. P(PF>1) > 90%:      {p_str}")
    stressed = pnls - 0.25
    print(f"    4. avg - 0.25% > 0:    {stressed.mean():+.2f}%   "
          f"{'PASS' if stressed.mean() > 0 else 'FAIL (interim)'}")
    conc = episode_concentration(accepted)
    print(f"    5. episode conc <=50%: {conc:.0%}   "
          f"{'PASS' if conc <= 0.5 else 'FAIL (interim)'}")
    print("\n  Interim FAILs are not a verdict until criterion 1 is met.")


if __name__ == "__main__":
    end_arg = None
    if "--end" in sys.argv:
        end_arg = sys.argv[sys.argv.index("--end") + 1]
    run(end_arg)
