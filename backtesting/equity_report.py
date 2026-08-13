"""
Equity-curve accounting beyond max drawdown.

Pure functions over a chronological trade list; usable on scanner replays,
journal counterfactuals, or live trade history.

Accounting contract (all of these were wrong before — see docs/tasks/
2026-08-research-evidence-hardening.md Phase 4):

  * The curve starts at equity 1.0 BEFORE the first trade. Without that
    baseline, `cummax` began at the post-first-trade equity, so a first-trade
    loss was invisible to drawdown and max DD was understated.

  * Time underwater is CALENDAR time: each drawdown reading is weighted by the
    elapsed interval it covers, and the final reading extends to the evaluation
    end. Counting post-trade observations instead answers a different question
    ("what fraction of trades happened while underwater") and is reported
    separately as `time_underwater_obs`.

  * An unrecovered final drawdown lasts until the evaluation END, not until the
    last trade. Measuring only to the last trade silently shortens the worst
    episode — exactly the one that matters.

  * CAGR, exposure and trades/year are defined over the REQUESTED evaluation
    window, not the span between the first and last trade.

  * Timezone contract: naive timestamps are interpreted as UTC. Mixed naive and
    aware inputs are normalised to UTC so comparisons are deterministic.

Trade record schema (dict): ts (ISO timestamp), pnl_pct (net %), hold_h.

CLI (replays the frozen scanner and reports on the result):
    python backtesting/equity_report.py ZEC-USD 2021-03-01 2026-07-12
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_SECONDS_PER_DAY = 86_400.0


def _as_utc(ts) -> pd.Timestamp:
    """Normalise any timestamp to UTC. Naive input is treated as UTC."""
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def equity_curve(trades: list[dict], start: Optional[str] = None) -> pd.DataFrame:
    """
    Unit equity, full compounding of net per-trade returns.

    The first row is the pre-trade baseline (eq = 1.0) so that a losing first
    trade registers as a drawdown from initial capital. `start` places that
    baseline; when omitted it sits at the first trade's timestamp.
    """
    if not trades:
        return pd.DataFrame(columns=["ts", "eq", "pnl", "hold_h"])

    ts_sorted = sorted(trades, key=lambda t: _as_utc(t["ts"]))
    base_ts = _as_utc(start) if start is not None else _as_utc(ts_sorted[0]["ts"])

    rows = [{"ts": base_ts, "eq": 1.0, "pnl": 0.0, "hold_h": 0}]
    eq = 1.0
    for t in ts_sorted:
        eq *= 1 + t["pnl_pct"] / 100
        rows.append({"ts": _as_utc(t["ts"]), "eq": eq,
                     "pnl": t["pnl_pct"], "hold_h": t.get("hold_h", 0)})
    return pd.DataFrame(rows)


def drawdown_series(eq: pd.Series) -> pd.Series:
    return eq / eq.cummax() - 1


def drawdown_episodes(df: pd.DataFrame, end: Optional[str] = None) -> list[dict]:
    """
    Contiguous underwater stretches.

    `end` closes an episode that never recovers: its duration runs to the
    evaluation boundary rather than to the last trade. Without it the deepest,
    still-open drawdown is reported as shorter than it really is.

    duration_days is fractional so that boundary-aware durations are exact.
    """
    if df.empty:
        return []
    dd = drawdown_series(df["eq"]).values
    episodes, start_i = [], None
    for i, v in enumerate(dd):
        if v < 0 and start_i is None:
            start_i = i
        elif v >= 0 and start_i is not None:
            episodes.append((start_i, i))
            start_i = None
    open_tail = start_i is not None
    if open_tail:
        episodes.append((start_i, len(dd) - 1))

    end_ts = _as_utc(end) if end is not None else None
    out = []
    for n, (s, e) in enumerate(episodes):
        start_ts = df["ts"].iloc[s]
        # A recovered episode ends when equity regained its peak (index e).
        # An unrecovered final episode runs to the evaluation end.
        is_open_tail = open_tail and n == len(episodes) - 1
        stop_ts = end_ts if (is_open_tail and end_ts is not None) else df["ts"].iloc[e]
        if stop_ts < start_ts:
            stop_ts = start_ts
        out.append({
            "start": start_ts,
            "end": stop_ts,
            "depth": float(dd[s:e + 1].min()),
            "duration_days": (stop_ts - start_ts).total_seconds() / _SECONDS_PER_DAY,
            "n_trades": e - s + 1,
            "recovered": not is_open_tail,
        })
    return out


def calendar_time_underwater(df: pd.DataFrame, end: Optional[str] = None) -> float:
    """
    Fraction of ELAPSED CALENDAR TIME spent below the running peak.

    Each drawdown reading is weighted by the interval it covers (from its own
    timestamp to the next), and the last reading extends to `end`. Trades are
    irregularly spaced, so counting observations instead of weighting intervals
    can differ enormously — a single long underwater gap counts once as an
    observation but dominates in calendar terms.
    """
    if df.empty:
        return 0.0
    dd = drawdown_series(df["eq"]).values
    ts = df["ts"].tolist()
    end_ts = _as_utc(end) if end is not None else ts[-1]
    if end_ts < ts[-1]:
        end_ts = ts[-1]

    total = (end_ts - ts[0]).total_seconds()
    if total <= 0:
        return 0.0
    under = 0.0
    for i in range(len(ts)):
        seg_end = ts[i + 1] if i + 1 < len(ts) else end_ts
        if dd[i] < 0:
            under += (seg_end - ts[i]).total_seconds()
    return under / total


def _safe_std(values: np.ndarray) -> float:
    """
    Sample std that returns nan instead of emitting a NumPy warning.

    np.std on an empty or single-element slice warns ("Degrees of freedom <= 0",
    "invalid value encountered") and returns nan. All-win series have an empty
    downside slice, which is a legitimate input here, not an error.
    """
    if values.size < 2:
        return float("nan")
    return float(values.std())


def summary(trades: list[dict], start: Optional[str] = None,
            end: Optional[str] = None) -> dict:
    """
    Full metrics dict for a chronological trade list.

    start/end define the evaluation window. They are strongly recommended: when
    omitted, the window falls back to the first/last trade, which understates
    both time underwater and the duration of an unrecovered final drawdown.
    """
    df = equity_curve(trades, start=start)
    if df.empty:
        return {"trades": 0}

    eq = df["eq"]
    dd = drawdown_series(eq)

    win_start = df["ts"].iloc[0]
    win_end = _as_utc(end) if end is not None else df["ts"].iloc[-1]
    if win_end < df["ts"].iloc[-1]:
        win_end = df["ts"].iloc[-1]

    span_days = max((win_end - win_start).total_seconds() / _SECONDS_PER_DAY, 1e-9)
    years = span_days / 365.25

    total_ret = float(eq.iloc[-1] - 1)
    cagr = float(eq.iloc[-1] ** (1 / years) - 1) if years > 0 else float("nan")
    episodes = drawdown_episodes(df, end=end)

    # Exclude the synthetic baseline row from per-trade statistics.
    r = df["pnl"].values[1:] / 100
    n_trades = len(r)
    if n_trades == 0:
        return {"trades": 0}

    wins, losses = r[r > 0], r[r <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else (
        float("inf") if wins.sum() > 0 else 0.0
    )
    tpy = n_trades / years if years > 0 else float("nan")

    r_std = _safe_std(r)
    sharpe = (float(r.mean() / r_std * np.sqrt(tpy))
              if r_std and np.isfinite(r_std) and r_std > 0 else float("nan"))
    downside = _safe_std(r[r < 0])
    sortino = (float(r.mean() / downside * np.sqrt(tpy))
               if downside and np.isfinite(downside) and downside > 0 else float("nan"))
    max_dd = float(dd.min())

    return {
        "trades": n_trades,
        "eval_start": win_start,
        "eval_end": win_end,
        "total_ret": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "avg_dd": float(dd[dd < 0].mean()) if (dd < 0).any() else 0.0,
        "n_dd_episodes": len(episodes),
        # Boundary-aware: an unrecovered episode runs to eval_end.
        "longest_dd_days": max((e["duration_days"] for e in episodes), default=0.0),
        # Calendar-weighted (the headline figure).
        "time_underwater": calendar_time_underwater(df, end=end),
        # Observation-weighted, kept only because it is separately named.
        "time_underwater_obs": float((dd < 0).mean()),
        "recovery_factor": (total_ret / abs(max_dd)) if max_dd < 0 else float("inf"),
        "calmar": (cagr / abs(max_dd)) if max_dd < 0 else float("inf"),
        "sharpe": sharpe,
        "sortino": sortino,
        "pf": pf,
        "expectancy": float(r.mean()),
        "win_rate": float((r > 0).mean()),
        "exposure": float(df["hold_h"].sum() / (span_days * 24)) if span_days else 0.0,
        "trades_per_year": tpy,
    }


def print_summary(m: dict, label: str = "") -> None:
    if m.get("trades", 0) == 0:
        print(f"  {label}: no trades")
        return
    pct = ("total_ret", "cagr", "max_dd", "avg_dd", "time_underwater",
           "time_underwater_obs", "expectancy", "win_rate", "exposure")
    print(f"\n  {label}")
    for k, v in m.items():
        if isinstance(v, pd.Timestamp):
            print(f"    {k:<20} {v.date()}")
        elif k in pct:
            if k in ("time_underwater", "time_underwater_obs"):
                print(f"    {k:<20} {v:.2%}")
            else:
                print(f"    {k:<20} {v:+.2%}")
        elif isinstance(v, float):
            print(f"    {k:<20} {v:.2f}")
        else:
            print(f"    {k:<20} {v}")


def _cli() -> None:
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    asset, start, end = sys.argv[1:4]
    from backtesting.signal_scanner import scan_asset
    warmup = (pd.Timestamp(start) - pd.Timedelta(days=90)).date().isoformat()
    period = {"label": f"{asset} {start}->{end}", "btc_move": "",
              "warmup": warmup, "start": start, "end": end}
    res = scan_asset(asset, period)
    # Only resolved trades enter equity accounting — a right-censored trade has
    # no realised outcome (see oos_replay / _simulate_trade `resolved`).
    trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
               "hold_h": s["trade"]["hold_h"]}
              for s in res.get("signals", [])
              if s["trade"].get("resolved", True)]
    print_summary(summary(trades, start=start, end=end),
                  f"{asset} {start} -> {end} (frozen scanner)")
    for ep in drawdown_episodes(equity_curve(trades, start=start), end=end):
        flag = "" if ep["recovered"] else "  [UNRECOVERED at eval end]"
        print(f"    dd episode: {ep['start'].date()} -> {ep['end'].date()}  "
              f"depth={ep['depth']:.1%}  {ep['duration_days']:.1f}d  "
              f"({ep['n_trades']} trades){flag}")


if __name__ == "__main__":
    _cli()
