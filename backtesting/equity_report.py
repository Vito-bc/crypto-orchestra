"""
Equity-curve accounting beyond max drawdown.

Pure functions over a chronological trade list; usable on scanner replays,
journal counterfactuals, or live trade history. Metrics: total return, CAGR,
max/avg drawdown, drawdown episode count and durations, time underwater,
recovery factor, Calmar, Sharpe, Sortino, profit factor, expectancy, win rate,
exposure, turnover (trades/year).

Trade record schema (dict): ts (ISO timestamp), pnl_pct (net %), hold_h.

CLI (replays the frozen scanner and reports on the result):
    python backtesting/equity_report.py ZEC-USD 2021-03-01 2026-07-12
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def equity_curve(trades: list[dict]) -> pd.DataFrame:
    """Unit equity, full compounding of net per-trade returns."""
    rows, eq = [], 1.0
    for t in trades:
        eq *= 1 + t["pnl_pct"] / 100
        rows.append({"ts": pd.Timestamp(t["ts"]), "eq": eq,
                     "pnl": t["pnl_pct"], "hold_h": t.get("hold_h", 0)})
    df = pd.DataFrame(rows)
    if not df.empty and df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize("UTC")
    return df


def drawdown_series(eq: pd.Series) -> pd.Series:
    return eq / eq.cummax() - 1


def drawdown_episodes(df: pd.DataFrame) -> list[dict]:
    """Contiguous underwater stretches: start/end index, depth, duration days."""
    dd = drawdown_series(df["eq"]).values
    episodes, start = [], None
    for i, v in enumerate(dd):
        if v < 0 and start is None:
            start = i
        elif v == 0 and start is not None:
            episodes.append((start, i))
            start = None
    if start is not None:
        episodes.append((start, len(dd) - 1))
    out = []
    for s, e in episodes:
        out.append({
            "start": df["ts"].iloc[s], "end": df["ts"].iloc[e],
            "depth": float(dd[s:e + 1].min()),
            "duration_days": (df["ts"].iloc[e] - df["ts"].iloc[s]).days,
            "n_trades": e - s + 1,
        })
    return out


def summary(trades: list[dict]) -> dict:
    """Full metrics dict for a chronological trade list."""
    df = equity_curve(trades)
    if df.empty:
        return {"trades": 0}
    eq = df["eq"]
    dd = drawdown_series(eq)
    span_days = max((df["ts"].iloc[-1] - df["ts"].iloc[0]).days, 1)
    years = span_days / 365.25
    total_ret = float(eq.iloc[-1] - 1)
    cagr = float(eq.iloc[-1] ** (1 / years) - 1) if years > 0 else float("nan")
    episodes = drawdown_episodes(df)

    r = df["pnl"].values / 100
    wins, losses = r[r > 0], r[r <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf")
    tpy = len(r) / years
    sharpe = float(r.mean() / r.std() * np.sqrt(tpy)) if r.std() > 0 else float("nan")
    downside = r[r < 0].std()
    sortino = (float(r.mean() / downside * np.sqrt(tpy))
               if downside and downside > 0 else float("nan"))
    max_dd = float(dd.min())

    return {
        "trades": len(df),
        "total_ret": total_ret,
        "cagr": cagr,
        "max_dd": max_dd,
        "avg_dd": float(dd[dd < 0].mean()) if (dd < 0).any() else 0.0,
        "n_dd_episodes": len(episodes),
        "longest_dd_days": max((e["duration_days"] for e in episodes), default=0),
        "time_underwater": float((dd < 0).mean()),
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
           "expectancy", "win_rate", "exposure")
    print(f"\n  {label}")
    for k, v in m.items():
        if k in pct:
            print(f"    {k:<18} {v:+.2%}" if k != "time_underwater" else
                  f"    {k:<18} {v:.0%}")
        elif isinstance(v, float):
            print(f"    {k:<18} {v:.2f}")
        else:
            print(f"    {k:<18} {v}")


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
    trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
               "hold_h": s["trade"]["hold_h"]} for s in res.get("signals", [])]
    print_summary(summary(trades), f"{asset} {start} -> {end} (frozen scanner)")
    for ep in drawdown_episodes(equity_curve(trades)):
        print(f"    dd episode: {ep['start'].date()} -> {ep['end'].date()}  "
              f"depth={ep['depth']:.1%}  {ep['duration_days']}d  "
              f"({ep['n_trades']} trades)")


if __name__ == "__main__":
    _cli()
