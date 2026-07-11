"""
Walk-Forward Optimization for CryptoOrchestra.

Validates per-asset ATR parameters out-of-sample (OOS).

Method:
  1. Split full_year into 3 rolling windows (3-month train, 2-month test)
  2. For each window, test 3 ATR stop levels per asset on TRAIN data
  3. Pick the best stop level per asset on train
  4. Apply winner to TEST data (out-of-sample)
  5. Report: does the in-sample winner generalize?

This answers: "Are we overfitting our ATR params, or do they genuinely work?"

Usage:
    python backtesting/walk_forward.py
    python backtesting/walk_forward.py --asset ZEC-USD
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from backtesting.signal_scanner import (
    ASSETS, ASSET_CONFIG, _download_and_compute, _detect_breakout_signal,
    _WHIPSAW_MAX_STOPS, _WHIPSAW_WINDOW_H,
)
from backtesting.backtest import attach_higher_timeframe_context, STRATEGY_CONFIG, FEE_RATE

# ── Walk-forward windows ──────────────────────────────────────────────────────
# Each window: warmup start, train start, test start, test end
WINDOWS = [
    {
        "label":        "Window 1",
        "train_start":  "2024-09-01",
        "test_start":   "2024-11-15",
        "test_end":     "2025-01-20",
        "train_regime": "Aug crash recovery",
        "test_regime":  "Trump rally",
    },
    {
        "label":        "Window 2",
        "train_start":  "2024-11-01",
        "test_start":   "2025-01-20",
        "test_end":     "2025-04-01",
        "train_regime": "Trump rally",
        "test_regime":  "Q1 2025 bear",
    },
    {
        "label":        "Window 3",
        "train_start":  "2025-01-20",
        "test_start":   "2025-04-01",
        "test_end":     "2025-06-28",
        "train_regime": "Q1 2025 bear",
        "test_regime":  "Current bear",
    },
]

# Single download range covering all windows — avoids multiple yfinance calls
_GLOBAL_WARMUP = "2024-08-15"   # gives 2 weeks warmup before Window 1 train start
_GLOBAL_END    = "2025-06-29"

# ATR stop candidates to test (target = stop * RR_RATIO)
STOP_CANDIDATES = [1.5, 2.0, 2.5, 3.0]
RR_RATIO        = 1.75   # target = stop * RR_RATIO for all candidates


# ── Core scanner (inline, no global ATR constants) ───────────────────────────

def _run_scan(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp,
              asset: str, atr_stop: float, atr_target: float) -> dict:
    """Run scanner on df slice [start_ts, end_ts] with given ATR params."""
    config   = STRATEGY_CONFIG.get(asset, STRATEGY_CONFIG["ETH-USD"])
    max_hold = config.get("max_hold_hours", 36)

    signals         = []
    blocked_vol     = 0
    blocked_4h      = 0
    blocked_cond    = 0
    blocked_whipsaw = 0
    skip_until      = -1
    recent_stop_ts: list[pd.Timestamp] = []

    start_idx = df.index.searchsorted(start_ts)

    for i in range(start_idx, len(df)):
        ts = df.index[i]
        if ts >= end_ts:
            break
        if i < skip_until:
            continue

        result = _detect_breakout_signal(df, i, ASSET_CONFIG.get(asset, {}))
        if result is None:
            continue

        price = float(df.iloc[i]["close"])

        if result.get("blocked") == "vol_gate":
            blocked_vol += 1; continue
        if result.get("blocked") == "4h_trend":
            blocked_4h += 1; continue
        if result.get("blocked") == "conditions":
            blocked_cond += 1; continue

        cutoff = ts - pd.Timedelta(hours=_WHIPSAW_WINDOW_H)
        recent_stop_ts = [t for t in recent_stop_ts if t >= cutoff]
        if len(recent_stop_ts) >= _WHIPSAW_MAX_STOPS:
            blocked_whipsaw += 1; continue

        # Simulate trade
        atr          = float(df.iloc[i]["atr"])
        stop_price   = round(price - atr_stop * atr, 2)
        target_price = round(price + atr_target * atr, 2)
        trade        = {"reason": "MAX_HOLD", "pnl_pct": 0.0, "hold_h": max_hold}

        for j in range(i + 1, min(i + max_hold + 1, len(df))):
            low  = float(df.iloc[j]["low"])
            high = float(df.iloc[j]["high"])
            if low <= stop_price:
                gross = stop_price * (1 - FEE_RATE)
                trade = {"reason": "STOP_LOSS", "hold_h": j - i,
                         "pnl_pct": round((gross - price * (1 + FEE_RATE)) / price * 100, 2)}
                break
            if high >= target_price:
                gross = target_price * (1 - FEE_RATE)
                trade = {"reason": "TAKE_PROFIT", "hold_h": j - i,
                         "pnl_pct": round((gross - price * (1 + FEE_RATE)) / price * 100, 2)}
                break

        if trade["reason"] == "MAX_HOLD":
            ep    = float(df.iloc[min(i + max_hold, len(df) - 1)]["close"])
            gross = ep * (1 - FEE_RATE)
            trade["pnl_pct"] = round((gross - price * (1 + FEE_RATE)) / price * 100, 2)

        if trade["reason"] == "STOP_LOSS":
            recent_stop_ts.append(ts)

        signals.append({"ts": ts, "price": price, "trade": trade})
        skip_until = i + trade["hold_h"] + 1

    returns = np.array([s["trade"]["pnl_pct"] for s in signals])
    n       = len(signals)
    wins    = int((returns > 0).sum()) if n else 0

    return {
        "n":        n,
        "wins":     wins,
        "win_rate": wins / n if n else 0.0,
        "avg_pnl":  float(returns.mean()) if n else 0.0,
        "total_pnl": float(returns.sum()) if n else 0.0,
    }


def _load_asset(asset: str) -> pd.DataFrame | None:
    """Download full range once, compute indicators, merge 4h context."""
    sig_df   = _download_and_compute(asset, _GLOBAL_WARMUP, _GLOBAL_END, "1h")
    trend_df = _download_and_compute(asset, _GLOBAL_WARMUP, _GLOBAL_END, "4h")
    if sig_df is None or trend_df is None:
        return None
    df = attach_higher_timeframe_context(sig_df, trend_df)
    if "time" in df.columns:
        df.index = pd.to_datetime(df["time"], utc=True)
    return df.dropna(subset=["rsi", "ema50", "atr"])


# ── Walk-forward runner ───────────────────────────────────────────────────────

def run_walk_forward(assets: list[str]) -> None:
    print("\n" + "=" * 70)
    print("WALK-FORWARD OPTIMIZATION — CryptoOrchestra")
    print(f"Stop candidates: {STOP_CANDIDATES}  |  R:R = {RR_RATIO:.2f} (fixed)")
    print("=" * 70)

    # Download each asset once covering all windows
    print(f"\nDownloading data ({_GLOBAL_WARMUP} → {_GLOBAL_END})...")
    asset_dfs: dict[str, pd.DataFrame | None] = {}
    for asset in assets:
        print(f"  {asset}...", end="", flush=True)
        df = _load_asset(asset)
        asset_dfs[asset] = df
        print(f" {len(df)} candles" if df is not None else " NO DATA")

    # Collect per-window per-asset results
    all_window_results = []

    for win in WINDOWS:
        print(f"\n{'─'*70}")
        print(f"{win['label']}  |  Train: {win['train_start']} → {win['test_start']}"
              f"  ({win['train_regime']})")
        print(f"{'':11}  |  Test:  {win['test_start']} → {win['test_end']}"
              f"  ({win['test_regime']})")
        print(f"{'─'*70}")

        train_start = pd.Timestamp(win["train_start"], tz="UTC")
        test_start  = pd.Timestamp(win["test_start"],  tz="UTC")
        test_end    = pd.Timestamp(win["test_end"],    tz="UTC")

        window_asset_results = {}

        for asset in assets:
            df = asset_dfs.get(asset)
            if df is None:
                print(f"\n  {asset}  NO DATA")
                continue
            print(f"\n  {asset}  ({len(df)} candles)")

            # Test all stop candidates on TRAIN period
            train_scores = []
            for stop in STOP_CANDIDATES:
                target = round(stop * RR_RATIO, 2)
                res    = _run_scan(df, train_start, test_start, asset, stop, target)
                train_scores.append((stop, target, res))

            # Pick best on train (highest avg P&L with at least 3 signals)
            valid = [(s, t, r) for s, t, r in train_scores if r["n"] >= 3]
            if not valid:
                valid = train_scores
            best_stop, best_target, best_train = max(valid, key=lambda x: x[2]["avg_pnl"])

            # Apply winner to TEST (out-of-sample)
            oos = _run_scan(df, test_start, test_end, asset, best_stop, best_target)

            # Also run current system params on OOS for comparison
            from backtesting.signal_scanner import ASSET_PARAMS
            cur_p    = ASSET_PARAMS.get(asset, {"atr_stop": 2.0, "atr_target": 3.5})
            cur_oos  = _run_scan(df, test_start, test_end, asset,
                                 cur_p["atr_stop"], cur_p["atr_target"])

            window_asset_results[asset] = {
                "best_stop":   best_stop,
                "best_target": best_target,
                "train":       best_train,
                "oos":         oos,
                "current_oos": cur_oos,
            }

            # Print train scores
            print(f"    TRAIN ({win['train_regime']}):")
            for stop, target, res in train_scores:
                marker = " ← BEST" if stop == best_stop else ""
                print(f"      stop={stop}x  target={target}x  "
                      f"n={res['n']:2d}  win={res['win_rate']:.0%}  "
                      f"avg={res['avg_pnl']:+.2f}%{marker}")

            # Print OOS result
            generalises = "✓ GENERALISES" if oos["avg_pnl"] >= best_train["avg_pnl"] * 0.5 else "✗ OVERFIT"
            print(f"    OOS   ({win['test_regime']})  [stop={best_stop}x]:")
            print(f"      n={oos['n']:2d}  win={oos['win_rate']:.0%}  "
                  f"avg={oos['avg_pnl']:+.2f}%   {generalises}")
            print(f"    Current params OOS [stop={cur_p['atr_stop']}x]:")
            print(f"      n={cur_oos['n']:2d}  win={cur_oos['win_rate']:.0%}  "
                  f"avg={cur_oos['avg_pnl']:+.2f}%")

        all_window_results.append((win, window_asset_results))

    # ── Final summary ────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("WALK-FORWARD SUMMARY")
    print(f"{'Asset':<10} {'W1 OOS':>10} {'W2 OOS':>10} {'W3 OOS':>10} "
          f"{'Avg OOS':>10} {'Verdict':>14}")
    print("-" * 70)

    for asset in assets:
        oos_pnls = []
        for _, wres in all_window_results:
            if asset in wres:
                oos_pnls.append(wres[asset]["oos"]["avg_pnl"])

        if not oos_pnls:
            continue

        avg_oos = np.mean(oos_pnls)
        verdict = "EDGE" if avg_oos > 0.0 else ("MARGINAL" if avg_oos > -0.5 else "WEAK")
        cols    = [f"{p:+.2f}%" for p in oos_pnls]
        while len(cols) < 3:
            cols.append("  n/a")
        print(f"{asset:<10} {cols[0]:>10} {cols[1]:>10} {cols[2]:>10} "
              f"{avg_oos:>+9.2f}%  {verdict:>14}")

    print("\nInterpretation:")
    print("  EDGE     = OOS avg P&L > 0%      — params generalise, system has edge")
    print("  MARGINAL = OOS avg P&L 0% to -0.5% — borderline, watch live results")
    print("  WEAK     = OOS avg P&L < -0.5%  — params overfit, needs rethink")
    print("=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    asset_arg = next((sys.argv[i + 1] for i, a in enumerate(sys.argv)
                      if a == "--asset" and i + 1 < len(sys.argv)), None)
    assets    = [asset_arg] if asset_arg else ASSETS
    run_walk_forward(assets)


if __name__ == "__main__":
    main()
