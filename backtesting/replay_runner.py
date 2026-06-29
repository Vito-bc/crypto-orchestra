"""
Historical Market Replay — runs the REAL live agents on past market data.

Unlike backtest.py (which simulates simplified logic), this patches get_snapshot()
and get_raw_df() to return historical candles, then runs the actual pipeline:
  - All 7 Claude sub-agents (real API calls, real reasoning)
  - Real orchestrator decision logic
  - Real limit_orders / position_tracker state management

This answers: "Would the system have fired correctly during the Trump rally / bull run?"

Replay periods available:
  trump_rally   : Nov 5 2024 – Jan 20 2025  BTC $68k -> $109k (+60%)
  bull_peak     : Jan 20 – Mar 1 2025        BTC $109k -> $78k (-28%)  bear test
  current_bear  : Mar 1 – Jun 1 2025         BTC $78k -> $60k          current conditions

Usage:
    python backtesting/replay_runner.py trump_rally
    python backtesting/replay_runner.py bull_peak
    python backtesting/replay_runner.py trump_rally --asset BTC-USD
    python backtesting/replay_runner.py trump_rally --step 4   # every 4h instead of 1h
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import yfinance as yf
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    attach_higher_timeframe_context,
    get_signal,
    get_symbol_config,
    evaluate_entry_components,
    prepare_timeframe_df,
)

# ── Replay periods ────────────────────────────────────────────────────────────

PERIODS = {
    "trump_rally": {
        "label":   "Trump Rally — Nov 2024 Bull Run",
        "start":   "2024-11-01",
        "end":     "2025-01-20",
        "context": "BTC rallied from $68k to $109k (+60%). Should fire BUY signals early.",
    },
    "bull_peak": {
        "label":   "Bull Peak & Reversal — Jan-Mar 2025",
        "start":   "2025-01-20",
        "end":     "2025-03-01",
        "context": "BTC peaked at $109k, reversed to $78k. Should fire SELL / no new BUY.",
    },
    "aug_crash": {
        "label":   "August 2024 Flash Crash",
        "start":   "2024-07-20",
        "end":     "2024-09-01",
        "context": "ETH crashed -30% on yen carry trade unwind. Stop losses critical.",
    },
    "current_bear": {
        "label":   "Current Bear / Sideways — Mar-Jun 2025",
        "start":   "2025-03-01",
        "end":     "2025-06-01",
        "context": "BTC $78k -> $60k, low volatility. System should stay mostly in HOLD.",
    },
}

ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]


# ── Data preparation ──────────────────────────────────────────────────────────

def _download_period(asset: str, start: str, end: str, extra_days: int = 90) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download 1h and 4h data for the replay period.
    Downloads extra_days before start so indicators (EMA200 etc.) are warm.
    """
    warmup_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=extra_days)).strftime("%Y-%m-%d")
    print(f"  Downloading {asset} 1h data ({warmup_start} to {end})...")
    df_1h = yf.download(asset, start=warmup_start, end=end, interval="1h", progress=False, auto_adjust=True)
    df_4h = yf.download(asset, start=warmup_start, end=end, interval="4h", progress=False, auto_adjust=True)

    if isinstance(df_1h.columns, pd.MultiIndex):
        df_1h.columns = df_1h.columns.get_level_values(0)
    if isinstance(df_4h.columns, pd.MultiIndex):
        df_4h.columns = df_4h.columns.get_level_values(0)

    df_1h.columns = [c.lower() for c in df_1h.columns]
    df_4h.columns = [c.lower() for c in df_4h.columns]

    df_1h.index = pd.to_datetime(df_1h.index, utc=True)
    df_4h.index = pd.to_datetime(df_4h.index, utc=True)

    return df_1h.dropna(), df_4h.dropna()


def _build_snapshot_at(df_full_1h: pd.DataFrame, df_full_4h: pd.DataFrame,
                        asset: str, ts: pd.Timestamp) -> dict | None:
    """
    Build a get_snapshot()-compatible dict using only data available at timestamp ts.
    Slices the DataFrames to simulate real-time knowledge cutoff.
    """
    df_1h = df_full_1h[df_full_1h.index <= ts].copy()
    df_4h = df_full_4h[df_full_4h.index <= ts].copy()

    if len(df_1h) < 55 or len(df_4h) < 15:
        return None

    # Run through the same indicator pipeline as the live system
    try:
        from backtesting.backtest import prepare_timeframe_df as _ptdf

        # Inject pre-sliced data instead of downloading
        with patch("backtesting.backtest.yf.download") as mock_dl:
            def _side_effect(ticker, **kwargs):
                interval = kwargs.get("interval", "1h")
                return df_1h.copy() if interval == "1h" else df_4h.copy()
            mock_dl.side_effect = _side_effect

            sig_df  = _ptdf(asset, "1h",  90)
            trend_df = _ptdf(asset, "4h", 90)
    except Exception:
        return None

    if sig_df is None or trend_df is None or len(sig_df) < 2:
        return None

    merged = attach_higher_timeframe_context(sig_df, trend_df)
    if len(merged) < 2:
        return None

    import math
    row      = merged.iloc[-1]
    prev_row = merged.iloc[-2]
    config   = get_symbol_config(asset)
    signal   = get_signal(row, prev_row, config)
    comps    = evaluate_entry_components(row, prev_row, config) or {}

    def _f(col):
        try:
            v = float(row[col])
            return None if math.isnan(v) else v
        except Exception:
            return None

    return {
        "symbol":            asset,
        "close":             _f("close"),
        "open":              _f("open"),
        "signal":            signal,
        "rsi_1h":            _f("rsi"),
        "macd_diff_1h":      _f("macd_diff"),
        "bb_pct_1h":         _f("bb_pct"),
        "volume_ratio_1h":   _f("volume_ratio"),
        "atr_1h":            _f("atr"),
        "ema50_1h":          _f("ema50"),
        "ema200_1h":         _f("ema200"),
        "close_4h":          _f("close_4h"),
        "ema50_4h":          _f("ema50_4h"),
        "ema200_4h":         _f("ema200_4h"),
        "trend_4h":          row.get("trend_4h", ""),
        "trend_strength_4h": _f("trend_strength_4h"),
        "trend_ok":          bool(comps.get("trend_ok",  False)),
        "macd_ok":           bool(comps.get("macd_ok",   False)),
        "rsi_ok":            bool(comps.get("rsi_ok",    False)),
        "bb_ok":             bool(comps.get("bb_ok",     False)),
        "volume_ok":         bool(comps.get("volume_ok", False)),
        "buy_ready":         bool(comps.get("buy_ready", False)),
        "adx_1h":            _f("adx") or 0.0,
        "vwap_1h":           _f("vwap"),
        "cvd_24h":           _f("cvd_24h"),
        "cvd_6h_ago":        None,
        "ewma_vol_daily":    _f("ewma_vol"),
    }


# ── Replay engine ─────────────────────────────────────────────────────────────

def run_replay(
    period_key: str,
    assets: list[str] | None = None,
    step_hours: int = 1,
) -> None:
    period   = PERIODS[period_key]
    targets  = assets or ASSETS
    log_dir  = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"replay_{period_key}.jsonl"

    print("\n" + "=" * 68)
    print(f"HISTORICAL REPLAY — {period['label']}")
    print(f"Period:  {period['start']}  ->  {period['end']}")
    print(f"Context: {period['context']}")
    print(f"Assets:  {targets}")
    print(f"Step:    every {step_hours}h")
    print(f"Log:     {log_path}")
    print("=" * 68)
    print("\nNOTE: Real Claude API calls will be made. Cost: ~$1-3 for full replay.")
    print("Press Ctrl+C to stop at any time.\n")

    # Download data for each asset
    asset_data: dict[str, tuple] = {}
    for asset in targets:
        try:
            df_1h, df_4h = _download_period(asset, period["start"], period["end"])
            asset_data[asset] = (df_1h, df_4h)
            print(f"  {asset}: {len(df_1h)} hourly candles ready")
        except Exception as exc:
            print(f"  {asset}: download failed — {exc}")

    if not asset_data:
        print("No data downloaded. Check internet connection.")
        return

    # Build replay timeline
    start_ts = pd.Timestamp(period["start"], tz="UTC")
    end_ts   = pd.Timestamp(period["end"],   tz="UTC")
    step     = timedelta(hours=step_hours)

    # Use the first asset's 1h index as the timeline
    first_df = list(asset_data.values())[0][0]
    timeline = [ts for ts in first_df.index if start_ts <= ts <= end_ts]
    if step_hours > 1:
        timeline = timeline[::step_hours]

    print(f"\nReplay timeline: {len(timeline)} steps from {timeline[0]} to {timeline[-1]}\n")

    # ── Stats tracking ────────────────────────────────────────────────────────
    total_steps   = 0
    total_buys    = 0
    total_holds   = 0
    total_errors  = 0
    asset_signals: dict[str, list] = {a: [] for a in targets}

    # Clear replay state files (positions / orders) so we start clean
    replay_positions = log_dir / "replay_open_positions.json"
    replay_orders    = log_dir / "replay_pending_orders.json"
    replay_positions.write_text("[]", encoding="utf-8")
    replay_orders.write_text("[]", encoding="utf-8")

    log_path.write_text("", encoding="utf-8")  # clear previous replay log

    for step_i, ts in enumerate(timeline, 1):
        print(f"\n[Step {step_i}/{len(timeline)}] {ts.strftime('%Y-%m-%d %H:%M UTC')}")

        for asset in targets:
            if asset not in asset_data:
                continue

            df_1h, df_4h = asset_data[asset]

            # Build the historical snapshot for this timestamp
            snap = _build_snapshot_at(df_1h, df_4h, asset, ts)
            if snap is None:
                print(f"  {asset}: insufficient data at this timestamp")
                continue

            current_price = snap.get("close") or 0.0
            print(f"  {asset}: ${current_price:,.2f}  RSI={snap.get('rsi_1h', 0):.0f}  "
                  f"trend_4h={snap.get('trend_4h', '?')}")

            # Patch get_snapshot and get_raw_df to return historical data
            merged_df = attach_higher_timeframe_context(
                df_1h[df_1h.index <= ts],
                df_4h[df_4h.index <= ts],
            )

            t0 = time.time()
            try:
                with (
                    patch("tools.price_data.get_snapshot",  return_value=snap),
                    patch("tools.price_data.get_raw_df",    return_value=merged_df),
                    # Patch Telegram so no real messages get sent during replay
                    patch("notifications.telegram.send_telegram_message", return_value=True),
                    # Patch position/order state files to use replay-specific files
                    patch("pipeline.position_tracker.POSITIONS_FILE", replay_positions),
                    patch("pipeline.limit_orders.ORDERS_FILE",         replay_orders),
                    patch("pipeline.runner.TRADE_HISTORY",             log_dir / "replay_trade_history.jsonl"),
                ):
                    from pipeline.runner import run_pipeline
                    decision = run_pipeline(asset)

                elapsed = time.time() - t0
                action  = decision.action.value
                conf    = decision.confidence

                asset_signals[asset].append(action)
                if action == "BUY":
                    total_buys += 1
                    print(f"  *** {asset} BUY  conf={conf:.0%}  ({elapsed:.1f}s) ***")
                else:
                    total_holds += 1
                    print(f"  {asset}: {action}  conf={conf:.0%}  ({elapsed:.1f}s)")

                # Log to replay JSONL
                record = {
                    "replay_ts":  ts.isoformat(),
                    "asset":      asset,
                    "price":      current_price,
                    "action":     action,
                    "confidence": conf,
                    "reasoning":  decision.reasoning[:200],
                    "veto":       decision.veto_triggered,
                    "veto_reason": decision.veto_reason,
                }
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record) + "\n")

            except KeyboardInterrupt:
                raise
            except Exception as exc:
                total_errors += 1
                print(f"  {asset}: ERROR — {exc}")

        total_steps += 1

        # Small pause between steps to avoid rate limiting on Claude API
        if step_i < len(timeline):
            time.sleep(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 68)
    print("REPLAY COMPLETE — SUMMARY")
    print("=" * 68)
    print(f"Period:    {period['label']}")
    print(f"Steps:     {total_steps}")
    print(f"BUY signals: {total_buys}   HOLD: {total_holds}   Errors: {total_errors}")
    print()
    for asset in targets:
        sigs = asset_signals[asset]
        if sigs:
            buys  = sigs.count("BUY")
            holds = sigs.count("HOLD") + sigs.count("SELL")
            print(f"  {asset:<12} BUY={buys:>3}  HOLD/SELL={holds:>3}  "
                  f"signal rate={buys/len(sigs):.1%}")
    print(f"\nReplay log: {log_path}")
    print("=" * 68)


# ── Main ──────────────────────────────────────────────────────────────────────

def _print_usage():
    print("Usage:")
    print("  python backtesting/replay_runner.py <period> [--asset ASSET] [--step N]")
    print()
    print("Periods:")
    for k, v in PERIODS.items():
        print(f"  {k:<16} {v['label']}")
    print()
    print("Options:")
    print("  --asset BTC-USD   replay only one asset (faster, cheaper)")
    print("  --step 4          sample every 4h instead of every 1h")


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("--help", "-h"):
        _print_usage()
        sys.exit(0)

    period_key = sys.argv[1]
    if period_key not in PERIODS:
        print(f"Unknown period '{period_key}'. Available: {list(PERIODS.keys())}")
        sys.exit(1)

    asset_arg = next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--asset" and i + 1 < len(sys.argv)), None)
    step_arg  = next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--step"  and i + 1 < len(sys.argv)), "1")

    assets     = [asset_arg] if asset_arg else None
    step_hours = max(1, int(step_arg))

    run_replay(period_key, assets=assets, step_hours=step_hours)


if __name__ == "__main__":
    main()
