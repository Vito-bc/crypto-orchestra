from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    attach_higher_timeframe_context,
    evaluate_entry_components,
    get_signal,
    get_symbol_config,
    prepare_timeframe_df,
)


DEFAULT_SYMBOL = "ETH-USD"
DEFAULT_SIGNAL_TIMEFRAME = "1h"
DEFAULT_LOOKBACK_DAYS = 90

LOG_DIR = Path("logs")
JSONL_LOG = LOG_DIR / "paper_signals.jsonl"
CSV_LOG = LOG_DIR / "paper_signals.csv"


def build_signal_snapshot(symbol: str = DEFAULT_SYMBOL, days: int = DEFAULT_LOOKBACK_DAYS) -> dict | None:
    signal_df = prepare_timeframe_df(symbol, DEFAULT_SIGNAL_TIMEFRAME, days)
    trend_df = prepare_timeframe_df(symbol, "4h", days)
    if signal_df is None or trend_df is None:
        return None

    df = attach_higher_timeframe_context(signal_df, trend_df)
    if len(df) < 2:
        return None

    row = df.iloc[-1]
    prev_row = df.iloc[-2]
    config = get_symbol_config(symbol)
    signal = get_signal(row, prev_row, config)
    entry_components = evaluate_entry_components(row, prev_row, config) or {}

    snapshot = {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "timeframe": DEFAULT_SIGNAL_TIMEFRAME,
        "lookback_days": days,
        "candle_time": str(row["time"]),
        "close": float(row["close"]),
        "signal": signal,
        "trend_ok": bool(entry_components.get("trend_ok", False)),
        "macd_ok": bool(entry_components.get("macd_ok", False)),
        "macd_cross_ok": bool(entry_components.get("macd_cross_ok", False)),
        "rsi_ok": bool(entry_components.get("rsi_ok", False)),
        "bb_ok": bool(entry_components.get("bb_ok", False)),
        "signal_count_ok": bool(entry_components.get("signal_count_ok", False)),
        "volume_ok": bool(entry_components.get("volume_ok", False)),
        "buy_ready": bool(entry_components.get("buy_ready", False)),
        "blocked_by_macd": bool(entry_components.get("blocked_by_macd", False)),
        "blocked_by_rsi": bool(entry_components.get("blocked_by_rsi", False)),
        "blocked_by_bb": bool(entry_components.get("blocked_by_bb", False)),
        "blocked_by_volume": bool(entry_components.get("blocked_by_volume", False)),
        "rsi_1h": float(row["rsi"]),
        "macd_diff_1h": float(row["macd_diff"]),
        "bb_pct_1h": float(row["bb_pct"]),
        "volume_ratio_1h": float(row["volume_ratio"]),
        "atr_1h": float(row["atr"]),
        "ema50_1h": float(row["ema50"]),
        "ema200_1h": float(row["ema200"]),
        "close_4h": float(row["close_4h"]) if "close_4h" in row else None,
        "ema50_4h": float(row["ema50_4h"]) if "ema50_4h" in row else None,
        "ema200_4h": float(row["ema200_4h"]) if "ema200_4h" in row else None,
        "trend_4h": row.get("trend_4h", ""),
        "trend_strength_4h": float(row["trend_strength_4h"]) if "trend_strength_4h" in row else None,
    }
    return snapshot


def append_logs(snapshot: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with JSONL_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot) + "\n")

    write_header = not CSV_LOG.exists()
    with CSV_LOG.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(snapshot.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(snapshot)


def print_snapshot(snapshot: dict) -> None:
    print("\nPHASE B PAPER SIGNAL")
    print("=" * 60)
    print(f"Symbol:          {snapshot['symbol']}")
    print(f"Candle Time:     {snapshot['candle_time']}")
    print(f"Signal:          {snapshot['signal']}")
    print(f"Close:           {snapshot['close']:.2f}")
    print(f"Trend OK:        {snapshot['trend_ok']}")
    print(f"MACD Cross OK:   {snapshot['macd_cross_ok']}")
    print(f"RSI OK:          {snapshot['rsi_ok']}")
    print(f"BB OK:           {snapshot['bb_ok']}")
    print(f"Volume OK:       {snapshot['volume_ok']}")
    print(f"Buy Ready:       {snapshot['buy_ready']}")
    print(f"Blocked by MACD: {snapshot['blocked_by_macd']}")
    print(f"Blocked by BB:   {snapshot['blocked_by_bb']}")
    print(f"Blocked by Volume: {snapshot['blocked_by_volume']}")
    print("-" * 60)
    print(f"RSI 1h:          {snapshot['rsi_1h']:.2f}")
    print(f"MACD Diff 1h:    {snapshot['macd_diff_1h']:.6f}")
    print(f"BB % 1h:         {snapshot['bb_pct_1h']:.3f}")
    print(f"Volume Ratio 1h: {snapshot['volume_ratio_1h']:.3f}")
    print(f"Trend 4h:        {snapshot['trend_4h']}")
    print(f"Trend Strength:  {snapshot['trend_strength_4h']:.6f}")
    print("-" * 60)
    print(f"JSONL Log:       {JSONL_LOG}")
    print(f"CSV Log:         {CSV_LOG}")


def main() -> None:
    snapshot = build_signal_snapshot()
    if snapshot is None:
        print("No signal snapshot could be built.")
        return

    append_logs(snapshot)
    print_snapshot(snapshot)


if __name__ == "__main__":
    main()
