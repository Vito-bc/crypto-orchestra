from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


LOG_PATH = Path("logs") / "paper_signals.jsonl"


def load_entries() -> list[dict]:
    if not LOG_PATH.exists():
        return []

    entries: list[dict] = []
    with LOG_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def main() -> None:
    entries = load_entries()
    if not entries:
        print("No paper signal logs found.")
        print(f"Expected log file: {LOG_PATH}")
        return

    signal_counts = Counter(entry.get("signal", "UNKNOWN") for entry in entries)
    blocked_counts = Counter()
    for entry in entries:
        if entry.get("blocked_by_macd"):
            blocked_counts["macd"] += 1
        if entry.get("blocked_by_rsi"):
            blocked_counts["rsi"] += 1
        if entry.get("blocked_by_bb"):
            blocked_counts["bb"] += 1
        if entry.get("blocked_by_volume"):
            blocked_counts["volume"] += 1

    last = entries[-1]
    buy_ready_entries = [entry for entry in entries if entry.get("buy_ready")]

    print("\nPHASE B PAPER LOG REVIEW")
    print("=" * 60)
    print(f"Log File:        {LOG_PATH}")
    print(f"Snapshots:       {len(entries)}")
    print(f"BUY Signals:     {signal_counts.get('BUY', 0)}")
    print(f"SELL Signals:    {signal_counts.get('SELL', 0)}")
    print(f"HOLD Signals:    {signal_counts.get('HOLD', 0)}")
    print("-" * 60)
    print(f"Blocked MACD:    {blocked_counts.get('macd', 0)}")
    print(f"Blocked RSI:     {blocked_counts.get('rsi', 0)}")
    print(f"Blocked BB:      {blocked_counts.get('bb', 0)}")
    print(f"Blocked Volume:  {blocked_counts.get('volume', 0)}")
    print("-" * 60)
    print("Latest Snapshot:")
    print(f"  Logged At:     {last.get('logged_at_utc')}")
    print(f"  Candle Time:   {last.get('candle_time')}")
    print(f"  Signal:        {last.get('signal')}")
    print(f"  Buy Ready:     {last.get('buy_ready')}")
    print(f"  Close:         {last.get('close'):.2f}")
    print(f"  RSI 1h:        {last.get('rsi_1h'):.2f}")
    print(f"  MACD Diff 1h:  {last.get('macd_diff_1h'):.6f}")
    print(f"  Volume Ratio:  {last.get('volume_ratio_1h'):.3f}")
    if buy_ready_entries:
        recent_buy = buy_ready_entries[-1]
        print("-" * 60)
        print("Most Recent Buy-Ready Snapshot:")
        print(f"  Logged At:     {recent_buy.get('logged_at_utc')}")
        print(f"  Candle Time:   {recent_buy.get('candle_time')}")
        print(f"  Signal:        {recent_buy.get('signal')}")
        print(f"  Close:         {recent_buy.get('close'):.2f}")
    else:
        print("-" * 60)
        print("Most Recent Buy-Ready Snapshot:")
        print("  None yet")


if __name__ == "__main__":
    main()
