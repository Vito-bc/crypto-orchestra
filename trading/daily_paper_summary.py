from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


LOG_DIR = Path("logs")
SIGNALS_LOG = LOG_DIR / "paper_signals.jsonl"
EVENTS_LOG = LOG_DIR / "paper_position_events.jsonl"
POSITION_STATE = LOG_DIR / "paper_position_eth.json"


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []

    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_position_state() -> dict | None:
    if not POSITION_STATE.exists():
        return None

    with POSITION_STATE.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def print_signal_summary(entries: list[dict]) -> None:
    signal_counts = Counter(entry.get("signal", "UNKNOWN") for entry in entries)
    buy_ready_count = sum(1 for entry in entries if entry.get("buy_ready"))

    print("Signal Summary:")
    print(f"  Snapshots:     {len(entries)}")
    print(f"  BUY:           {signal_counts.get('BUY', 0)}")
    print(f"  SELL:          {signal_counts.get('SELL', 0)}")
    print(f"  HOLD:          {signal_counts.get('HOLD', 0)}")
    print(f"  Buy Ready:     {buy_ready_count}")

    if entries:
        last = entries[-1]
        print(f"  Latest Signal: {last.get('signal')} @ {last.get('candle_time')}")
        print(f"  Latest Close:  {last.get('close', 0.0):.2f}")


def print_event_summary(events: list[dict]) -> None:
    open_events = [event for event in events if event.get("event") == "OPEN_LONG"]
    close_events = [event for event in events if event.get("event") == "CLOSE_LONG"]
    pnl_values = [float(event.get("pnl_pct", 0.0)) for event in close_events]
    hold_values = [float(event.get("hold_hours", 0.0)) for event in close_events]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value <= 0]
    reason_counts = Counter(event.get("reason", "UNKNOWN") for event in close_events)

    print("Paper Trade Summary:")
    print(f"  Opens:         {len(open_events)}")
    print(f"  Closes:        {len(close_events)}")
    print(f"  Wins:          {len(wins)}")
    print(f"  Losses:        {len(losses)}")
    print(f"  Win Rate:      {(len(wins) / len(close_events) * 100) if close_events else 0.0:.1f}%")
    print(f"  Avg PnL %:     {avg(pnl_values):.4f}")
    print(f"  Avg Hold Hrs:  {avg(hold_values):.2f}")
    print(f"  Cum PnL %:     {sum(pnl_values):.4f}")

    if reason_counts:
        print("  Close Reasons:")
        for reason, count in sorted(reason_counts.items()):
            print(f"    {reason}: {count}")

    if close_events:
        last = close_events[-1]
        print(f"  Last Close:    {last.get('reason')} @ {last.get('candle_time')}")


def print_position_state(state: dict | None) -> None:
    print("Current Position:")
    if not state:
        print("  No position state file yet.")
        return

    print(f"  Status:        {state.get('status')}")
    print(f"  Last Action:   {state.get('last_action')}")
    print(f"  Entry Price:   {state.get('entry_price')}")
    print(f"  Stop Price:    {state.get('stop_price')}")
    print(f"  Target Price:  {state.get('target_price')}")
    print(f"  Updated UTC:   {state.get('last_updated_utc')}")


def main() -> None:
    signal_entries = load_jsonl(SIGNALS_LOG)
    paper_events = load_jsonl(EVENTS_LOG)
    state = load_position_state()

    print("\nPHASE B DAILY PAPER SUMMARY")
    print("=" * 60)
    print(f"Signals Log:     {SIGNALS_LOG}")
    print(f"Events Log:      {EVENTS_LOG}")
    print(f"State File:      {POSITION_STATE}")
    print("-" * 60)
    print_signal_summary(signal_entries)
    print("-" * 60)
    print_event_summary(paper_events)
    print("-" * 60)
    print_position_state(state)


if __name__ == "__main__":
    main()
