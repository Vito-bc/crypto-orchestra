from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


LOG_DIR = Path("logs")
SIGNALS_LOG = LOG_DIR / "paper_signals.jsonl"
EVENTS_LOG = LOG_DIR / "paper_position_events.jsonl"
POSITION_STATE = LOG_DIR / "paper_position_eth.json"
HEALTH_LOG = LOG_DIR / "paper_runner_health.jsonl"


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


def build_signal_summary_lines(entries: list[dict]) -> list[str]:
    signal_counts = Counter(entry.get("signal", "UNKNOWN") for entry in entries)
    buy_ready_count = sum(1 for entry in entries if entry.get("buy_ready"))

    lines = [
        "Signal Summary:",
        f"  Snapshots:     {len(entries)}",
        f"  BUY:           {signal_counts.get('BUY', 0)}",
        f"  SELL:          {signal_counts.get('SELL', 0)}",
        f"  HOLD:          {signal_counts.get('HOLD', 0)}",
        f"  Buy Ready:     {buy_ready_count}",
    ]

    if entries:
        last = entries[-1]
        lines.append(f"  Latest Signal: {last.get('signal')} @ {last.get('candle_time')}")
        lines.append(f"  Latest Close:  {last.get('close', 0.0):.2f}")
        lines.append(f"  Regime Mode:   {last.get('regime_mode', 'UNKNOWN')}")
        lines.append(f"  Regime Reason: {last.get('regime_reason', 'N/A')}")
    return lines


def build_event_summary_lines(events: list[dict]) -> list[str]:
    open_events = [event for event in events if event.get("event") == "OPEN_LONG"]
    close_events = [event for event in events if event.get("event") == "CLOSE_LONG"]
    pnl_values = [float(event.get("pnl_pct", 0.0)) for event in close_events]
    hold_values = [float(event.get("hold_hours", 0.0)) for event in close_events]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value <= 0]
    reason_counts = Counter(event.get("reason", "UNKNOWN") for event in close_events)

    lines = [
        "Paper Trade Summary:",
        f"  Opens:         {len(open_events)}",
        f"  Closes:        {len(close_events)}",
        f"  Wins:          {len(wins)}",
        f"  Losses:        {len(losses)}",
        f"  Win Rate:      {(len(wins) / len(close_events) * 100) if close_events else 0.0:.1f}%",
        f"  Avg PnL %:     {avg(pnl_values):.4f}",
        f"  Avg Hold Hrs:  {avg(hold_values):.2f}",
        f"  Cum PnL %:     {sum(pnl_values):.4f}",
    ]

    if reason_counts:
        lines.append("  Close Reasons:")
        for reason, count in sorted(reason_counts.items()):
            lines.append(f"    {reason}: {count}")

    if close_events:
        last = close_events[-1]
        lines.append(f"  Last Close:    {last.get('reason')} @ {last.get('candle_time')}")
    return lines


def build_position_state_lines(state: dict | None) -> list[str]:
    if not state:
        return ["Current Position:", "  No position state file yet."]

    return [
        "Current Position:",
        f"  Status:        {state.get('status')}",
        f"  Last Action:   {state.get('last_action')}",
        f"  Entry Price:   {state.get('entry_price')}",
        f"  Stop Price:    {state.get('stop_price')}",
        f"  Target Price:  {state.get('target_price')}",
        f"  Updated UTC:   {state.get('last_updated_utc')}",
    ]


def build_health_lines(entries: list[dict]) -> list[str]:
    if not entries:
        return ["Runner Health:", "  No runner health logs yet."]

    status_counts = Counter(entry.get("status", "UNKNOWN") for entry in entries)
    last = entries[-1]
    return [
        "Runner Health:",
        f"  Success:       {status_counts.get('SUCCESS', 0)}",
        f"  No Snapshot:   {status_counts.get('NO_SNAPSHOT', 0)}",
        f"  Errors:        {status_counts.get('ERROR', 0)}",
        f"  Last Status:   {last.get('status')}",
        f"  Last Logged:   {last.get('logged_at_utc')}",
    ]


def build_summary_text() -> str:
    signal_entries = load_jsonl(SIGNALS_LOG)
    paper_events = load_jsonl(EVENTS_LOG)
    state = load_position_state()
    health_entries = load_jsonl(HEALTH_LOG)

    sections = [
        "PHASE B DAILY PAPER SUMMARY",
        "=" * 60,
        f"Signals Log:     {SIGNALS_LOG}",
        f"Events Log:      {EVENTS_LOG}",
        f"State File:      {POSITION_STATE}",
        f"Health Log:      {HEALTH_LOG}",
        "-" * 60,
        *build_signal_summary_lines(signal_entries),
        "-" * 60,
        *build_event_summary_lines(paper_events),
        "-" * 60,
        *build_position_state_lines(state),
        "-" * 60,
        *build_health_lines(health_entries),
    ]
    return "\n".join(sections)


def main() -> None:
    print()
    print(build_summary_text())


if __name__ == "__main__":
    main()
