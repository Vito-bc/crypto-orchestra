"""
Daily Telegram summary — sends a P&L snapshot to Telegram once per day.

Reads the same logs as dashboard.py but formats them as a short Telegram message.
Scheduled via Windows Task Scheduler (register_daily_summary_task.ps1).

Usage:
    python pipeline/daily_summary.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notifications.telegram import send_telegram_message

TRADE_HISTORY  = ROOT / "logs" / "trade_history.jsonl"
POSITIONS_FILE = ROOT / "logs" / "open_positions.json"
DECISIONS_LOG  = ROOT / "logs" / "agent_decisions.jsonl"


def _load_trades() -> list[dict]:
    if not TRADE_HISTORY.exists():
        return []
    return [json.loads(l) for l in TRADE_HISTORY.read_text(encoding="utf-8").strip().splitlines() if l.strip()]


def _load_open() -> list[dict]:
    if not POSITIONS_FILE.exists():
        return []
    try:
        return [r for r in json.loads(POSITIONS_FILE.read_text(encoding="utf-8")) if r.get("status") == "OPEN"]
    except Exception:
        return []


def _load_decisions() -> list[dict]:
    if not DECISIONS_LOG.exists():
        return []
    results = []
    for line in DECISIONS_LOG.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            results.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # skip malformed lines
    return results


def build_summary() -> str:
    trades    = _load_trades()
    open_pos  = _load_open()
    decisions = [d for d in _load_decisions() if "action" in d]
    now       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "Crypto Orchestra — Daily Summary",
        f"Date: {now}",
        "",
    ]

    # P&L
    if trades:
        wins      = [t for t in trades if t["pnl_usd"] > 0]
        total_pnl = sum(t["pnl_usd"] for t in trades)
        win_rate  = len(wins) / len(trades) * 100
        equity    = 10_000 + total_pnl
        lines += [
            f"Closed trades: {len(trades)}  |  Win rate: {win_rate:.0f}%",
            f"Total P&L:     {'+'if total_pnl>=0 else ''}{total_pnl:.2f} USD",
            f"Paper equity:  ${equity:,.2f}",
        ]
    else:
        lines.append("Closed trades: 0  (no exits yet)")

    # Open positions
    lines.append("")
    if open_pos:
        lines.append(f"Open positions: {len(open_pos)}")
        for p in open_pos:
            entry = p["entry_price"]
            stop  = p["stop_price"]
            tgt   = p["target_price"]
            held  = (datetime.now(timezone.utc) - datetime.fromisoformat(p["entry_time"])).total_seconds() / 3600
            lines.append(
                f"  {p['asset']}  entry ${entry:,.2f}  "
                f"stop ${stop:,.2f}  target ${tgt:,.2f}  "
                f"held {held:.1f}h"
            )
    else:
        lines.append("Open positions: 0")

    # Signal activity
    if decisions:
        from collections import Counter
        actions = Counter(d.get("action") for d in decisions)
        lines += [
            "",
            f"Decisions (all-time): {len(decisions)}",
            f"  BUY={actions['BUY']}  SELL={actions['SELL']}  HOLD={actions['HOLD']}",
        ]

    return "\n".join(lines)


def main() -> None:
    msg  = build_summary()
    sent = send_telegram_message(msg)
    print(msg)
    print(f"\nTelegram: {'sent' if sent else 'skipped (no token?)'}")


if __name__ == "__main__":
    # Redirect stdout+stderr to log when running under pythonw.exe (Task Scheduler)
    import os as _os
    import io as _io
    import traceback as _tb
    _log_path = ROOT / "logs" / "daily_summary.log"
    _log_path.parent.mkdir(exist_ok=True)
    _is_pythonw = _os.path.basename(sys.executable).lower() == "pythonw.exe"
    if _is_pythonw:
        _lf = open(_log_path, "a", encoding="utf-8", buffering=1)
        sys.stdout = _io.TextIOWrapper(_lf.buffer, encoding="utf-8", line_buffering=True)
        sys.stderr = sys.stdout
    try:
        main()
    except Exception:
        _tb.print_exc()
        sys.exit(1)
