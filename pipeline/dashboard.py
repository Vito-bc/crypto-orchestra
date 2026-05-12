"""
P&L Dashboard — live view of system performance.

Reads:
  logs/trade_history.jsonl  — closed trades
  logs/open_positions.json  — open positions (unrealized P&L via live price)
  logs/agent_decisions.jsonl — all orchestrator decisions (signal frequency)

Usage:
    python pipeline/dashboard.py          # full dashboard
    python pipeline/dashboard.py --short  # summary only
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TRADE_HISTORY   = ROOT / "logs" / "trade_history.jsonl"
POSITIONS_FILE  = ROOT / "logs" / "open_positions.json"
DECISIONS_LOG   = ROOT / "logs" / "agent_decisions.jsonl"

SHORT_MODE = "--short" in sys.argv

W = 65  # display width


# ── Helpers ───────────────────────────────────────────────────────────────────

def _div(char: str = "-") -> None:
    print(char * W)


def _header(title: str) -> None:
    print(f"\n{title}")
    _div()


def _load_trades() -> list[dict]:
    if not TRADE_HISTORY.exists():
        return []
    lines = TRADE_HISTORY.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def _load_open() -> list[dict]:
    if not POSITIONS_FILE.exists():
        return []
    try:
        data = json.loads(POSITIONS_FILE.read_text(encoding="utf-8"))
        return [r for r in data if r.get("status") == "OPEN"]
    except (json.JSONDecodeError, OSError):
        return []


def _load_decisions() -> list[dict]:
    if not DECISIONS_LOG.exists():
        return []
    lines = DECISIONS_LOG.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def _pct(n: float, d: float) -> str:
    return f"{n/d*100:.1f}%" if d else "n/a"


def _sign(v: float) -> str:
    return f"+${v:.2f}" if v >= 0 else f"-${abs(v):.2f}"


def _fmt_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso).astimezone(timezone.utc)
        return dt.strftime("%m-%d %H:%M")
    except Exception:
        return iso[:13]


# ── ASCII equity curve ────────────────────────────────────────────────────────

def _equity_curve(trades: list[dict], width: int = W - 2) -> None:
    if not trades:
        print("  (no closed trades yet)")
        return

    PAPER_START = 10_000.0
    equity = [PAPER_START]
    for t in sorted(trades, key=lambda x: x.get("exit_time", "")):
        equity.append(equity[-1] + t["pnl_usd"])

    lo, hi = min(equity), max(equity)
    span   = hi - lo or 1.0
    rows   = 6
    cols   = min(len(equity), width)
    step   = max(1, len(equity) // cols)
    sample = equity[::step][-cols:]

    chart = [[" "] * len(sample) for _ in range(rows)]
    for col, val in enumerate(sample):
        row = rows - 1 - int((val - lo) / span * (rows - 1))
        row = max(0, min(rows - 1, row))
        chart[row][col] = "*"

    print(f"  ${hi:,.0f} |")
    for i, row in enumerate(chart):
        label = f"  ${lo + (rows-1-i) * span/(rows-1):,.0f} |" if i in (0, rows//2, rows-1) else " " * 12 + "|"
        print(label + "".join(row))
    print(" " * 12 + "+" + "-" * len(sample))
    print(f"  Start: ${PAPER_START:,.0f}   Current: ${equity[-1]:,.2f}   "
          f"Net: {_sign(equity[-1] - PAPER_START)}")


# ── Sections ──────────────────────────────────────────────────────────────────

def section_summary(trades: list[dict]) -> None:
    _header("SUMMARY")
    if not trades:
        print("  No closed trades yet — system is running, waiting for signals.")
        return

    wins   = [t for t in trades if t["pnl_usd"] > 0]
    losses = [t for t in trades if t["pnl_usd"] <= 0]
    total_pnl  = sum(t["pnl_usd"]      for t in trades)
    total_fees = sum(t.get("entry_fee_usd", 0) + t.get("exit_fee_usd", 0) for t in trades)
    avg_win    = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
    avg_loss   = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    pf         = abs(avg_win / avg_loss) if avg_loss and avg_win else float("inf") if wins else 0
    avg_hold   = sum(t.get("hold_hours", 0) for t in trades) / len(trades)

    paper_end  = 10_000 + total_pnl
    total_ret  = total_pnl / 10_000 * 100

    print(f"  Trades:        {len(trades):>4}    Wins: {len(wins)}  Losses: {len(losses)}")
    print(f"  Win rate:      {_pct(len(wins), len(trades)):>6}   Profit factor: {pf:.2f}")
    print(f"  Total P&L:   {_sign(total_pnl):>8}   Return: {total_ret:+.3f}%")
    print(f"  Total fees:  {_sign(-total_fees):>8}   Avg hold: {avg_hold:.1f}h")
    print(f"  Paper equity: ${paper_end:,.2f}  (started $10,000.00)")

    # Simple max drawdown from equity curve
    equity = [10_000.0]
    for t in sorted(trades, key=lambda x: x.get("exit_time", "")):
        equity.append(equity[-1] + t["pnl_usd"])
    peak = 10_000.0; max_dd = 0.0
    for v in equity:
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd: max_dd = dd
    print(f"  Max drawdown:  {max_dd:.2f}%")


def section_equity_curve(trades: list[dict]) -> None:
    _header("EQUITY CURVE  (paper $10,000)")
    _equity_curve(trades)


def section_by_asset(trades: list[dict]) -> None:
    _header("BY ASSET")
    if not trades:
        print("  (no data)")
        return
    assets = sorted(set(t["asset"] for t in trades))
    print(f"  {'Asset':<10} {'Trades':>6} {'Win%':>6} {'P&L':>10} {'PF':>6} {'Avg hold':>9}")
    _div()
    for asset in assets:
        sub  = [t for t in trades if t["asset"] == asset]
        wins = [t for t in sub if t["pnl_usd"] > 0]
        losses = [t for t in sub if t["pnl_usd"] <= 0]
        pnl  = sum(t["pnl_usd"] for t in sub)
        aw   = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
        al   = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
        pf   = abs(aw / al) if al and aw else float("inf") if wins else 0
        hold = sum(t.get("hold_hours", 0) for t in sub) / len(sub)
        print(f"  {asset:<10} {len(sub):>6} {_pct(len(wins), len(sub)):>6} "
              f"{_sign(pnl):>10} {pf:>6.2f} {hold:>7.1f}h")


def section_exit_reasons(trades: list[dict]) -> None:
    _header("EXIT REASONS")
    if not trades:
        print("  (no data)")
        return
    from collections import Counter
    reasons = Counter(t.get("reason", "UNKNOWN") for t in trades)
    for reason, count in reasons.most_common():
        bar = "#" * int(count / len(trades) * 30)
        print(f"  {reason:<16} {count:>3} ({count/len(trades)*100:.0f}%)  {bar}")


def section_signal_activity(decisions: list[dict]) -> None:
    _header("SIGNAL ACTIVITY  (all orchestrator decisions)")
    if not decisions:
        print("  (no decisions logged yet)")
        return
    from collections import Counter
    actions = Counter(d.get("action", "?") for d in decisions)
    assets  = Counter(d.get("asset",  "?") for d in decisions)
    total   = len(decisions)
    print(f"  Total decisions: {total}")
    for action, count in [("BUY", actions["BUY"]), ("SELL", actions["SELL"]), ("HOLD", actions["HOLD"])]:
        bar = "#" * int(count / total * 30) if total else ""
        print(f"  {action:<6} {count:>4} ({_pct(count, total):>5})  {bar}")
    print(f"\n  By asset:")
    for asset, count in assets.most_common():
        print(f"    {asset:<10} {count} decisions")

    # Recent signal dates
    dated = [d for d in decisions if d.get("logged_at_utc")]
    if dated:
        dated.sort(key=lambda x: x["logged_at_utc"])
        first = _fmt_dt(dated[0]["logged_at_utc"])
        last  = _fmt_dt(dated[-1]["logged_at_utc"])
        print(f"\n  Active since: {first} UTC   Last run: {last} UTC")


def section_open_positions(open_pos: list[dict]) -> None:
    _header("OPEN POSITIONS")
    if not open_pos:
        print("  No open positions.")
        return

    # Try to get current prices
    try:
        from tools.price_data import get_snapshot
        prices = {}
        for p in open_pos:
            asset = p.get("asset", "")
            if asset and asset not in prices:
                snap = get_snapshot(asset)
                if snap:
                    prices[asset] = snap["close"]
    except Exception:
        prices = {}

    for p in open_pos:
        asset       = p.get("asset", "?")
        entry       = p.get("entry_price", 0)
        stop        = p.get("stop_price", 0)
        target      = p.get("target_price", 0)
        qty_usd     = p.get("qty_usd", 0)
        hwm         = p.get("high_water_mark") or entry
        entry_time  = p.get("entry_time", "")
        pos_id      = p.get("id", "?")[:8]

        # Held hours
        try:
            t0 = datetime.fromisoformat(entry_time)
            held_h = (datetime.now(timezone.utc) - t0).total_seconds() / 3600
        except Exception:
            held_h = 0

        current = prices.get(asset, 0)
        if current and entry:
            unrealized_pct = (current - entry) / entry * 100
            unrealized_usd = qty_usd * (current - entry) / entry
            price_str = f"  current ${current:,.2f}  unrealized {unrealized_pct:+.2f}% ({_sign(unrealized_usd)})"
        else:
            price_str = "  (price unavailable)"

        print(f"  #{pos_id}  {asset}  entry ${entry:,.2f}  stop ${stop:,.2f}  target ${target:,.2f}")
        print(f"           peak ${hwm:,.2f}  held {held_h:.1f}h{price_str}")


def section_recent_trades(trades: list[dict], n: int = 15) -> None:
    _header(f"LAST {min(n, len(trades))} TRADES")
    if not trades:
        print("  (no closed trades yet)")
        return

    recent = sorted(trades, key=lambda x: x.get("exit_time", ""), reverse=True)[:n]
    print(f"  {'Date':<12} {'Asset':<10} {'Entry':>9} {'Exit':>9} {'P&L':>9} {'%':>7} {'Reason':<14} {'Hold'}")
    _div()
    for t in recent:
        pnl   = t["pnl_usd"]
        sign  = "+" if pnl >= 0 else ""
        print(f"  {_fmt_dt(t.get('exit_time','?')):<12} "
              f"{t.get('asset','?'):<10} "
              f"${t.get('entry_price',0):>8,.0f} "
              f"${t.get('exit_price', 0):>8,.0f} "
              f"{sign}${abs(pnl):>7.2f} "
              f"{t.get('pnl_pct',0)*100:>+6.1f}% "
              f"{t.get('reason','?'):<14} "
              f"{t.get('hold_hours',0):.1f}h")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    trades    = _load_trades()
    open_pos  = _load_open()
    decisions = _load_decisions()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * W)
    print("CRYPTO ORCHESTRA — P&L DASHBOARD".center(W))
    print(f"Generated: {now}".center(W))
    print("=" * W)

    section_summary(trades)

    if not SHORT_MODE:
        section_equity_curve(trades)
        section_by_asset(trades)
        section_exit_reasons(trades)

    section_signal_activity(decisions)
    section_open_positions(open_pos)

    if not SHORT_MODE:
        section_recent_trades(trades)

    print("\n" + "=" * W)
    print(f"  Logs: {TRADE_HISTORY.parent}")
    print(f"  Run 'python pipeline/dashboard.py --short' for summary only")
    print("=" * W + "\n")


if __name__ == "__main__":
    main()
