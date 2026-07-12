"""
Live Position Tracker — completes the trading loop.

Flow:
  1. A limit order fills → open_position_from_order() creates and persists a Position
  2. Every pipeline run → check_positions() runs the trailing stop logic, then
     closes any positions that hit stop / target / max-hold
  3. close_position() computes net P&L and appends to trade history

Trailing stop logic (percentage-based, no ATR needed in live system):
  BREAK_EVEN_PCT      : once price rises this % above entry, stop → entry price
  TRAIL_ACTIVATION_PCT: once price rises this % above entry, begin trailing
  TRAIL_PCT           : trail stop stays this % below the high-water mark

Fees (Coinbase Advanced base tier):
  Entry: 0.4% maker (limit order)
  Exit:  0.6% taker (market order on stop/target/max-hold)

State : logs/open_positions.json
History: logs/trade_history.jsonl
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

ROOT           = Path(__file__).resolve().parents[1]
POSITIONS_FILE = ROOT / "logs" / "open_positions.json"
TRADE_HISTORY  = ROOT / "logs" / "trade_history.jsonl"

MAKER_FEE_RATE       = 0.004   # 0.4% entry (Coinbase Advanced base tier maker)
TAKER_FEE_RATE       = 0.006   # 0.6% exit  (Coinbase Advanced base tier taker)
PAPER_BALANCE        = int(os.getenv("LIVE_BALANCE_USD", "10000"))  # set LIVE_BALANCE_USD in .env for live trading
DEFAULT_POS_PCT      = 0.05    # 5% of balance if risk agent omits size

# Per-asset max hold — extended to match crypto momentum duration (Liu & Tsyvinski 2021:
# momentum lasts 1-4 weeks). Short 8-12h holds lost to 0.6% fee drag every trade.
MAX_HOLD_HOURS_BY_ASSET = {"ETH-USD": 36, "BTC-USD": 48}  # ETH: 24→36 to allow 3.5x ATR target
MAX_HOLD_HOURS          = 36   # default fallback for SOL/ZEC

# Trailing stop parameters — widened to survive normal 1h volatility before trend develops.
# Previous 0.5% break-even was within hourly noise, stopping out positions before any move.
BREAK_EVEN_PCT       = 0.015   # +1.5% above entry → stop moves to break-even
TRAIL_ACTIVATION_PCT = 0.020   # +2.0% above entry → trailing stop activates
TRAIL_PCT            = 0.015   # trail 1.5% below high-water mark

# Hold extension: instead of force-closing at MAX_HOLD, re-evaluate and optionally extend.
# Research (Liu & Tsyvinski 2021): crypto momentum lasts 1-4 weeks, not 8h.
MAX_EXTENSIONS  = 3   # max extensions per position (total: base + 3×8 = 32-36h)
EXTENSION_HOURS = 8   # each approved extension adds this many hours


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    id:              str
    asset:           str
    entry_price:     float
    stop_price:      float
    target_price:    float
    qty_usd:         float
    entry_time:      str
    order_id:        str
    status:          str        # OPEN | CLOSED
    high_water_mark:          Optional[float] = None   # highest price seen since open
    extensions_used:          int            = 0      # hold extensions granted so far
    extension_trailing_stop:  Optional[float] = None  # ATR-anchored stop added during extensions
    # Populated on close
    exit_price:      Optional[float] = None
    exit_time:       Optional[str]   = None
    exit_reason:     Optional[str]   = None
    pnl_usd:         Optional[float] = None
    pnl_pct:         Optional[float] = None

    @property
    def qty_coins(self) -> float:
        return self.qty_usd / self.entry_price

    def held_hours(self) -> float:
        t0 = datetime.fromisoformat(self.entry_time)
        if t0.tzinfo is None:  # guard against legacy records without UTC offset
            t0 = t0.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t0).total_seconds() / 3600

    def effective_hold_limit(self) -> float:
        """Base max-hold + hours granted via extensions so far."""
        base = MAX_HOLD_HOURS_BY_ASSET.get(self.asset, MAX_HOLD_HOURS)
        return base + self.extensions_used * EXTENSION_HOURS

    def needs_extension_review(self) -> bool:
        """Current hold window elapsed AND extensions still available → re-evaluate."""
        return (
            self.extensions_used < MAX_EXTENSIONS
            and self.held_hours() >= self.effective_hold_limit()
        )

    def is_max_hold(self) -> bool:
        """All extensions exhausted AND time exceeded → force close."""
        return (
            self.extensions_used >= MAX_EXTENSIONS
            and self.held_hours() >= self.effective_hold_limit()
        )

    def check_exit(self, current_price: float) -> Optional[str]:
        if current_price <= self.stop_price:
            return "STOP_LOSS"
        if self.extension_trailing_stop and current_price <= self.extension_trailing_stop:
            return "STOP_LOSS"  # ATR trailing stop hit during extended hold
        if current_price >= self.target_price:
            return "TAKE_PROFIT"
        if self.is_max_hold():
            return "MAX_HOLD"
        return None

    def compute_trailing_stop(self, current_price: float) -> float:
        """
        Return the new stop_price after applying break-even and trailing logic.
        Always >= existing stop_price (stops only move up, never down).
        """
        new_stop = self.stop_price
        hwm      = max(self.high_water_mark or self.entry_price, current_price)

        # Break-even trigger
        if current_price >= self.entry_price * (1 + BREAK_EVEN_PCT):
            new_stop = max(new_stop, self.entry_price)

        # Trailing stop (activates above TRAIL_ACTIVATION_PCT gain)
        if hwm >= self.entry_price * (1 + TRAIL_ACTIVATION_PCT):
            trail = hwm * (1 - TRAIL_PCT)
            new_stop = max(new_stop, trail)

        return round(new_stop, 2)


# ── Persistence ───────────────────────────────────────────────────────────────

def _load_raw() -> list[dict]:
    if not POSITIONS_FILE.exists():
        return []
    try:
        return json.loads(POSITIONS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save_raw(positions: list[dict]) -> None:
    """
    Atomic write: serialize to a temp file in the same directory, then
    os.replace() — a POSIX-atomic rename on the same filesystem.
    If the process crashes mid-write the original file is untouched.
    """
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(positions, indent=2)
    fd, tmp = tempfile.mkstemp(dir=POSITIONS_FILE.parent, prefix=".pos_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp, POSITIONS_FILE)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _append_history(record: dict) -> None:
    TRADE_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with TRADE_HISTORY.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def count_recent_stops(asset: str, hours: int = 48) -> int:
    """Count STOP_LOSS exits for this asset in the last `hours` hours."""
    if not TRADE_HISTORY.exists():
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    count = 0
    try:
        for line in TRADE_HISTORY.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("asset") != asset or rec.get("reason") != "STOP_LOSS":
                continue
            ts_str = rec.get("closed_at_utc") or rec.get("exit_time") or ""
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts.timestamp() >= cutoff:
                    count += 1
            except (ValueError, AttributeError):
                continue
    except Exception:
        pass
    return count


# ── Public API ────────────────────────────────────────────────────────────────

def open_position_from_order(order: "PendingOrder", fill_price: float) -> Position:  # type: ignore[name-defined]
    from backtesting.signal_scanner import ASSET_CONFIG as _SCANNER_CFG
    _cfg       = _SCANNER_CFG.get(order.asset, {})
    _stop_mult = _cfg.get("atr_stop", 2.0)
    _tgt_mult  = _cfg.get("atr_target", 3.5)

    pct     = order.position_size_pct or DEFAULT_POS_PCT
    qty_usd = round(PAPER_BALANCE * pct, 2)

    # Reconstruct ATR from the original limit order, then re-anchor stop/target
    # to the actual fill price. Without this, a gap-down fill (fill < limit)
    # puts the stop above the entry price — an immediate false trigger.
    atr          = (order.limit_price - order.stop_price) / _stop_mult
    stop_price   = round(fill_price - _stop_mult * atr, 2)
    target_price = round(fill_price + _tgt_mult  * atr, 2)

    pos = Position(
        id=order.id,
        asset=order.asset,
        entry_price=round(fill_price, 2),
        stop_price=stop_price,
        target_price=target_price,
        qty_usd=qty_usd,
        entry_time=datetime.now(timezone.utc).isoformat(),
        order_id=order.id,
        status="OPEN",
        high_water_mark=round(fill_price, 2),
    )
    raw = _load_raw()
    raw.append(asdict(pos))
    _save_raw(raw)

    entry_fee = qty_usd * MAKER_FEE_RATE
    print(f"[Position] OPENED #{pos.id} — {pos.asset}  entry ${fill_price:,.2f}  "
          f"size ${qty_usd:,.0f}  fee ${entry_fee:.2f}  "
          f"stop ${pos.stop_price:,.2f}  target ${pos.target_price:,.2f}  "
          f"break-even at ${pos.entry_price*(1+BREAK_EVEN_PCT):,.2f} (+{BREAK_EVEN_PCT:.1%})")
    return pos


def get_open_positions(asset: Optional[str] = None) -> list[Position]:
    raw = _load_raw()
    positions = [Position(**r) for r in raw if r["status"] == "OPEN"]
    if asset:
        positions = [p for p in positions if p.asset == asset]
    return positions


def close_position(pos: Position, exit_price: float, reason: str) -> dict:
    """
    Close a position: place a real market sell on Coinbase (or log it in dry-run),
    compute P&L, and append to trade history.
    """
    from exchange.coinbase_client import place_market_sell

    # Place the exit order on Coinbase (market sell, taker fee)
    place_market_sell(
        product_id=pos.asset,
        base_size_coins=pos.qty_coins,
        client_order_id=f"exit-{pos.id}",
    )

    gross_proceeds = pos.qty_coins * exit_price
    entry_fee      = pos.qty_usd    * MAKER_FEE_RATE
    exit_fee       = gross_proceeds * TAKER_FEE_RATE
    net_pnl        = gross_proceeds - pos.qty_usd - entry_fee - exit_fee
    pnl_pct        = net_pnl / pos.qty_usd * 100
    now            = datetime.now(timezone.utc).isoformat()

    raw = _load_raw()
    for r in raw:
        if r["id"] == pos.id and r["status"] == "OPEN":
            r.update(status="CLOSED", exit_price=round(exit_price, 2),
                     exit_time=now, exit_reason=reason,
                     pnl_usd=round(net_pnl, 2), pnl_pct=round(pnl_pct, 4))
    _save_raw(raw)

    record = {
        "closed_at_utc": now,
        "id":            pos.id,
        "asset":         pos.asset,
        "entry_price":   pos.entry_price,
        "exit_price":    round(exit_price, 2),
        "entry_time":    pos.entry_time,
        "exit_time":     now,
        "reason":        reason,
        "qty_usd":       pos.qty_usd,
        "entry_fee_usd": round(entry_fee, 2),
        "exit_fee_usd":  round(exit_fee, 2),
        "pnl_usd":       round(net_pnl, 2),
        "pnl_pct":       round(pnl_pct, 4),
        "hold_hours":    round(pos.held_hours(), 1),
    }
    _append_history(record)

    outcome = "PROFIT" if net_pnl > 0 else "LOSS"
    print(f"[Position] CLOSED #{pos.id} ({reason}) — {pos.asset}  "
          f"exit ${exit_price:,.2f}  P&L ${net_pnl:+,.2f} ({pnl_pct:+.2f}%)  "
          f"[{outcome}]  held {record['hold_hours']:.1f}h")
    return record


def check_positions(
    asset: str,
    current_price: float,
    on_extension_review: Optional[Callable[["Position"], bool]] = None,
) -> list[dict]:
    """
    1. Update high-water mark and trailing stop for every open position.
    2. At MAX_HOLD: if on_extension_review is provided, call it instead of force-closing.
       The callback receives the Position, may set pos.extension_trailing_stop,
       and returns True (extend) or False (close).
    3. Close any positions that hit their stop / target / exhausted max-hold.
    Returns list of closed trade records.
    """
    raw     = _load_raw()
    changed = False
    to_close: list[tuple[Position, str]] = []

    for r in raw:
        if r["status"] != "OPEN" or r["asset"] != asset:
            continue

        pos = Position(**r)

        # Update high-water mark
        hwm = max(pos.high_water_mark or pos.entry_price, current_price)
        if hwm != pos.high_water_mark:
            r["high_water_mark"] = hwm
            pos.high_water_mark  = hwm
            changed = True

        # Apply trailing stop
        new_stop = pos.compute_trailing_stop(current_price)
        if new_stop != pos.stop_price:
            old_stop = pos.stop_price
            r["stop_price"] = new_stop
            pos.stop_price  = new_stop
            changed = True
            stop_type = "break-even" if new_stop == pos.entry_price else "trailing"
            print(f"[Position] STOP RAISED #{pos.id} ({stop_type})  "
                  f"${old_stop:,.2f} -> ${new_stop:,.2f}  "
                  f"price=${current_price:,.2f}  peak=${hwm:,.2f}")

        reason = pos.check_exit(current_price)
        if reason:
            to_close.append((pos, reason))
        elif pos.needs_extension_review():
            if on_extension_review is not None and on_extension_review(pos):
                # Extension granted — save new extension count + ATR stop
                r["extensions_used"] = pos.extensions_used + 1
                if pos.extension_trailing_stop is not None:
                    r["extension_trailing_stop"] = pos.extension_trailing_stop
                changed = True
                new_limit = MAX_HOLD_HOURS_BY_ASSET.get(asset, MAX_HOLD_HOURS) + r["extensions_used"] * EXTENSION_HOURS
                print(
                    f"[Position] HOLD EXTENDED #{pos.id} ({asset}) — "
                    f"ext #{r['extensions_used']}/{MAX_EXTENSIONS}  "
                    f"new limit {new_limit:.0f}h"
                    + (f"  ext-stop ${pos.extension_trailing_stop:,.2f}"
                       if pos.extension_trailing_stop else "")
                )
            else:
                to_close.append((pos, "MAX_HOLD"))

    if changed:
        _save_raw(raw)

    closed_records = []
    for pos, reason in to_close:
        closed_records.append(close_position(pos, current_price, reason))
    return closed_records


def print_summary() -> None:
    open_pos = get_open_positions()
    print(f"\n{'='*60}")
    print(f"POSITION SUMMARY  ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})")
    print(f"{'='*60}")

    if open_pos:
        print(f"Open positions ({len(open_pos)}):")
        for p in open_pos:
            gain_pct = (p.high_water_mark / p.entry_price - 1) * 100 if p.high_water_mark else 0
            print(f"  #{p.id}  {p.asset:<10} entry ${p.entry_price:,.2f}  "
                  f"stop ${p.stop_price:,.2f}  target ${p.target_price:,.2f}  "
                  f"peak +{gain_pct:.2f}%  held {p.held_hours():.1f}h")
    else:
        print("  No open positions.")

    if TRADE_HISTORY.exists():
        lines  = TRADE_HISTORY.read_text(encoding="utf-8").strip().splitlines()
        recent = [json.loads(l) for l in lines[-10:]]
        if recent:
            print(f"\nLast {len(recent)} closed trades:")
            total_pnl = sum(r["pnl_usd"] for r in recent)
            for r in recent:
                sign = "+" if r["pnl_usd"] >= 0 else ""
                print(f"  #{r['id']}  {r['asset']:<10} {r['reason']:<12} "
                      f"P&L ${sign}{r['pnl_usd']:.2f} ({sign}{r['pnl_pct']:.2f}%)  "
                      f"{r['hold_hours']:.1f}h")
            sign = "+" if total_pnl >= 0 else ""
            print(f"  {'-'*50}")
            print(f"  Net P&L (last {len(recent)}):  ${sign}{total_pnl:.2f}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    print_summary()
