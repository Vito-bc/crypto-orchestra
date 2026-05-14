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

Fees:
  Entry: 0.2% maker (limit order)
  Exit:  0.4% taker (market order on stop/target/max-hold)

State : logs/open_positions.json
History: logs/trade_history.jsonl
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT           = Path(__file__).resolve().parents[1]
POSITIONS_FILE = ROOT / "logs" / "open_positions.json"
TRADE_HISTORY  = ROOT / "logs" / "trade_history.jsonl"

MAKER_FEE_RATE       = 0.002   # 0.2% entry (limit/maker)
TAKER_FEE_RATE       = 0.004   # 0.4% exit  (market/taker)
PAPER_BALANCE        = 10_000  # USD paper capital
DEFAULT_POS_PCT      = 0.05    # 5% of balance if risk agent omits size

# Per-asset max hold (backtests: 88% of wins are MAX_HOLD; 48h+ degrades results)
MAX_HOLD_HOURS_BY_ASSET = {"ETH-USD": 8, "BTC-USD": 12}
MAX_HOLD_HOURS          = 12   # default fallback

# Trailing stop parameters (tuned via backtesting/trailing_stop_tune.py)
BREAK_EVEN_PCT       = 0.005   # +0.5% above entry → stop moves to break-even
TRAIL_ACTIVATION_PCT = 0.005   # +0.5% above entry → trailing stop activates
TRAIL_PCT            = 0.008   # trail 0.8% below high-water mark


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
    high_water_mark: Optional[float] = None   # highest price seen since open
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
        return (datetime.now(timezone.utc) - t0).total_seconds() / 3600

    def is_max_hold(self) -> bool:
        limit = MAX_HOLD_HOURS_BY_ASSET.get(self.asset, MAX_HOLD_HOURS)
        return self.held_hours() >= limit

    def check_exit(self, current_price: float) -> Optional[str]:
        if current_price <= self.stop_price:
            return "STOP_LOSS"
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
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    POSITIONS_FILE.write_text(json.dumps(positions, indent=2), encoding="utf-8")


def _append_history(record: dict) -> None:
    TRADE_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with TRADE_HISTORY.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# ── Public API ────────────────────────────────────────────────────────────────

def open_position_from_order(order: "PendingOrder", fill_price: float) -> Position:  # type: ignore[name-defined]
    from pipeline.limit_orders import ATR_STOP_MULT, ATR_TARGET_MULT

    pct     = order.position_size_pct or DEFAULT_POS_PCT
    qty_usd = round(PAPER_BALANCE * pct, 2)

    # Reconstruct ATR from the original limit order, then re-anchor stop/target
    # to the actual fill price. Without this, a gap-down fill (fill < limit)
    # puts the stop above the entry price — an immediate false trigger.
    atr        = (order.limit_price - order.stop_price) / ATR_STOP_MULT
    stop_price  = round(fill_price - ATR_STOP_MULT  * atr, 2)
    target_price = round(fill_price + ATR_TARGET_MULT * atr, 2)

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


def check_positions(asset: str, current_price: float) -> list[dict]:
    """
    1. Update high-water mark and trailing stop for every open position.
    2. Close any positions that hit their stop / target / max-hold.
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
