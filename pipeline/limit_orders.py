"""
Limit Order Manager — tracks pending buy orders at support levels.

When the orchestrator signals BUY, we place a limit order at the nearest
support level rather than executing a market order. Benefits:
  - Maker fee (0.2%) vs taker fee (0.4%) — saves 0.4% per round trip
  - Better entry price at a proven support zone
  - Natural confirmation: price must return to support before we commit

Orders are persisted in logs/pending_orders.json.
Each order expires after ORDER_TTL_HOURS (default 24h) if not filled.

Exchange integration (DRY_RUN=true by default):
  - place_limit_order() sends a real limit order to Coinbase and stores the
    exchange order_id alongside our internal id.
  - check_and_fill() queries Coinbase for fill status (live) or simulates fill
    by price comparison (dry run).
  - cancel_open_orders() cancels on Coinbase before clearing locally.

Fee note: Coinbase Advanced base tier: 0.4% maker (limit orders), 0.6% taker (market orders).
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

ROOT        = Path(__file__).resolve().parents[1]
ORDERS_FILE = ROOT / "logs" / "pending_orders.json"

ORDER_TTL_HOURS = 24    # unfilled orders are cancelled after this
MAKER_FEE_RATE  = 0.004  # 0.4% Coinbase Advanced base tier maker fee

# Per-asset ATR multipliers — tuned from full_year signal scanner (371 trades).
# ETH/SOL use wider stops to avoid intraday wick stop-outs while maintaining R:R ≥ 1.75.
_ASSET_ATR: dict[str, tuple[float, float]] = {
    "BTC-USD": (2.0, 3.5),   # stop, target — R:R = 1.75
    "ETH-USD": (2.5, 4.5),   # stop, target — R:R = 1.80
    "SOL-USD": (2.5, 4.5),   # stop, target — R:R = 1.80
    "ZEC-USD": (2.0, 3.5),   # stop, target — R:R = 1.75
}
_DEFAULT_ATR = (2.0, 3.5)


def _atr_mults(asset: str) -> tuple[float, float]:
    return _ASSET_ATR.get(asset, _DEFAULT_ATR)


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class PendingOrder:
    id:                str
    asset:             str
    limit_price:       float
    stop_price:        float
    target_price:      float
    position_size_pct: float | None
    placed_at:         str    # ISO UTC
    expires_at:        str    # ISO UTC
    reasoning:         str
    status:            str    # OPEN | FILLED | CANCELLED | EXPIRED
    exchange_order_id: Optional[str] = field(default=None)  # Coinbase order ID
    epoch_id:          Optional[str] = field(default=None)  # stamped at placement, not at close

    @classmethod
    def create(
        cls,
        asset:             str,
        limit_price:       float,
        atr:               float,
        position_size_pct: float | None,
        reasoning:         str = "",
        ttl_hours:         int = ORDER_TTL_HOURS,
    ) -> "PendingOrder":
        from pipeline.risk_epoch import get_current_epoch as _get_epoch
        _epoch = _get_epoch()
        now = datetime.now(timezone.utc)
        return cls(
            id=str(uuid.uuid4()),
            asset=asset,
            limit_price=round(limit_price, 2),
            stop_price=round(limit_price - _atr_mults(asset)[0] * atr, 2),
            target_price=round(limit_price + _atr_mults(asset)[1] * atr, 2),
            position_size_pct=position_size_pct,
            placed_at=now.isoformat(),
            expires_at=(now + timedelta(hours=ttl_hours)).isoformat(),
            reasoning=reasoning,
            status="OPEN",
            exchange_order_id=None,
            epoch_id=_epoch["epoch_id"] if _epoch else None,
        )

    def is_expired(self) -> bool:
        expires = datetime.fromisoformat(self.expires_at)
        if expires.tzinfo is None:  # guard against legacy records without UTC offset
            expires = expires.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= expires

    def would_fill(self, current_price: float) -> bool:
        """Limit BUY fills when price drops to or below the limit price (dry-run simulation)."""
        return self.status == "OPEN" and current_price <= self.limit_price


# ── Persistence helpers ───────────────────────────────────────────────────────

def _load_raw() -> list[dict]:
    if not ORDERS_FILE.exists():
        return []
    try:
        rows = json.loads(ORDERS_FILE.read_text(encoding="utf-8"))
        # Backwards compat: fill in fields missing from older records
        for r in rows:
            r.setdefault("exchange_order_id", None)
            r.setdefault("epoch_id", None)
        return rows
    except (json.JSONDecodeError, OSError) as e:
        # Fail-closed: file exists but is unreadable → safer to raise than return []
        # (returning [] would make the placement guard think no orders are open)
        raise RuntimeError(f"pending_orders.json is corrupt or unreadable: {e}") from e


def _save_raw(orders: list[dict]) -> None:
    """Atomic write — temp file + os.replace() so a crash never corrupts the orders file."""
    ORDERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(orders, indent=2)
    fd, tmp = tempfile.mkstemp(dir=ORDERS_FILE.parent, prefix=".ord_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp, ORDERS_FILE)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Public API ────────────────────────────────────────────────────────────────

def place_limit_order(
    asset:             str,
    limit_price:       float,
    atr:               float,
    position_size_pct: float | None,
    reasoning:         str = "",
) -> PendingOrder:
    """Create a limit buy order, send it to Coinbase, and persist it locally."""
    from exchange.coinbase_client import is_dry_run, place_limit_buy
    from pipeline.position_tracker import PAPER_BALANCE

    order = PendingOrder.create(
        asset=asset,
        limit_price=limit_price,
        atr=atr,
        position_size_pct=position_size_pct,
        reasoning=reasoning,
    )

    if not is_dry_run() and order.epoch_id is None:
        raise RuntimeError(
            f"Live order rejected for {asset}: no active risk epoch. "
            "Start an epoch with 'python pipeline/start_epoch.py' before going live."
        )

    pct        = position_size_pct or 0.02
    quote_usd  = round(PAPER_BALANCE * pct, 2)
    exch_id    = place_limit_buy(
        product_id=asset,
        quote_size_usd=quote_usd,
        limit_price=limit_price,
        client_order_id=order.id,
    )
    order.exchange_order_id = exch_id

    raw = _load_raw()
    raw.append(asdict(order))
    _save_raw(raw)
    return order


def get_open_orders(asset: str) -> list[PendingOrder]:
    return [
        PendingOrder(**r)
        for r in _load_raw()
        if r["asset"] == asset and r["status"] == "OPEN"
    ]


def check_and_fill(asset: str, current_price: float) -> list[PendingOrder]:
    """
    Inspect all open orders for the asset.

    Live mode:  query Coinbase for fill status.
    Dry-run:    simulate fill when current_price <= limit_price.

    Marks expired orders as EXPIRED.
    Returns the list of orders that just filled.
    """
    from exchange.coinbase_client import cancel_order, check_order_filled, is_dry_run

    raw    = _load_raw()
    filled = []
    dry    = is_dry_run()

    for r in raw:
        if r["asset"] != asset or r["status"] != "OPEN":
            continue
        order = PendingOrder(**r)
        if order.is_expired():
            if not dry:
                exch_id = r.get("exchange_order_id")
                if exch_id:
                    try:
                        cancel_order(exch_id)
                    except Exception as cancel_exc:
                        # Cancel may fail if the order filled at the TTL boundary — verify
                        try:
                            is_already_filled, _ = check_order_filled(exch_id)
                            if is_already_filled:
                                r["status"] = "FILLED"
                                filled.append(PendingOrder(**r))
                                print(f"[LimitOrders] Order {exch_id} filled at TTL boundary — treating as fill")
                                continue
                        except Exception:
                            pass
                        print(f"[LimitOrders] Could not cancel expired order {exch_id}: {cancel_exc}")
            r["status"] = "EXPIRED"
            continue

        if dry:
            if order.would_fill(current_price):
                r["status"] = "FILLED"
                filled.append(PendingOrder(**r))
        else:
            exch_id = r.get("exchange_order_id")
            if exch_id:
                is_filled, fill_price = check_order_filled(exch_id)
                if is_filled:
                    r["status"] = "FILLED"
                    filled.append(PendingOrder(**r))
            else:
                # Fallback: treat as dry-run simulation for orders without exchange id
                if order.would_fill(current_price):
                    r["status"] = "FILLED"
                    filled.append(PendingOrder(**r))

    _save_raw(raw)
    return filled


def cancel_open_orders(asset: str) -> int:
    """Cancel all open orders for an asset on Coinbase and locally. Returns count cancelled."""
    from exchange.coinbase_client import cancel_order

    raw   = _load_raw()
    count = 0
    for r in raw:
        if r["asset"] == asset and r["status"] == "OPEN":
            exch_id = r.get("exchange_order_id")
            if exch_id:
                cancel_order(exch_id)
            r["status"] = "CANCELLED"
            count += 1
    _save_raw(raw)
    return count
