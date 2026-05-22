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
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

ROOT        = Path(__file__).resolve().parents[1]
ORDERS_FILE = ROOT / "logs" / "pending_orders.json"

ORDER_TTL_HOURS = 24    # unfilled orders are cancelled after this
MAKER_FEE_RATE  = 0.002  # 0.2% Coinbase maker fee per side
ATR_STOP_MULT   = 2.5
ATR_TARGET_MULT = 4.0


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
        now = datetime.now(timezone.utc)
        return cls(
            id=str(uuid.uuid4())[:8],
            asset=asset,
            limit_price=round(limit_price, 2),
            stop_price=round(limit_price - ATR_STOP_MULT   * atr, 2),
            target_price=round(limit_price + ATR_TARGET_MULT * atr, 2),
            position_size_pct=position_size_pct,
            placed_at=now.isoformat(),
            expires_at=(now + timedelta(hours=ttl_hours)).isoformat(),
            reasoning=reasoning,
            status="OPEN",
            exchange_order_id=None,
        )

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) >= datetime.fromisoformat(self.expires_at)

    def would_fill(self, current_price: float) -> bool:
        """Limit BUY fills when price drops to or below the limit price (dry-run simulation)."""
        return self.status == "OPEN" and current_price <= self.limit_price


# ── Persistence helpers ───────────────────────────────────────────────────────

def _load_raw() -> list[dict]:
    if not ORDERS_FILE.exists():
        return []
    try:
        rows = json.loads(ORDERS_FILE.read_text(encoding="utf-8"))
        # Backwards compat: add exchange_order_id if missing (pre-Coinbase orders)
        for r in rows:
            r.setdefault("exchange_order_id", None)
        return rows
    except (json.JSONDecodeError, OSError):
        return []


def _save_raw(orders: list[dict]) -> None:
    ORDERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ORDERS_FILE.write_text(json.dumps(orders, indent=2), encoding="utf-8")


# ── Public API ────────────────────────────────────────────────────────────────

def place_limit_order(
    asset:             str,
    limit_price:       float,
    atr:               float,
    position_size_pct: float | None,
    reasoning:         str = "",
) -> PendingOrder:
    """Create a limit buy order, send it to Coinbase, and persist it locally."""
    from exchange.coinbase_client import place_limit_buy
    from pipeline.position_tracker import PAPER_BALANCE

    order = PendingOrder.create(
        asset=asset,
        limit_price=limit_price,
        atr=atr,
        position_size_pct=position_size_pct,
        reasoning=reasoning,
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
    from exchange.coinbase_client import check_order_filled, is_dry_run

    raw    = _load_raw()
    filled = []
    dry    = is_dry_run()

    for r in raw:
        if r["asset"] != asset or r["status"] != "OPEN":
            continue
        order = PendingOrder(**r)
        if order.is_expired():
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
