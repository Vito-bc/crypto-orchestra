"""
Limit Order Manager — tracks pending buy orders at support levels.

When the orchestrator signals BUY, we place a limit order at the nearest
support level rather than executing a market order. Benefits:
  - Maker entry rather than taker entry (the spread between the two is the
    saving; both rates come from pipeline.fees, never from this module)
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

Fees: see `pipeline.fees`. The maker schedule in force at PLACEMENT is stamped
on each PendingOrder and carried into the Position when it fills — Coinbase
prices an order at the tier in force when the order was placed, and an order
may rest here for 24h across a tier change.
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
# No fee constant lives here. This module defined its own MAKER_FEE_RATE that
# nothing in it used, while position_tracker defined a second one that decided
# actual P&L — two independent copies of the same number, free to drift.
# The operational schedule is pipeline.fees.active_schedule().

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
    status:            str    # OPEN | FILLED | CANCELLED | EXPIRED | FEE_ERROR
    # FEE_ERROR: see docs/operations/fee_error_recovery.md for how it arises
    # and how to repair it. It intentionally has no automatic exit.
    exchange_order_id: Optional[str] = field(default=None)  # Coinbase order ID
    epoch_id:          Optional[str] = field(default=None)  # stamped at placement, not at close
    # Fee schedule stamped at PLACEMENT, because Coinbase prices an order at the
    # tier in force when it is placed — not when it fills. An order rests for up
    # to ORDER_TTL_HOURS, so a tier change can fall between the two.
    fee_schedule_id:   Optional[str]   = field(default=None)
    maker_fee_rate:    Optional[float] = field(default=None)
    # Set only when status == "FEE_ERROR": the price the order would have
    # filled at, had its fee stamp resolved. Repair replays the fill at THIS
    # price, not at whatever price is current when the stamp is fixed — the
    # order does not get a second chance at a better price because its own
    # metadata was broken.
    fee_error_price:   Optional[float] = field(default=None)

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
        from pipeline.fees import active_schedule as _fee_schedule
        from pipeline.risk_epoch import get_current_epoch as _get_epoch
        _epoch = _get_epoch()
        _fees = _fee_schedule()
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
            fee_schedule_id=_fees.schedule_id,
            maker_fee_rate=_fees.maker_rate,
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
        # Backwards compat: fill in fields missing from older records. This
        # never touches fee_schedule_id/maker_fee_rate — there is no load-time
        # migration that invents a fee schedule for a row that lacks one (see
        # pipeline.fees.entry_rate_for_record). An absent fee stamp is left
        # exactly as absent, so it fails loudly if it ever reaches fee
        # resolution instead of quietly becoming "legacy".
        for r in rows:
            r.setdefault("exchange_order_id", None)
            r.setdefault("epoch_id", None)
            r.setdefault("fee_error_price", None)
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
    from pipeline.sizing import trade_size_pct

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

    pct        = position_size_pct if position_size_pct is not None else trade_size_pct()
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
    """
    Orders still unresolved for this asset: OPEN, or FEE_ERROR (genuinely
    fillable but blocked on a corrupt fee stamp, awaiting repair).

    Used by runner.py's new-entry placement guard to decide whether this
    asset already has something outstanding. A FEE_ERROR order has already
    met its fill condition and has not yet become a tracked position — it
    must keep blocking a NEW entry exactly like an OPEN order does, or a
    second order can fill for the same asset while the first sits unresolved.

    This must NOT be used to decide whether the pre-fill entry-veto guard
    applies — see get_pending_orders() below.
    """
    return [
        PendingOrder(**r)
        for r in _load_raw()
        if r["asset"] == asset and r["status"] in ("OPEN", "FEE_ERROR")
    ]


def get_pending_orders(asset: str) -> list[PendingOrder]:
    """
    Orders that have NOT yet met their fill condition — status OPEN only.

    Used by runner.py's pre-fill guard, which re-validates entry conditions
    before a fill happens. FEE_ERROR is deliberately excluded: an order in
    that state already filled (or already met its fill condition) and is
    only waiting on its fee stamp to be repaired. Re-applying an entry veto
    to it is meaningless — the entry already happened — and doing so anyway
    used to make the guard cancel nothing (cancel_open_orders() only touches
    OPEN orders) while a repaired order sat blocked forever, since the guard
    returned before check_and_fill() (the only place recovery happens) was
    ever reached.
    """
    return [
        PendingOrder(**r)
        for r in _load_raw()
        if r["asset"] == asset and r["status"] == "OPEN"
    ]


def _has_usable_fill_price(price: object) -> bool:
    """
    A price good enough to open a position at.

    None/zero/negative/non-numeric all fail. check_order_filled()
    (exchange/coinbase_client.py) returns (True, None) when the exchange
    confirms a fill but reports no average_filled_price — a bare
    `is not None` check would still accept a stray 0.0 and open a position at
    entry_price=0.
    """
    return isinstance(price, (int, float)) and not isinstance(price, bool) and price > 0


def _fee_stamp_is_fillable(r: dict) -> bool:
    """
    Can this order's entry fee actually be resolved?

    Checked BEFORE the FILLED transition below, not after. This function's
    caller persists every status change for the whole batch in one
    `_save_raw(raw)` call at the end — so if a corrupt stamp were only
    discovered later, in `open_position_from_order()`, the order would
    already be on disk as FILLED with no position ever created for it and no
    way back to OPEN (status != "OPEN" means it is never looked at again).
    Refusing the fill here instead leaves the order OPEN and retried next
    cycle: recoverable, rather than orphaned.

    This checks ONLY the fee stamp. A fill price, when one is in play, is a
    separate and independent gate — see _has_usable_fill_price() and its call
    sites in check_and_fill() — because "no fee schedule" and "no usable
    price" are different failures with different meanings on a FEE_ERROR
    record, and conflating them into one boolean would blur which one a
    caller is actually re-checking on recovery.

    Review note: in live mode this can diverge from what the exchange
    actually did (the exchange may have genuinely filled the order while we
    decline to record it locally). That is a corrupt LOCAL stamp — a data
    defect this code introduced — not exchange-state drift, and is out of
    scope for the reconciler. Fixing the stamp and re-running resolves it.
    """
    from pipeline.fees import FeeConfigurationError, entry_rate_for_record

    try:
        entry_rate_for_record(r.get("fee_schedule_id"), r.get("maker_fee_rate"))
        return True
    except FeeConfigurationError as exc:
        print(f"[LimitOrders] order {r.get('id')} ({r.get('asset')}) has an "
              f"unresolvable entry fee stamp — NOT filling this cycle: {exc}")
        return False


def _alert_fee_error(r: dict, reason: str) -> None:
    """
    Surface a new FEE_ERROR beyond the console — an unrepaired FEE_ERROR
    order blocks every new entry for that asset indefinitely (see
    docs/operations/fee_error_recovery.md), so a print-only trail that
    nobody is tailing lets it sit silently. Never blocks the caller: a
    failed notification must not stop the order from being marked FEE_ERROR.
    """
    try:
        from notifications.telegram import send_telegram_message
        send_telegram_message(
            f"[FEE_ERROR] {r.get('asset')} order {r.get('id')} blocked — {reason}. "
            "New entries for this asset are blocked until it is repaired. "
            "See docs/operations/fee_error_recovery.md."
        )
    except Exception as exc:
        print(f"[LimitOrders] FEE_ERROR alert not delivered: {exc}")


def check_and_fill(asset: str, current_price: float) -> list[PendingOrder]:
    """
    Inspect all open orders for the asset.

    Live mode:  query Coinbase for fill status.
    Dry-run:    simulate fill when current_price <= limit_price.

    Marks expired orders as EXPIRED. An order that has genuinely met its fill
    condition but carries an unresolvable fee stamp is marked FEE_ERROR
    instead of being left implicitly OPEN — the previous behaviour left no
    trace of the block, so once the order's TTL later passed, the ordinary
    expiry branch above silently reclassified a real fill as EXPIRED, and
    repairing the stamp afterward could never recover it (EXPIRED is never
    revisited). FEE_ERROR is exempt from TTL expiry and is retried every
    cycle: once the stamp is fixed, it fills at `fee_error_price` — the price
    it was blocked at — not at whatever price is current when repaired.

    Returns the list of orders that just filled.
    """
    from exchange.coinbase_client import cancel_order, check_order_filled, is_dry_run

    raw    = _load_raw()
    filled = []
    dry    = is_dry_run()

    for r in raw:
        if r["asset"] != asset:
            continue

        if r["status"] == "FEE_ERROR":
            # A FEE_ERROR row must never carry an unusable fee_error_price —
            # every path that sets this status enforces that (see the price
            # checks below) — but this is checked again here, defensively,
            # rather than trusted: recovery is what would format it with
            # `:,.2f` and open a position at it, so this is exactly the place
            # an invariant violation would otherwise crash or silently
            # substitute today's price.
            if not _has_usable_fill_price(r.get("fee_error_price")):
                print(f"[LimitOrders] order {r.get('id')} ({r.get('asset')}) is FEE_ERROR "
                      f"with no usable recorded fill price ({r.get('fee_error_price')!r}) "
                      "— this should be unreachable; leaving blocked for manual review")
            elif _fee_stamp_is_fillable(r):
                r["status"] = "FILLED"
                filled.append(PendingOrder(**r))
                print(f"[LimitOrders] Order {r.get('id')} recovered after fee stamp "
                      f"repair — filling at ${r.get('fee_error_price'):,.2f} "
                      "(the price it was blocked at, not today's price)")
            continue

        if r["status"] != "OPEN":
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
                            is_already_filled, fill_price = check_order_filled(exch_id)
                            if is_already_filled:
                                if not _has_usable_fill_price(fill_price):
                                    # Confirmed filled, but the exchange reported
                                    # no usable price. Recording FEE_ERROR with
                                    # fee_error_price=None would crash the
                                    # recovery print below and invite runner to
                                    # silently substitute today's price instead
                                    # of the price this order actually filled
                                    # at. Leave status untouched (still OPEN,
                                    # never reaches the EXPIRED assignment
                                    # below) and retry next cycle — the exact
                                    # same cancel-fails-because-already-filled
                                    # path will re-poll for a price then.
                                    print(f"[LimitOrders] Order {exch_id} filled at TTL boundary "
                                          "but the exchange reported no usable fill price — "
                                          "retrying next cycle rather than recording an "
                                          "unusable price or substituting today's")
                                elif _fee_stamp_is_fillable(r):
                                    r["status"] = "FILLED"
                                    filled.append(PendingOrder(**r))
                                    print(f"[LimitOrders] Order {exch_id} filled at TTL boundary — treating as fill")
                                else:
                                    # Filled on the exchange but its LOCAL stamp is
                                    # corrupt: EXPIRED below would discard a real
                                    # fill entirely. FEE_ERROR keeps it recoverable
                                    # and remembers the price it filled at.
                                    r["status"] = "FEE_ERROR"
                                    r["fee_error_price"] = fill_price
                                    print(f"[LimitOrders] Order {exch_id} filled at TTL boundary "
                                          "but has a corrupt fee stamp — marked FEE_ERROR, not EXPIRED")
                                    _alert_fee_error(r, "corrupt fee stamp at TTL boundary")
                                continue
                        except Exception:
                            pass
                        print(f"[LimitOrders] Could not cancel expired order {exch_id}: {cancel_exc}")
            r["status"] = "EXPIRED"
            continue

        if dry:
            if order.would_fill(current_price):
                if _fee_stamp_is_fillable(r):
                    r["status"] = "FILLED"
                    filled.append(PendingOrder(**r))
                else:
                    r["status"] = "FEE_ERROR"
                    r["fee_error_price"] = current_price
                    _alert_fee_error(r, "corrupt fee stamp")
        else:
            exch_id = r.get("exchange_order_id")
            if exch_id:
                is_filled, fill_price = check_order_filled(exch_id)
                if is_filled:
                    if not _has_usable_fill_price(fill_price):
                        # See the TTL-boundary branch above for why this
                        # leaves status alone instead of recording None.
                        print(f"[LimitOrders] order {r.get('id')} ({asset}) reported "
                              "FILLED with no usable fill price — retrying next cycle")
                    elif _fee_stamp_is_fillable(r):
                        r["status"] = "FILLED"
                        filled.append(PendingOrder(**r))
                    else:
                        r["status"] = "FEE_ERROR"
                        r["fee_error_price"] = fill_price
                        _alert_fee_error(r, "corrupt fee stamp")
            else:
                # Fallback: treat as dry-run simulation for orders without exchange id
                if order.would_fill(current_price):
                    if _fee_stamp_is_fillable(r):
                        r["status"] = "FILLED"
                        filled.append(PendingOrder(**r))
                    else:
                        r["status"] = "FEE_ERROR"
                        r["fee_error_price"] = current_price
                        _alert_fee_error(r, "corrupt fee stamp")

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
