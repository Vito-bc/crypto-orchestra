"""
Startup reconciliation gate.

See docs/adr/001-reconciliation-truth-model.md for the full truth model.

Usage (startup gate pattern):
    report = run_startup_reconciliation(
        list_orders_fn=lambda: coinbase_client.list_all_orders(),
        cancel_order_fn=coinbase_client.cancel_order,
        db_path=db_path,
    )
    if not report.allowed_to_trade:
        sys.exit(f"Startup blocked: {report.unresolved}")

Architecture:
  Step 1 — acquire reconciliation lease (start_reconciliation inside BEGIN tx)
  Step 2 — snapshot SUBMITTING orders AND EXPIRED/CANCELLED orders that have an
            exchange_order_id (these need late-fill checking); release SQLite
            before any Coinbase call
  Step 3 — fetch Coinbase orders (caller handles pagination, passes full list)
  Step 4 — resolve each SUBMITTING order per ADR 001 state table:
              found OPEN/PARTIAL  → TX-B: attach exchange_order_id → OPEN
              found FILLED        → TX-B: OPEN → apply fills → FILLED
              found CANCELLED     → TX-B: OPEN → apply fills → CANCELLED
              found EXPIRED       → TX-B: OPEN → apply fills → EXPIRED
              not found           → leave SUBMITTING, add to unresolved
  Step 5 — for EXPIRED/CANCELLED orders: check Coinbase for fills not yet in
            the ledger (late fills); apply any found, then check stacking
  Step 6 — for stacking conflicts: issue cancel for the active ENTRY order;
            UNRESOLVED until Coinbase confirms the cancel
  Step 7 — complete_reconciliation with full report

Gate:
  is_entry_placement_allowed(db_path, freshness_minutes) → (allowed, reason)
  Returns False when:
    - any reconciliation_run is RUNNING
    - last completed run has unresolved items
    - last completed run is older than freshness_minutes
    - no reconciliation has ever run
  Does NOT block: signal scanner, shadow journal, EXIT orders, CANCEL orders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from pipeline.ledger import (
    apply_fill,
    complete_reconciliation,
    get_db,
    start_reconciliation,
    transition_order,
)

_CB_TERMINAL_ACCEPTED = frozenset({"FILLED", "CANCELLED", "EXPIRED"})
_CB_LIVE = frozenset({"OPEN", "PENDING_CANCEL", "QUEUED", "CANCEL_QUEUED"})


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class CoinbaseFill:
    """One fill event from Coinbase (normalised by caller)."""
    exchange_fill_id: str
    fill_price: float
    fill_qty_base: float
    fee_usd: float = 0.0
    filled_at: str = ""


@dataclass
class CoinbaseOrder:
    """Minimal Coinbase order snapshot (normalised by caller)."""
    client_order_id: str     # == our local order UUID
    exchange_order_id: str
    status: str              # OPEN | FILLED | CANCELLED | EXPIRED | …
    fills: list[CoinbaseFill] = field(default_factory=list)


@dataclass
class ResolvedItem:
    order_id: str
    asset: str
    action: str   # "open"|"filled"|"cancelled"|"expired"|"stacking_cancelled"|"late_fill"
    detail: str = ""

    def to_dict(self) -> dict:
        return {"order_id": self.order_id, "asset": self.asset,
                "action": self.action, "detail": self.detail}


@dataclass
class UnresolvedItem:
    order_id: str
    asset: str
    reason: str   # "not_found"|"stacking_cancel_failed"|"unknown_coinbase_status"|"error:…"

    def to_dict(self) -> dict:
        return {"order_id": self.order_id, "asset": self.asset, "reason": self.reason}


@dataclass
class ReconciliationReport:
    run_id: int
    discovered: list[dict]
    resolved: list[ResolvedItem]
    unresolved: list[UnresolvedItem]
    started_at: str
    completed_at: str

    @property
    def allowed_to_trade(self) -> bool:
        return len(self.unresolved) == 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _already_applied_fills(order_id: str, conn) -> set[str]:
    """Return exchange_fill_ids already in the ledger for this order."""
    rows = conn.execute(
        "SELECT exchange_fill_id FROM fills"
        " WHERE order_id=? AND exchange_fill_id IS NOT NULL",
        (order_id,),
    ).fetchall()
    return {r[0] for r in rows}


def _apply_coinbase_fills(
    order_id: str,
    fills: list[CoinbaseFill],
    conn,
    reconciliation_mode: bool = True,
    already_applied: set[str] | None = None,
) -> None:
    """Apply Coinbase fills to an order.  Skips fills already in the ledger."""
    known = already_applied or set()
    for f in fills:
        if f.exchange_fill_id in known:
            continue
        try:
            apply_fill(
                order_id=order_id,
                fill_price=f.fill_price,
                fill_qty_base=f.fill_qty_base,
                exchange_fill_id=f.exchange_fill_id,
                fee_usd=f.fee_usd,
                filled_at=f.filled_at or None,
                conn=conn,
                reconciliation_mode=reconciliation_mode,
            )
        except RuntimeError as exc:
            msg = str(exc)
            if "already recorded" in msg or "previously recorded" in msg:
                continue
            raise


def _detect_stacking(
    asset: str,
    conn,
    exclude_order_id: str | None = None,
) -> Optional[str]:
    """
    Return the active ENTRY order_id for `asset` that is NOT the order we just
    resolved (exclude_order_id).  Returns None when there is no conflict.
    """
    if exclude_order_id:
        row = conn.execute(
            "SELECT id FROM orders"
            " WHERE asset=? AND purpose='ENTRY' AND id != ?"
            " AND status IN ('SUBMITTING','OPEN','PARTIAL')"
            " ORDER BY placed_at LIMIT 1",
            (asset, exclude_order_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM orders"
            " WHERE asset=? AND purpose='ENTRY'"
            " AND status IN ('SUBMITTING','OPEN','PARTIAL')"
            " ORDER BY placed_at LIMIT 1",
            (asset,),
        ).fetchone()
    return row["id"] if row else None


def _handle_stacking_conflict(
    triggering_order_id: str,
    conflicting_order_id: str,
    asset: str,
    cancel_order_fn: Callable[[str], bool],
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem]]:
    """
    Issue a cancel for the conflicting active ENTRY order.
    Returns resolved if cancel confirmed, unresolved if not.
    Per ADR 001: never locally CANCEL without Coinbase confirmation.
    """
    row = conn.execute(
        "SELECT exchange_order_id FROM orders WHERE id=?",
        (conflicting_order_id,),
    ).fetchone()
    exchange_oid = row["exchange_order_id"] if row else None

    cancelled_ok = False
    if exchange_oid:
        cancelled_ok = cancel_order_fn(exchange_oid)

    if cancelled_ok:
        transition_order(conflicting_order_id, "CANCELLED", conn=conn)
        return (
            ResolvedItem(
                triggering_order_id, asset, "stacking_cancelled",
                detail=f"cancelled conflicting entry {conflicting_order_id}",
            ),
            None,
        )
    return (
        None,
        UnresolvedItem(conflicting_order_id, asset, "stacking_cancel_failed"),
    )


def _resolve_one_submitting(
    *,
    order_id: str,
    asset: str,
    cb_order: Optional[CoinbaseOrder],
    cancel_order_fn: Callable[[str], bool],
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem]]:
    """
    Resolve a single SUBMITTING order according to ADR 001 state table.
    Returns (resolved, None) or (None, unresolved).
    """
    if cb_order is None:
        return None, UnresolvedItem(order_id, asset, "not_found")

    cb_status = cb_order.status

    if cb_status not in _CB_LIVE | _CB_TERMINAL_ACCEPTED:
        return None, UnresolvedItem(order_id, asset, f"unknown_coinbase_status:{cb_status}")

    # Attach exchange_order_id and move to OPEN (always first).
    transition_order(order_id, "OPEN", exchange_order_id=cb_order.exchange_order_id, conn=conn)

    # Apply any fills.
    if cb_order.fills:
        _apply_coinbase_fills(order_id, cb_order.fills, conn)

    # Close out locally if terminal on Coinbase.
    local_action = "open"
    if cb_status == "FILLED":
        local_action = "filled"
    elif cb_status in ("CANCELLED", "CANCEL_QUEUED", "PENDING_CANCEL"):
        transition_order(order_id, "CANCELLED", conn=conn)
        local_action = "cancelled"
    elif cb_status == "EXPIRED":
        transition_order(order_id, "EXPIRED", conn=conn)
        local_action = "expired"

    # Check for late-fill stacking: did applying fills create a position while
    # another active ENTRY exists for the same asset?
    # Exclude the current order (it may still appear OPEN during fill application).
    if cb_order.fills:
        conflicting_order_id = _detect_stacking(asset, conn, exclude_order_id=order_id)
        if conflicting_order_id:
            stacking_res, stacking_unres = _handle_stacking_conflict(
                triggering_order_id=order_id,
                conflicting_order_id=conflicting_order_id,
                asset=asset,
                cancel_order_fn=cancel_order_fn,
                conn=conn,
            )
            if stacking_unres:
                return None, stacking_unres
            # stacking was resolved — return the stacking_cancelled resolution
            return stacking_res, None

    return ResolvedItem(order_id, asset, local_action), None


def _check_late_fills_for_terminal_order(
    *,
    order_id: str,
    asset: str,
    exchange_order_id: str,
    cb_order: Optional[CoinbaseOrder],
    cancel_order_fn: Callable[[str], bool],
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem]]:
    """
    For an EXPIRED or CANCELLED order, check if Coinbase has fills that were
    never applied to the ledger (late fills arriving after local expiry/cancel).

    This covers the stacking scenario described in ADR 001 Decision 4:
      - order A was locally EXPIRED/CANCELLED
      - order B (new ENTRY for same asset) was subsequently placed
      - reconciler discovers a late Coinbase fill for A
      - must apply the fill, then cancel B before allowing new ENTRY placements
    """
    if cb_order is None or not cb_order.fills:
        return None, None  # nothing to do

    already_applied = _already_applied_fills(order_id, conn)
    new_fills = [f for f in cb_order.fills if f.exchange_fill_id not in already_applied]
    if not new_fills:
        return None, None  # all fills already recorded

    # Apply the late fills (reconciliation_mode=True allows fills on terminal orders).
    _apply_coinbase_fills(
        order_id, new_fills, conn,
        reconciliation_mode=True,
        already_applied=already_applied,
    )

    # Check for stacking conflict after the late fill created a position.
    conflicting_order_id = _detect_stacking(asset, conn, exclude_order_id=order_id)
    if conflicting_order_id:
        stacking_res, stacking_unres = _handle_stacking_conflict(
            triggering_order_id=order_id,
            conflicting_order_id=conflicting_order_id,
            asset=asset,
            cancel_order_fn=cancel_order_fn,
            conn=conn,
        )
        if stacking_unres:
            return None, stacking_unres
        return stacking_res, None

    return ResolvedItem(order_id, asset, "late_fill",
                        detail=f"{len(new_fills)} new fill(s) from Coinbase"), None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_startup_reconciliation(
    *,
    list_orders_fn: Callable[[], list[CoinbaseOrder]],
    cancel_order_fn: Callable[[str], bool],
    db_path: Optional[Path] = None,
) -> ReconciliationReport:
    """
    Mandatory startup gate.  Resolves SUBMITTING orders and late fills on
    terminal orders before any new ENTRY placement is allowed.

    Args:
        list_orders_fn:  Returns all Coinbase orders for the strategy portfolio
                         (caller handles pagination; include all statuses and fills).
        cancel_order_fn: cancel_order(exchange_order_id) → True if confirmed.
        db_path:         Override ledger path (tests only).

    Returns ReconciliationReport.  Check .allowed_to_trade before proceeding.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    # ── Step 1: acquire reconciliation lease ─────────────────────────────────
    with get_db(db_path) as conn:
        run_id = start_reconciliation(conn)

    # ── Step 2: snapshot orders; release SQLite before network calls ──────────
    with get_db(db_path) as conn:
        submitting_rows = conn.execute(
            "SELECT id, asset FROM orders WHERE status='SUBMITTING'"
        ).fetchall()
        # EXPIRED/CANCELLED orders with exchange_order_id may have late fills.
        terminal_rows = conn.execute(
            "SELECT id, asset, exchange_order_id FROM orders"
            " WHERE status IN ('EXPIRED','CANCELLED')"
            "   AND exchange_order_id IS NOT NULL"
        ).fetchall()

    discovered = [{"order_id": r["id"], "asset": r["asset"]} for r in submitting_rows]

    if not submitting_rows and not terminal_rows:
        completed_at = datetime.now(timezone.utc).isoformat()
        with get_db(db_path) as conn:
            complete_reconciliation(run_id, discovered, [], [], conn)
        return ReconciliationReport(
            run_id=run_id, discovered=discovered,
            resolved=[], unresolved=[],
            started_at=started_at, completed_at=completed_at,
        )

    # ── Step 3: fetch Coinbase orders (no SQLite connection held) ─────────────
    coinbase_orders: list[CoinbaseOrder] = list_orders_fn()
    cb_by_client_id: dict[str, CoinbaseOrder] = {
        o.client_order_id: o for o in coinbase_orders
    }

    resolved: list[ResolvedItem] = []
    unresolved: list[UnresolvedItem] = []

    # ── Steps 4–6: resolve SUBMITTING orders ──────────────────────────────────
    for row in submitting_rows:
        order_id, asset = row["id"], row["asset"]
        cb_order = cb_by_client_id.get(order_id)
        try:
            with get_db(db_path) as conn:
                res, unres = _resolve_one_submitting(
                    order_id=order_id,
                    asset=asset,
                    cb_order=cb_order,
                    cancel_order_fn=cancel_order_fn,
                    conn=conn,
                )
        except Exception as exc:
            unresolved.append(UnresolvedItem(order_id, asset, f"error:{exc}"))
            continue
        if res:
            resolved.append(res)
        if unres:
            unresolved.append(unres)

    # ── Step 5: check EXPIRED/CANCELLED orders for late fills ─────────────────
    for row in terminal_rows:
        order_id, asset = row["id"], row["asset"]
        # Look up by client_order_id (== our local order UUID) on Coinbase.
        cb_order = cb_by_client_id.get(order_id)
        try:
            with get_db(db_path) as conn:
                res, unres = _check_late_fills_for_terminal_order(
                    order_id=order_id,
                    asset=asset,
                    exchange_order_id=row["exchange_order_id"],
                    cb_order=cb_order,
                    cancel_order_fn=cancel_order_fn,
                    conn=conn,
                )
        except Exception as exc:
            unresolved.append(UnresolvedItem(order_id, asset, f"error:{exc}"))
            continue
        if res:
            resolved.append(res)
        if unres:
            unresolved.append(unres)

    # ── Step 7: complete reconciliation_run ───────────────────────────────────
    completed_at = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        complete_reconciliation(
            run_id,
            discovered=discovered,
            resolved=[r.to_dict() for r in resolved],
            unresolved=[u.to_dict() for u in unresolved],
            conn=conn,
        )

    return ReconciliationReport(
        run_id=run_id,
        discovered=discovered,
        resolved=resolved,
        unresolved=unresolved,
        started_at=started_at,
        completed_at=completed_at,
    )


def is_entry_placement_allowed(
    db_path: Optional[Path] = None,
    freshness_minutes: int = 60,
) -> tuple[bool, str]:
    """
    Gate check for new ENTRY placements.

    Returns (True, "ok") when:
      - no reconciliation is RUNNING
      - last completed reconciliation has zero unresolved items
      - last completed reconciliation is within freshness_minutes

    Returns (False, reason) otherwise.

    DOES NOT block: signal scanner, EXIT orders, CANCEL orders.
    Call this before every new ENTRY placement in addition to the TX-A gate.
    """
    with get_db(db_path) as conn:
        running = conn.execute(
            "SELECT 1 FROM reconciliation_runs WHERE status='RUNNING' LIMIT 1"
        ).fetchone()
        if running:
            return False, "reconciliation is currently RUNNING"

        last = conn.execute(
            "SELECT status, completed_at, unresolved FROM reconciliation_runs"
            " WHERE status != 'RUNNING'"
            " ORDER BY id DESC LIMIT 1"
        ).fetchone()

    if last is None:
        return False, "no reconciliation has ever completed"

    if last["status"] == "FAILED":
        return False, "last reconciliation run FAILED"

    try:
        unres = json.loads(last["unresolved"] or "[]")
    except (ValueError, TypeError):
        unres = []
    if unres:
        return False, f"last reconciliation has {len(unres)} unresolved item(s)"

    if not last["completed_at"]:
        return False, "last reconciliation has no completed_at timestamp"

    try:
        completed_at = datetime.fromisoformat(last["completed_at"])
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return False, "last reconciliation has unparseable completed_at"

    age = datetime.now(timezone.utc) - completed_at
    if age > timedelta(minutes=freshness_minutes):
        return False, (
            f"last reconciliation is {age.total_seconds() / 60:.0f}min old "
            f"(limit: {freshness_minutes}min) — run reconciliation again before trading"
        )

    return True, "ok"
