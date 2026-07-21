"""
Startup reconciliation gate.

See docs/adr/001-reconciliation-truth-model.md for the full truth model.

v1 scope (ADR 001 Decision 6):
  If the local snapshot has no SUBMITTING or EXPIRED/CANCELLED-with-exchange-id
  orders, this function returns COMPLETE without querying Coinbase.  Orphan
  Coinbase orders, fills on OPEN/PARTIAL orders, and balance discrepancies are
  NOT checked.  Do not use this as a full live gate.

Three-phase design prevents holding SQLite write locks during network I/O:
  Phase A — short SQLite transactions, no network:
    apply fills, attach exchange_order_id, detect stacking, collect
    _PendingCancel descriptors.
  Phase B — network only, no SQLite:
    issue cancel requests.
  Phase C — short SQLite transactions:
    commit confirmed cancel outcomes.

cancel_order_fn contract (ADR 001 Decision 7):
  Must return True ONLY when Coinbase confirms CANCELLED status — not merely
  when Batch Cancel returns success=True (that only queues the request).
  CANCEL_QUEUED / PENDING_CANCEL from get_order → False (UNRESOLVED).

Usage:
    report = run_startup_reconciliation(
        list_orders_fn=lambda: coinbase_client.list_all_orders(),
        cancel_order_fn=coinbase_client.cancel_order,
        db_path=db_path,
    )
    if not report.allowed_to_trade:
        sys.exit(f"Startup blocked: {report.unresolved}")
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

# Live statuses: order accepted but outcome not yet terminal.
# CANCEL_QUEUED / PENDING_CANCEL mean a cancel was requested but not confirmed.
_CB_LIVE = frozenset({
    "OPEN", "PENDING", "QUEUED",
    "CANCEL_QUEUED", "PENDING_CANCEL",
})
# Terminal statuses where Coinbase accepted and then closed the order.
_CB_TERMINAL_ACCEPTED = frozenset({"FILLED", "CANCELLED", "EXPIRED"})


# ---------------------------------------------------------------------------
# Public data models
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
    client_order_id: str      # == our local order UUID
    exchange_order_id: str
    status: str               # OPEN | FILLED | CANCELLED | EXPIRED | …
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
    # "not_found" | "cancel_pending" | "filled_missing_fills" |
    # "stacking_cancel_failed" | "stacking_no_exchange_id" |
    # "status_mismatch:local=X,cb=Y" | "unknown_coinbase_status:Z" | "error:…"
    reason: str

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
# Internal models
# ---------------------------------------------------------------------------

@dataclass
class _PendingCancel:
    """
    Stacking conflict found during Phase A.  Cancel must be issued in Phase B
    (no SQLite connection held) so a write lock is not held during network I/O.
    """
    triggering_order_id: str
    conflicting_order_id: str
    conflicting_exchange_id: str
    asset: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _already_applied_fills(order_id: str, conn) -> set[str]:
    """Return exchange_fill_ids already recorded in the ledger for this order."""
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
    """Apply Coinbase fills to an order, skipping fills already in the ledger."""
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
    Return the active ENTRY order_id for `asset` that is NOT exclude_order_id.
    Returns None when there is no stacking conflict.
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


def _build_pending_cancel(
    triggering_order_id: str,
    conflicting_order_id: str,
    asset: str,
    conn,
) -> tuple[Optional[_PendingCancel], Optional[UnresolvedItem]]:
    """
    Look up the exchange_order_id for the conflicting order and return a
    _PendingCancel descriptor for Phase B.  No network I/O.
    Returns (None, unresolved) when exchange_order_id is missing.
    """
    row = conn.execute(
        "SELECT exchange_order_id FROM orders WHERE id=?",
        (conflicting_order_id,),
    ).fetchone()
    exchange_oid = row["exchange_order_id"] if row else None

    if not exchange_oid:
        return None, UnresolvedItem(conflicting_order_id, asset, "stacking_no_exchange_id")

    return _PendingCancel(
        triggering_order_id=triggering_order_id,
        conflicting_order_id=conflicting_order_id,
        conflicting_exchange_id=exchange_oid,
        asset=asset,
    ), None


def _resolve_one_submitting(
    *,
    order_id: str,
    asset: str,
    cb_order: Optional[CoinbaseOrder],
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem], Optional[_PendingCancel]]:
    """
    Resolve a single SUBMITTING order per the ADR 001 state table.
    Returns exactly one non-None element across (resolved, unresolved, pending).
    No network I/O.

    ADR 001 state table:
      found OPEN/PENDING/QUEUED         → attach exchange_order_id → OPEN
      found CANCEL_QUEUED/PENDING_CANCEL → OPEN + UNRESOLVED(cancel_pending)
      found FILLED (with fills)          → OPEN → apply fills → (ledger auto-FILLED)
      found FILLED (no fills)            → OPEN + UNRESOLVED(filled_missing_fills)
      found CANCELLED                    → OPEN → apply fills → CANCELLED
      found EXPIRED                      → OPEN → apply fills → EXPIRED
      not found                          → UNRESOLVED(not_found)
    """
    if cb_order is None:
        return None, UnresolvedItem(order_id, asset, "not_found"), None

    cb_status = cb_order.status

    if cb_status not in _CB_LIVE | _CB_TERMINAL_ACCEPTED:
        return None, UnresolvedItem(order_id, asset, f"unknown_coinbase_status:{cb_status}"), None

    # CANCEL_QUEUED / PENDING_CANCEL: cancel was requested but not confirmed.
    # The order may still execute.  Attach exchange_order_id (acknowledge the
    # order exists) then leave OPEN + UNRESOLVED.  Re-poll on next run.
    if cb_status in ("CANCEL_QUEUED", "PENDING_CANCEL"):
        transition_order(order_id, "OPEN", exchange_order_id=cb_order.exchange_order_id, conn=conn)
        return None, UnresolvedItem(order_id, asset, "cancel_pending"), None

    # All other statuses: attach exchange_order_id and move to OPEN.
    transition_order(order_id, "OPEN", exchange_order_id=cb_order.exchange_order_id, conn=conn)

    if cb_status == "FILLED":
        # List Orders does not reliably include fill records (see ADR 001).
        # Without fills we cannot create a position or confirm execution.
        if not cb_order.fills:
            return None, UnresolvedItem(order_id, asset, "filled_missing_fills"), None
        _apply_coinbase_fills(order_id, cb_order.fills, conn)
        local_action = "filled"

    elif cb_status == "CANCELLED":
        if cb_order.fills:
            _apply_coinbase_fills(order_id, cb_order.fills, conn)
        transition_order(order_id, "CANCELLED", conn=conn)
        local_action = "cancelled"

    elif cb_status == "EXPIRED":
        if cb_order.fills:
            _apply_coinbase_fills(order_id, cb_order.fills, conn)
        transition_order(order_id, "EXPIRED", conn=conn)
        local_action = "expired"

    else:
        # OPEN / PENDING / QUEUED: live, unfilled.
        if cb_order.fills:
            _apply_coinbase_fills(order_id, cb_order.fills, conn)
        local_action = "open"

    # After fills are applied, check for stacking (exclude the current order).
    if cb_order.fills:
        conflicting_id = _detect_stacking(asset, conn, exclude_order_id=order_id)
        if conflicting_id:
            pending, unres = _build_pending_cancel(
                triggering_order_id=order_id,
                conflicting_order_id=conflicting_id,
                asset=asset,
                conn=conn,
            )
            if unres:
                return None, unres, None
            return None, None, pending

    return ResolvedItem(order_id, asset, local_action), None, None


def _check_late_fills_for_terminal_order(
    *,
    order_id: str,
    asset: str,
    exchange_order_id: str,
    local_status: str,
    cb_order: Optional[CoinbaseOrder],
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem], Optional[_PendingCancel]]:
    """
    For an EXPIRED or CANCELLED order in the ledger, check Coinbase for:
      1. Status mismatch: local=terminal but Coinbase=live → UNRESOLVED.
         Possible for GTC orders that were never effectively cancelled.
      2. Late fills: fills that arrived after local expiry/cancel.
         If late fill creates a position while another ENTRY is OPEN → stacking.

    No network I/O.  Returns (resolved, None, None) | (None, unresolved, None)
    | (None, None, pending_cancel).
    """
    if cb_order is None:
        return None, None, None  # Coinbase doesn't know this order — nothing actionable

    cb_status = cb_order.status

    # Status mismatch: we marked it terminal but Coinbase says it's still live.
    if cb_status in _CB_LIVE:
        return None, UnresolvedItem(
            order_id, asset,
            f"status_mismatch:local={local_status},cb={cb_status}",
        ), None

    if not cb_order.fills:
        return None, None, None

    already_applied = _already_applied_fills(order_id, conn)
    new_fills = [f for f in cb_order.fills if f.exchange_fill_id not in already_applied]
    if not new_fills:
        return None, None, None

    # Apply late fills in reconciliation_mode (allowed on terminal orders).
    _apply_coinbase_fills(
        order_id, new_fills, conn,
        reconciliation_mode=True,
        already_applied=already_applied,
    )

    # Late fill may have created a position; check for stacking conflict.
    conflicting_id = _detect_stacking(asset, conn, exclude_order_id=order_id)
    if conflicting_id:
        pending, unres = _build_pending_cancel(
            triggering_order_id=order_id,
            conflicting_order_id=conflicting_id,
            asset=asset,
            conn=conn,
        )
        if unres:
            return None, unres, None
        return None, None, pending

    return ResolvedItem(
        order_id, asset, "late_fill",
        detail=f"{len(new_fills)} new fill(s) from Coinbase",
    ), None, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_startup_reconciliation(
    *,
    list_orders_fn: Callable[[], list[CoinbaseOrder]],
    cancel_order_fn: Callable[[str], bool],
    get_order_fn: Callable[[str], Optional[CoinbaseOrder]] | None = None,
    db_path: Optional[Path] = None,
) -> ReconciliationReport:
    """
    Mandatory startup gate.  Resolves SUBMITTING orders and late fills on
    terminal orders before any new ENTRY placement is allowed.

    cancel_order_fn(exchange_order_id) -> bool
      Must return True ONLY when Coinbase CONFIRMS the order is CANCELLED.
      Batch Cancel success=True is NOT confirmation (it queues the request).

    get_order_fn(exchange_order_id) -> Optional[CoinbaseOrder]
      Required for safe stacking resolution.  After a cancel is confirmed,
      the reconciler fetches the cancelled order's final fills and applies
      any partial fills that occurred in the cancellation window.
      If None, stacking stays UNRESOLVED(cancelled_fills_unverified) — safer
      than assuming zero fills and immediately unblocking the asset.

    v1 limitation: if no local SUBMITTING or EXPIRED/CANCELLED rows exist,
    returns COMPLETE without querying Coinbase.  Orphan orders, OPEN/PARTIAL
    fills, and balances are not checked (ADR 001 Decision 6).
    """
    started_at = datetime.now(timezone.utc).isoformat()

    with get_db(db_path) as conn:
        run_id = start_reconciliation(conn)

    discovered: list[dict] = []
    resolved: list[ResolvedItem] = []
    unresolved: list[UnresolvedItem] = []

    try:
        # Step 2: snapshot — release SQLite before any Coinbase call.
        with get_db(db_path) as conn:
            submitting_rows = conn.execute(
                "SELECT id, asset FROM orders WHERE status='SUBMITTING'"
            ).fetchall()
            terminal_rows = conn.execute(
                "SELECT id, asset, exchange_order_id, status FROM orders"
                " WHERE status IN ('EXPIRED','CANCELLED')"
                "   AND exchange_order_id IS NOT NULL"
            ).fetchall()

        discovered = [{"order_id": r["id"], "asset": r["asset"]} for r in submitting_rows]

        if not submitting_rows and not terminal_rows:
            # v1: no uncertain local orders → complete without Coinbase call.
            # Orphan orders, OPEN/PARTIAL fills, and balances are not checked.
            pass  # falls through to complete_reconciliation below

        else:
            # Step 3: fetch Coinbase (no SQLite connection held).
            coinbase_orders: list[CoinbaseOrder] = list_orders_fn()
            cb_by_client_id: dict[str, CoinbaseOrder] = {
                o.client_order_id: o for o in coinbase_orders
            }

            # ── Phase A: apply fills/transitions, detect stacking (no network) ──
            raw_pending: list[_PendingCancel] = []

            for row in submitting_rows:
                order_id, asset = row["id"], row["asset"]
                cb_order = cb_by_client_id.get(order_id)
                try:
                    with get_db(db_path) as conn:
                        res, unres, pending = _resolve_one_submitting(
                            order_id=order_id,
                            asset=asset,
                            cb_order=cb_order,
                            conn=conn,
                        )
                except Exception as exc:
                    unresolved.append(UnresolvedItem(order_id, asset, f"error:{exc}"))
                    continue
                if res:
                    resolved.append(res)
                if unres:
                    unresolved.append(unres)
                if pending:
                    raw_pending.append(pending)

            for row in terminal_rows:
                order_id, asset = row["id"], row["asset"]
                cb_order = cb_by_client_id.get(order_id)
                try:
                    with get_db(db_path) as conn:
                        res, unres, pending = _check_late_fills_for_terminal_order(
                            order_id=order_id,
                            asset=asset,
                            exchange_order_id=row["exchange_order_id"],
                            local_status=row["status"],
                            cb_order=cb_order,
                            conn=conn,
                        )
                except Exception as exc:
                    unresolved.append(UnresolvedItem(order_id, asset, f"error:{exc}"))
                    continue
                if res:
                    resolved.append(res)
                if unres:
                    unresolved.append(unres)
                if pending:
                    raw_pending.append(pending)

            # Deduplicate by conflicting_order_id: multiple terminal orders for
            # the same asset may all detect the same active ENTRY as a conflict.
            # Send at most one cancel per conflicting order.
            seen_conflict_ids: set[str] = set()
            pending_cancels: list[_PendingCancel] = []
            for pc in raw_pending:
                if pc.conflicting_order_id not in seen_conflict_ids:
                    seen_conflict_ids.add(pc.conflicting_order_id)
                    pending_cancels.append(pc)

            # ── Phase B: issue cancels (no SQLite held) ───────────────────────
            cancel_results: dict[str, bool] = {}
            cancel_errors: set[str] = set()
            for pc in pending_cancels:
                try:
                    confirmed = cancel_order_fn(pc.conflicting_exchange_id)
                    cancel_results[pc.conflicting_order_id] = confirmed
                except Exception as exc:
                    cancel_results[pc.conflicting_order_id] = False
                    cancel_errors.add(pc.conflicting_order_id)
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"stacking_cancel_error:{exc}",
                    ))

            # ── Phase C: verify fills then commit cancel outcomes ─────────────
            # A confirmed cancel does NOT mean zero fills: an order may have
            # partially filled between Phase A and the cancel becoming effective.
            # get_order_fn fetches the final state with fills.  Without it we
            # cannot safely declare stacking resolved.
            for pc in pending_cancels:
                if pc.conflicting_order_id in cancel_errors:
                    continue  # already added to unresolved via exception path

                confirmed = cancel_results.get(pc.conflicting_order_id, False)
                if not confirmed:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset, "stacking_cancel_failed",
                    ))
                    continue

                if get_order_fn is None:
                    # Fills unverified — leave UNRESOLVED; next reconciliation
                    # will re-check once get_order_fn is wired.
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset, "cancelled_fills_unverified",
                    ))
                    continue

                try:
                    conflict_cb = get_order_fn(pc.conflicting_exchange_id)
                except Exception as exc:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"stacking_get_order_error:{exc}",
                    ))
                    continue

                # Validate the response before trusting it — any deviation from
                # what we expect must leave B untouched in the ledger.
                if conflict_cb is None:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        "cancelled_get_order_returned_none",
                    ))
                    continue
                if conflict_cb.exchange_order_id != pc.conflicting_exchange_id:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"cancelled_order_id_mismatch:"
                        f"expected={pc.conflicting_exchange_id},"
                        f"got={conflict_cb.exchange_order_id}",
                    ))
                    continue
                if conflict_cb.status != "CANCELLED":
                    # Exchange may not have propagated the state yet.
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"cancelled_unexpected_status:{conflict_cb.status}",
                    ))
                    continue

                fills_applied = 0
                stacked_exposure = False
                try:
                    with get_db(db_path) as conn:
                        if conflict_cb.fills:
                            already_applied = _already_applied_fills(
                                pc.conflicting_order_id, conn
                            )
                            new_fills = [
                                f for f in conflict_cb.fills
                                if f.exchange_fill_id not in already_applied
                            ]
                            if new_fills:
                                _apply_coinbase_fills(
                                    pc.conflicting_order_id, new_fills, conn,
                                    reconciliation_mode=True,
                                    already_applied=already_applied,
                                )
                                fills_applied = len(new_fills)
                        transition_order(pc.conflicting_order_id, "CANCELLED", conn=conn)
                        # Partial fills from the cancel window may have created an
                        # OPEN position for B while A's position is already OPEN —
                        # two OPEN positions for the same asset need human review.
                        if fills_applied > 0:
                            b_pos = conn.execute(
                                "SELECT 1 FROM positions"
                                " WHERE entry_order_id=? AND status='OPEN'",
                                (pc.conflicting_order_id,),
                            ).fetchone()
                            if b_pos:
                                stacked_exposure = True
                except Exception as exc:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"stacking_commit_error:{exc}",
                    ))
                    continue

                if stacked_exposure:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        "stacked_exposure_after_partial_fill",
                    ))
                else:
                    detail = f"cancelled conflicting entry {pc.conflicting_order_id}"
                    if fills_applied:
                        detail += (
                            f"; {fills_applied} partial fill(s) applied from cancel window"
                        )
                    resolved.append(ResolvedItem(
                        pc.triggering_order_id, pc.asset, "stacking_cancelled", detail=detail,
                    ))

    except Exception:
        # Ensure the run never stays in RUNNING state after an unexpected error.
        completed_at = datetime.now(timezone.utc).isoformat()
        with get_db(db_path) as conn:
            complete_reconciliation(
                run_id,
                discovered=discovered,
                resolved=[],
                unresolved=[{"order_id": "?", "asset": "?", "reason": "reconciler_exception"}],
                conn=conn,
            )
        raise

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
    Fails closed on malformed unresolved JSON — does not silently allow trading.

    DOES NOT block: signal scanner, EXIT orders, CANCEL orders.
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

    # Fail closed on malformed JSON — never silently allow trading.
    try:
        unres = json.loads(last["unresolved"] or "[]")
        if not isinstance(unres, list):
            return False, "last reconciliation has malformed unresolved field (not a list)"
    except (ValueError, TypeError):
        return False, "last reconciliation has unparseable unresolved field"

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
