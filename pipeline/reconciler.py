"""
Startup reconciliation gate.

See docs/adr/001-reconciliation-truth-model.md for the full truth model.

Five-phase design prevents holding SQLite write locks during network I/O:
  Phase A — short SQLite transactions, no network:
    Apply fills from list_orders_fn to OPEN/PARTIAL orders; attach
    exchange_order_id to SUBMITTING orders; detect position stacking;
    collect _PendingCancel descriptors.
  Phase B — network only, no SQLite:
    Issue cancel requests for stacking/TTL/late-fill conflicts.
  Phase C — short SQLite transactions:
    Commit confirmed cancel outcomes; transition SUBMITTING orders whose
    Coinbase status is terminal.
  Phase D — network + short SQLite transactions:
    For OPEN/PARTIAL orders not seen in list_orders_fn, call get_order_fn
    to fetch current status and apply any fills.
  Phase E — short SQLite transactions:
    Enforce TTL expiry and persistent-stacking invariant across positions.
    Blocked when stacked positions from a prior run are detected.
  Terminal enrichment (within Phase D):
    For CANCELLED/EXPIRED orders with exchange_order_id that have not yet
    been finalized (fills_finalized_at IS NULL), call get_order_fn to
    retrieve late fills.  None → UNRESOLVED.  Fills applied in
    reconciliation_mode.  fills_finalized_at set after identity/status
    validation + 10-minute settlement window.

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
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

_log = logging.getLogger(__name__)

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

# Minimum age of a terminal event before fills are finalized.  Prevents permanent
# finalization while the Coinbase read-model is still propagating after expiry/cancel.
_TERMINAL_SETTLEMENT_MINUTES = 10


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
    Stacking conflict found during Phase A/D.  Cancel must be issued in Phase B/E
    (no SQLite connection held) so a write lock is not held during network I/O.
    reason values: "LATE_FILL_STACKING" | "POSITION_STACKING" | "TTL_EXPIRED"
    """
    triggering_order_id: str
    conflicting_order_id: str
    conflicting_exchange_id: str
    asset: str
    reason: str = "POSITION_STACKING"


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
        # Do NOT catch RuntimeError here: apply_fill handles same-order replay
        # internally (returns early without raising).  Any exception that reaches
        # here — including "previously recorded for a different order" — is a
        # ledger-integrity violation that must propagate to the caller so the
        # reconciler can add it to UNRESOLVED rather than silently dropping it.
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
        reason="LATE_FILL_STACKING",
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

    # Identity check: production make_get_order_fn() returns the actual order_id
    # from the Coinbase response (not the input parameter) so mismatches are real.
    # A wrong exchange_order_id here means Coinbase returned a different order than
    # we asked for — do not apply fills or finalize.
    if cb_order.exchange_order_id != exchange_order_id:
        return None, UnresolvedItem(
            order_id, asset,
            f"terminal_exchange_id_mismatch:"
            f"expected={exchange_order_id},"
            f"got={cb_order.exchange_order_id}",
        ), None

    if cb_order.client_order_id != order_id:
        return None, UnresolvedItem(
            order_id, asset,
            f"terminal_client_id_mismatch:"
            f"expected={order_id},"
            f"got={cb_order.client_order_id}",
        ), None

    cb_status = cb_order.status

    # Status mismatch: local=terminal but Coinbase says still live.
    if cb_status in _CB_LIVE:
        return None, UnresolvedItem(
            order_id, asset,
            f"status_mismatch:local={local_status},cb={cb_status}",
        ), None

    # Unknown/unexpected status: FAILED, UNKNOWN, empty, etc. must not silently
    # produce a clean result that triggers finalization.  Only FILLED, CANCELLED,
    # EXPIRED are accepted terminal states.
    if cb_status not in _CB_TERMINAL_ACCEPTED:
        return None, UnresolvedItem(
            order_id, asset,
            f"unknown_coinbase_status:{cb_status}",
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


def _resolve_one_open_partial(
    *,
    order_id: str,
    asset: str,
    exchange_order_id: str,
    cb_order: CoinbaseOrder,
    conn,
) -> tuple[Optional[ResolvedItem], Optional[UnresolvedItem], Optional[_PendingCancel]]:
    """
    Reconcile one OPEN or PARTIAL ledger order against its Coinbase state.

    cb_order MUST be enriched with fills via get_order_fn (not from
    list_orders_fn which returns fills=[]).

    State table (mirrors _resolve_one_submitting but skips the OPEN transition
    because exchange_order_id is already set):
      OPEN/PENDING/QUEUED, no position  → apply new partial fills (if any) → open
      OPEN/PENDING/QUEUED + position    → return _PendingCancel for Phase E
                                          (position stacking — this ENTRY order
                                          coexists with an already-open position)
      CANCEL_QUEUED/PENDING_CANCEL      → apply new partial fills + UNRESOLVED(cancel_pending)
      FILLED (fills present)            → apply fills → ledger auto-FILLED → stacking check
      FILLED (no fills)                 → UNRESOLVED(filled_missing_fills)
      CANCELLED                         → apply partial fills → CANCELLED → stacking check
      EXPIRED                           → apply partial fills → EXPIRED → stacking check
      unknown status                    → UNRESOLVED(unknown_coinbase_status:X)
      exchange_order_id mismatch        → UNRESOLVED(exchange_id_mismatch:…)

    TTL expiry for live orders is handled in the Phase D loop (has access to
    expires_at from the snapshot); this function is only called after TTL check.
    """
    if cb_order.exchange_order_id != exchange_order_id:
        return None, UnresolvedItem(
            order_id, asset,
            f"exchange_id_mismatch:expected={exchange_order_id},"
            f"got={cb_order.exchange_order_id}",
        ), None

    cb_status = cb_order.status

    if cb_status not in _CB_LIVE | _CB_TERMINAL_ACCEPTED:
        return None, UnresolvedItem(order_id, asset, f"unknown_coinbase_status:{cb_status}"), None

    # Apply any new fills (idempotent via already_applied dedup).
    fills_applied = 0
    if cb_order.fills:
        already_applied = _already_applied_fills(order_id, conn)
        new_fills = [f for f in cb_order.fills if f.exchange_fill_id not in already_applied]
        if new_fills:
            _apply_coinbase_fills(
                order_id, new_fills, conn,
                reconciliation_mode=True,
                already_applied=already_applied,
            )
            fills_applied = len(new_fills)

    if cb_status in ("CANCEL_QUEUED", "PENDING_CANCEL"):
        return None, UnresolvedItem(order_id, asset, "cancel_pending"), None

    if cb_status == "FILLED":
        if not cb_order.fills:
            return None, UnresolvedItem(order_id, asset, "filled_missing_fills"), None
        # Fills were applied above; ledger auto-transitions on full fill.
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
        return ResolvedItem(order_id, asset, "filled"), None, None

    if cb_status == "CANCELLED":
        transition_order(order_id, "CANCELLED", conn=conn)
        if fills_applied:
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
        return ResolvedItem(order_id, asset, "cancelled"), None, None

    if cb_status == "EXPIRED":
        transition_order(order_id, "EXPIRED", conn=conn)
        if fills_applied:
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
        return ResolvedItem(order_id, asset, "expired"), None, None

    # OPEN / PENDING / QUEUED — still live.
    # Check for position stacking: if a position already exists for this asset,
    # this active ENTRY order should be cancelled to prevent double exposure.
    # Survives across runs: if Phase E cancel fails, the next run re-detects and retries.
    # Exclude positions created by THIS order's own fills — those are expected.
    # Stacking is only a problem when a DIFFERENT order's fills created a position.
    existing_pos = conn.execute(
        "SELECT 1 FROM positions"
        " WHERE asset=? AND status IN ('OPEN', 'CLOSING') AND entry_order_id != ?",
        (asset, order_id),
    ).fetchone()
    if existing_pos:
        return None, None, _PendingCancel(
            triggering_order_id=order_id,
            conflicting_order_id=order_id,
            conflicting_exchange_id=exchange_order_id,
            asset=asset,
        )

    detail = f"{fills_applied} new partial fill(s)" if fills_applied else ""
    return ResolvedItem(order_id, asset, "open", detail=detail), None, None


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
      Required for fill verification.  Used in three places:
        1. SUBMITTING orders found on Coinbase — enriches fills before
           _resolve_one_submitting (list_orders_fn returns fills=[]).
        2. Stacking resolution — fetches partial fills from cancel window.
        3. OPEN/PARTIAL reconciliation — fetches current fills on each run.
      If None: SUBMITTING orders are resolved with empty fills (partial fills
      on CANCELLED/EXPIRED orders are silently missed); OPEN/PARTIAL orders
      are not checked; stacking stays UNRESOLVED(cancelled_fills_unverified).
    """
    started_at = datetime.now(timezone.utc).isoformat()

    with get_db(db_path) as conn:
        run_id = start_reconciliation(conn)

    discovered: list[dict] = []
    resolved: list[ResolvedItem] = []
    unresolved: list[UnresolvedItem] = []

    try:
        # ── Persistent stacking invariant ────────────────────────────────────
        # Check before any Coinbase call: if positions are already stacked from
        # a previous run (e.g., Phase E stacked_exposure was not resolved), block
        # trading immediately.  Two OPEN/CLOSING positions for the same asset mean
        # two concurrent long positions — a risk invariant violation regardless of
        # whether new fills arrive this run.
        with get_db(db_path) as conn:
            _stacked = conn.execute(
                "SELECT asset, COUNT(*) AS cnt FROM positions"
                " WHERE status IN ('OPEN','CLOSING')"
                " GROUP BY asset HAVING cnt > 1"
            ).fetchall()
        for _sr in _stacked:
            unresolved.append(UnresolvedItem(
                order_id=f"position_stacking:{_sr['asset']}",
                asset=_sr["asset"],
                reason=f"existing_stacked_positions:count={_sr['cnt']}",
            ))

        # Step 2: snapshot — release SQLite before any Coinbase call.
        with get_db(db_path) as conn:
            submitting_rows = conn.execute(
                "SELECT id, asset FROM orders WHERE status='SUBMITTING'"
            ).fetchall()
            terminal_rows = conn.execute(
                "SELECT id, asset, exchange_order_id, status,"
                "       cancelled_at, expired_at"
                "  FROM orders"
                " WHERE status IN ('EXPIRED','CANCELLED')"
                "   AND exchange_order_id IS NOT NULL"
                "   AND fills_finalized_at IS NULL"
            ).fetchall()
            # OPEN/PARTIAL orders need per-order fill verification on each run.
            # Only query when get_order_fn is wired (v1 limitation: no orphan check).
            open_partial_rows = (
                conn.execute(
                    "SELECT id, asset, exchange_order_id, expires_at FROM orders"
                    " WHERE status IN ('OPEN','PARTIAL')"
                    "   AND exchange_order_id IS NOT NULL"
                ).fetchall()
                if get_order_fn is not None else []
            )

        discovered = [{"order_id": r["id"], "asset": r["asset"]} for r in submitting_rows]

        # In production mode (get_order_fn is wired), always call list_orders_fn()
        # to enable orphan detection — even when no local uncertain orders exist.
        # In test/DRY_RUN mode (get_order_fn=None), keep the fast path.
        _production_mode = get_order_fn is not None
        _need_list_orders = bool(submitting_rows or terminal_rows or _production_mode)

        if not _need_list_orders and not open_partial_rows:
            # Fast path: no uncertain orders, not production mode.
            # No Coinbase call needed.
            pass  # falls through to complete_reconciliation below

        else:
            # Step 3: fetch Coinbase (no SQLite connection held).
            if _need_list_orders:
                coinbase_orders: list[CoinbaseOrder] = list_orders_fn()
            else:
                coinbase_orders = []
            cb_by_client_id: dict[str, CoinbaseOrder] = {
                o.client_order_id: o for o in coinbase_orders
            }

            # ── Orphan detection ───────────────────────────────────────────────
            # In production mode, any Coinbase order whose client_order_id is not
            # in the local ledger is an orphan (JSON-path order, manual order, or
            # order placed by another process).  Must be UNRESOLVED so the trader
            # knows the complete picture before sizing or placing new orders.
            if _production_mode and coinbase_orders:
                with get_db(db_path) as conn:
                    _local_ids: set[str] = {
                        r[0] for r in conn.execute("SELECT id FROM orders").fetchall()
                    }
                for _cid, _cb in cb_by_client_id.items():
                    if _cid and _cid not in _local_ids:
                        unresolved.append(UnresolvedItem(
                            _cb.exchange_order_id or _cid,
                            "UNKNOWN",
                            f"orphan_coinbase_order:client_id={_cid}",
                        ))

            # ── Phase A: apply fills/transitions, detect stacking (no network) ──
            raw_pending: list[_PendingCancel] = []

            for row in submitting_rows:
                order_id, asset = row["id"], row["asset"]
                cb_order = cb_by_client_id.get(order_id)

                # Enrich fills before resolving.  list_orders_fn returns
                # CoinbaseOrder(fills=[]) — fills must come from get_order_fn.
                # Fail-closed: if enrichment fails, leave UNRESOLVED rather
                # than silently losing partial fills on CANCELLED/EXPIRED.
                if cb_order is not None and get_order_fn is not None:
                    try:
                        enriched = get_order_fn(cb_order.exchange_order_id)
                    except Exception as exc:
                        unresolved.append(
                            UnresolvedItem(order_id, asset, f"get_order_error:{exc}")
                        )
                        continue
                    if enriched is None:
                        # get_order_fn returns None on fill inconsistency or
                        # API error.  Do NOT fall back to empty-fills cb_order —
                        # that would silently drop partial fills.
                        unresolved.append(
                            UnresolvedItem(order_id, asset, "get_order_returned_none")
                        )
                        continue
                    cb_order = enriched

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
                exchange_order_id = row["exchange_order_id"]
                cb_order = cb_by_client_id.get(order_id)

                # Production mode: enrich terminal orders via get_order_fn to retrieve
                # actual fills (list_orders_fn always returns fills=[]).  None is
                # UNRESOLVED, not a safe fallback — make_get_order_fn() returns None
                # for transport errors and fill-aggregate mismatches, not only for a
                # confirmed order-not-found.
                _enrichment_ok = False
                if get_order_fn is not None:
                    try:
                        enriched = get_order_fn(exchange_order_id)
                    except Exception as exc:
                        unresolved.append(UnresolvedItem(
                            order_id, asset, f"terminal_get_order_error:{exc}"
                        ))
                        continue
                    # Treat None as UNRESOLVED — production make_get_order_fn()
                    # returns None for transport errors, incomplete pagination, and
                    # aggregate mismatches, not only for a confirmed order-not-found.
                    # Falling back to list_orders_fn (fills=[]) would silently drop
                    # partial fills on terminal orders.
                    if enriched is None:
                        unresolved.append(UnresolvedItem(
                            order_id, asset, "terminal_get_order_returned_none"
                        ))
                        continue
                    cb_order = enriched
                    _enrichment_ok = True

                try:
                    with get_db(db_path) as conn:
                        res, unres, pending = _check_late_fills_for_terminal_order(
                            order_id=order_id,
                            asset=asset,
                            exchange_order_id=exchange_order_id,
                            local_status=row["status"],
                            cb_order=cb_order,
                            conn=conn,
                        )
                        # Mark the order finalized so it is excluded from all
                        # future startup reconciliation runs.  Three conditions:
                        #  1. Production enrichment succeeded (not DRY_RUN/test).
                        #  2. No UNRESOLVED detected (status mismatch re-checks next run).
                        #  3. Settlement window: terminal event >= _TERMINAL_SETTLEMENT_MINUTES
                        #     old — prevents permanent finalization while the Coinbase
                        #     read-model is still propagating after expiry/cancel.
                        if _enrichment_ok and unres is None:
                            _terminal_ts = row["cancelled_at"] or row["expired_at"]
                            _past_settlement = False
                            if _terminal_ts:
                                try:
                                    _term_dt = datetime.fromisoformat(_terminal_ts)
                                    if _term_dt.tzinfo is None:
                                        _term_dt = _term_dt.replace(tzinfo=timezone.utc)
                                    _past_settlement = (
                                        datetime.now(timezone.utc) - _term_dt
                                        >= timedelta(minutes=_TERMINAL_SETTLEMENT_MINUTES)
                                    )
                                except ValueError:
                                    _log.warning(
                                        "malformed_terminal_timestamp order_id=%s ts=%r",
                                        order_id, _terminal_ts,
                                    )
                            if _past_settlement:
                                conn.execute(
                                    "UPDATE orders SET fills_finalized_at=? WHERE id=?",
                                    (datetime.now(timezone.utc).isoformat(), order_id),
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
                        # Check for a B position regardless of whether fills were
                        # applied this run — a previous crashed run may have
                        # already applied fills and created the position.
                        b_pos = conn.execute(
                            "SELECT 1 FROM positions"
                            " WHERE entry_order_id=? AND status IN ('OPEN', 'CLOSING')",
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

            # ── Phase D: OPEN/PARTIAL lifecycle ──────────────────────────────
            # Runs after Phase C so that orders cancelled by stacking resolution
            # are already CANCELLED in the ledger before this loop reaches them.
            # We re-check the current ledger status before each order — an order
            # that was transitioned to CANCELLED/EXPIRED by Phase C must be
            # skipped here to avoid a double-transition error.
            phase_d_pending: list[_PendingCancel] = []
            _run_now = datetime.now(timezone.utc)

            for row in open_partial_rows:
                order_id = row["id"]
                asset = row["asset"]
                exchange_order_id = row["exchange_order_id"]
                expires_at_str: str | None = row["expires_at"]

                # Re-check current status: Phase C (stacking cancel) may have
                # already transitioned this order since the snapshot was taken.
                with get_db(db_path) as conn:
                    current = conn.execute(
                        "SELECT status FROM orders WHERE id=?", (order_id,)
                    ).fetchone()
                if current is None or current["status"] not in ("OPEN", "PARTIAL"):
                    continue  # transitioned by an earlier phase this run — skip

                try:
                    cb_order = get_order_fn(exchange_order_id)  # type: ignore[misc]
                except Exception as exc:
                    unresolved.append(
                        UnresolvedItem(order_id, asset, f"get_order_error:{exc}")
                    )
                    continue
                if cb_order is None:
                    unresolved.append(
                        UnresolvedItem(order_id, asset, "get_order_returned_none")
                    )
                    continue

                # TTL check: if our local TTL has expired and the order is still
                # live on Coinbase (GTC orders don't auto-expire), queue for Phase E
                # cancellation instead of proceeding with normal resolution.
                _is_ttl_expired = False
                if (expires_at_str
                        and cb_order.status in _CB_LIVE
                        and cb_order.status not in ("CANCEL_QUEUED", "PENDING_CANCEL")):
                    try:
                        _exp = datetime.fromisoformat(expires_at_str)
                        if _exp.tzinfo is None:
                            _exp = _exp.replace(tzinfo=timezone.utc)
                        if _run_now > _exp:
                            _is_ttl_expired = True
                    except ValueError:
                        # Corrupted expires_at: fail-closed so a GTC order is never
                        # left unprotected by a silently disabled TTL guard.
                        unresolved.append(UnresolvedItem(
                            order_id, asset, "malformed_expires_at",
                        ))
                        continue

                if _is_ttl_expired:
                    phase_d_pending.append(_PendingCancel(
                        triggering_order_id=order_id,
                        conflicting_order_id=order_id,
                        conflicting_exchange_id=exchange_order_id,
                        asset=asset,
                        reason="TTL_EXPIRED",
                    ))
                    continue  # Phase E handles the cancel + fill verification

                try:
                    with get_db(db_path) as conn:
                        res, unres, pending = _resolve_one_open_partial(
                            order_id=order_id,
                            asset=asset,
                            exchange_order_id=exchange_order_id,
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
                    phase_d_pending.append(pending)  # handled by Phase E below

            # ── Phase E: cancel Phase D conflicts (position stacking + TTL) ──
            # Same pattern as Phase B+C but for conflicts detected in Phase D.
            # Dedup by conflicting_order_id first.
            seen_e_ids: set[str] = set()
            phase_e_cancels: list[_PendingCancel] = []
            for pc in phase_d_pending:
                if pc.conflicting_order_id not in seen_e_ids:
                    seen_e_ids.add(pc.conflicting_order_id)
                    phase_e_cancels.append(pc)

            # Phase E-B: issue cancels (no SQLite held).
            e_cancel_results: dict[str, bool] = {}
            e_cancel_errors: set[str] = set()
            for pc in phase_e_cancels:
                try:
                    confirmed = cancel_order_fn(pc.conflicting_exchange_id)
                    e_cancel_results[pc.conflicting_order_id] = confirmed
                except Exception as exc:
                    e_cancel_results[pc.conflicting_order_id] = False
                    e_cancel_errors.add(pc.conflicting_order_id)
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"position_stacking_cancel_error:{exc}",
                    ))

            # Phase E-C: verify fills and commit.
            for pc in phase_e_cancels:
                if pc.conflicting_order_id in e_cancel_errors:
                    continue  # already added to unresolved above

                confirmed = e_cancel_results.get(pc.conflicting_order_id, False)
                if not confirmed:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        "position_stacking_cancel_failed",
                    ))
                    continue

                # get_order_fn is always set in Phase D (it gates open_partial_rows).
                try:
                    conflict_cb = get_order_fn(pc.conflicting_exchange_id)  # type: ignore[misc]
                except Exception as exc:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"position_stacking_get_order_error:{exc}",
                    ))
                    continue
                if conflict_cb is None:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        "position_stacking_get_order_none",
                    ))
                    continue
                if conflict_cb.exchange_order_id != pc.conflicting_exchange_id:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"position_stacking_id_mismatch:"
                        f"expected={pc.conflicting_exchange_id},"
                        f"got={conflict_cb.exchange_order_id}",
                    ))
                    continue
                if conflict_cb.status != "CANCELLED":
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"position_stacking_unexpected_status:{conflict_cb.status}",
                    ))
                    continue

                _e_fills_applied = 0
                _e_stacked_exposure = False
                try:
                    with get_db(db_path) as conn:
                        if conflict_cb.fills:
                            _already = _already_applied_fills(pc.conflicting_order_id, conn)
                            _new = [f for f in conflict_cb.fills
                                    if f.exchange_fill_id not in _already]
                            if _new:
                                _apply_coinbase_fills(
                                    pc.conflicting_order_id, _new, conn,
                                    reconciliation_mode=True,
                                    already_applied=_already,
                                )
                                _e_fills_applied = len(_new)
                        transition_order(pc.conflicting_order_id, "CANCELLED", conn=conn)
                        # Stacking = two or more concurrent open positions for the same
                        # asset.  A partial fill during the cancel window may create an
                        # additional position alongside one that already existed.
                        # We count rather than checking entry_order_id because for TTL
                        # cancels the conflicting order IS the only order — its fill
                        # produces one position, which is fine; only a second position
                        # (from a pre-existing trade) constitutes stacked exposure.
                        _pos_count = conn.execute(
                            "SELECT COUNT(*) FROM positions"
                            " WHERE asset=? AND status IN ('OPEN', 'CLOSING')",
                            (pc.asset,),
                        ).fetchone()[0]
                        if _pos_count > 1:
                            _e_stacked_exposure = True
                except Exception as exc:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        f"position_stacking_commit_error:{exc}",
                    ))
                    continue
                if _e_stacked_exposure:
                    unresolved.append(UnresolvedItem(
                        pc.conflicting_order_id, pc.asset,
                        "stacked_exposure_after_partial_fill",
                    ))
                else:
                    _e_reason = pc.reason.lower()
                    _e_detail = f"cancelled {pc.conflicting_order_id}"
                    if _e_fills_applied:
                        _e_detail += (
                            f"; {_e_fills_applied} partial fill(s) applied from cancel window"
                        )
                    resolved.append(ResolvedItem(
                        pc.triggering_order_id, pc.asset,
                        f"{_e_reason}_cancelled",
                        detail=_e_detail,
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


def _gate_check_on_conn(
    conn,
    freshness_minutes: int,
) -> tuple[bool, str]:
    """
    Evaluate the ENTRY placement gate against an existing open connection.

    Called from within a BEGIN IMMEDIATE transaction so the check and any
    subsequent INSERT are atomic — no reconciliation run can complete between
    the check and the write while we hold the write lock.

    Same semantics as is_entry_placement_allowed() but accepts a connection
    rather than opening one.  Callers must NOT commit/rollback the connection.
    """
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

    For atomic enforcement inside the outbox TX-A, use _gate_check_on_conn()
    with the existing connection rather than this function.
    """
    with get_db(db_path) as conn:
        return _gate_check_on_conn(conn, freshness_minutes)
