# ADR 001 — Startup Reconciliation Truth Model

**Status:** Accepted  
**Date:** 2026-07-21  
**Applies to:** `pipeline/reconciler.py`, `pipeline/ledger.py`

---

## Context

Every system restart may have SUBMITTING orders in the ledger whose external
fate is unknown: the process may have crashed after TX-A committed but before
TX-B recorded the Coinbase response.  There may also be fills, cancellations,
or late fills that arrived after the process died.

We need a mandatory startup gate that resolves this uncertainty before any new
ENTRY placement is allowed.

---

## Decisions

### 1. "Not found" on Coinbase means UNRESOLVED, not REJECTED

**Rejected alternative:** mark a SUBMITTING order REJECTED if it is absent from
the Coinbase order list.

**Reason it is wrong:** Coinbase List Orders is paginated, has time-range
filters, is eventually consistent after order creation, and can return
incomplete results due to API errors or wrong portfolio scope.  "Not found"
has multiple causes only one of which is "never placed."  Treating absence as
definitive rejection can cause us to lose track of a live Coinbase order and
allow a duplicate ENTRY for the same asset.

**Decision:** not-found after an exhaustive, paginated search → leave as
SUBMITTING → add to `unresolved` in the reconciliation report → retry with
exponential backoff.  Only explicit `success=false` from Coinbase at
submission time (outbox TX-B path) produces REJECTED.

Retry policy: 3 attempts, 30-second backoff between attempts.  After 3
failures → permanent UNRESOLVED → human review required.

### 2. CANCELLED and EXPIRED are not REJECTED

**Why this matters:** CANCELLED and EXPIRED mean Coinbase accepted the order.
The order has an `exchange_order_id` and may have partial fills.  REJECTED
means Coinbase never accepted the order and cannot have fills.

**Decision:** when Coinbase returns CANCELLED or EXPIRED for a SUBMITTING
order, the reconciler must:
1. Attach the `exchange_order_id` (transition SUBMITTING → OPEN).
2. Fetch and apply any fills via `apply_fill(reconciliation_mode=True)`.
3. Transition OPEN → CANCELLED or EXPIRED.

Skipping step 1–2 would silently discard fills and misstate position size.

### 3. UNRESOLVED blocks new ENTRY placements, not signal evaluation

**Decision:** while any `unresolved` items exist:
- New ENTRY placement is **blocked** for all assets.
- Signal scanner continues; shadow journal continues.
- Risk-reducing EXIT orders and CANCEL orders remain permitted.

**Reason:** losing OOS observations by halting the scanner is irreversible.
Blocking new capital deployment is reversible once unresolved items clear.

### 4. Late-fill stacking: record the fill, then cancel the active ENTRY

**Scenario:** order A is EXPIRED in the ledger.  Order B (new ENTRY for the
same asset) is placed after A expires (the stacking guard correctly allows it).
Reconciler discovers that Coinbase filled order A (late fill, e.g. a race with
the expiry).

**Decision:**
1. Apply the fill for order A → creates a position for A's asset.
2. Detect stacking: position now exists AND order B (OPEN/PARTIAL) exists for
   the same asset.
3. Issue `cancel_order_fn(B.exchange_order_id)` to Coinbase.
4. If cancel is confirmed: transition B to CANCELLED in the ledger.
5. If cancel fails or is ambiguous: add B to `unresolved`; new ENTRY for that
   asset remains blocked until resolution.

**Why not discard the fill:** fills are immutable facts (see CONTEXT.md).  The
late fill created real exposure.  Ignoring it to preserve a cleaner local state
would misstate position size and P&L.

**Why cancel the newer order, not the older position:** the position from order
A reflects real exchange execution.  Order B has no fills yet and is the
recoverable one.

### 5. Freshness threshold on reconciliation runs

**Decision:** `is_entry_placement_allowed()` requires the last completed
reconciliation to be within `freshness_minutes` (default: 60 minutes).  A
stale reconciliation is treated the same as no reconciliation.

**Reason:** a reconciliation that ran 3 hours ago cannot speak to fills or
cancellations that arrived in the last 3 hours.  The threshold is configurable
for testing.

### 6. Reconciliation scope for initial implementation (v1)

The first implementation covers:
- SUBMITTING orders → resolve or UNRESOLVED
- Late-fill stacking detection → cancel + UNRESOLVED if unconfirmed
- Gate function: `is_entry_placement_allowed()`

Out of scope for v1 (subsequent iterations):
- OPEN/PARTIAL order re-verification against Coinbase
- Orphan Coinbase order detection (orders on Coinbase not in our ledger)
- Available/hold balance reconciliation
- Account NAV reconciliation

These are logged as v2 scope.  v1 is sufficient as a gate before shadow wiring.

---

## State Transition Table for SUBMITTING Orders

| SUBMITTING | Coinbase status | Coinbase fills | Action |
|---|---|---|---|
| Found | OPEN / PENDING | (any) | Attach exchange_order_id → OPEN; apply new fills |
| Found | FILLED | present | Attach exchange_order_id → OPEN; apply all fills → FILLED |
| Found | CANCELLED | none | Attach exchange_order_id → OPEN → CANCELLED |
| Found | CANCELLED | present | Attach exchange_order_id → OPEN; apply fills → CANCELLED |
| Found | EXPIRED | none | Attach exchange_order_id → OPEN → EXPIRED |
| Found | EXPIRED | present | Attach exchange_order_id → OPEN; apply fills → EXPIRED |
| Not found | — | — | Leave SUBMITTING; add to unresolved; retry N times |
| Not found after N retries | — | — | Permanent UNRESOLVED; human review |

After applying fills: check for stacking conflict (see Decision 4).

---

## Consequences

- SUBMITTING orders are never silently abandoned: they are either resolved or
  explicitly UNRESOLVED with human escalation.
- Partial fills on CANCELLED/EXPIRED orders are captured; position size is
  always accurate.
- The gate prevents new capital deployment when state is uncertain; it does not
  stop observation or risk-reducing actions.
- The stacking guard (outbox TX-A gate check) prevents most stacking scenarios
  at placement time; reconciler handles the residual late-fill race.
