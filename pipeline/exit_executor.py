"""
Ledger-based EXIT executor.

Reads OPEN/CLOSING positions from the SQLite ledger for a given asset,
evaluates stop-loss / take-profit / max-hold conditions, and places SELL
orders via the two-transaction outbox pattern.

Guaranteed properties:
  1. One active EXIT per position — enforced by idx_one_active_exit_per_position.
  2. TX-A records the SUBMITTING EXIT intent before any network call.
  3. Ambiguous Coinbase response leaves the order SUBMITTING; startup reconciler
     resolves it on the next boot.
  4. Repeated pipeline ticks are idempotent: if an active EXIT already exists
     for a position, no second SELL is placed.
  5. ENTRY gates and circuit breakers are never consulted — EXIT is always
     risk-reducing and must never be blocked by speculative guards.
  6. Product rules come from product_state.get_rules_for_exit() (cache → LKG →
     defaults), never from a live Coinbase call. A metadata outage degrades the
     rounding precision and raises an alert; it never skips the SELL.

coinbase_sell_fn interface: Callable[[order_id: str, asset: str, qty_base: float], str]
  Returns exchange_order_id on success.
  Raise CoinbaseRejected for definitive refusals (400 + known code).
  Any other exception = ambiguous → leaves order SUBMITTING.
"""

from __future__ import annotations

import logging
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

_log = logging.getLogger(__name__)

from pipeline.ledger import (
    get_db,
    get_dust_positions,
    get_open_positions_for_asset,
    update_position_extensions,
    update_position_stop,
)
from pipeline.outbox import PlacementBlocked, place_exit_outbox
from pipeline.product_state import get_rules_for_exit
from pipeline.position_tracker import (
    BREAK_EVEN_PCT,
    EXTENSION_HOURS,
    MAX_EXTENSIONS,
    MAX_HOLD_HOURS,
    MAX_HOLD_HOURS_BY_ASSET,
    TRAIL_ACTIVATION_PCT,
    TRAIL_PCT,
)


# Cooldown for "EXIT rounded on degraded product rules" alerts, keyed by asset.
# The value is a monotonic deadline: alerts for that asset are suppressed until
# time.monotonic() passes it.  Two cooldown lengths, so a failed send retries
# soon without a same-pass storm, and a delivered alert stays quiet for an hour:
#   • delivered  → _RULES_ALERT_OK_COOLDOWN_S (1h): the operator has been told.
#   • send failed → _RULES_ALERT_RETRY_COOLDOWN_S (60s): try again next tick, but
#     not again within this pass.
_rules_alert_until: dict[str, float] = {}
_RULES_ALERT_OK_COOLDOWN_S    = 3600.0
_RULES_ALERT_RETRY_COOLDOWN_S = 60.0

# DUST_SETTLED event ids whose alert has been CONFIRMED delivered.  A DUST alert
# must reach the operator at least once: the position is durable exposure that
# blocks ENTRY and epoch changes until manually written off, and once DUST it no
# longer appears in any OPEN/CLOSING exit pass, so a lost alert would never be
# retried on its own.  The key is the position_event id of the latest DUST_SETTLED
# event — NOT the position id — because a position may go DUST → CLOSING
# (DUST_REVIVED) → DUST again, and each DUST episode writes a new event id, so the
# re-dusted exposure is a genuinely new notification.  In-memory is sufficient: a
# restart re-sends once (harmless at-least-once); the ledger is the durable record.
_dust_delivered: set[int] = set()

# Short monotonic retry deadlines, keyed by DUST_SETTLED event id, for alerts
# whose send failed.  Within one scheduler pass the per-asset sweep and the
# global Step 3b sweep run milliseconds apart; without this an undelivered alert
# would be retried immediately, so a network timeout could cost two ~15s stalls
# per asset in a single pass.  The deadline suppresses the intra-pass retry while
# still allowing the next (hourly) pass to try again.
_dust_retry_until: dict[int, float] = {}
_DUST_RETRY_COOLDOWN_S = 60.0


def sweep_dust_alerts(db_path: Optional[Path] = None, asset: Optional[str] = None) -> None:
    """
    Re-notify for every unresolved DUST position until the alert is delivered.

    Reads DUST positions from the ledger (durable) rather than reacting only to
    the moment of transition, so a Telegram failure does not permanently lose a
    DUST notification — a later pass re-sends it.  Delivery identity is the latest
    DUST_SETTLED event id, so a re-dusted position is alerted again.  A failed
    send arms a short per-event retry deadline so the same pass does not retry
    (and stall) twice.  Never raises.
    """
    try:
        with get_db(db_path) as conn:
            rows = get_dust_positions(asset, conn)
    except Exception as exc:
        _log.warning("dust_sweep_db_read_failed asset=%s exc=%s", asset, exc)
        return

    now = time.monotonic()
    pending = []
    for r in rows:
        eid = r["dust_event_id"]
        if eid is None:
            # Degenerate: DUST with no DUST_SETTLED event. Alert without dedup
            # (at-least-once is the safe direction) but do not track.
            _log.warning("dust_position_without_event pos=%s", r["id"])
            pending.append((None, r))
            continue
        if eid in _dust_delivered:
            continue
        retry_at = _dust_retry_until.get(eid)
        if retry_at is not None and now < retry_at:
            continue   # intra-pass / cooldown suppression
        pending.append((eid, r))

    if not pending:
        return

    by_asset: dict[str, list] = {}
    for eid, r in pending:
        by_asset.setdefault(r["asset"], []).append((eid, r))

    from notifications.telegram import send_telegram_message
    for a, items in by_asset.items():
        try:
            lines = "\n".join(
                f"  pos={r['id']}  qty_base_remaining={r['qty_base_remaining']}"
                for _eid, r in items
            )
            msg = (
                f"CRITICAL: DUST position(s) — manual action required\n"
                f"asset={a}  count={len(items)}\n"
                f"{lines}\n"
                "Residual open exposure that blocks ENTRY and epoch changes until "
                "resolved.\n"
                "Action: write off manually or wait for a fill that restores qty."
            )
            _log.warning(msg)
            delivered = bool(send_telegram_message(msg))
        except Exception as exc:
            _log.warning("dust_sweep_send_failed asset=%s exc=%s", a, exc)
            delivered = False

        for eid, _r in items:
            if eid is None:
                continue
            if delivered:
                _dust_delivered.add(eid)
                _dust_retry_until.pop(eid, None)
            else:
                _dust_retry_until[eid] = now + _DUST_RETRY_COOLDOWN_S
        if not delivered:
            _log.warning(
                "dust_alert_undelivered asset=%s count=%d — retry after %.0fs",
                a, len(items), _DUST_RETRY_COOLDOWN_S)


def _flush_degraded_exit_alert(asset: str, events: list[dict]) -> None:
    """
    Send ONE aggregated degraded-rules alert for an asset, AFTER every EXIT for
    that asset has been placed.

    Two problems this closes versus alerting inline per position:
      • A Telegram send can block on a network timeout (up to 15s in
        notifications/telegram.py).  Doing it inside the per-position loop, before
        place_exit_outbox, delays a risk-reducing SELL by that timeout — and once
        per degraded position.  Collecting events and flushing here means the
        network call happens only after all SELLs are on the wire.
      • With no cooldown armed on a failed send, the next degraded position in the
        same pass would immediately retry, so several positions could each eat a
        15s timeout back to back.  A single aggregated send per asset per pass,
        plus a short retry cooldown, removes the storm.

    Never raises — an alerting failure must not affect EXIT.
    """
    if not events:
        return
    try:
        now = time.monotonic()
        until = _rules_alert_until.get(asset)
        if until is not None and now < until:
            _log.warning(
                "exit_rules_degraded asset=%s positions=%d (alert suppressed — cooldown)",
                asset, len(events))
            return

        first = events[0]
        source = first.get("source", "unknown")
        age_note = ""
        if first.get("age_s") is not None:
            age_note = f"  age={first['age_s'] / 3600:.1f}h"
        pos_list = ", ".join(e["pos_id"][:8] for e in events)
        # Aggregate the real placement outcomes so the alert states what happened
        # rather than assuming every degraded EXIT was accepted.
        outcomes: dict[str, int] = {}
        for e in events:
            key = e.get("outcome", "UNKNOWN")
            outcomes[key] = outcomes.get(key, 0) + 1
        outcome_str = ", ".join(f"{k}={v}" for k, v in sorted(outcomes.items()))
        msg = (
            f"[ExitExecutor] EXIT attempted using DEGRADED product rules\n"
            f"asset={asset}  positions={len(events)} [{pos_list}]\n"
            f"outcomes: {outcome_str}\n"
            f"source={source}{age_note}  "
            f"base_increment={first.get('base_increment')}  "
            f"base_min_size={first.get('base_min_size')}\n"
            "Rounding was not based on live exchange metadata. Verify each "
            "outcome above — any REJECTED/FAILED/SUBMITTING needs Coinbase "
            "connectivity and the LKG file checked."
        )
        _log.warning(msg)
        from notifications.telegram import send_telegram_message
        # A delivered alert earns the long quiet window; a failed send earns only
        # the short retry window, so the next tick can try again without this pass
        # hammering a dead endpoint.
        if send_telegram_message(msg):
            _rules_alert_until[asset] = now + _RULES_ALERT_OK_COOLDOWN_S
        else:
            _rules_alert_until[asset] = now + _RULES_ALERT_RETRY_COOLDOWN_S
            _log.warning(
                "degraded_rules_alert_send_failed asset=%s — retry after %.0fs",
                asset, _RULES_ALERT_RETRY_COOLDOWN_S)
    except Exception as exc:
        _log.warning("degraded_rules_alert_failed asset=%s exc=%s", asset, exc)


def _held_hours(opened_at: Optional[str]) -> float:
    if not opened_at:
        return 0.0
    try:
        t0 = datetime.fromisoformat(opened_at)
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t0).total_seconds() / 3600
    except ValueError:
        return 0.0


def _effective_hold_limit(extensions_used: int, asset: str) -> float:
    return MAX_HOLD_HOURS_BY_ASSET.get(asset, MAX_HOLD_HOURS) + extensions_used * EXTENSION_HOURS


def _compute_trailing_stop(
    current_price: float,
    entry_price: float,
    current_stop: float,
    high_water_mark: float,
) -> float:
    """Return new stop price (monotonically non-decreasing)."""
    new_stop = current_stop
    hwm = max(high_water_mark, current_price)
    if current_price >= entry_price * (1 + BREAK_EVEN_PCT):
        new_stop = max(new_stop, entry_price)
    if hwm >= entry_price * (1 + TRAIL_ACTIVATION_PCT):
        new_stop = max(new_stop, round(hwm * (1 - TRAIL_PCT), 2))
    return round(new_stop, 2)


def _check_exit_condition(
    stop_price: float,
    target_price: Optional[float],
    extension_trailing_stop: Optional[float],
    extensions_used: int,
    asset: str,
    opened_at: Optional[str],
    current_price: float,
) -> Optional[str]:
    if current_price <= stop_price:
        return "STOP_LOSS"
    if extension_trailing_stop and current_price <= extension_trailing_stop:
        return "STOP_LOSS"
    if target_price and current_price >= target_price:
        return "TAKE_PROFIT"
    hours = _held_hours(opened_at)
    if extensions_used >= MAX_EXTENSIONS and hours >= _effective_hold_limit(extensions_used, asset):
        return "MAX_HOLD"
    return None


def _needs_extension_review(extensions_used: int, asset: str, opened_at: Optional[str]) -> bool:
    return (
        extensions_used < MAX_EXTENSIONS
        and _held_hours(opened_at) >= _effective_hold_limit(extensions_used, asset)
    )


def _has_active_exit(position_id: str, conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM orders"
        " WHERE position_id=? AND purpose='EXIT'"
        "   AND status IN ('SUBMITTING','OPEN','PARTIAL')",
        (position_id,),
    ).fetchone() is not None


def run_exit_executor(
    asset: str,
    current_price: float,
    coinbase_sell_fn: Callable[[str, str, str], str],
    db_path: Optional[Path] = None,
    on_extension_review: Optional[Callable] = None,
) -> list[dict]:
    """
    Evaluate all OPEN/CLOSING ledger positions for `asset`. For each:
      - Update HWM and trailing stop in the ledger.
      - Check stop-loss / take-profit / max-hold exit conditions.
      - If extension review applies: call on_extension_review (if provided).
          True  → persist extensions_used + 1, skip exit this tick.
          False → treat as MAX_HOLD, place exit.
      - Place a SELL order via place_exit_outbox if an exit is triggered.
      - Skip positions that already have an active EXIT order (idempotent).

    Never raises: exceptions are caught per-position and included in the
    returned action dict so other positions are still evaluated.

    Returns list of action dicts, one per evaluated position with fields:
      position_id, asset, exit_reason, result (ExitPlaceResult | None),
      and optionally: note (str) or error (str).
    """
    actions: list[dict] = []
    # Degraded-rules notifications are collected here and flushed once, after
    # every placement attempt, so a blocking Telegram send can never sit in front
    # of a SELL.  DUST notifications are handled separately by sweep_dust_alerts,
    # which reads durable DUST state from the ledger and retries until delivered.
    degraded_events: list[dict] = []

    try:
        # Pure SELECT — no RESERVED lock needed.
        with get_db(db_path, begin_immediate=False) as conn:
            positions = list(get_open_positions_for_asset(asset, conn))
    except Exception as exc:
        return [{"position_id": "UNKNOWN", "asset": asset, "exit_reason": None,
                 "result": None, "error": f"db_read_positions_failed:{exc}"}]

    for pos in positions:
        pos_id = pos["id"]
        try:
            entry_price = pos["entry_price"] or current_price
            stop_price = pos["stop_price"] or 0.0
            hwm = pos["high_water_mark"] or entry_price
            extensions_used = pos["extensions_used"] or 0

            # ── Update HWM + trailing stop ────────────────────────────────────────
            new_hwm = max(hwm, current_price)
            new_stop = _compute_trailing_stop(current_price, entry_price, stop_price, new_hwm)
            if new_hwm != hwm or new_stop != stop_price:
                try:
                    with get_db(db_path) as conn:
                        update_position_stop(pos_id, new_stop, new_hwm, conn=conn)
                except Exception as exc:
                    # Log but do NOT skip — a hard stop may already be crossed.
                    # Proceed with exit check using the last known stop_price.
                    _log.warning(
                        "update_stop_failed pos=%s exc=%s — continuing with exit check",
                        pos_id, exc)
                    new_stop = stop_price
            else:
                new_stop = stop_price

            # ── Check immediate exit condition ────────────────────────────────────
            reason = _check_exit_condition(
                stop_price=new_stop,
                target_price=pos["target_price"],
                extension_trailing_stop=pos["extension_trailing_stop"],
                extensions_used=extensions_used,
                asset=asset,
                opened_at=pos["opened_at"],
                current_price=current_price,
            )

            # ── CLOSING positions always continue their exit ──────────────────────
            # CLOSING means an exit was already placed (partial fill) or DUST was
            # revived by a late ENTRY fill.  The remainder must be sold even if the
            # current price has recovered above the stop — do not defer via extension
            # review; skip straight to placing the sell.
            if reason is None and pos["status"] == "CLOSING":
                reason = "CONTINUE_EXIT"

            # ── Extension review when max-hold not yet exhausted ──────────────────
            if reason is None and _needs_extension_review(extensions_used, asset, pos["opened_at"]):
                if on_extension_review is not None:
                    # Wrap Row in SimpleNamespace for attribute access (.entry_price etc.)
                    try:
                        pos_proxy = types.SimpleNamespace(**dict(pos))
                        pos_proxy.stop_price = new_stop
                        pos_proxy.high_water_mark = new_hwm
                        extend = on_extension_review(pos_proxy)
                    except Exception as exc:
                        _log.warning(
                            "extension_review_callback_failed pos=%s exc=%s — treating as MAX_HOLD",
                            pos_id, exc)
                        extend = False
                        pos_proxy = None
                    if extend:
                        ext_stop = getattr(pos_proxy, "extension_trailing_stop", None)
                        try:
                            with get_db(db_path) as conn:
                                update_position_extensions(
                                    pos_id,
                                    extensions_used=extensions_used + 1,
                                    extension_trailing_stop=ext_stop,
                                    conn=conn,
                                )
                        except Exception as exc:
                            actions.append({
                                "position_id": pos_id, "asset": asset,
                                "exit_reason": None, "result": None,
                                "error": f"update_extensions_failed:{exc}",
                            })
                        else:
                            actions.append({
                                "position_id": pos_id, "asset": asset,
                                "exit_reason": None, "result": None,
                                "note": "extension_granted",
                            })
                        continue
                    else:
                        reason = "MAX_HOLD"
                else:
                    reason = "MAX_HOLD"

            if reason is None:
                continue

            # ── Skip if active EXIT already exists (idempotent) ───────────────────
            try:
                # Advisory pre-check only: place_exit_outbox re-checks this
                # authoritatively inside TX-A's BEGIN IMMEDIATE, so a read-only
                # transaction here is sufficient and avoids competing for RESERVED.
                with get_db(db_path, begin_immediate=False) as conn:
                    already_active = _has_active_exit(pos_id, conn)
            except Exception as exc:
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": reason, "result": None,
                    "error": f"active_exit_check_failed:{exc}",
                })
                continue
            if already_active:
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": reason, "result": None,
                    "note": "active_exit_already_exists",
                })
                continue

            qty_base = pos["qty_base_remaining"] or 0.0
            if qty_base <= 0:
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": reason, "result": None,
                    "error": "zero_qty_base_remaining",
                })
                continue

            # Resolve exchange product rules before acquiring the write lock —
            # never under the BEGIN IMMEDIATE lock, per the outbox TX-A principle.
            #
            # This MUST NOT be a live Coinbase call.  get_rules_for_exit resolves
            # cache → LKG → defaults and never raises, so a Coinbase metadata
            # outage can no longer skip a triggered stop-loss.  Previously this
            # called get_product_info() directly and `continue`d on failure,
            # deferring the SELL until the next scheduler tick ~60 minutes later.
            _rules = get_rules_for_exit(asset)
            _base_increment = _rules["base_increment"]
            _base_min_size  = _rules["base_min_size"]
            _degraded = bool(_rules.get("stale"))

            def _note_degraded(outcome: str) -> None:
                # Record the degraded-rules use WITH the real placement outcome,
                # collected here (not sent) so a blocking Telegram call never
                # delays a SELL, and so the aggregated alert reports what actually
                # happened rather than assuming the SELL landed.  A degraded EXIT
                # that was REJECTED/DUST/FAILED must not read as "placed".
                if _degraded:
                    degraded_events.append(
                        {"pos_id": pos_id, "outcome": outcome, **_rules}
                    )

            # ── Place exit via two-transaction outbox ─────────────────────────────
            try:
                result = place_exit_outbox(
                    position_id=pos_id,
                    exit_reason=reason,
                    coinbase_sell_fn=coinbase_sell_fn,
                    db_path=db_path,
                    base_increment=_base_increment,
                    base_min_size=_base_min_size,
                )
            except PlacementBlocked as exc:
                exc_str = str(exc)
                if exc_str.startswith("DUST:"):
                    # The position is now DUST in the ledger; sweep_dust_alerts
                    # (below, and again in run_all_assets) reads it from durable
                    # state and retries the alert until delivered.
                    _note_degraded("DUST")
                else:
                    _note_degraded("BLOCKED")
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": reason, "result": None,
                    "note": f"placement_blocked:{exc}",
                })
                continue
            except Exception as exc:
                _note_degraded("FAILED")
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": reason, "result": None,
                    "error": f"place_exit_failed:{exc}",
                })
                continue

            # result.status is the truthful outcome: OPEN, SUBMITTING or REJECTED.
            _note_degraded(getattr(result, "status", "UNKNOWN"))
            actions.append({
                "position_id": pos_id,
                "asset": asset,
                "exit_reason": reason,
                "result": result,
            })

        except Exception as exc:
            actions.append({
                "position_id": pos_id, "asset": asset,
                "exit_reason": None, "result": None,
                "error": f"unexpected_per_position_error:{exc}",
            })

    # Every placement attempt for this asset is complete.  Only now do we make
    # any (possibly blocking) Telegram call, so no notification ever sits in
    # front of a risk-reducing SELL.  The DUST sweep reads durable ledger state
    # and retries any previously undelivered alert.
    sweep_dust_alerts(db_path=db_path, asset=asset)
    _flush_degraded_exit_alert(asset, degraded_events)

    return actions
