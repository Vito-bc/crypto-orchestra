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

coinbase_sell_fn interface: Callable[[order_id: str, asset: str, qty_base: float], str]
  Returns exchange_order_id on success.
  Raise CoinbaseRejected for definitive refusals (400 + known code).
  Any other exception = ambiguous → leaves order SUBMITTING.
"""

from __future__ import annotations

import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from pipeline.ledger import (
    get_db,
    get_open_positions_for_asset,
    update_position_extensions,
    update_position_stop,
)
from pipeline.outbox import ExitPlaceResult, PlacementBlocked, place_exit_outbox
from pipeline.position_tracker import (
    BREAK_EVEN_PCT,
    EXTENSION_HOURS,
    MAX_EXTENSIONS,
    MAX_HOLD_HOURS,
    MAX_HOLD_HOURS_BY_ASSET,
    TRAIL_ACTIVATION_PCT,
    TRAIL_PCT,
)


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
    coinbase_sell_fn: Callable[[str, str, float], str],
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

    with get_db(db_path) as conn:
        positions = list(get_open_positions_for_asset(asset, conn))

    for pos in positions:
        pos_id = pos["id"]
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
                actions.append({
                    "position_id": pos_id, "asset": asset,
                    "exit_reason": None, "result": None,
                    "error": f"update_stop_failed:{exc}",
                })
                continue
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

        # ── Extension review when max-hold not yet exhausted ──────────────────
        if reason is None and _needs_extension_review(extensions_used, asset, pos["opened_at"]):
            if on_extension_review is not None:
                # Wrap Row in SimpleNamespace for attribute access (.entry_price etc.)
                pos_proxy = types.SimpleNamespace(**dict(pos))
                pos_proxy.stop_price = new_stop
                pos_proxy.high_water_mark = new_hwm
                extend = on_extension_review(pos_proxy)
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
        with get_db(db_path) as conn:
            already_active = _has_active_exit(pos_id, conn)
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

        # ── Place exit via two-transaction outbox ─────────────────────────────
        try:
            result = place_exit_outbox(
                position_id=pos_id,
                exit_reason=reason,
                coinbase_sell_fn=coinbase_sell_fn,
                db_path=db_path,
            )
        except PlacementBlocked as exc:
            actions.append({
                "position_id": pos_id, "asset": asset,
                "exit_reason": reason, "result": None,
                "note": f"placement_blocked:{exc}",
            })
            continue
        except Exception as exc:
            actions.append({
                "position_id": pos_id, "asset": asset,
                "exit_reason": reason, "result": None,
                "error": f"place_exit_failed:{exc}",
            })
            continue

        actions.append({
            "position_id": pos_id,
            "asset": asset,
            "exit_reason": reason,
            "result": result,
        })

    return actions
