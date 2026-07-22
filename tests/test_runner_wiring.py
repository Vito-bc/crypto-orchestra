"""
Tests for pipeline/runner.py startup wiring (P1 coverage).

Verifies the four properties identified in the audit:
  1. make_list_orders_fn() calls list_reconciliation_orders (not get_orders)
  2. Network failure propagates from make_list_orders_fn(), not silently []
  3. UNRESOLVED items → Telegram alert + _startup_reconciliation() returns False
  4. run_all_assets(target_asset=...) still runs startup reconciliation (no bypass)

No real DB or network calls are made in any test.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 1. make_list_orders_fn() uses list_reconciliation_orders
# ---------------------------------------------------------------------------

def test_list_orders_fn_calls_reconciliation_fn(monkeypatch):
    """make_list_orders_fn() must call list_reconciliation_orders, not any other method."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    call_log: list[str] = []

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "list_reconciliation_orders", lambda **kw: (
        call_log.append("list_reconciliation_orders") or []
    ))

    fn = _adapter.make_list_orders_fn()
    result = fn()

    assert "list_reconciliation_orders" in call_log
    assert result == []


def test_list_orders_fn_returns_empty_in_dry_run(monkeypatch):
    """In DRY_RUN mode, make_list_orders_fn() must return [] without any API call."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    monkeypatch.setattr(_cb, "_DRY_RUN", True)

    called = []
    monkeypatch.setattr(_cb, "list_reconciliation_orders", lambda **kw: (
        called.append(1) or []
    ))

    fn = _adapter.make_list_orders_fn()
    result = fn()

    assert result == []
    assert called == [], "list_reconciliation_orders must not be called in DRY_RUN"


# ---------------------------------------------------------------------------
# 2. Network failure propagates — not silently []
# ---------------------------------------------------------------------------

def test_list_orders_fn_propagates_network_failure(monkeypatch):
    """
    An exchange outage must raise, not return [].

    If make_list_orders_fn() swallows the exception it would return [] and the
    reconciler would conclude there are no pending orders (false clean state).
    This is explicitly fail-closed: the exception must reach the caller.
    """
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(
        _cb, "list_reconciliation_orders",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("exchange timeout")),
    )

    fn = _adapter.make_list_orders_fn()
    with pytest.raises(RuntimeError, match="exchange timeout"):
        fn()


def test_list_orders_fn_propagates_auth_failure(monkeypatch):
    """Auth errors must also propagate (not be absorbed)."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    class FakeCBError(Exception):
        pass

    monkeypatch.setattr(_cb, "_DRY_RUN", False)

    def _raise(**kw):
        raise FakeCBError("401 unauthorized")

    monkeypatch.setattr(_cb, "list_reconciliation_orders", _raise)

    fn = _adapter.make_list_orders_fn()
    with pytest.raises(FakeCBError):
        fn()


# ---------------------------------------------------------------------------
# 3. _startup_reconciliation() — UNRESOLVED → Telegram + return False
# ---------------------------------------------------------------------------

from pipeline.reconciler import UnresolvedItem, ReconciliationReport


def _now_str() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _clean_report() -> ReconciliationReport:
    return ReconciliationReport(
        run_id=1, discovered=[], resolved=[], unresolved=[],
        started_at=_now_str(), completed_at=_now_str(),
    )


def _unresolved_report() -> ReconciliationReport:
    return ReconciliationReport(
        run_id=1, discovered=[], resolved=[],
        unresolved=[
            UnresolvedItem(
                order_id=str(uuid.uuid4()),
                asset="ZEC-USD",
                reason="not_found",
            )
        ],
        started_at=_now_str(), completed_at=_now_str(),
    )


# _startup_reconciliation() uses lazy `from X import Y` inside the function,
# so we patch the source module attributes, not pipeline.runner attributes.

def test_startup_reconciliation_returns_false_on_exception():
    """When run_startup_reconciliation raises, _startup_reconciliation() must return False."""
    from pipeline.runner import _startup_reconciliation

    telegram_calls: list[str] = []

    with (
        patch("pipeline.ledger.run_migrations"),
        patch(
            "pipeline.reconciler.run_startup_reconciliation",
            side_effect=RuntimeError("exchange is down"),
        ),
        patch("exchange.adapter.make_list_orders_fn", return_value=lambda: []),
        patch("exchange.adapter.make_get_order_fn", return_value=lambda eid: None),
        patch("exchange.coinbase_client.cancel_order", return_value=True),
        patch("exchange.coinbase_client.is_dry_run", return_value=True),
        patch(
            "pipeline.runner.send_telegram_message",
            side_effect=lambda msg: telegram_calls.append(msg),
        ),
    ):
        result = _startup_reconciliation()

    entry_ok, _report = result
    assert entry_ok is False
    assert any("exchange is down" in m for m in telegram_calls), (
        "Telegram alert must include the exception message"
    )


def test_startup_reconciliation_returns_false_on_unresolved():
    """When UNRESOLVED items exist, _startup_reconciliation() returns False + alerts Telegram."""
    from pipeline.runner import _startup_reconciliation

    telegram_calls: list[str] = []
    report = _unresolved_report()

    with (
        patch("pipeline.ledger.run_migrations"),
        patch("pipeline.reconciler.run_startup_reconciliation", return_value=report),
        patch("exchange.adapter.make_list_orders_fn", return_value=lambda: []),
        patch("exchange.adapter.make_get_order_fn", return_value=lambda eid: None),
        patch("exchange.coinbase_client.cancel_order", return_value=True),
        patch("exchange.coinbase_client.is_dry_run", return_value=True),
        patch(
            "pipeline.runner.send_telegram_message",
            side_effect=lambda msg: telegram_calls.append(msg),
        ),
    ):
        result = _startup_reconciliation()

    entry_ok, _report = result
    assert entry_ok is False
    u = report.unresolved[0]
    assert any(u.order_id in m for m in telegram_calls), (
        "Telegram alert must include the unresolved order_id"
    )
    assert any(u.asset in m for m in telegram_calls), (
        "Telegram alert must include the unresolved asset"
    )


def test_startup_reconciliation_returns_true_on_clean():
    """When reconciliation completes cleanly, _startup_reconciliation() returns True."""
    from pipeline.runner import _startup_reconciliation

    with (
        patch("pipeline.ledger.run_migrations"),
        patch("pipeline.reconciler.run_startup_reconciliation", return_value=_clean_report()),
        patch("exchange.adapter.make_list_orders_fn", return_value=lambda: []),
        patch("exchange.adapter.make_get_order_fn", return_value=lambda eid: None),
        patch("exchange.coinbase_client.cancel_order", return_value=True),
        patch("exchange.coinbase_client.is_dry_run", return_value=True),
        patch("pipeline.runner.send_telegram_message"),
    ):
        result = _startup_reconciliation()

    entry_ok, _report = result
    assert entry_ok is True


# ---------------------------------------------------------------------------
# 4. run_all_assets(target_asset=...) goes through startup reconciliation
# ---------------------------------------------------------------------------

def test_run_all_assets_single_asset_calls_startup_reconciliation():
    """
    python pipeline/runner.py ZEC-USD must not bypass startup reconciliation.
    run_all_assets(target_asset="ZEC-USD") must invoke _startup_reconciliation().
    """
    from pipeline.runner import run_all_assets

    startup_called = []

    # _startup_reconciliation returns (False, None) → run_all_assets returns {} without running pipeline
    with patch("pipeline.runner._startup_reconciliation", side_effect=lambda: (
        startup_called.append(True) or (False, None)
    )):
        result = run_all_assets(target_asset="ZEC-USD")

    assert startup_called, "_startup_reconciliation() was not called"
    assert result == {}, "run_all_assets should return {} when startup fails"


def test_run_all_assets_no_asset_calls_startup_reconciliation():
    """run_all_assets() with no filter also calls startup reconciliation."""
    from pipeline.runner import run_all_assets

    startup_called = []

    with patch("pipeline.runner._startup_reconciliation", side_effect=lambda: (
        startup_called.append(True) or (False, None)
    )):
        result = run_all_assets()

    assert startup_called
    assert result == {}


def test_run_all_assets_halts_when_startup_blocked():
    """When _startup_reconciliation() returns False, run_all_assets halts before pipeline."""
    from pipeline.runner import run_all_assets

    pipeline_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation", return_value=(False, None)),
        patch(
            "pipeline.runner.run_pipeline",
            side_effect=lambda asset: pipeline_calls.append(asset),
        ),
    ):
        result = run_all_assets(target_asset="ZEC-USD")

    assert pipeline_calls == [], "run_pipeline must not be called when startup is blocked"
    assert result == {}


# ---------------------------------------------------------------------------
# 5. PENDING_CANCEL removed from live_statuses (P0-2)
# ---------------------------------------------------------------------------

def test_list_reconciliation_orders_excludes_pending_cancel(monkeypatch):
    """
    The live_statuses passed to list_orders must NOT include PENDING_CANCEL —
    Coinbase List Orders API would return 400 for that filter value.
    CANCEL_QUEUED is the valid filter; pending_cancel is a response field only.
    """
    import exchange.coinbase_client as _cb

    captured_statuses: list[list[str]] = []

    def fake_list_one_query(client, order_status, page_limit, start_date=None):
        captured_statuses.append(list(order_status))
        return []

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "_list_orders_one_query", fake_list_one_query)
    monkeypatch.setattr(_cb, "_get_client", lambda: object())

    _cb.list_reconciliation_orders()

    all_statuses = [s for batch in captured_statuses for s in batch]
    assert "PENDING_CANCEL" not in all_statuses, (
        f"PENDING_CANCEL must not appear in any order_status filter: {captured_statuses}"
    )
    assert "CANCEL_QUEUED" in all_statuses, (
        "CANCEL_QUEUED must be included in live_statuses"
    )
