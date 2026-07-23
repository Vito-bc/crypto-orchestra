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
    with (
        patch("pipeline.runner._startup_reconciliation", side_effect=lambda: (
            startup_called.append(True) or (False, None)
        )),
        patch("pipeline.runner._get_open_position_assets", return_value=[]),
    ):
        result = run_all_assets(target_asset="ZEC-USD")

    assert startup_called, "_startup_reconciliation() was not called"
    assert result == {}, "run_all_assets should return {} when startup fails"


def test_run_all_assets_no_asset_calls_startup_reconciliation():
    """run_all_assets() with no filter also calls startup reconciliation."""
    from pipeline.runner import run_all_assets

    startup_called = []

    with (
        patch("pipeline.runner._startup_reconciliation", side_effect=lambda: (
            startup_called.append(True) or (False, None)
        )),
        patch("pipeline.runner._get_open_position_assets", return_value=[]),
    ):
        result = run_all_assets()

    assert startup_called
    assert result == {}


def test_run_all_assets_halts_when_startup_blocked():
    """When _startup_reconciliation() returns False, run_all_assets halts before pipeline."""
    from pipeline.runner import run_all_assets

    pipeline_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation", return_value=(False, None)),
        patch("pipeline.runner._get_open_position_assets", return_value=[]),
        patch(
            "pipeline.runner.run_pipeline",
            side_effect=lambda asset: pipeline_calls.append(asset),
        ),
    ):
        result = run_all_assets(target_asset="ZEC-USD")

    assert pipeline_calls == [], "run_pipeline must not be called when startup is blocked"
    assert result == {}


# ---------------------------------------------------------------------------
# 5. Global EXIT block — unknown orphan and reconciliation exception
# ---------------------------------------------------------------------------

def test_unknown_orphan_sets_global_exit_block():
    """An UNKNOWN-asset orphan in the reconciliation report must block EXIT for all positions."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    report_with_unknown = ReconciliationReport(
        run_id=1, discovered=[], resolved=[],
        unresolved=[UnresolvedItem(
            order_id="exch-orphan-abc", asset="UNKNOWN",
            reason="orphan_coinbase_order:client_id=unknown-client:side=SELL",
        )],
        started_at=_now_str(), completed_at=_now_str(),
    )

    exit_calls: list[str] = []
    telegram_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation", return_value=(False, report_with_unknown)),
        patch("pipeline.runner._get_open_position_assets", return_value=["ETH-USD", "ZEC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot", return_value={"close": 2000.0}),
    ):
        result = run_all_assets()

    assert exit_calls == [], (
        "EXIT must be blocked for all positions when there is an UNKNOWN-asset orphan"
    )
    assert any("BLOCKED" in m for m in telegram_calls), (
        "Telegram alert must fire for each blocked asset"
    )


def test_reconciliation_exception_blocks_decommissioned_asset_exit():
    """When reconciliation raises (report=None), EXIT must be blocked for ALL open positions,
    including decommissioned assets not in ASSETS."""
    from pipeline.runner import run_all_assets

    exit_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation", return_value=(False, None)),
        # BTC-USD is not in ASSETS but has an open position
        patch("pipeline.runner._get_open_position_assets", return_value=["BTC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message"),
        patch("pipeline.runner.get_snapshot", return_value={"close": 50000.0}),
    ):
        result = run_all_assets()

    assert "BTC-USD" not in exit_calls, (
        "BTC-USD EXIT must be blocked when reconciliation failed, even though not in ASSETS"
    )
    assert result == {}


def test_get_open_position_assets_failure_blocks_all_exit():
    """When _get_open_position_assets() returns None (DB failure), all EXIT is blocked
    and a Telegram alert is sent."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    exit_calls: list[str] = []
    telegram_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=None),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
    ):
        result = run_all_assets(target_asset="ZEC-USD")

    assert exit_calls == [], "No EXIT must run when open position read failed"
    assert any("CRITICAL" in m or "BLOCKED" in m for m in telegram_calls), (
        "Telegram alert must fire when position read fails"
    )


def test_cli_target_asset_does_not_restrict_exit_supervisor():
    """EXIT supervisor must check ALL open positions even when target_asset is given.
    target_asset restricts only the ENTRY pipeline, not risk management."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    exit_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        # Two assets open: target_asset=ZEC-USD but ETH-USD also has a position
        patch("pipeline.runner._get_open_position_assets",
              return_value=["ETH-USD", "ZEC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message"),
        patch("pipeline.runner.get_snapshot", return_value={"close": 2000.0}),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
    ):
        run_all_assets(target_asset="ZEC-USD")

    assert "ETH-USD" in exit_calls, (
        "ETH-USD EXIT must run even when CLI target_asset=ZEC-USD"
    )
    assert "ZEC-USD" in exit_calls, "ZEC-USD EXIT must also run"


# ---------------------------------------------------------------------------
# 6. Asset-specific orphan blocks only that asset; position read + snapshot alerts
# ---------------------------------------------------------------------------

def test_known_asset_orphan_blocks_only_that_asset():
    """A ZEC-USD orphan in the reconciliation report must block ZEC EXIT but allow ETH EXIT."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    report_zec_orphan = ReconciliationReport(
        run_id=1, discovered=[], resolved=[],
        unresolved=[UnresolvedItem(
            order_id="exch-zec-orphan", asset="ZEC-USD",
            reason="orphan_coinbase_order:client_id=some-id:side=SELL",
        )],
        started_at=_now_str(), completed_at=_now_str(),
    )

    exit_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(False, report_zec_orphan)),
        patch("pipeline.runner._get_open_position_assets",
              return_value=["ETH-USD", "ZEC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message"),
        patch("pipeline.runner.get_snapshot", return_value={"close": 2000.0}),
    ):
        run_all_assets()

    assert "ETH-USD" in exit_calls, "ETH-USD EXIT must proceed — no ZEC orphan affects it"
    assert "ZEC-USD" not in exit_calls, "ZEC-USD EXIT must be blocked by its orphan"


def test_empty_client_order_id_becomes_exchange_only_orphan(monkeypatch):
    """An order with empty client_order_id must become an EXCHANGE_ONLY sentinel, not be silently dropped."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "list_reconciliation_orders", lambda: [
        {"client_order_id": "", "order_id": "EX-MANUAL-001", "status": "OPEN",
         "product_id": "ZEC-USD", "side": "SELL"},
    ])

    fn = _adapter.make_list_orders_fn()
    orders = fn()

    assert len(orders) == 1, "exchange-only order must not be dropped"
    assert orders[0].client_order_id.startswith("EXCHANGE_ONLY:"), (
        "empty client_order_id must be replaced with EXCHANGE_ONLY sentinel"
    )
    assert orders[0].exchange_order_id == "EX-MANUAL-001"
    assert orders[0].product_id == "ZEC-USD"
    assert orders[0].side == "SELL"


def test_list_orders_fn_transfers_product_id_and_side(monkeypatch):
    """make_list_orders_fn() must pass product_id and side through to CoinbaseOrder."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "list_reconciliation_orders", lambda: [
        {"client_order_id": "test-client-uuid", "order_id": "EX-99", "status": "OPEN",
         "product_id": "ZEC-USD", "side": "SELL"},
    ])

    fn = _adapter.make_list_orders_fn()
    orders = fn()

    assert len(orders) == 1
    assert orders[0].product_id == "ZEC-USD", "product_id must be propagated"
    assert orders[0].side == "SELL", "side must be propagated"


def test_list_orders_fn_raises_on_missing_exchange_id(monkeypatch):
    """An order with no exchange_order_id must raise RuntimeError — not be silently dropped."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "list_reconciliation_orders", lambda: [
        {"client_order_id": "some-uuid", "order_id": "", "status": "OPEN",
         "product_id": "ZEC-USD", "side": "SELL"},
    ])

    fn = _adapter.make_list_orders_fn()
    with pytest.raises(RuntimeError, match="exchange order_id"):
        fn()


def test_get_order_fn_transfers_product_id_and_side(monkeypatch):
    """make_get_order_fn() must populate product_id and side from the Get Order API response."""
    import exchange.coinbase_client as _cb
    import exchange.adapter as _adapter
    from unittest.mock import MagicMock

    mock_client = MagicMock()
    mock_client.get_order.return_value = {
        "order": {
            "order_id": "EX-456",
            "client_order_id": "local-uuid-def",
            "status": "OPEN",
            "product_id": "ZEC-USD",
            "side": "SELL",
        }
    }

    monkeypatch.setattr(_cb, "_DRY_RUN", False)
    monkeypatch.setattr(_cb, "_get_client", lambda: mock_client)
    monkeypatch.setattr(_cb, "_resp_to_dict", lambda r: r)
    monkeypatch.setattr(_cb, "fetch_fills_for_order", lambda eid: [])

    fn = _adapter.make_get_order_fn()
    order = fn("EX-456")

    assert order is not None
    assert order.product_id == "ZEC-USD", "product_id must be populated from Get Order response"
    assert order.side == "SELL", "side must be populated from Get Order response"


def test_no_price_snapshot_sends_critical_alert():
    """When get_snapshot() returns None for an open position, a CRITICAL Telegram alert must fire."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    exit_calls: list[str] = []
    telegram_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=["ZEC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot", return_value=None),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
        patch("pipeline.runner._snapshot_alert_cooldown", {}),  # reset cooldown so alert always fires
    ):
        run_all_assets(target_asset="ZEC-USD")

    assert exit_calls == [], "_check_open_positions must not be called when no snapshot"
    assert any("CRITICAL" in m for m in telegram_calls), (
        "CRITICAL Telegram alert must fire when price snapshot is unavailable for an open position"
    )


def test_snapshot_exception_sends_critical_alert():
    """When get_snapshot() raises, a CRITICAL Telegram alert must fire and EXIT must not run."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    exit_calls: list[str] = []
    telegram_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=["ZEC-USD"]),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: exit_calls.append(a)),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot",
              side_effect=RuntimeError("yfinance connection timeout")),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
        patch("pipeline.runner._snapshot_alert_cooldown", {}),
    ):
        run_all_assets(target_asset="ZEC-USD")

    assert exit_calls == [], "EXIT must not run when snapshot raises"
    assert any("CRITICAL" in m for m in telegram_calls), (
        "CRITICAL Telegram alert must fire when get_snapshot() raises"
    )


def test_snapshot_alert_suppressed_within_cooldown():
    """The CRITICAL no-snapshot alert must not fire again within the cooldown window."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock
    import time as _time

    telegram_calls: list[str] = []
    # Pre-fill cooldown as if an alert was sent 5 minutes ago
    cooldown_dict = {"ZEC-USD": _time.monotonic() - 300}

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=["ZEC-USD"]),
        patch("pipeline.runner._check_open_positions"),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot", return_value=None),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
        patch("pipeline.runner._snapshot_alert_cooldown", cooldown_dict),
    ):
        run_all_assets(target_asset="ZEC-USD")

    assert not any("CRITICAL" in m for m in telegram_calls), (
        "CRITICAL alert must be suppressed within the cooldown window"
    )


def test_snapshot_alert_fires_with_low_system_uptime():
    """First alert must fire even when time.monotonic() < cooldown (system uptime < 1h).
    Old code used 0.0 as default which gave now - 0.0 = uptime, suppressing first alert
    on a fresh boot.  None sentinel must distinguish 'never alerted' from 'alerted at t=0'.
    """
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock, patch

    telegram_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=["ZEC-USD"]),
        patch("pipeline.runner._check_open_positions"),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot", return_value=None),
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
        patch("pipeline.runner._snapshot_alert_cooldown", {}),  # empty — never alerted
        patch("pipeline.runner.time") as mock_time,
    ):
        mock_time.monotonic.return_value = 300.0  # only 5 min uptime
        run_all_assets(target_asset="ZEC-USD")

    assert any("CRITICAL" in m for m in telegram_calls), (
        "First CRITICAL alert must fire even when system uptime is < 1h (monotonic() == 300)"
    )


def test_snapshot_recovery_clears_cooldown_and_sends_recovered():
    """Successful snapshot after a failure must clear the cooldown and send RECOVERED alert."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock
    import time as _time

    telegram_calls: list[str] = []
    # Simulate: previous failure was 5 min ago (within cooldown)
    cooldown_dict: dict = {"ZEC-USD": _time.monotonic() - 300}

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=["ZEC-USD"]),
        patch("pipeline.runner._check_open_positions"),
        patch("pipeline.runner.send_telegram_message",
              side_effect=lambda m: telegram_calls.append(m)),
        patch("pipeline.runner.get_snapshot", return_value={"close": 50.0}),  # snapshot back
        patch("pipeline.runner.run_pipeline", return_value=MagicMock()),
        patch("pipeline.runner._snapshot_alert_cooldown", cooldown_dict),
    ):
        run_all_assets(target_asset="ZEC-USD")

    assert any("RECOVERED" in m for m in telegram_calls), (
        "RECOVERED alert must be sent when snapshot is restored after a failure"
    )
    assert "ZEC-USD" not in cooldown_dict, (
        "Asset must be removed from cooldown dict after recovery"
    )


def test_position_read_failure_also_halts_entry_pipeline():
    """When _get_open_position_assets() returns None, ENTRY pipeline must also be halted."""
    from pipeline.runner import run_all_assets
    from unittest.mock import MagicMock

    pipeline_calls: list[str] = []

    with (
        patch("pipeline.runner._startup_reconciliation",
              return_value=(True, _clean_report())),
        patch("pipeline.runner._get_open_position_assets", return_value=None),
        patch("pipeline.runner.send_telegram_message"),
        patch("pipeline.runner.run_pipeline",
              side_effect=lambda a, **kw: pipeline_calls.append(a)),
    ):
        result = run_all_assets(target_asset="ZEC-USD")

    assert pipeline_calls == [], (
        "run_pipeline must not be called when open-position read failed — DB is unreliable"
    )
    assert result == {}


# ---------------------------------------------------------------------------
# 7. PENDING_CANCEL removed from live_statuses (P0-2)
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
