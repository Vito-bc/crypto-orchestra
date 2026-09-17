"""
Prospective paper/shadow fee accounting — behaviour, not constants.

The defect this covers: maker/taker rates were duplicated across operational
modules, had drifted below the account's real tier, and `close_position()`
recomputed the ENTRY fee from the CURRENT constant at CLOSE time — so adopting
a new rate silently restated trades that were already open.

Tests assert on money that comes out of the accounting path wherever possible,
so pinning a constant somewhere new cannot satisfy them.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import pipeline.fees as fees
import pipeline.position_tracker as pt


# ── The authority itself ─────────────────────────────────────────────────────

def test_the_active_schedule_is_the_audited_measured_tier() -> None:
    s = fees.active_schedule()
    assert (s.maker_rate, s.taker_rate) == (0.006, 0.012)
    assert s.tier_name == "Intro 1"
    assert s.effective_from is not None
    # Provenance must name the audited checkpoint, not just assert a number.
    assert "stf_cost_report_2026-09-15.json" in s.source


def test_roles_are_not_interchangeable() -> None:
    s = fees.active_schedule()
    assert s.rate_for(fees.MAKER) == 0.006
    assert s.rate_for(fees.TAKER) == 0.012
    assert s.rate_for(fees.TAKER) > s.rate_for(fees.MAKER)


@pytest.mark.parametrize("role", ["", None, "MAKER", "limit", "both", 0])
def test_an_unknown_execution_role_raises_rather_than_picking_a_side(role) -> None:
    """Defaulting would charge maker for a taker execution — a silent undercharge."""
    with pytest.raises(fees.FeeConfigurationError, match="unknown execution role"):
        fees.active_schedule().rate_for(role)


def test_a_transposed_schedule_cannot_be_constructed() -> None:
    """Taker below maker means the roles were swapped; every exit would undercharge."""
    with pytest.raises(fees.FeeConfigurationError, match="transposed"):
        fees.FeeSchedule("bad", "t", maker_rate=0.012, taker_rate=0.006,
                         effective_from=None, source="test")


@pytest.mark.parametrize("rate", [0.6, 1.2, 60.0, float("nan"), float("inf"),
                                  -0.006, "0.006", None, True])
def test_a_percent_or_unusable_rate_is_refused(rate) -> None:
    """0.6 where 0.006 was meant is a 100x overcharge, and must not construct."""
    with pytest.raises(fees.FeeConfigurationError):
        fees.FeeSchedule("bad", "t", maker_rate=rate, taker_rate=0.012,
                         effective_from=None, source="test")


def test_an_unknown_schedule_id_raises_and_never_defaults() -> None:
    with pytest.raises(fees.FeeConfigurationError, match="unknown fee schedule id"):
        fees.schedule_by_id("coinbase-whatever-2027")


# ── Entry-leg resolution: a placed order is never repriced ───────────────────
#
# entry_rate_for_record() itself never infers legacy status. That inference
# happens exactly once, at load time, in migrate_legacy_fee_stamp() (tested
# further below) — by the time a record reaches entry_rate_for_record(), a
# genuinely historical record and a current one look identical: both carry a
# real, registered fee_schedule_id.

def test_a_record_with_no_schedule_id_is_refused() -> None:
    """
    Review (both rounds): no id means the record either bypassed the
    load-time migration or had its id stripped afterward. There is no rate
    that can be assumed safe, so this is always refused — never a fallback
    to the legacy rate.
    """
    with pytest.raises(fees.FeeConfigurationError, match="no fee_schedule_id"):
        fees.entry_rate_for_record(None, None)


def test_a_rate_with_no_schedule_id_is_refused_even_if_it_looks_familiar() -> None:
    """
    Review P1 (this round): "accepts any rate matching any registered maker
    rate, regardless of record date." A bare rate that happens to equal
    LEGACY's or CURRENT's own maker rate is not provenance — it must be
    refused exactly like any other unstamped record.
    """
    with pytest.raises(fees.FeeConfigurationError, match="no fee_schedule_id"):
        fees.entry_rate_for_record(None, 0.004)
    with pytest.raises(fees.FeeConfigurationError, match="no fee_schedule_id"):
        fees.entry_rate_for_record(None, 0.006)


def test_a_stamped_entry_rate_beats_the_active_one() -> None:
    assert fees.entry_rate_for_record(fees.LEGACY_SCHEDULE.schedule_id, 0.004) == 0.004


def test_a_legacy_schedule_id_alone_resolves_to_its_maker_rate() -> None:
    assert fees.entry_rate_for_record(fees.LEGACY_SCHEDULE.schedule_id) == 0.004


def test_an_unknown_id_on_a_record_raises_even_when_a_rate_is_present() -> None:
    """Review finding 2: a stamped id must exist, not merely be ignored."""
    with pytest.raises(fees.FeeConfigurationError, match="unknown fee schedule id"):
        fees.entry_rate_for_record("coinbase-made-up-2027", 0.006)


def test_a_record_whose_id_contradicts_its_rate_raises() -> None:
    """
    Review finding 2: the id said Intro 1 (0.006) and the rate said 0.004.
    Picking either one silently would mis-state the trade.
    """
    with pytest.raises(fees.FeeConfigurationError, match="contradicts itself"):
        fees.entry_rate_for_record("coinbase-intro-1-2026-09", 0.004)


def test_a_transposed_pair_can_no_longer_be_expressed() -> None:
    """
    Review finding 2 closed structurally, not by a check: entry resolution
    accepts ONE maker rate, so the (0.012, 0.006) transposition that previously
    slipped through has nowhere to enter. The exit rate comes from a
    FeeSchedule, whose constructor rejects taker < maker.
    """
    import inspect

    sig = inspect.signature(fees.entry_rate_for_record)
    assert "taker_fee_rate" not in sig.parameters
    assert not hasattr(fees, "rates_for_record"), (
        "the position-wide resolver must be gone, not merely unused")


def test_entry_resolution_never_returns_the_active_maker_rate_for_an_old_order() -> None:
    active_maker = fees.active_schedule().maker_rate
    for args in [(fees.LEGACY_SCHEDULE.schedule_id, None),
                 (fees.LEGACY_SCHEDULE.schedule_id, 0.004)]:
        assert fees.entry_rate_for_record(*args) != active_maker


# ── There is no load-time migration — absence is refused, never inferred ────
#
# Review P1 finding 1 (this round): an earlier version of this module had a
# migrate_legacy_fee_stamp() that inferred "this record predates fee
# stamping" from the structural absence of fee_schedule_id, and used that
# inference to STAMP a real rate onto the row during _load_raw(). Two defects
# followed: it disagreed with entry_rate_for_record() (which refuses an id
# that contradicts a rate beside it) whenever a present maker_fee_rate did
# not match the inferred legacy rate, and — because _load_raw() mutates rows
# in place and every caller re-persists the WHOLE list via _save_raw() — an
# unrelated save (e.g. check_and_fill() for a different asset) could write
# that invented stamp onto disk for a row nothing had actually touched.
# There is no live record left needing this inference (see the PR
# description for the inventory), and the function has been deleted rather
# than reconciled: an absent fee_schedule_id is refused by
# entry_rate_for_record() exactly like any other unstamped record, at the
# point where it would actually decide money — never invented earlier.

def test_migrate_legacy_fee_stamp_no_longer_exists() -> None:
    assert not hasattr(fees, "migrate_legacy_fee_stamp"), (
        "the load-time legacy inference must be gone, not merely unused")


# ── End-to-end paper accounting ──────────────────────────────────────────────

# Recorded fees are rounded to cents. At the production $100 balance a 2%
# position is $2.00, so a 0.6% fee is $0.012 and rounds to $0.01 — rounding,
# not the rate, would decide the assertion. A $10,000 balance gives a $200
# notional and fees that land exactly on a cent.
BALANCE = 10_000.0
NOTIONAL = BALANCE * 0.02


@pytest.fixture
def paper_env(tmp_path, monkeypatch):
    """Isolated position/history files — never the real logs/."""
    monkeypatch.setattr(pt, "POSITIONS_FILE", tmp_path / "open_positions.json")
    monkeypatch.setattr(pt, "TRADE_HISTORY", tmp_path / "trade_history.jsonl")
    monkeypatch.setattr(pt, "PAPER_BALANCE", BALANCE)
    return tmp_path


def _order(asset="ZEC-USD", limit=30.0, stop=29.0, target=31.75,
           fee_schedule_id="coinbase-intro-1-2026-09", maker_fee_rate=0.006):
    """An order as PendingOrder.create() stamps it — fee schedule included."""
    return SimpleNamespace(id="ord-1", asset=asset, limit_price=limit,
                           stop_price=stop, target_price=target,
                           position_size_pct=0.02, epoch_id="epoch-1",
                           fee_schedule_id=fee_schedule_id,
                           maker_fee_rate=maker_fee_rate)


def _close(pos, exit_price, reason="TAKE_PROFIT"):
    with patch("exchange.coinbase_client.place_market_sell", return_value="DRY"):
        return pt.close_position(pos, exit_price=exit_price, reason=reason)


def test_the_order_stamps_the_schedule_in_force_at_placement(monkeypatch) -> None:
    """
    Coinbase prices an order at the tier in force when it was PLACED, and an
    order may rest for 24h. The stamp therefore belongs on the order, not on
    the fill.
    """
    import pipeline.limit_orders as lo

    monkeypatch.setattr(lo, "ORDERS_FILE", lo.ROOT / "logs" / "nonexistent.json")
    order = lo.PendingOrder.create(asset="ZEC-USD", limit_price=30.0, atr=0.5,
                                   position_size_pct=0.02, reasoning="t")
    assert order.fee_schedule_id == "coinbase-intro-1-2026-09"
    assert order.maker_fee_rate == 0.006


def test_the_position_carries_the_orders_entry_leg_only(paper_env) -> None:
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    assert pos.entry_fee_schedule_id == "coinbase-intro-1-2026-09"
    assert pos.entry_fee_rate == 0.006
    # No taker field: the exit is a different order, priced when it is sent.
    assert not hasattr(pos, "taker_fee_rate")

    stored = json.loads((paper_env / "open_positions.json").read_text())[0]
    assert stored["entry_fee_rate"] == 0.006
    assert "taker_fee_rate" not in stored


def test_a_new_maker_entry_is_charged_at_point_six_percent(paper_env) -> None:
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    record = _close(pos, exit_price=30.0)
    # 0.6% maker on the $200 notional.
    assert record["entry_fee_usd"] == pytest.approx(NOTIONAL * 0.006, abs=1e-9)


def test_a_new_taker_exit_is_charged_at_one_point_two_percent(paper_env) -> None:
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    record = _close(pos, exit_price=30.0)
    # Flat exit: proceeds equal notional, so the exit fee is 1.2% of it.
    assert record["exit_fee_usd"] == pytest.approx(NOTIONAL * 0.012, abs=1e-9)


@pytest.mark.parametrize("reason", ["TAKE_PROFIT", "STOP_LOSS", "MAX_HOLD"])
def test_every_exit_path_is_charged_taker_because_every_exit_is_a_market_sell(
        paper_env, reason) -> None:
    """
    The operational exit semantics: close_position() calls place_market_sell()
    unconditionally, so no exit may be priced as a maker fill — including the
    take-profit, which the historical backtest models as maker.
    """
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    record = _close(pos, exit_price=30.0, reason=reason)
    assert record["exit_fee_usd"] == pytest.approx(NOTIONAL * 0.012, abs=1e-9)


def test_current_accounting_reflects_the_higher_tier(paper_env) -> None:
    """A flat round trip now costs 1.8% of notional, where it used to cost 1.0%."""
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    record = _close(pos, exit_price=30.0)

    legacy_cost = NOTIONAL * 0.004 + NOTIONAL * 0.006   # what the old schedule charged
    current_cost = -record["pnl_usd"]
    assert current_cost == pytest.approx(NOTIONAL * 0.006 + NOTIONAL * 0.012, abs=1e-9)
    assert current_cost > legacy_cost


def test_a_position_with_structurally_absent_fee_fields_is_refused_not_legacy(
        paper_env) -> None:
    """
    Review P1 finding 1: there is no load-time migration that infers a
    schedule for a row that structurally never had entry_fee_schedule_id /
    entry_fee_rate — as one written by pre-fee-stamping code would look on
    disk. Loading it must not silently upgrade it to LEGACY_SCHEDULE; closing
    it must refuse exactly like any other unstamped record.
    """
    pt.open_position_from_order(_order(), fill_price=30.0)

    # Simulate a genuinely pre-feature record: the keys are ENTIRELY ABSENT
    # (not null) from the persisted row, exactly as the old code would have
    # written it before these fields existed.
    raw = json.loads(pt.POSITIONS_FILE.read_text())
    del raw[0]["entry_fee_schedule_id"]
    del raw[0]["entry_fee_rate"]
    pt.POSITIONS_FILE.write_text(json.dumps(raw))

    loaded = pt.get_open_positions()[0]
    assert loaded.entry_fee_schedule_id is None, (
        "loading a structurally pre-feature row must NOT invent a schedule for it")

    with pytest.raises(fees.FeeConfigurationError, match="no fee_schedule_id"):
        _close(loaded, exit_price=30.0)


def test_a_record_with_a_rate_but_no_id_is_refused_not_legacy(paper_env) -> None:
    """
    Review P1 finding 1, the sharper case: a row with a RATE present (looking
    like real data) but no id at all must still be refused — a familiar rate
    is not provenance, and there is no migration left to reinterpret it.
    """
    pt.open_position_from_order(_order(), fill_price=30.0)

    raw = json.loads(pt.POSITIONS_FILE.read_text())
    del raw[0]["entry_fee_schedule_id"]
    raw[0]["entry_fee_rate"] = 0.006  # present, and correct-looking — must not matter
    pt.POSITIONS_FILE.write_text(json.dumps(raw))

    loaded = pt.get_open_positions()[0]
    assert loaded.entry_fee_schedule_id is None
    assert loaded.entry_fee_rate == 0.006

    with pytest.raises(fees.FeeConfigurationError, match="no fee_schedule_id"):
        _close(loaded, exit_price=30.0)


def test_an_order_placed_under_the_old_tier_keeps_it_when_it_fills_later(
        paper_env) -> None:
    """
    The second half of finding 1: an order resting across a tier change is
    filled at the tier it was PLACED under, not the tier at fill.
    """
    stale = _order(fee_schedule_id=fees.LEGACY_SCHEDULE.schedule_id,
                   maker_fee_rate=0.004)
    pos = pt.open_position_from_order(stale, fill_price=30.0)
    assert pos.entry_fee_rate == 0.004

    record = _close(pos, exit_price=30.0)
    assert record["entry_fee_usd"] == pytest.approx(NOTIONAL * 0.004, abs=1e-9)
    assert record["exit_fee_usd"] == pytest.approx(NOTIONAL * 0.012, abs=1e-9)


def test_the_two_legs_are_recorded_under_their_own_schedules(paper_env) -> None:
    """A trade that straddles a tier change must show both, not one."""
    stale = _order(fee_schedule_id=fees.LEGACY_SCHEDULE.schedule_id,
                   maker_fee_rate=0.004)
    record = _close(pt.open_position_from_order(stale, fill_price=30.0), 30.0)
    assert record["entry_fee_schedule_id"] == fees.LEGACY_SCHEDULE.schedule_id
    assert record["exit_fee_schedule_id"] == "coinbase-intro-1-2026-09"


def test_the_trade_history_record_carries_its_own_fee_provenance(paper_env) -> None:
    """The history file must be auditable without re-deriving the code of the day."""
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    _close(pos, exit_price=31.0)

    line = (paper_env / "trade_history.jsonl").read_text().strip().splitlines()[-1]
    rec = json.loads(line)
    assert rec["entry_fee_schedule_id"] == "coinbase-intro-1-2026-09"
    assert rec["exit_fee_schedule_id"] == "coinbase-intro-1-2026-09"
    assert (rec["entry_fee_rate"], rec["exit_fee_rate"]) == (0.006, 0.012)


@pytest.mark.parametrize("corrupt", [
    {"entry_fee_rate": 0.6},                                  # percent, not fraction
    {"entry_fee_schedule_id": "coinbase-made-up-2027"},        # unknown schedule
    {"entry_fee_rate": 0.004},                                 # contradicts its id
    {"entry_fee_schedule_id": None},                           # stripped after stamping
])
def test_a_corrupt_entry_stamp_blocks_settlement_rather_than_undercharging(
        paper_env, corrupt) -> None:
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    broken = pt.Position(**{**pos.__dict__, **corrupt})
    with pytest.raises(fees.FeeConfigurationError):
        _close(broken, exit_price=30.0)


@pytest.mark.parametrize("corrupt", [
    {"entry_fee_rate": 0.6},
    {"entry_fee_schedule_id": "coinbase-made-up-2027"},
    {"entry_fee_rate": 0.004},
])
def test_a_corrupt_entry_stamp_is_caught_before_the_exit_order_is_sent(
        paper_env, corrupt) -> None:
    """
    Review finding 2. close_position() used to call place_market_sell() FIRST
    and validate fees afterward, so a corrupt stamp left a real (or dry-run)
    exit sent with no P&L recorded and no way to re-send it. Fees must resolve
    before the sell is placed, not after.
    """
    pos = pt.open_position_from_order(_order(), fill_price=30.0)
    broken = pt.Position(**{**pos.__dict__, **corrupt})

    with patch("exchange.coinbase_client.place_market_sell") as mock_sell:
        with pytest.raises(fees.FeeConfigurationError):
            pt.close_position(broken, exit_price=30.0, reason="TAKE_PROFIT")
        mock_sell.assert_not_called()

    # And the position was never mutated to CLOSED by the failed attempt.
    stored = json.loads((paper_env / "open_positions.json").read_text())[0]
    assert stored["status"] == "OPEN"


# ── Findings 1 & 3: the ORDER's stamp, checked before it is trusted ─────────

def _pending_order_row(fee_schedule_id, maker_fee_rate, order_id="ord-2",
                        *, placed_at=None, expires_at=None):
    """
    A raw pending_orders.json row, as PendingOrder.create() would persist it.

    Review P2 (finding 4, prior round): this used to hard-code an absolute
    placed_at/expires_at pair. is_expired() compares against the real wall
    clock, so once that fixed date passed, orders the tests assume are NOT
    expired started failing for real. Timestamps are computed relative to
    datetime.now() at test-run time instead, with an explicit override for
    tests that need to control which side of expiry the row falls on.
    """
    now = datetime.now(timezone.utc)
    if placed_at is None:
        placed_at = (now - timedelta(minutes=10)).isoformat()
    if expires_at is None:
        expires_at = (now + timedelta(hours=23, minutes=50)).isoformat()
    return {
        "id": order_id, "asset": "ZEC-USD", "limit_price": 30.0,
        "stop_price": 29.0, "target_price": 31.75, "position_size_pct": 0.02,
        "placed_at": placed_at, "expires_at": expires_at,
        "reasoning": "t", "status": "OPEN", "exchange_order_id": None,
        "epoch_id": None, "fee_schedule_id": fee_schedule_id,
        "maker_fee_rate": maker_fee_rate,
    }


def test_a_corrupt_order_stamp_is_not_filled_and_marked_fee_error(
        tmp_path, monkeypatch) -> None:
    """
    Review finding 1 (original) + review P2 finding 2 (follow-up).
    check_and_fill() used to mark an order FILLED and persist it in the same
    call that later raised in open_position_from_order — orphaning the order
    (FILLED, no position, never revisited because it is no longer OPEN).

    Leaving it silently OPEN instead (the first fix) was its own defect: on a
    later cycle, once the order's TTL passed, the ordinary expiry branch
    would mark it EXPIRED with no record it had ever been genuinely
    fillable, and repairing the stamp afterward could never recover it.
    FEE_ERROR is the explicit, recoverable state: distinct from OPEN, exempt
    from TTL expiry, and it remembers the price it was blocked at.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004)  # id/rate disagree
    orders_file.write_text(json.dumps([row]))

    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)  # <= limit, would fill

    assert filled == []
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "FEE_ERROR", "an unresolvable stamp must not reach FILLED or OPEN"
    assert stored["fee_error_price"] == 25.0


def test_a_fee_error_order_survives_past_its_ttl_without_expiring(
        tmp_path, monkeypatch) -> None:
    """
    Review P2 finding 2 (prior round), the core of it: the reviewer's
    reproduction was an order blocked into an unresolved-fee state WHILE
    STILL OPEN, whose TTL then passed before repair. It must not be silently
    reclassified as EXPIRED on that later cycle — that would discard the fact
    that it was genuinely fillable, with no way back.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    now = datetime.now(timezone.utc)
    # Not yet expired: the block must come from the fee stamp, not from TTL.
    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004,  # id/rate disagree
                              expires_at=(now + timedelta(hours=1)).isoformat())
    orders_file.write_text(json.dumps([row]))

    # First cycle, still within TTL: blocked, marked FEE_ERROR.
    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    assert json.loads(orders_file.read_text())[0]["status"] == "FEE_ERROR"

    # Time passes — the order's TTL now would have elapsed, had it still been
    # OPEN for the ordinary expiry check to see.
    stored = json.loads(orders_file.read_text())
    stored[0]["expires_at"] = (now - timedelta(hours=1)).isoformat()
    orders_file.write_text(json.dumps(stored))

    # Second cycle: still not reclassified as EXPIRED, and not silently filled.
    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "FEE_ERROR", "must not be silently expired while unrepaired"


def test_repairing_a_fee_error_stamp_recovers_the_order_at_the_blocked_price(
        tmp_path, monkeypatch) -> None:
    """
    Review P2 finding 2 (prior round): repairing the stamp after the fact
    must actually flip the ORDER to FILLED. (The follow-up review found that
    the resulting POSITION still used the wrong price — see
    test_recovered_fee_error_fill_creates_the_position_at_the_blocked_price
    below, which is the real regression test for that.)
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    now = datetime.now(timezone.utc)
    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004,  # id/rate disagree
                              expires_at=(now + timedelta(hours=1)).isoformat())
    orders_file.write_text(json.dumps([row]))

    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    blocked = json.loads(orders_file.read_text())[0]
    assert blocked["status"] == "FEE_ERROR"
    assert blocked["fee_error_price"] == 25.0

    # Time passes past the original TTL, AND the stamp is repaired.
    blocked["expires_at"] = (now - timedelta(hours=1)).isoformat()
    blocked["maker_fee_rate"] = 0.006  # fix the contradicting rate
    orders_file.write_text(json.dumps([blocked]))

    filled = lo.check_and_fill("ZEC-USD", current_price=40.0)

    assert [o.id for o in filled] == ["ord-2"]
    assert filled[0].fee_error_price == 25.0, (
        "the recovered order must still carry the price it was blocked at")
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "FILLED"


def test_recovered_fee_error_fill_creates_the_position_at_the_blocked_price(
        tmp_path, monkeypatch) -> None:
    """
    Review P1 (this round), finding 2: check_and_fill() recorded the price a
    FEE_ERROR order was blocked at, but runner._check_pending_fills() ignored
    it and passed the CURRENT price into open_position_from_order(),
    format_limit_order_filled(), and the LIMIT_ORDER_FILLED event instead. A
    $30 limit blocked at $25 and repaired after price moved to $40 must still
    open a position — and log a fill — at $25, not $40, which would silently
    change quantity, stops, targets, exit notional and P&L.

    This exercises the REAL runner path end to end, including its pre-fill
    guard (get_pending_orders(), not get_open_orders() — see finding 3): the
    order is FEE_ERROR, not OPEN, by the time _check_pending_fills() runs, so
    the real guard sees nothing pending and never re-applies an entry veto to
    a fill that already happened.
    """
    import pipeline.limit_orders as lo
    import pipeline.runner as runner

    monkeypatch.setattr(lo, "ORDERS_FILE", tmp_path / "orders.json")
    monkeypatch.setattr(pt, "POSITIONS_FILE", tmp_path / "positions.json")
    monkeypatch.setattr(pt, "TRADE_HISTORY", tmp_path / "history.jsonl")
    monkeypatch.setattr(pt, "PAPER_BALANCE", BALANCE)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004)  # id/rate disagree
    lo.ORDERS_FILE.write_text(json.dumps([row]))

    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    blocked = json.loads(lo.ORDERS_FILE.read_text())
    assert blocked[0]["fee_error_price"] == 25.0
    blocked[0]["maker_fee_rate"] = 0.006  # repair
    lo.ORDERS_FILE.write_text(json.dumps(blocked))

    logged_events: list[tuple[str, str, dict]] = []
    with patch.object(runner, "_log_order_event",
                      side_effect=lambda *a: logged_events.append(a)), \
         patch.object(runner, "send_telegram_message"), \
         patch("notifications.telegram.format_limit_order_filled", return_value=""), \
         patch("notifications.telegram.format_position_opened", return_value=""), \
         patch("exchange.coinbase_client.place_market_sell", return_value="DRY"):
        runner._check_pending_fills("ZEC-USD", current_price=40.0)

    pos = pt.get_open_positions("ZEC-USD")[0]
    assert pos.entry_price == 25.0, "must fill at the blocked price, not the repair-time price"

    filled_events = [d for (_asset, kind, d) in logged_events
                     if kind == "LIMIT_ORDER_FILLED"]
    assert filled_events[0]["fill_price"] == 25.0


def test_fee_error_recovery_proceeds_even_when_entry_filters_would_veto_a_new_entry(
        tmp_path, monkeypatch) -> None:
    """
    Review P1 finding 3: the pre-fill guard used to key off get_open_orders(),
    which also counts FEE_ERROR orders as outstanding, so it re-ran entry
    filters for an order that had already filled — and on a veto, cancelled
    nothing (cancel_open_orders() only touches OPEN orders) while returning
    before check_and_fill() (the only place recovery happens) was reached.
    A repaired FEE_ERROR order must recover regardless of what a NEW entry's
    filters say, because the entry already happened.
    """
    import pipeline.limit_orders as lo
    import pipeline.runner as runner

    monkeypatch.setattr(lo, "ORDERS_FILE", tmp_path / "orders.json")
    monkeypatch.setattr(pt, "POSITIONS_FILE", tmp_path / "positions.json")
    monkeypatch.setattr(pt, "TRADE_HISTORY", tmp_path / "history.jsonl")
    monkeypatch.setattr(pt, "PAPER_BALANCE", BALANCE)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004)  # id/rate disagree
    lo.ORDERS_FILE.write_text(json.dumps([row]))

    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    blocked = json.loads(lo.ORDERS_FILE.read_text())
    blocked[0]["maker_fee_rate"] = 0.006  # repair
    lo.ORDERS_FILE.write_text(json.dumps(blocked))

    with patch.object(runner, "_check_entry_filters",
                       return_value=(False, "vetoed for a NEW entry", 1.0)) as mock_filters, \
         patch.object(runner, "send_telegram_message"), \
         patch("notifications.telegram.format_limit_order_filled", return_value=""), \
         patch("notifications.telegram.format_position_opened", return_value=""), \
         patch("exchange.coinbase_client.place_market_sell", return_value="DRY"):
        runner._check_pending_fills("ZEC-USD", current_price=40.0)
        mock_filters.assert_not_called(), (
            "the guard must not even consult entry filters for a FEE_ERROR-only asset")

    stored = json.loads(lo.ORDERS_FILE.read_text())[0]
    assert stored["status"] == "FILLED", "recovery must not be blocked by a vetoing entry filter"
    pos = pt.get_open_positions("ZEC-USD")
    assert len(pos) == 1 and pos[0].entry_price == 25.0


def test_a_valid_order_stamp_still_fills_normally(tmp_path, monkeypatch) -> None:
    """The new check must not block an ordinary, correctly-stamped fill."""
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row("coinbase-intro-1-2026-09", 0.006)
    orders_file.write_text(json.dumps([row]))

    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert [o.id for o in filled] == ["ord-2"]
    assert json.loads(orders_file.read_text())[0]["status"] == "FILLED"


def test_an_order_structurally_missing_fee_fields_is_blocked_not_legacy(
        tmp_path, monkeypatch) -> None:
    """
    Review P1 finding 1: an order whose row structurally never had
    fee_schedule_id/maker_fee_rate at all — as one written by pre-fee-
    stamping code would look on disk — must NOT be silently migrated to
    LEGACY and filled. There is no load-time migration left to do that; it
    is refused by entry_rate_for_record() exactly like any other unstamped
    record, and the fill is blocked (FEE_ERROR) rather than recorded.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row(None, None)
    del row["fee_schedule_id"]
    del row["maker_fee_rate"]
    orders_file.write_text(json.dumps([row]))
    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert filled == []
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "FEE_ERROR"
    assert stored["fee_error_price"] == 25.0


def test_an_order_with_fee_fields_present_but_null_is_not_treated_as_legacy(
        tmp_path, monkeypatch) -> None:
    """
    Review (both rounds): a row where the fee keys EXIST but are null is a
    different, more serious case than a row that never had them at all — the
    former can only happen to a record that was already stamped (or
    migrated) and then had its stamp stripped. It must not fill, and must
    not be treated as legacy.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row(None, None)  # keys present, explicitly null
    orders_file.write_text(json.dumps([row]))
    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert filled == []
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "FEE_ERROR"


def test_an_order_well_before_its_expiry_fills_normally(tmp_path, monkeypatch) -> None:
    """Review P2 (finding 4, prior round): a correctly-stamped order safely inside its TTL fills."""
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    now = datetime.now(timezone.utc)
    row = _pending_order_row("coinbase-intro-1-2026-09", 0.006,
                              expires_at=(now + timedelta(hours=1)).isoformat())
    orders_file.write_text(json.dumps([row]))
    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert [o.id for o in filled] == ["ord-2"]


def test_an_order_past_its_expiry_is_expired_not_filled(tmp_path, monkeypatch) -> None:
    """Review P2 (finding 4, prior round): an order past TTL is EXPIRED even if it would otherwise fill."""
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    now = datetime.now(timezone.utc)
    row = _pending_order_row("coinbase-intro-1-2026-09", 0.006,
                              expires_at=(now - timedelta(hours=1)).isoformat())
    orders_file.write_text(json.dumps([row]))
    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert filled == []
    assert json.loads(orders_file.read_text())[0]["status"] == "EXPIRED"


def test_a_fee_error_order_still_counts_as_open_for_placement_guards(
        tmp_path, monkeypatch) -> None:
    """
    Review P1 (this round), finding 3: FEE_ERROR must keep blocking a NEW
    entry for the same asset. runner.py's new-entry placement guard keys off
    get_open_orders(asset) being non-empty (unlike the pre-fill guard, which
    deliberately uses get_pending_orders() — OPEN only — so it does not
    re-veto a fill that already happened; see
    test_fee_error_recovery_proceeds_even_when_entry_filters_would_veto_a_new_entry).
    Before this fix a FEE_ERROR order stopped counting as open the instant it
    was blocked, so a second qualifying order could fill for the same asset
    while the first sat unresolved (reproduced by the review as two open
    positions after filling a second order and repairing the first).
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    row = _pending_order_row("coinbase-intro-1-2026-09", 0.004)  # id/rate disagree
    orders_file.write_text(json.dumps([row]))

    assert lo.check_and_fill("ZEC-USD", current_price=25.0) == []
    assert json.loads(orders_file.read_text())[0]["status"] == "FEE_ERROR"

    existing = lo.get_open_orders("ZEC-USD")
    assert [o.id for o in existing] == ["ord-2"], (
        "a FEE_ERROR order must still be visible to the placement guards")


def test_an_unrelated_asset_operation_does_not_rewrite_another_rows_fee_stamp(
        tmp_path, monkeypatch) -> None:
    """
    Review P2 finding 3 (this round), the concrete reproduction: with the
    deleted migrate_legacy_fee_stamp() still in place, _load_raw() mutated
    EVERY row in the file (not just the asset being processed) by stamping
    fee_schedule_id/maker_fee_rate onto any row missing them, and every
    caller re-persists the WHOLE list via _save_raw() — so a
    check_and_fill("ZEC-USD", ...) rewrote an unrelated, untouched BTC row
    with an invented legacy fee stamp. There is no such migration left; an
    unrelated row's fee fields must come out of an unrelated operation
    byte-for-byte as they went in.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)
    monkeypatch.setenv("DRY_RUN", "true")

    btc_row = _pending_order_row(None, None, order_id="btc-untouched")
    btc_row["asset"] = "BTC-USD"
    del btc_row["fee_schedule_id"]
    del btc_row["maker_fee_rate"]
    zec_row = _pending_order_row("coinbase-intro-1-2026-09", 0.006, order_id="zec-1")
    orders_file.write_text(json.dumps([btc_row, zec_row]))

    filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert [o.id for o in filled] == ["zec-1"]
    stored_btc = json.loads(orders_file.read_text())[0]
    assert stored_btc["id"] == "btc-untouched"
    assert stored_btc["status"] == "OPEN", "an unrelated asset's order must not be touched"
    assert "fee_schedule_id" not in stored_btc, (
        "an unrelated row must not gain an invented fee stamp as a side "
        "effect of another asset's check_and_fill()")
    assert "maker_fee_rate" not in stored_btc


def test_a_confirmed_fill_with_no_usable_price_is_not_recorded_as_fee_error(
        tmp_path, monkeypatch) -> None:
    """
    Finding 5 (not from the review): check_order_filled()
    (exchange/coinbase_client.py) returns (True, None) when the exchange
    confirms FILLED but reports no average_filled_price. Recording that as
    FEE_ERROR with fee_error_price=None used to crash the recovery print's
    `:,.2f` formatting, and separately let runner's fallback silently
    substitute today's price for the price the order was actually blocked
    at — exactly what FEE_ERROR exists to prevent. It must retry instead of
    fabricating a price.
    """
    import pipeline.limit_orders as lo

    orders_file = tmp_path / "orders.json"
    monkeypatch.setattr(lo, "ORDERS_FILE", orders_file)

    now = datetime.now(timezone.utc)
    row = _pending_order_row("coinbase-intro-1-2026-09", 0.006,
                              expires_at=(now + timedelta(hours=1)).isoformat())
    row["exchange_order_id"] = "live-order-1"
    orders_file.write_text(json.dumps([row]))

    with patch("exchange.coinbase_client.is_dry_run", return_value=False), \
         patch("exchange.coinbase_client.check_order_filled", return_value=(True, None)):
        filled = lo.check_and_fill("ZEC-USD", current_price=25.0)

    assert filled == [], "an unusable fill price must never be recorded as a fill"
    stored = json.loads(orders_file.read_text())[0]
    assert stored["status"] == "OPEN", (
        "must retry next cycle, not record an unusable price as FEE_ERROR")
    assert stored["fee_error_price"] is None


def test_the_filled_event_fee_matches_what_the_position_actually_uses(
        monkeypatch) -> None:
    """
    Review finding 3 (prior round), exercised through the real call site
    (`_check_pending_fills`, as the review reproduced it). The FILLED event
    used to log the order's raw `maker_fee_rate` field — None for a legacy
    order — while `open_position_from_order()`, called two lines later on the
    SAME order, correctly resolves that None to 0.004. The event and the
    position it describes must agree.
    """
    import pipeline.runner as runner

    legacy_order = SimpleNamespace(
        id="o1", asset="ZEC-USD", limit_price=30.0, stop_price=29.0,
        target_price=31.75, fee_schedule_id=fees.LEGACY_SCHEDULE.schedule_id,
        maker_fee_rate=None, position_size_pct=0.02, epoch_id=None)

    logged_events: list[tuple[str, str, dict]] = []

    with patch.object(runner, "get_pending_orders", return_value=[]), \
         patch.object(runner, "check_and_fill", return_value=[legacy_order]), \
         patch.object(runner, "_log_order_event",
                      side_effect=lambda *a: logged_events.append(a)), \
         patch.object(runner, "send_telegram_message"), \
         patch("notifications.telegram.format_limit_order_filled", return_value=""), \
         patch("notifications.telegram.format_position_opened", return_value=""), \
         patch.object(runner, "open_position_from_order") as mock_open_pos:
        mock_open_pos.return_value = SimpleNamespace(
            id="o1", entry_price=30.0, stop_price=29.0, target_price=31.75,
            qty_usd=200.0, epoch_id=None)
        runner._check_pending_fills("ZEC-USD", current_price=30.0)

    filled_events = [d for (_asset, kind, d) in logged_events
                     if kind == "LIMIT_ORDER_FILLED"]
    assert len(filled_events) == 1
    logged_rate = filled_events[0]["maker_fee"]

    assert logged_rate == 0.004, (
        "the event must log the RESOLVED legacy rate, not the raw None field")
    # And it must be exactly what the position this same fill creates will use.
    position_rate = fees.entry_rate_for_record(
        legacy_order.fee_schedule_id, legacy_order.maker_fee_rate)
    assert logged_rate == position_rate


# ── The research side must not move ──────────────────────────────────────────

def test_historical_research_fee_assumptions_are_untouched() -> None:
    """
    Today's measured tier must not be back-projected into 2020-2026 research.
    These constants belong to the frozen historical mechanism.
    """
    from backtesting.signal_scanner import _ENTRY_FEE, _SL_FEE, _TP_FEE

    assert (_ENTRY_FEE, _TP_FEE, _SL_FEE) == (0.004, 0.004, 0.006)


def test_the_execution_replay_research_model_is_untouched() -> None:
    from backtesting.execution_replay import (
        _MAKER_ENTRY, _MAKER_TP, _TAKER_SL,
    )

    assert (_MAKER_ENTRY, _MAKER_TP, _TAKER_SL) == (0.004, 0.004, 0.006)


def test_operational_fees_are_not_imported_by_the_research_scanner() -> None:
    """
    A one-way boundary: research must not start reading the operational tier,
    or a future tier change would silently restate historical results.
    """
    from pathlib import Path

    for module in ("backtesting/signal_scanner.py",
                   "backtesting/execution_replay.py",
                   "backtesting/research_runner.py"):
        src = Path(module).read_text(encoding="utf-8")
        assert "pipeline.fees" not in src, f"{module} reads the operational tier"


def test_replay_runner_pins_the_frozen_schedule_not_the_live_tier(paper_env) -> None:
    """
    Review P2 finding 3 (prior round): backtesting/replay_runner.py runs the
    REAL pipeline.limit_orders / pipeline.position_tracker over 2024-2025
    market data. Those modules read pipeline.fees.active_schedule() exactly
    like live paper trading does, so without an explicit override a replay of
    a past period would inherit whatever tier is measured TODAY. replay_runner
    patches pipeline.fees.CURRENT_SCHEDULE to LEGACY_SCHEDULE for the
    duration of the replay; this proves that pin actually changes what the
    shared accounting path charges, through the same open/close functions a
    real replay calls.
    """
    from pathlib import Path

    src = Path("backtesting/replay_runner.py").read_text(encoding="utf-8")
    assert 'patch("pipeline.fees.CURRENT_SCHEDULE"' in src, (
        "replay_runner must pin the fee schedule before running the real pipeline")

    # The entry order is stamped as PendingOrder.create() would stamp it
    # DURING a pinned replay — from active_schedule(), which the patch below
    # redirects to LEGACY_SCHEDULE. schedule_by_id() resolves it independently
    # of the CURRENT_SCHEDULE patch, since LEGACY_SCHEDULE is registered under
    # its own id regardless of which schedule is "current".
    replay_order = _order(fee_schedule_id=fees.LEGACY_SCHEDULE.schedule_id,
                           maker_fee_rate=fees.LEGACY_SCHEDULE.maker_rate)

    with patch.object(fees, "CURRENT_SCHEDULE", fees.LEGACY_SCHEDULE):
        pos = pt.open_position_from_order(replay_order, fill_price=30.0)
        record = _close(pos, exit_price=30.0)

    # Pinned to the frozen 0.4%/0.6% model, not today's measured 0.6%/1.2%.
    assert record["entry_fee_rate"] == 0.004
    assert record["exit_fee_rate"] == 0.006
    assert record["entry_fee_schedule_id"] == fees.LEGACY_SCHEDULE.schedule_id
    assert record["exit_fee_schedule_id"] == fees.LEGACY_SCHEDULE.schedule_id
