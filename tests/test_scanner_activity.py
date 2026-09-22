"""
Tests for the scanner-activity tally — the counter replacement for
scanner-gate HOLDs that used to be written to agent_decisions.jsonl as if
they were decisions.

Core invariants:
  - a gate skip is tallied, never written as a decision (that half of the
    fix lives in pipeline/runner.py and is covered by
    tests/test_runner_wiring.py-style integration tests instead);
  - `hours_evaluated` counts EVERY evaluation, real or skipped;
  - a stale in-progress day is flushed to DAILY_LOG the moment a later
    event proves it is over, and only then;
  - an unrecognised gate reason raises rather than being silently absorbed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import pipeline.scanner_activity as sa


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(sa, "STATE_FILE", tmp_path / "scanner_activity_state.json")
    monkeypatch.setattr(sa, "DAILY_LOG", tmp_path / "scanner_activity.jsonl")


def _moment(date: str, hour: int = 12) -> datetime:
    return datetime.fromisoformat(f"{date}T{hour:02d}:00:00+00:00")


# ── Basic tallying ────────────────────────────────────────────────────────────

def test_a_real_evaluation_only_advances_hours_evaluated() -> None:
    sa.record_evaluation("ETH-USD", now=_moment("2026-08-01"))

    [row] = sa.read_activity()
    assert row["hours_evaluated"] == 1
    assert row["hours_scanner_silent"] == 0
    assert row["hours_blocked"] == {}


def test_no_signal_advances_both_evaluated_and_silent() -> None:
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-01"))

    [row] = sa.read_activity()
    assert row["hours_evaluated"] == 1
    assert row["hours_scanner_silent"] == 1
    assert row["hours_blocked"] == {}


def test_a_declared_block_reason_lands_in_the_breakdown_not_silent() -> None:
    sa.record_evaluation("ETH-USD", sa.V3_ENFORCEMENT_BLOCK, now=_moment("2026-08-01"))
    sa.record_evaluation("ETH-USD", sa.IDEMPOTENCY_DUPLICATE, now=_moment("2026-08-01", 13))

    [row] = sa.read_activity()
    assert row["hours_evaluated"] == 2
    assert row["hours_scanner_silent"] == 0
    assert row["hours_blocked"] == {
        sa.V3_ENFORCEMENT_BLOCK: 1,
        sa.IDEMPOTENCY_DUPLICATE: 1,
    }


def test_repeated_same_reason_accumulates() -> None:
    for hour in range(5):
        sa.record_evaluation("ZEC-USD", sa.NO_SIGNAL, now=_moment("2026-08-01", hour))

    [row] = sa.read_activity()
    assert row["hours_evaluated"] == 5
    assert row["hours_scanner_silent"] == 5


def test_an_undeclared_gate_reason_raises() -> None:
    with pytest.raises(ValueError, match="unknown scanner gate reason"):
        sa.record_evaluation("ETH-USD", "made_up_reason", now=_moment("2026-08-01"))
    # And nothing was recorded — the bad call must not corrupt the tally.
    assert sa.read_activity() == []


# ── Multi-asset isolation ────────────────────────────────────────────────────

def test_assets_are_tallied_independently() -> None:
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-01"))
    sa.record_evaluation("ZEC-USD", now=_moment("2026-08-01"))
    sa.record_evaluation("ZEC-USD", now=_moment("2026-08-01", 13))

    rows = {r["asset"]: r for r in sa.read_activity()}
    assert rows["ETH-USD"]["hours_evaluated"] == 1
    assert rows["ETH-USD"]["hours_scanner_silent"] == 1
    assert rows["ZEC-USD"]["hours_evaluated"] == 2
    assert rows["ZEC-USD"]["hours_scanner_silent"] == 0


# ── Day rollover ─────────────────────────────────────────────────────────────

def test_a_new_day_flushes_the_prior_day_to_the_daily_log() -> None:
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-01"))
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-01", 13))
    # A later date closes 08-01 for ETH-USD.
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-02"))

    assert sa.DAILY_LOG.exists()
    closed = [json.loads(l) for l in sa.DAILY_LOG.read_text(encoding="utf-8").splitlines()]
    assert len(closed) == 1
    assert closed[0]["date"] == "2026-08-01"
    assert closed[0]["hours_scanner_silent"] == 2

    rows = sa.read_activity()
    assert rows[0]["date"] == "2026-08-01"   # closed, from DAILY_LOG
    assert rows[1]["date"] == "2026-08-02"   # in progress, from STATE_FILE
    assert rows[1]["hours_scanner_silent"] == 1


def test_a_multi_week_gap_closes_cleanly_with_no_interpolation() -> None:
    """
    The pipeline has genuinely been disabled for weeks at a time. The next
    event after a long gap must close the old day exactly as it stood — no
    days are fabricated for the gap.
    """
    sa.record_evaluation("ZEC-USD", sa.NO_SIGNAL, now=_moment("2026-07-17"))
    sa.record_evaluation("ZEC-USD", now=_moment("2026-09-21"))

    closed = [json.loads(l) for l in sa.DAILY_LOG.read_text(encoding="utf-8").splitlines()]
    assert len(closed) == 1
    assert closed[0]["date"] == "2026-07-17"

    rows = {r["date"]: r for r in sa.read_activity()}
    assert set(rows) == {"2026-07-17", "2026-09-21"}


def test_read_activity_with_no_data_is_empty() -> None:
    assert sa.read_activity() == []


# ── State-file durability shape ─────────────────────────────────────────────

def test_state_file_is_a_plain_json_dict_keyed_by_asset() -> None:
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL, now=_moment("2026-08-01"))

    raw = json.loads(sa.STATE_FILE.read_text(encoding="utf-8"))
    assert raw == {
        "ETH-USD": {
            "date": "2026-08-01",
            "asset": "ETH-USD",
            "hours_evaluated": 1,
            "hours_scanner_silent": 1,
            "hours_blocked": {},
        }
    }


def test_a_malformed_daily_log_line_is_skipped_not_fatal(tmp_path) -> None:
    sa.DAILY_LOG.parent.mkdir(parents=True, exist_ok=True)
    sa.DAILY_LOG.write_text(
        '{"date": "2026-08-01", "asset": "ETH-USD", "hours_evaluated": 3, '
        '"hours_scanner_silent": 3, "hours_blocked": {}}\n'
        "not json at all\n",
        encoding="utf-8",
    )

    rows = sa.read_activity()
    assert len(rows) == 1
    assert rows[0]["date"] == "2026-08-01"


def test_a_missing_state_file_behaves_like_no_data() -> None:
    assert not sa.STATE_FILE.exists()
    assert sa.read_activity() == []


# ── now defaults to real UTC if not supplied (sanity, not exhaustive) ────────

def test_now_defaults_to_current_utc_date() -> None:
    sa.record_evaluation("ETH-USD", sa.NO_SIGNAL)
    [row] = sa.read_activity()
    assert row["date"] == datetime.now(timezone.utc).strftime("%Y-%m-%d")
