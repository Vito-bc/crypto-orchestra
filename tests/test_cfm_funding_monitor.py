"""CFM funding monitor parsing, arithmetic, durability and coverage tests."""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import backtesting.cfm_funding_monitor as monitor


FIXTURE = Path(__file__).parent / "fixtures" / "cfm_public_products_2026-09-20.json"


def _payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


class _PublicClient:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = []

    def get_public_products(self, **kwargs):
        self.calls.append(kwargs)
        return self.payload


def _row(product: str, funding_time: datetime, annualized_rate: float = 0.2) -> dict:
    return {
        "product": product,
        "funding_time": funding_time.isoformat().replace("+00:00", "Z"),
        "annualized_rate": annualized_rate,
    }


def test_parses_the_recorded_public_product_payload() -> None:
    observed_at = datetime(2026, 9, 20, 15, 59, 40, tzinfo=timezone.utc)
    rows = monitor.parse_products(_payload(), observed_at=observed_at)

    assert [row["product"] for row in rows] == list(monitor.PRODUCTS)
    assert rows[0] == {
        "observed_at": "2026-09-20T15:59:40Z",
        "product": "BIP-20DEC30-CDE",
        "asset": "BTC",
        "funding_rate": 0.00002,
        "funding_time": "2026-09-20T16:00:00Z",
        "funding_interval": "3600s",
        "funding_interval_seconds": 3600.0,
        "annualized_rate": pytest.approx(0.17532),
    }
    assert rows[1]["funding_rate"] == 0.000008
    assert rows[1]["annualized_rate"] == pytest.approx(0.070128)


def test_annualisation_uses_the_product_interval() -> None:
    expected = 0.00002 * (365.25 * 24 * 3600 / 3600)
    assert monitor.annualise(0.00002, 3600) == pytest.approx(expected)


def test_poll_uses_the_cost_probes_public_client_and_no_authenticated_call() -> None:
    source = inspect.getsource(monitor)
    assert "from backtesting.stf_cost_probe import _public_client" in source
    assert "get_public_products(product_type=\"FUTURE\")" in source
    assert "RESTClient" not in source
    assert "key_file" not in source


def test_poll_deduplicates_on_product_and_funding_time(tmp_path, monkeypatch) -> None:
    destination = tmp_path / "cfm_funding.jsonl"
    client = _PublicClient(_payload())
    monkeypatch.setattr(monitor, "OBSERVATIONS", destination)
    monkeypatch.setattr(monitor, "send_telegram_message", lambda message: True)

    first = monitor.poll(client=client)
    second = monitor.poll(client=client)

    assert len(first) == 2
    assert second == []
    assert client.calls == [
        {"product_type": "FUTURE"},
        {"product_type": "FUTURE"},
    ]
    stored = [json.loads(line) for line in destination.read_text().splitlines()]
    assert len(stored) == 2
    assert len({(row["product"], row["funding_time"]) for row in stored}) == 2


def _hours(product: str, as_of: datetime, count: int, rate: float = 0.2) -> list:
    """`count` consecutive hourly observations ending at `as_of`."""
    return [_row(product, as_of - timedelta(hours=i), rate) for i in range(count)]


# ── Two coverage checks, neither substituting for the other ──────────────────

def test_the_declared_seven_day_coverage_minimum_is_80_percent_of_168() -> None:
    """
    The depth requirement is a declared convention, and it is declared here so
    that loosening it is a visible edit rather than a drifting default.
    """
    assert monitor.TRAILING_SLOTS == 168
    assert monitor.TRAILING_COVERAGE_FRACTION == 0.80
    assert monitor.MIN_TRAILING_COVERAGE == 135


def test_freshness_alone_no_longer_publishes_a_mean() -> None:
    """
    The defect this covers: coverage was checked over 24 hours, then the mean
    was taken over whatever existed inside the 7-day window. Three days after
    the monitor was built that meant a 43-observation, 1.8-day mean reported
    as a "trailing 7-day mean" and compared against a 7-day threshold.

    24 of 24 recent hours is full marks on freshness and must still not be
    enough to publish a seven-day statistic.
    """
    product = "BIP-20DEC30-CDE"
    as_of = datetime(2026, 9, 22, 15, tzinfo=timezone.utc)

    status = monitor.trailing_status(_hours(product, as_of, 44), product, as_of)

    assert status["coverage_last_24h"] == 24
    assert status["freshness_24h_status"] == "OK"
    assert status["observations_in_trailing_7d"] == 44
    assert status["trailing_7d_coverage_status"] == "INSUFFICIENT"
    assert status["status"] == "UNAVAILABLE"
    assert status["mean_annualized_rate"] is None
    assert any("depth" in reason for reason in status["unavailable_reasons"])


def test_depth_alone_no_longer_publishes_a_mean_either() -> None:
    """
    The mirror case: a week that is 96% covered but stopped a day ago is not a
    current reading. The 24-hour freshness check answers a question the depth
    check cannot, which is why both are kept.
    """
    product = "BIP-20DEC30-CDE"
    as_of = datetime(2026, 9, 22, 15, tzinfo=timezone.utc)
    stale = _hours(product, as_of - timedelta(hours=25), 140)

    status = monitor.trailing_status(stale, product, as_of)

    assert status["coverage_last_24h"] == 0
    assert status["freshness_24h_status"] == "INSUFFICIENT"
    assert status["observations_in_trailing_7d"] >= monitor.MIN_TRAILING_COVERAGE
    assert status["trailing_7d_coverage_status"] == "OK"
    assert status["status"] == "UNAVAILABLE"
    assert status["mean_annualized_rate"] is None
    assert any("freshness" in reason for reason in status["unavailable_reasons"])


def test_the_mean_is_published_only_when_both_windows_are_covered() -> None:
    product = "BIP-20DEC30-CDE"
    as_of = datetime(2026, 9, 27, 16, tzinfo=timezone.utc)

    short = _hours(product, as_of, monitor.MIN_TRAILING_COVERAGE - 1)
    assert monitor.trailing_status(short, product, as_of)["status"] == "UNAVAILABLE"

    enough = _hours(product, as_of, monitor.MIN_TRAILING_COVERAGE)
    status = monitor.trailing_status(enough, product, as_of)

    assert status["status"] == "AVAILABLE"
    assert status["freshness_24h_status"] == "OK"
    assert status["trailing_7d_coverage_status"] == "OK"
    assert status["observations_in_trailing_7d"] == monitor.MIN_TRAILING_COVERAGE
    assert status["unavailable_reasons"] == []
    assert status["mean_annualized_rate"] == pytest.approx(0.2)


def test_the_mean_is_taken_over_the_observations_that_earned_availability() -> None:
    """The published mean must average the 7-day window, not the last 24h."""
    product = "BIP-20DEC30-CDE"
    as_of = datetime(2026, 9, 27, 16, tzinfo=timezone.utc)
    rows = _hours(product, as_of, 24, rate=0.4)
    rows += [_row(product, as_of - timedelta(hours=i), 0.1)
             for i in range(24, monitor.MIN_TRAILING_COVERAGE)]

    status = monitor.trailing_status(rows, product, as_of)

    n = monitor.MIN_TRAILING_COVERAGE
    assert status["status"] == "AVAILABLE"
    assert status["mean_annualized_rate"] == pytest.approx(
        (24 * 0.4 + (n - 24) * 0.1) / n)


def test_a_covered_week_with_a_tolerable_gap_still_reports() -> None:
    """
    80% of 168 is meant to survive an outage the size of a reboot plus a patch
    cycle, not to demand a perfect week — lost hours are unrecoverable, so a
    100% rule would blind the monitor for a full week after one missed poll.
    """
    product = "ETP-20DEC30-CDE"
    as_of = datetime(2026, 9, 27, 16, tzinfo=timezone.utc)
    week = _hours(product, as_of, 168)
    # Drop a contiguous 32-hour outage from the middle of the week.
    with_gap = week[:60] + week[92:]

    status = monitor.trailing_status(with_gap, product, as_of)

    assert status["observations_in_trailing_7d"] == 136
    assert status["status"] == "AVAILABLE"


def test_records_carry_both_denominators_and_the_recorded_condition(
    tmp_path, monkeypatch
) -> None:
    """
    A reader of one JSONL line must see which window was binding, and must see
    that a level crossing is not the condition CLAUDE.md records.
    """
    destination = tmp_path / "cfm_funding.jsonl"
    monkeypatch.setattr(monitor, "OBSERVATIONS", destination)
    monkeypatch.setattr(monitor, "send_telegram_message", lambda message: True)

    monitor.poll(client=_PublicClient(_payload()))

    stored = [json.loads(line) for line in destination.read_text().splitlines()]
    assert stored
    for row in stored:
        assert row["trailing_7d_status"] == "UNAVAILABLE"
        assert row["trailing_7d_mean_annualized"] is None
        assert row["coverage_required"] == monitor.MIN_COVERAGE
        assert row["trailing_7d_slots"] == 168
        assert row["trailing_7d_coverage_required"] == 135
        assert row["trailing_7d_coverage_status"] == "INSUFFICIENT"
        assert row["trailing_7d_unavailable_reasons"]
        assert "longer than a typical cycle" in row["reevaluation_condition"]
        assert "notification, not this condition" in row["reevaluation_condition"]
    by_asset = {row["asset"]: row["reevaluation_sustain_days"] for row in stored}
    assert by_asset == {"BTC": 17.0, "ETH": 21.3}


def test_failed_telegram_send_cannot_block_the_durable_write(
    tmp_path, monkeypatch
) -> None:
    destination = tmp_path / "cfm_funding.jsonl"
    client = _PublicClient(_payload())
    monkeypatch.setattr(monitor, "OBSERVATIONS", destination)

    def fail(_message):
        assert destination.read_text(encoding="utf-8"), "alert ran before append"
        raise RuntimeError("Telegram unavailable")

    monkeypatch.setattr(monitor, "send_telegram_message", fail)
    written = monitor.poll(client=client)

    assert len(written) == 2
    assert len(destination.read_text(encoding="utf-8").splitlines()) == 2


def test_alerts_use_both_declared_thresholds_and_only_fire_on_a_crossing() -> None:
    available = {
        "status": "AVAILABLE",
        "coverage_last_24h": 24,
        "observations_in_trailing_7d": 160,
        "unavailable_reasons": [],
        "mean_annualized_rate": 0.14,
    }
    crossed_realised = {
        **available,
        "mean_annualized_rate": 0.15,
    }
    messages = monitor._threshold_alerts(
        "BIP-20DEC30-CDE", available, crossed_realised
    )
    assert len(messages) == 1
    assert "14.8%/yr" in messages[0]
    assert "ABOVE" in messages[0]

    stable = monitor._threshold_alerts(
        "BIP-20DEC30-CDE", crossed_realised, crossed_realised
    )
    assert stable == []

    crossed_typical = {
        **available,
        "mean_annualized_rate": 0.39,
    }
    eth_before = {**available, "mean_annualized_rate": 0.38}
    messages = monitor._threshold_alerts(
        "ETP-20DEC30-CDE", eth_before, crossed_typical
    )
    assert len(messages) == 1
    assert "38.7%/yr" in messages[0]
    assert "typical-cycle" in messages[0]


def test_a_threshold_alert_states_the_condition_a_crossing_does_not_meet() -> None:
    """
    CLAUDE.md's recorded condition is a SUSTAINED excess, not a crossing. The
    alert must say so where it is read, not only in a document.
    """
    covered = {
        "status": "AVAILABLE",
        "coverage_last_24h": 24,
        "observations_in_trailing_7d": 160,
        "unavailable_reasons": [],
        "mean_annualized_rate": 0.14,
    }
    crossed = {**covered, "mean_annualized_rate": 0.15}

    message = monitor._threshold_alerts("BIP-20DEC30-CDE", covered, crossed)[0]

    assert "17.0 days" in message
    assert "sustained above break-even" in message
    assert "notification, not that condition" in message
    # Both denominators, so the alert cannot be read as a covered week when it
    # is not one.
    assert "24/24 in the last 24h" in message
    assert "160/168 in the trailing 7 days" in message


def test_a_coverage_alarm_names_the_window_that_failed() -> None:
    covered = {
        "status": "AVAILABLE",
        "coverage_last_24h": 24,
        "observations_in_trailing_7d": 160,
        "unavailable_reasons": [],
        "mean_annualized_rate": 0.14,
    }
    lost_depth = {
        "status": "UNAVAILABLE",
        "coverage_last_24h": 24,
        "observations_in_trailing_7d": 44,
        "unavailable_reasons": ["depth: 44/168 hourly observations ..."],
        "mean_annualized_rate": None,
    }

    message = monitor._threshold_alerts("BIP-20DEC30-CDE", covered, lost_depth)[0]

    assert "coverage alarm" in message
    assert "UNAVAILABLE" in message
    assert "depth: 44/168" in message


def test_the_ops_doc_declares_the_same_coverage_contract_as_the_code() -> None:
    """
    An operator reads the doc, not the module. A minimum changed in one place
    and not the other is the same class of defect as the one this coverage
    check exists to fix.
    """
    doc = (Path(__file__).resolve().parents[1] / "docs" / "operations"
           / "cfm_funding_monitor.md").read_text(encoding="utf-8")

    assert f"{monitor.MIN_COVERAGE} of 24 hourly slots" in doc
    assert (f"{monitor.MIN_TRAILING_COVERAGE} of {monitor.TRAILING_SLOTS} "
            f"hourly slots (80%)") in doc
    # The condition a crossing does not meet, with both cycle lengths.
    assert "sustained above break-even for longer than a typical cycle" in doc
    for product, spec in monitor.PRODUCTS.items():
        assert f"{spec['typical_cycle_days']:.1f} d" in doc, product


def test_runner_and_scheduler_encode_the_declared_windows_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    runner = (root / "scripts" / "run_cfm_funding_monitor.bat").read_text(
        encoding="utf-8"
    )
    scheduler = (root / "scripts" / "register_cfm_funding_monitor_task.ps1").read_text(
        encoding="utf-8"
    )

    assert "PYTHONIOENCODING=utf-8" in runner
    assert "backtesting\\cfm_funding_monitor.py" in runner
    assert 'CryptoOrchestra-CFM-FundingMonitor' in scheduler
    assert "-Minute 2" in scheduler
    assert "-Hours 1" in scheduler
    assert "-Minutes 5" in scheduler
    assert "-AllowStartIfOnBatteries" in scheduler
    assert "-DontStopIfGoingOnBatteries" in scheduler
    assert "-WakeToRun" in scheduler
    assert "$settings.StartWhenAvailable = $true" in scheduler
