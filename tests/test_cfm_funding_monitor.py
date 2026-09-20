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


def test_coverage_alarm_makes_the_mean_unavailable_below_20_of_24() -> None:
    product = "BIP-20DEC30-CDE"
    as_of = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)
    nineteen = [_row(product, as_of - timedelta(hours=i)) for i in range(19)]

    status = monitor.trailing_status(nineteen, product, as_of)

    assert status["coverage_last_24h"] == 19
    assert status["status"] == "UNAVAILABLE"
    assert status["mean_annualized_rate"] is None

    twenty = nineteen + [_row(product, as_of - timedelta(hours=19), 0.4)]
    status = monitor.trailing_status(twenty, product, as_of)
    assert status["coverage_last_24h"] == 20
    assert status["status"] == "AVAILABLE"
    assert status["mean_annualized_rate"] == pytest.approx((19 * 0.2 + 0.4) / 20)


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
