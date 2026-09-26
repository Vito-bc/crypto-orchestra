"""The event variant changes sampling, not the scanner or ensemble."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from agent_shadow.candidates import scan_since
from agent_shadow.variants import get_variant


def _cross_frame() -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=22, freq="h", tz="UTC")
    frame = pd.DataFrame(index=index)
    frame["close"] = [99.0] * 12 + [101.0] * 4 + [99.0] + [101.0] * 4 + [99.0]
    frame["ema50"] = 100.0
    frame["rsi"] = 50.0
    frame["adx"] = 30.0
    frame["volume_ratio"] = 1.4
    frame["cvd_24h"] = 1.0
    frame["close_4h"] = 110.0
    frame["ema50_4h"] = 100.0
    frame["close_1d"] = 110.0
    frame["ema200_1d"] = 100.0
    frame["atr"] = 1.0
    return frame


def _scan(monkeypatch, frame: pd.DataFrame, end: int, start: int = 12):
    monkeypatch.setattr(
        "agent_shadow.candidates.scanner.build_merged_frame",
        lambda *args, **kwargs: (frame, None),
    )
    return scan_since(
        ["ZEC-USD"],
        {"ZEC-USD": frame.index[start]},
        now=frame.index[end] + pd.Timedelta(hours=1, minutes=5),
        per_event=True,
    )


def test_multi_bar_cross_produces_one_candidate(monkeypatch) -> None:
    frame = _cross_frame()
    result = _scan(monkeypatch, frame, end=15)

    assert result.bars_examined == {"ZEC-USD": 4}
    assert [item.candle_time for item in result.candidates] == [
        frame.index[12].isoformat()
    ]
    assert result.examined_through == {"ZEC-USD": frame.index[15].isoformat()}


def test_two_distinct_crosses_produce_two_candidates(monkeypatch) -> None:
    frame = _cross_frame()
    result = _scan(monkeypatch, frame, end=20)

    assert [item.candle_time for item in result.candidates] == [
        frame.index[12].isoformat(), frame.index[17].isoformat()
    ]
    assert len({item.event_id for item in result.candidates}) == 2


def test_first_qualifying_bar_can_follow_cross_bar(monkeypatch) -> None:
    frame = _cross_frame()
    frame.loc[frame.index[12], "volume_ratio"] = 0.5
    result = _scan(monkeypatch, frame, end=15)

    assert [item.candle_time for item in result.candidates] == [
        frame.index[13].isoformat()
    ]


def test_later_qualifying_bar_reuses_cross_id_across_runs(monkeypatch) -> None:
    frame = _cross_frame()
    first = _scan(monkeypatch, frame, end=12)
    later = _scan(monkeypatch, frame, end=15, start=13)

    assert len(first.candidates) == len(later.candidates) == 1
    assert first.candidates[0].event_id == later.candidates[0].event_id
    assert later.candidates[0].candle_time == frame.index[13].isoformat()


def test_event_variant_appends_without_altering_wide_record() -> None:
    registry = Path(__file__).resolve().parents[1] / "docs/operations/agent_shadow_variants.jsonl"
    lines = registry.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert hashlib.sha256(lines[0].encode()).hexdigest() == (
        "3f3598c4e254232952e9173194656c4b9d278aff18db0e2cfa217006f817fd5a"
    )
    wide, event = (json.loads(line) for line in lines)
    assert event == get_variant("agent-shadow-event-v1", registry)
    for field in ("assets", "agents", "orchestrator_weighting", "models"):
        assert event[field] == wide[field]
    assert event["candidate_definition"]["stage"] == "wide"
    assert event["candidate_definition"]["unit"] == "ema50_cross"


def test_registration_is_the_activation_boundary() -> None:
    root = Path(__file__).resolve().parents[1]
    wide_batch = (root / "scripts/run_agent_shadow.bat").read_text(encoding="utf-8")
    event_batch = (root / "scripts/run_agent_shadow_event.bat").read_text(encoding="utf-8")
    registration = (root / "scripts/register_agent_shadow_task.ps1").read_text(
        encoding="utf-8"
    )

    assert "--variant agent-shadow-wide-v1" in wide_batch
    assert "--variant agent-shadow-event-v1" not in wide_batch
    assert "--variant agent-shadow-event-v1" in event_batch
    assert 'Join-Path $root "scripts\\run_agent_shadow_event.bat"' in registration
    assert "New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root" in registration
