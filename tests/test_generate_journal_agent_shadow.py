"""
AgentShadow/ — one note per shadow decision (observation only), plus one
index note listing decisions newest-first with their variant.

logs/agent_shadow.jsonl is currently EMPTY (no candidate has occurred since
the hourly task was registered on 2026-09-21) — the generator must handle a
missing OR empty log with a placeholder, never a crash. See
docs/operations/agent_shadow.md for the record schema this renders.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import backtesting.generate_journal as gj


@pytest.fixture()
def vault(tmp_path, monkeypatch):
    destination = tmp_path / "vault"
    monkeypatch.setattr(gj, "VAULT", destination)
    monkeypatch.setattr(gj, "ROOT", tmp_path)
    (tmp_path / "logs").mkdir(exist_ok=True)
    gj._written_paths.clear()
    yield destination
    gj._written_paths.clear()


def _shadow_log(tmp_path, records: list[dict]) -> Path:
    path = tmp_path / "logs" / "agent_shadow.jsonl"
    path.write_text(
        "\n".join(json.dumps(r) for r in records) + ("\n" if records else ""),
        encoding="utf-8")
    return path


def _vote(cid, agent, direction="BUY", candle_time="2026-09-21T14:00:00+00:00",
          asset="ETH-USD", **overrides):
    record = {
        "record_type": "agent_vote", "candidate_id": cid, "event_id": f"evt-{cid}",
        "variant_id": "agent-shadow-wide-v1", "asset": asset, "candle_time": candle_time,
        "n_met": 3, "entry_price": 2500.0, "atr": 30.0, "atr_stop": 2.5,
        "atr_target": 4.5, "max_hold_hours": 36, "agent_name": agent,
        "direction": direction, "confidence": 0.6,
        "reasoning_sha256": "abc", "reasoning": f"{agent} says {direction}",
        "model_id": "claude-haiku-4-5-20251001", "latency_ms": 800.0,
        "input_tokens": 500, "output_tokens": 120, "api_calls": 1,
        "cost_usd": 0.0012, "error": None, "signal_payload": {},
    }
    record.update(overrides)
    return record


def _decision(cid, direction="BUY", candle_time="2026-09-21T14:00:00+00:00",
              asset="ETH-USD", **overrides):
    record = {
        "record_type": "orchestrator_decision", "candidate_id": cid,
        "event_id": f"evt-{cid}", "variant_id": "agent-shadow-wide-v1",
        "asset": asset, "candle_time": candle_time, "n_met": 3,
        "entry_price": 2500.0, "atr": 30.0, "atr_stop": 2.5, "atr_target": 4.5,
        "max_hold_hours": 36, "agent_name": "orchestrator", "direction": direction,
        "confidence": 0.58, "reasoning_sha256": "xyz",
        "reasoning": "orchestrator combined reasoning",
        "model_id": "claude-sonnet-4-6", "latency_ms": 1200.0,
        "input_tokens": 1500, "output_tokens": 300, "api_calls": 1,
        "cost_usd": 0.009, "error": None, "decision_payload": {},
    }
    record.update(overrides)
    return record


def _attachment(cid, candle_time="2026-09-21T14:00:00+00:00", asset="ETH-USD"):
    return {
        "record_type": "price_attachment", "attachment_id": f"att-{cid}",
        "candidate_id": cid, "event_id": f"evt-{cid}",
        "variant_id": "agent-shadow-wide-v1", "asset": asset,
        "data_providers": {"candles": "coinbase"}, "candle_time": candle_time,
        "price_path": {
            "4h": {"close": 2510.0, "return": 0.004},
            "24h": {"close": 2550.0, "return": 0.02},
            "72h": {"close": 2600.0, "return": 0.04},
            "7d": {"close": 2480.0, "return": -0.008},
        },
        "atr_bracket": {"outcome": "TAKE_PROFIT", "hours": 30, "price": 2575.0,
                         "stop_price": 2425.0, "target_price": 2575.0},
    }


# ── Missing / empty log ──────────────────────────────────────────────────────

def test_missing_log_writes_a_placeholder_not_a_crash(vault) -> None:
    gj.generate_agent_shadow_notes()

    notes = list((vault / "AgentShadow").glob("*.md"))
    assert [n.name for n in notes] == ["Index.md"]
    text = (vault / "AgentShadow" / "Index.md").read_text(encoding="utf-8")
    assert "Awaiting first candidate" in text
    assert "OBSERVATION ONLY" in text


def test_empty_log_file_also_writes_the_placeholder(vault, tmp_path) -> None:
    _shadow_log(tmp_path, [])

    gj.generate_agent_shadow_notes()

    notes = list((vault / "AgentShadow").glob("*.md"))
    assert [n.name for n in notes] == ["Index.md"]
    assert "Awaiting first candidate" in (
        vault / "AgentShadow" / "Index.md").read_text(encoding="utf-8")


# ── A populated log ───────────────────────────────────────────────────────

def test_one_note_per_candidate_with_full_agent_detail(vault, tmp_path) -> None:
    cid = "cand-aaaa"
    records = [_vote(cid, agent) for agent in
               ("technical", "macro", "sentiment", "whale", "risk", "news")]
    records.append(_decision(cid))
    _shadow_log(tmp_path, records)

    gj.generate_agent_shadow_notes()

    notes = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    assert len(notes) == 1
    text = notes[0].read_text(encoding="utf-8")
    for agent in ("technical", "macro", "sentiment", "whale", "risk", "news"):
        assert agent in text
    assert "orchestrator combined reasoning" in text
    assert "n_met" in text
    assert "agent-shadow-wide-v1" in text
    assert "OBSERVATION ONLY" in text


def test_price_attachment_renders_when_present(vault, tmp_path) -> None:
    cid = "cand-bbbb"
    records = [_vote(cid, "technical"), _decision(cid), _attachment(cid)]
    _shadow_log(tmp_path, records)

    gj.generate_agent_shadow_notes()

    [note] = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    text = note.read_text(encoding="utf-8")
    assert "TAKE_PROFIT" in text
    assert "+2.000%" in text or "0.02" in text
    assert "Not yet attached" not in text


def test_unattached_candidate_says_so_honestly(vault, tmp_path) -> None:
    cid = "cand-cccc"
    _shadow_log(tmp_path, [_vote(cid, "technical"), _decision(cid)])

    gj.generate_agent_shadow_notes()

    [note] = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    text = note.read_text(encoding="utf-8")
    assert "Not yet attached" in text
    assert "TAKE_PROFIT" not in text


def test_index_lists_newest_first_with_variant(vault, tmp_path) -> None:
    older = "cand-old1"
    newer = "cand-new1"
    _shadow_log(tmp_path, [
        _vote(older, "technical", candle_time="2026-09-21T10:00:00+00:00"),
        _decision(older, candle_time="2026-09-21T10:00:00+00:00"),
        _vote(newer, "technical", candle_time="2026-09-21T18:00:00+00:00", asset="ZEC-USD"),
        _decision(newer, candle_time="2026-09-21T18:00:00+00:00", asset="ZEC-USD"),
    ])

    gj.generate_agent_shadow_notes()

    index = (vault / "AgentShadow" / "Index.md").read_text(encoding="utf-8")
    newer_pos = index.index("2026-09-21T18:00:00+00:00")
    older_pos = index.index("2026-09-21T10:00:00+00:00")
    assert newer_pos < older_pos, "index must list newest candle time first"
    assert "agent-shadow-wide-v1" in index
    assert "OBSERVATION ONLY" in index


def test_a_candidate_with_votes_but_no_decision_yet_is_still_rendered(
    vault, tmp_path
) -> None:
    """The orchestrator step can lag the vote step by design; the note must
    say so rather than silently omitting the candidate."""
    cid = "cand-dddd"
    _shadow_log(tmp_path, [_vote(cid, "technical")])

    gj.generate_agent_shadow_notes()

    notes = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    assert len(notes) == 1
    text = notes[0].read_text(encoding="utf-8")
    assert "not yet recorded" in text.lower()


def test_every_shadow_note_and_the_index_are_marked_owned(vault, tmp_path) -> None:
    cid = "cand-eeee"
    _shadow_log(tmp_path, [_vote(cid, "technical"), _decision(cid)])

    gj.generate_agent_shadow_notes()

    for note in (vault / "AgentShadow").glob("*.md"):
        assert gj._MARKER in note.read_text(encoding="utf-8")


def test_malformed_lines_are_skipped_not_fatal(vault, tmp_path) -> None:
    path = tmp_path / "logs" / "agent_shadow.jsonl"
    cid = "cand-ffff"
    path.write_text(
        json.dumps(_vote(cid, "technical")) + "\n"
        "not json at all\n"
        + json.dumps(_decision(cid)) + "\n",
        encoding="utf-8",
    )

    gj.generate_agent_shadow_notes()  # must not raise

    notes = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    assert len(notes) == 1


# ── LIVE / BACKFILL labelling (schema 2) ─────────────────────────────────────

def _timed(record: dict, timing: str, lag: float, decided_at: str) -> dict:
    return {**record, "timing": timing, "lag_hours_after_bar_close": lag,
            "decided_at": decided_at,
            "bar_close_time": "2026-09-21T15:00:00+00:00"}


def test_note_renders_the_flag_and_the_lag(vault, tmp_path) -> None:
    cid = "cand-live"
    _shadow_log(tmp_path, [
        _timed(_vote(cid, "technical"), "BACKFILL", 5.1, "2026-09-21T20:06:00+00:00"),
        _timed(_decision(cid, earliest_vote_at="2026-09-21T20:06:00+00:00",
                         latest_vote_at="2026-09-21T20:06:00+00:00"),
               "BACKFILL", 5.12, "2026-09-21T20:07:00+00:00"),
    ])

    gj.generate_agent_shadow_notes()

    [note] = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    text = note.read_text(encoding="utf-8")
    assert "timing: BACKFILL" in text
    assert "lag_hours_after_bar_close: 5.12" in text
    assert "**BACKFILL** — decided 5.12h after the bar closed" in text
    assert "timing=BACKFILL  lag=5.10h after bar close" in text  # the vote's own


def test_index_shows_timing_per_row_and_the_population_split(vault, tmp_path) -> None:
    live, back = "cand-a", "cand-b"
    _shadow_log(tmp_path, [
        _timed(_decision(live, candle_time="2026-09-21T18:00:00+00:00"),
               "LIVE", 0.1, "2026-09-21T19:06:00+00:00"),
        _timed(_decision(back, candle_time="2026-09-21T10:00:00+00:00"),
               "BACKFILL", 8.1, "2026-09-21T19:06:00+00:00"),
    ])

    gj.generate_agent_shadow_notes()

    index = (vault / "AgentShadow" / "Index.md").read_text(encoding="utf-8")
    assert "Populations: BACKFILL 1, LIVE 1" in index
    assert "| LIVE | 0.10 |" in index
    assert "| BACKFILL | 8.10 |" in index
    assert "applies to each on its own" in index


def test_a_record_without_timing_says_so(vault, tmp_path) -> None:
    """Schema-1 records predate the flag; never guess one for them."""
    cid = "cand-old"
    _shadow_log(tmp_path, [_vote(cid, "technical"), _decision(cid)])

    gj.generate_agent_shadow_notes()

    [note] = [p for p in (vault / "AgentShadow").glob("*.md") if p.name != "Index.md"]
    assert "timing: NOT RECORDED" in note.read_text(encoding="utf-8")


def test_a_log_of_only_checkpoints_is_still_awaiting_first_candidate(
    vault, tmp_path
) -> None:
    _shadow_log(tmp_path, [{
        "record_type": "scan_checkpoint", "variant_id": "agent-shadow-wide-v1",
        "run_at": "2026-09-23T10:05:00+00:00",
        "examined_through": {"ZEC-USD": "2026-09-23T09:00:00+00:00"},
    }])

    gj.generate_agent_shadow_notes()

    notes = list((vault / "AgentShadow").glob("*.md"))
    assert [n.name for n in notes] == ["Index.md"]
    text = notes[0].read_text(encoding="utf-8")
    assert "Awaiting first candidate" in text
    assert "ZEC-USD 2026-09-23T09:00:00+00:00" in text
