"""
AgentOutputs/ shows decisions, not silence — and the vault is a faithful
rebuild each run, not an accumulating pile.

Covers:
  - _is_gate_skip_record's structural classification (empty votes AND empty
    agent_signals), independent of reasoning text;
  - generate_agent_notes() end to end: real decisions get one note each,
    gate-skip records never do, order-lifecycle events are excluded, and
    everything excluded is folded into the single Scanner Activity note;
  - the one-time legacy-AgentOutputs migration;
  - the marker-based ownership/pruning mechanism _write()/_prune_stale_notes()
    share with every owned folder, including AgentShadow/.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import backtesting.generate_journal as gj
import pipeline.scanner_activity as sa


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def vault(tmp_path, monkeypatch):
    """
    A throwaway vault, with the module's write-tracking state reset.

    Also redirects pipeline.scanner_activity's own paths, even though
    generate_agent_notes() only reads them (read_activity() never writes):
    otherwise a test's result would depend on whatever happens to be in the
    real, machine-specific logs/scanner_activity.jsonl at the time it runs.
    """
    destination = tmp_path / "vault"
    monkeypatch.setattr(gj, "VAULT", destination)
    monkeypatch.setattr(sa, "STATE_FILE", tmp_path / "scanner_activity_state.json")
    monkeypatch.setattr(sa, "DAILY_LOG", tmp_path / "scanner_activity.jsonl")
    gj._written_paths.clear()
    yield destination
    gj._written_paths.clear()


def _decisions_log(tmp_path, monkeypatch, records: list[dict]) -> Path:
    """
    Writes the scratch log and redirects ONLY `DECISIONS_LOG_PATH` — never
    `gj.ROOT` — so `_orchestrator_action_threshold()` keeps reading the real,
    committed `agents/orchestrator.py` instead of a tmp_path that does not
    have one. The decisions log and the orchestrator threshold are
    independent inputs; a test redirecting one must not silently lose the
    other.
    """
    path = tmp_path / "logs" / "agent_decisions.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(r) for r in records) + ("\n" if records else ""),
        encoding="utf-8")
    monkeypatch.setattr(gj, "DECISIONS_LOG_PATH", path)
    return path


def _real_decision(asset="ETH-USD", action="HOLD", ts="2026-04-15T16:31:47.914305+00:00"):
    return {
        "logged_at_utc": ts, "asset": asset, "action": action, "confidence": 0.6,
        "reasoning": "orchestrator combined reasoning", "veto_triggered": False,
        "veto_reason": None, "overrides": [], "position_size_pct": None,
        "stop_loss_price": None, "take_profit_price": None,
        "votes": [{"agent": "macro", "signal": "BUY", "confidence": 0.7, "weight_applied": 0.3}],
        "agent_signals": [{"agent": "macro", "signal": "BUY", "confidence": 0.7,
                            "reasoning": "bull regime", "metrics": {}}],
        "data_providers": {},
    }


def _gate_skip(asset="ETH-USD", ts="2026-07-11T00:05:00+00:00"):
    return {
        "logged_at_utc": ts, "asset": asset, "action": "HOLD", "confidence": 0.0,
        "reasoning": "Scanner gate: no breakout signal on last closed candle.",
        "veto_triggered": False, "veto_reason": None, "overrides": [],
        "position_size_pct": None, "stop_loss_price": None, "take_profit_price": None,
        "votes": [], "agent_signals": [], "data_providers": {},
    }


def _order_event(asset="ETH-USD", ts="2026-04-15T16:00:00+00:00"):
    return {"logged_at_utc": ts, "asset": asset, "event": "LIMIT_ORDER_FILLED",
            "order_id": "ord-1"}


# ── _is_gate_skip_record ─────────────────────────────────────────────────────

def test_gate_skip_is_recognised_by_empty_votes_and_signals() -> None:
    assert gj._is_gate_skip_record(_gate_skip()) is True


def test_a_real_decision_with_votes_is_not_a_gate_skip() -> None:
    assert gj._is_gate_skip_record(_real_decision()) is False


def test_classification_ignores_reasoning_text() -> None:
    """The test is structural, not string-matched — a real decision whose
    reasoning happens to mention 'Scanner gate' must not be misclassified."""
    record = _real_decision()
    record["reasoning"] = "Scanner gate fired; agents then voted BUY."
    assert gj._is_gate_skip_record(record) is False


# ── generate_agent_notes(): the split that matters ──────────────────────────

def test_real_decisions_each_get_their_own_note(vault, tmp_path, monkeypatch) -> None:
    _decisions_log(tmp_path, monkeypatch, [
        _real_decision(asset="ETH-USD", ts="2026-04-15T16:31:47.914305+00:00"),
        _real_decision(asset="BTC-USD", ts="2026-04-15T19:37:03.707992+00:00"),
    ])

    gj.generate_agent_notes()

    notes = sorted(p.name for p in (vault / "AgentOutputs").glob("*.md")
                    if p.name != "Scanner Activity.md")
    assert len(notes) == 2
    assert any("ETH-USD" in n for n in notes)
    assert any("BTC-USD" in n for n in notes)


def test_gate_skip_records_produce_no_individual_note(vault, tmp_path, monkeypatch) -> None:
    _decisions_log(tmp_path, monkeypatch, [
        _real_decision(),
        _gate_skip(), _gate_skip(), _gate_skip(),
    ])

    gj.generate_agent_notes()

    notes = list((vault / "AgentOutputs").glob("*.md"))
    assert len(notes) == 2  # one real decision + Scanner Activity.md
    for note in notes:
        text = note.read_text(encoding="utf-8")
        assert "Scanner gate: no breakout signal" not in text or note.name == "Scanner Activity.md"


def test_gate_skips_are_folded_into_scanner_activity(vault, tmp_path, monkeypatch) -> None:
    _decisions_log(tmp_path, monkeypatch, [
        _gate_skip(asset="ETH-USD", ts="2026-07-11T00:05:00+00:00"),
        _gate_skip(asset="ETH-USD", ts="2026-07-11T01:05:00+00:00"),
        _gate_skip(asset="ZEC-USD", ts="2026-07-12T00:05:00+00:00"),
    ])

    gj.generate_agent_notes()

    page = (vault / "AgentOutputs" / "Scanner Activity.md").read_text(encoding="utf-8")
    assert "| 2026-07-11 | ETH-USD | 2 |" in page
    assert "| 2026-07-12 | ZEC-USD | 1 |" in page
    assert "3 scanner-gate-silent record(s)" in page


def test_order_lifecycle_events_are_excluded_entirely(vault, tmp_path, monkeypatch) -> None:
    """No 'action' field — a different record shape, already covered by
    TradeJournal/ via trade_history.jsonl."""
    _decisions_log(tmp_path, monkeypatch, [
        _real_decision(), _order_event(), _order_event(ts="2026-04-15T17:00:00+00:00"),
    ])

    gj.generate_agent_notes()

    notes = list((vault / "AgentOutputs").glob("*.md"))
    assert len(notes) == 2  # one real decision + Scanner Activity.md
    page = (vault / "AgentOutputs" / "Scanner Activity.md").read_text(encoding="utf-8")
    assert "LIMIT_ORDER_FILLED" not in page


def test_missing_agent_decisions_log_writes_only_the_placeholder(
    vault, tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(gj, "DECISIONS_LOG_PATH", tmp_path / "logs" / "agent_decisions.jsonl")

    gj.generate_agent_notes()

    notes = list((vault / "AgentOutputs").glob("*.md"))
    assert [n.name for n in notes] == ["Scanner Activity.md"]


# ── _orchestrator_action_threshold(): parsed, not hardcoded ─────────────────

def test_threshold_is_parsed_from_the_real_orchestrator_docstring() -> None:
    """
    Pinned against the committed source, like generate_research_notes()'s
    other _require()-based parses: if agents/orchestrator.py's docstring
    line changes, this test — not a silent drift — is what catches it.
    """
    assert gj._orchestrator_action_threshold() == 0.55


def test_threshold_parser_raises_if_the_docstring_line_is_gone(
    tmp_path, monkeypatch
) -> None:
    stub = tmp_path / "orchestrator.py"
    stub.write_text('"""No threshold documented here."""\n', encoding="utf-8")
    monkeypatch.setattr(gj, "ORCHESTRATOR_PY_PATH", stub)

    with pytest.raises(RuntimeError, match="confidence-for-HOLD threshold"):
        gj._orchestrator_action_threshold()


def test_threshold_parser_follows_a_changed_docstring_value(
    tmp_path, monkeypatch
) -> None:
    stub = tmp_path / "orchestrator.py"
    stub.write_text(
        '"""\nConfidence < 0.40 always produces HOLD\n"""\n', encoding="utf-8")
    monkeypatch.setattr(gj, "ORCHESTRATOR_PY_PATH", stub)

    assert gj._orchestrator_action_threshold() == 0.40


# ── _is_individual_worthy(): the three-way OR ────────────────────────────────

def test_non_hold_action_is_always_individually_worthy() -> None:
    record = _real_decision(action="SELL")
    record["confidence"] = 0.0
    assert gj._is_individual_worthy(record, threshold=0.55) is True


def test_high_confidence_hold_is_individually_worthy() -> None:
    record = _real_decision(action="HOLD")
    record["confidence"] = 0.55
    assert gj._is_individual_worthy(record, threshold=0.55) is True


def test_low_confidence_hold_is_not_individually_worthy() -> None:
    record = _real_decision(action="HOLD")
    record["confidence"] = 0.54
    assert gj._is_individual_worthy(record, threshold=0.55) is False


def test_order_tied_hold_is_individually_worthy_even_at_low_confidence() -> None:
    """
    The load-bearing branch: of this project's 7 real trades, 2 carry
    confidence below 0.55 (0.46 and 0.41) and would be invisible as
    individual notes without this exact match on runner.py's stamp.
    """
    record = _real_decision(action="HOLD")
    record["confidence"] = 0.10
    record["reasoning"] = "[Limit] Order #abc123 placed at support $99.00. rest of reasoning"
    assert gj._is_individual_worthy(record, threshold=0.55) is True


def test_order_tie_match_is_a_prefix_not_a_substring() -> None:
    """A decision that merely MENTIONS a placed order in passing must not be
    swept in — only runner.py's actual stamp, at the start of `reasoning`."""
    record = _real_decision(action="HOLD")
    record["confidence"] = 0.10
    record["reasoning"] = "Noting that Order #abc123 was placed earlier is irrelevant here."
    assert gj._is_individual_worthy(record, threshold=0.55) is False


def test_generate_agent_notes_splits_individual_from_aggregate(
    vault, tmp_path, monkeypatch
) -> None:
    worthy = _real_decision(asset="ETH-USD", action="SELL",
                             ts="2026-04-15T16:31:47.914305+00:00")
    not_worthy = _real_decision(asset="ETH-USD", action="HOLD",
                                 ts="2026-04-15T17:00:00+00:00")
    not_worthy["confidence"] = 0.10
    _decisions_log(tmp_path, monkeypatch, [worthy, not_worthy])

    gj.generate_agent_notes()

    notes = list((vault / "AgentOutputs").glob("*.md"))
    assert len(notes) == 2  # one individual note + Scanner Activity.md
    page = (vault / "AgentOutputs" / "Scanner Activity.md").read_text(encoding="utf-8")
    assert "1 real decision(s)" in page
    assert "Agent-era decisions" in page


# ── One-time legacy migration ────────────────────────────────────────────────

def test_legacy_per_day_notes_are_removed(vault) -> None:
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    legacy = folder / "2026-08-16_decisions.md"
    legacy.write_text("# Agent Decisions — 2026-08-16\n\nsome content\n", encoding="utf-8")

    removed = gj._migrate_legacy_agent_outputs(folder)

    assert removed == 1
    assert not legacy.exists()


def test_migration_only_touches_the_exact_legacy_shape(vault) -> None:
    """A same-named file that was NOT actually written by the old generator
    (wrong heading) must survive — filename pattern alone is not proof."""
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    lookalike = folder / "2026-08-16_decisions.md"
    lookalike.write_text("# Something else entirely\n", encoding="utf-8")

    removed = gj._migrate_legacy_agent_outputs(folder)

    assert removed == 0
    assert lookalike.exists()


def test_migration_on_a_missing_folder_is_a_noop() -> None:
    assert gj._migrate_legacy_agent_outputs(Path("does/not/exist")) == 0


# ── Ownership marker + pruning (shared by every owned folder) ───────────────

def test_write_tags_every_note_with_the_marker(vault) -> None:
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    gj._write(folder / "note.md", "# hello\n")

    assert gj._MARKER in (folder / "note.md").read_text(encoding="utf-8")


def test_prune_removes_marker_tagged_files_not_rewritten_this_run(vault) -> None:
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    gj._write(folder / "old.md", "# old\n")
    gj._written_paths.clear()  # simulate a NEW run that does not rewrite it

    removed = gj._prune_stale_notes()

    assert removed == 1
    assert not (folder / "old.md").exists()


def test_prune_never_touches_a_file_without_the_marker(vault) -> None:
    """A human-authored file inside an owned folder — no marker, never ours."""
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    human = folder / "my_own_notes.md"
    human.write_text("# my own thoughts, not generator output\n", encoding="utf-8")

    removed = gj._prune_stale_notes()

    assert removed == 0
    assert human.exists()


def test_prune_leaves_files_this_run_rewrote(vault) -> None:
    folder = vault / "AgentOutputs"
    folder.mkdir(parents=True)
    gj._write(folder / "kept.md", "# kept\n")
    # _written_paths still has this path — simulating "written this run"

    removed = gj._prune_stale_notes()

    assert removed == 0
    assert (folder / "kept.md").exists()


def test_prune_only_touches_owned_folders(vault) -> None:
    """A marker-tagged file OUTSIDE the owned folders is not this generator's
    to prune — only _OWNED_FOLDERS (+ README.md) are ever scanned."""
    other = vault / "MarketNotes"
    other.mkdir(parents=True)
    tagged = other / "stray.md"
    tagged.write_text(f"# stray\n{gj._MARKER}\n", encoding="utf-8")

    removed = gj._prune_stale_notes()

    assert removed == 0
    assert tagged.exists()


# ── _veto_category(): buckets, not raw text ──────────────────────────────────

def test_no_veto_is_its_own_category() -> None:
    record = _real_decision()
    record["veto_triggered"] = False
    assert gj._veto_category(record) == "(no veto)"


def test_news_veto_prefix_is_recognised() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "[NewsVeto] critical exploit disclosed"
    assert gj._veto_category(record) == "NewsVeto (critical news)"


def test_risk_veto_prefix_is_recognised() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "[RiskVeto] RiskAgent absent"
    assert "RiskVeto" in gj._veto_category(record)


def test_circuit_breaker_prefix_is_recognised() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "[CircuitBreaker] drawdown halt"
    assert "CircuitBreaker" in gj._veto_category(record)


def test_macro_bear_keyword_is_recognised_case_insensitively() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "Macro regime is bear with an explicit SELL signal"
    assert "BEAR" in gj._veto_category(record)


def test_risk_metric_phrasing_is_recognised() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "ok_to_trade=false due to ATR volatility"
    assert "Risk veto" in gj._veto_category(record)


def test_unrecognised_veto_text_falls_to_the_honest_catch_all() -> None:
    record = _real_decision()
    record["veto_triggered"] = True
    record["reasoning"] = "Some brand new veto phrasing nobody has seen before."
    assert gj._veto_category(record) == "other veto (uncategorised)"


def test_veto_breakdown_produces_a_bounded_number_of_rows_not_one_per_instance() -> None:
    """The defect this covers: grouping on raw `veto_reason` text produced a
    row for nearly every instance (653 of 911 on the real log). Many records
    sharing a category, each with distinct free-text reasoning, must collapse
    to one row."""
    records = [
        {**_real_decision(), "veto_triggered": True,
         "reasoning": f"Macro regime is BEAR, unique detail #{i}"}
        for i in range(50)
    ]
    table = gj._veto_reason_breakdown(records)
    assert table.count("\n") < 5  # header + separator + exactly one data row
    assert "| Macro regime veto (BEAR / FULL_BEAR) | 50 |" in table


# ── _confidence_bucket() / _confidence_histogram() ───────────────────────────

def test_confidence_bucket_boundaries() -> None:
    assert gj._confidence_bucket(0.0) == "0.0\u20130.1"
    assert gj._confidence_bucket(0.09) == "0.0\u20130.1"
    assert gj._confidence_bucket(0.10) == "0.1\u20130.2"
    assert gj._confidence_bucket(0.54) == "0.5\u20130.6"
    assert gj._confidence_bucket(1.0) == "0.9\u20131.0"


def test_confidence_histogram_sums_to_input_count() -> None:
    records = [{"confidence": c} for c in (0.05, 0.12, 0.12, 0.99, 0.5)]
    table = gj._confidence_histogram(records)
    total = sum(int(line.split("|")[-2].strip())
                for line in table.splitlines() if line.startswith("| 0"))
    assert total == len(records)


# ── Individual notes render agent_signals and state empty votes plainly ─────

def test_individual_note_with_empty_votes_states_the_fact_not_a_generic_gap(
    vault, tmp_path, monkeypatch
) -> None:
    """
    Checked against the real log: empty `votes` alongside non-empty
    `agent_signals` happens on 13 of 1995 real records, all NewsVeto
    pre-check early-returns — not a general property of any era. An
    individually-worthy record with this shape must render agent_signals in
    full and say plainly, and specifically, why votes is empty.
    """
    record = _real_decision(action="SELL")  # non-HOLD -> individually worthy
    record["votes"] = []
    record["agent_signals"] = [
        {"agent": "macro", "signal": "SELL", "confidence": 0.7,
         "reasoning": "bear regime detail", "metrics": {}},
    ]
    _decisions_log(tmp_path, monkeypatch, [record])

    gj.generate_agent_notes()

    [note] = [p for p in (vault / "AgentOutputs").glob("*.md")
              if p.name != "Scanner Activity.md"]
    text = note.read_text(encoding="utf-8")
    assert "bear regime detail" in text  # agent_signals rendered in full
    assert "were not persisted on this specific record" in text
    assert "not a general gap in this era" in text


# ── _require()'s own error path must not crash on a scratch-file source ─────

def test_require_reports_a_source_outside_root_without_crashing(tmp_path) -> None:
    """
    `_require`'s failure message formats `source.relative_to(ROOT)` — which
    itself raises ValueError when `source` is a scratch/test file, not
    something under the real ROOT. That must not replace the intended
    RuntimeError with a confusing ValueError from the error path itself.
    """
    outside = tmp_path / "not_under_root.py"
    outside.write_text("nothing relevant here", encoding="utf-8")

    with pytest.raises(RuntimeError, match=re.escape(str(outside))):
        gj._require(r"NEVER MATCHES", "irrelevant text", outside, "a test fact")


# ── The ops doc stays in lockstep with the code ──────────────────────────────

def test_ops_doc_declares_the_same_threshold_as_the_code() -> None:
    doc = (Path(__file__).resolve().parents[1] / "docs" / "operations"
           / "agent_outputs_selection.md").read_text(encoding="utf-8")
    threshold = gj._orchestrator_action_threshold()
    assert f"{threshold:.2f}" in doc or f"0.{int(threshold * 100)}" in doc
    assert "[Limit] Order #" in doc
    assert "_BUY_THRESHOLD" in doc
