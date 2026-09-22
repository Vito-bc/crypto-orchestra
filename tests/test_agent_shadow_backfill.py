"""
Catch-up behaviour of the agent shadow: a run examines every closed bar since
the last one this variant recorded, not only the newest.

Hermetic: the scanner's frame builder and detector are stubbed, agents and
orchestrator are recorded fixtures, and no model or network call is made.
Every test drives ``agent_shadow.runner.main`` end to end except where a unit
of it is named explicitly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import agent_shadow.runner as runner
from agent_shadow.budget import SpendCeilingExceeded
from agent_shadow.log_store import ShadowLog
from agent_shadow.variants import get_variant
from schemas.signals import AgentSignal, TradeDecision

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads(
    (ROOT / "tests" / "fixtures" / "agent_shadow_responses.json").read_text(encoding="utf-8"))
REGISTRY = ROOT / "docs" / "operations" / "agent_shadow_variants.jsonl"
ASSET = "ZEC-USD"


def _t(text: str) -> pd.Timestamp:
    return pd.Timestamp(text, tz="UTC")


def _variant() -> dict:
    return dict(get_variant("agent-shadow-wide-v1", REGISTRY), assets=[ASSET])


class _Agent:
    def __init__(self, name: str) -> None:
        self.name = name

    def run(self, asset: str) -> AgentSignal:
        return AgentSignal.model_validate(FIXTURE["signals"][self.name])


class _Orchestrator:
    def decide(self, asset, signals) -> TradeDecision:
        return TradeDecision.model_validate(FIXTURE["decision"])


class _Env:
    """One shadow deployment: a log, a spend file, a clock and a market."""

    def __init__(self, tmp_path: Path, monkeypatch) -> None:
        self.log_path = tmp_path / "agent_shadow.jsonl"
        self.spend_path = tmp_path / "spend.json"
        self.now = datetime(2026, 9, 22, 12, 5, tzinfo=timezone.utc)
        self.candidate_bars: set[pd.Timestamp] | None = None  # None = every bar
        self.last_bar: pd.Timestamp | None = None  # simulate data lag
        self.monkeypatch = monkeypatch

        monkeypatch.setattr(runner, "get_variant", lambda *a, **k: _variant())
        monkeypatch.setattr(runner, "assert_variant_matches_runtime", lambda *a: None)
        monkeypatch.setattr(runner, "_agent_factories", lambda: {
            name: (lambda name=name: _Agent(name)) for name in _variant()["agents"]})
        monkeypatch.setattr(runner, "_orchestrator_factory", _Orchestrator)
        monkeypatch.setattr(runner, "_utc_now", lambda: self.now)
        monkeypatch.setattr("agent_shadow.candidates.scanner.build_merged_frame",
                            self._frame)
        monkeypatch.setattr("agent_shadow.candidates.scanner._detect_breakout_signal",
                            self._detect)

    def _frame(self, asset, start, end, cfg, btc_regime_applicable=False):
        last = self.last_bar or (_t(end) - pd.Timedelta(hours=1))
        index = pd.date_range(_t(start), last, freq="h")
        frame = pd.DataFrame({"close": 50.0, "atr": 1.0}, index=index)
        return frame, None

    def _detect(self, frame, position, cfg, btc_regime_applicable=False):
        stamp = frame.index[position]
        if self.candidate_bars is None or stamp in self.candidate_bars:
            return {"blocked": None, "n_conditions": 4}
        return None

    def run(self, capsys, *args: str) -> dict:
        runner.main(["--log", str(self.log_path), "--spend-file", str(self.spend_path),
                     *args])
        return json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    def records(self, kind: str | None = None) -> list[dict]:
        records = ShadowLog(self.log_path).records()
        return [r for r in records if kind is None or r.get("record_type") == kind]

    def seed_checkpoint(self, examined_through: str) -> None:
        ShadowLog(self.log_path).append({
            "record_type": "scan_checkpoint",
            "variant_id": _variant()["variant_id"],
            "examined_through": {ASSET: _t(examined_through).isoformat()},
        })


@pytest.fixture()
def env(tmp_path, monkeypatch) -> _Env:
    return _Env(tmp_path, monkeypatch)


def _decided_times(env: _Env) -> list[str]:
    return sorted(r["candle_time"] for r in env.records("orchestrator_decision"))


# ── A gap is backfilled, not thinned ─────────────────────────────────────────

def test_after_a_gap_every_missed_hour_is_examined(env, capsys) -> None:
    """The defect: a catch-up run examined only the newest bar."""
    env.seed_checkpoint("2026-09-22T05:00")  # last examined before a 6h sleep

    summary = env.run(capsys)

    assert summary["bars_examined"] == 6
    assert summary["decisions"] == 6
    assert _decided_times(env) == [
        _t(f"2026-09-22T{h:02d}:00").isoformat() for h in range(6, 12)]


def test_live_versus_backfill_is_derived_from_the_lag(env, capsys) -> None:
    """
    Run at 12:05: the 11:00 bar closed at 12:00, so it is decided within its
    own hour (LIVE); every older bar in the same run is BACKFILL. Nothing
    about how the run was invoked decides the label.
    """
    env.seed_checkpoint("2026-09-22T08:00")

    env.run(capsys)

    by_bar = {r["candle_time"]: r for r in env.records("orchestrator_decision")}
    newest = by_bar[_t("2026-09-22T11:00").isoformat()]
    older = by_bar[_t("2026-09-22T09:00").isoformat()]
    assert newest["timing"] == "LIVE"
    assert 0 <= newest["lag_hours_after_bar_close"] < 1
    assert older["timing"] == "BACKFILL"
    assert older["lag_hours_after_bar_close"] == pytest.approx(2 + 5 / 60, abs=1e-3)
    assert older["decided_at"] == env.now.isoformat()
    for vote in env.records("agent_vote"):
        assert {"decided_at", "bar_close_time", "lag_hours_after_bar_close",
                "timing"} <= vote.keys()


def test_timing_boundary_is_one_hour_after_bar_close() -> None:
    live = runner.timing_fields("2026-09-22T10:00:00+00:00",
                                datetime(2026, 9, 22, 11, 59, tzinfo=timezone.utc))
    late = runner.timing_fields("2026-09-22T10:00:00+00:00",
                                datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc))
    assert live["timing"] == "LIVE"
    assert late["timing"] == "BACKFILL"
    assert late["bar_close_time"] == "2026-09-22T11:00:00+00:00"


# ── Re-running is free ──────────────────────────────────────────────────────

def test_rerunning_immediately_writes_nothing_new(env, capsys) -> None:
    env.seed_checkpoint("2026-09-22T05:00")
    env.run(capsys)
    decisions_before = len(env.records("orchestrator_decision"))

    summary = env.run(capsys)

    assert summary["decisions"] == 0
    assert len(env.records("orchestrator_decision")) == decisions_before


def test_dedup_holds_when_the_checkpoint_was_lost(env, capsys) -> None:
    """
    A run killed after writing decisions but before its checkpoint (the task
    has a 20-minute limit) resumes at the newest DECIDED bar, inclusive, and
    re-finds it. It is skipped — never decided twice.
    """
    env.seed_checkpoint("2026-09-22T05:00")
    env.run(capsys)
    kept = [r for r in env.records() if r.get("record_type") != "scan_checkpoint"
            or r.get("run_at") is None]  # drop the run's checkpoint, keep the seed
    env.log_path.write_text("".join(json.dumps(r) + "\n" for r in kept), encoding="utf-8")

    summary = env.run(capsys)

    assert summary["decisions"] == 0
    assert summary["skipped"] == 1
    assert len(env.records("orchestrator_decision")) == 6


def test_the_same_candidates_twice_are_all_skipped(tmp_path) -> None:
    """Dedup at the runner itself, independent of any resume point."""
    from agent_shadow.budget import MonthlyBudget
    from agent_shadow.candidates import Candidate

    candidates = [
        Candidate(event_id=f"e{h}", asset=ASSET,
                  candle_time=_t(f"2026-09-22T{h:02d}:00").isoformat(),
                  n_met=4, entry_price=50.0, atr=1.0, atr_stop=2.0,
                  atr_target=3.5, max_hold_hours=36)
        for h in range(6, 9)
    ]
    log = ShadowLog(tmp_path / "shadow.jsonl")
    kwargs = dict(log=log, budget=MonthlyBudget(tmp_path / "spend.json"),
                  candidate_cap=10,
                  agent_factories={n: (lambda n=n: _Agent(n)) for n in _variant()["agents"]},
                  orchestrator_factory=_Orchestrator)

    first = runner.run_candidates(candidates, _variant(), **kwargs)
    second = runner.run_candidates(candidates, _variant(), **kwargs)

    assert first.counts["decisions"] == 3
    assert (second.counts["decisions"], second.counts["skipped"],
            second.counts["deferred"]) == (0, 3, 0)


# ── The look-back cap ───────────────────────────────────────────────────────

def test_lookback_cap_truncates_and_says_so(env, capsys) -> None:
    env.candidate_bars = set()  # no candidates; this is about bars examined
    env.seed_checkpoint("2026-09-18T00:00")  # resume 09-18 01:00, 106h behind

    runner.main(["--log", str(env.log_path), "--spend-file", str(env.spend_path),
                 "--lookback-hours", "72"])
    captured = capsys.readouterr()
    summary = json.loads(captured.out.strip().splitlines()[-1])

    assert "LOOK-BACK CAP" in captured.err
    assert summary["lookback_truncated"] == [ASSET]
    assert summary["bars_examined"] == 73  # cutoff-72h .. cutoff, inclusive
    checkpoint = env.records("scan_checkpoint")[-1]
    truncation = checkpoint["lookback_truncated"][ASSET]
    assert truncation["examined_from"] == _t("2026-09-19T11:00").isoformat()
    assert truncation["bars_never_examined"] == 34  # 09-18 01:00 .. 09-19 10:00


# ── Caps defer; they never drop ─────────────────────────────────────────────

def test_candidate_cap_defers_and_the_next_run_picks_up(env, capsys) -> None:
    env.seed_checkpoint("2026-09-22T05:00")  # six owed bars

    first = env.run(capsys, "--candidate-cap", "4")

    assert (first["decisions"], first["deferred"], first["skipped"]) == (4, 2, 0)
    checkpoint = env.records("scan_checkpoint")[-1]
    assert checkpoint["examined_through"][ASSET] == _t("2026-09-22T09:00").isoformat()
    assert [d["reason"] for d in checkpoint["deferred"]] == ["candidate_cap"] * 2

    second = env.run(capsys, "--candidate-cap", "4")

    assert (second["decisions"], second["deferred"]) == (2, 0)
    assert _decided_times(env) == [
        _t(f"2026-09-22T{h:02d}:00").isoformat() for h in range(6, 12)]


class _CallingAgent(_Agent):
    """A fixture agent that spends through the budgeted client, as real ones do."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.model = None
        usage = SimpleNamespace(input_tokens=10, output_tokens=10)
        self.client = SimpleNamespace(messages=SimpleNamespace(
            create=lambda **kw: SimpleNamespace(usage=usage)))

    def run(self, asset: str) -> AgentSignal:
        self.client.messages.create(model=self.model, max_tokens=64, messages=[])
        return super().run(asset)


class _CallingOrchestrator(_Orchestrator):
    def __init__(self) -> None:
        self.model = None
        usage = SimpleNamespace(input_tokens=10, output_tokens=10)
        self.client = SimpleNamespace(messages=SimpleNamespace(
            create=lambda **kw: SimpleNamespace(usage=usage)))

    def decide(self, asset, signals) -> TradeDecision:
        self.client.messages.create(model=self.model, max_tokens=64, messages=[])
        return super().decide(asset, signals)


class _CountingBudget:
    """Refuses every reservation after ``allow`` of them — a spend ceiling."""

    def __init__(self, allow: int | None) -> None:
        self.allow = allow
        self.calls = 0

    def reserve(self, model, kwargs):
        self.calls += 1
        if self.allow is not None and self.calls > self.allow:
            raise SpendCeilingExceeded("AGENT SHADOW SPEND CEILING: fixture refusal")
        return f"r{self.calls}", 0.0

    def settle(self, reservation_id, actual_usd):
        return 0.0


def test_spend_ceiling_defers_mid_candidate_and_the_next_run_completes_it(
    env, capsys, monkeypatch
) -> None:
    env.seed_checkpoint("2026-09-22T07:00")  # four owed bars: 08..11
    monkeypatch.setattr(runner, "_agent_factories", lambda: {
        name: (lambda name=name: _CallingAgent(name)) for name in _variant()["agents"]})
    monkeypatch.setattr(runner, "_orchestrator_factory", _CallingOrchestrator)
    # 7 calls per candidate: two full candidates, then three votes of the third.
    budget = _CountingBudget(allow=17)
    monkeypatch.setattr(runner, "MonthlyBudget", lambda **kw: budget)

    first = env.run(capsys)

    assert (first["decisions"], first["deferred"]) == (2, 2)
    owed = _t("2026-09-22T10:00").isoformat()
    partial = [v for v in env.records("agent_vote") if v["candle_time"] == owed]
    assert len(partial) == 3, "paid-for votes are kept for reuse"
    assert all(v["error"] is None for v in env.records("agent_vote")), \
        "a ceiling refusal must never be recorded as a NEUTRAL vote"
    assert owed not in _decided_times(env)

    budget.allow = None
    env.now = datetime(2026, 9, 22, 12, 40, tzinfo=timezone.utc)
    second = env.run(capsys)

    assert (second["decisions"], second["deferred"]) == (2, 0)
    assert second["votes"] == 3 + 6  # the rest of bar 10:00, all of bar 11:00
    decision = next(d for d in env.records("orchestrator_decision")
                    if d["candle_time"] == owed)
    assert decision["earliest_vote_at"] < decision["latest_vote_at"], \
        "a decision built from two runs' votes must say so"


# ── Cold start ──────────────────────────────────────────────────────────────

def test_cold_start_examines_only_the_newest_bar(env, capsys) -> None:
    """Every bar in the 120-day warm-up is a candidate here; none is replayed."""
    summary = env.run(capsys)

    assert summary["cold_start"] == [ASSET]
    assert summary["bars_examined"] == 1
    assert _decided_times(env) == [_t("2026-09-22T11:00").isoformat()]


def test_the_first_quiet_run_still_leaves_a_resume_point(env, capsys) -> None:
    """
    The log holds no candidate yet. Without a checkpoint a sleep before the
    first candidate would thin the stream exactly as before this change.
    """
    env.candidate_bars = set()
    env.run(capsys)
    env.now = datetime(2026, 9, 22, 18, 5, tzinfo=timezone.utc)
    env.candidate_bars = None

    summary = env.run(capsys)

    assert summary["cold_start"] == []
    assert summary["bars_examined"] == 6  # 12:00 .. 17:00
    assert summary["decisions"] == 6


def test_a_bar_missing_from_the_data_is_not_marked_examined(env, capsys) -> None:
    env.seed_checkpoint("2026-09-22T05:00")
    env.last_bar = _t("2026-09-22T09:00")  # 10:00 and 11:00 not served yet

    env.run(capsys)

    checkpoint = env.records("scan_checkpoint")[-1]
    assert checkpoint["examined_through"][ASSET] == _t("2026-09-22T09:00").isoformat()
    env.last_bar = None
    summary = env.run(capsys)
    assert summary["bars_examined"] == 2
    assert summary["decisions"] == 2


def test_resume_points_ignore_other_variants() -> None:
    records = [
        {"record_type": "scan_checkpoint", "variant_id": "other",
         "examined_through": {ASSET: "2026-09-22T10:00:00+00:00"}},
    ]
    assert runner.resume_points(records, "agent-shadow-wide-v1", [ASSET]) == {ASSET: None}


def test_every_backfill_frame_is_built_coinbase_strict(env, capsys, monkeypatch) -> None:
    from backtesting import signal_scanner as scanner

    seen: list[bool] = []
    original = env._frame

    def spy(*args, **kwargs):
        seen.append(scanner.STRICT_COINBASE_ONLY)
        return original(*args, **kwargs)

    monkeypatch.setattr("agent_shadow.candidates.scanner.build_merged_frame", spy)
    env.seed_checkpoint("2026-09-22T05:00")

    env.run(capsys)

    assert seen == [True]
    assert scanner.STRICT_COINBASE_ONLY is False, "strict mode must be restored"
