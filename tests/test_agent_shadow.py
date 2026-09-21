"""Hermetic contract tests for the log-only agent shadow."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from agent_shadow.attachments import attach_price_paths
from agent_shadow.budget import (
    BudgetedClient,
    CallTelemetry,
    MonthlyBudget,
    SpendCeilingExceeded,
)
from agent_shadow.candidates import Candidate, produce_candidates
from agent_shadow.log_store import DEFAULT_LOG, ShadowLog
from agent_shadow.runner import assert_variant_matches_runtime, run_candidates
from agent_shadow.variants import append_variant, get_variant, load_variants
from schemas.signals import AgentSignal, TradeDecision

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "agent_shadow_responses.json"
REGISTRY = ROOT / "docs" / "operations" / "agent_shadow_variants.jsonl"
SHADOW_PACKAGE = ROOT / "agent_shadow"


def _fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _variant() -> dict:
    return get_variant("agent-shadow-wide-v1", REGISTRY)


def _candidate() -> Candidate:
    return Candidate(
        event_id="event-fixture",
        asset="ZEC-USD",
        candle_time="2026-09-20T12:00:00+00:00",
        n_met=3,
        entry_price=50.0,
        atr=1.0,
        atr_stop=2.0,
        atr_target=3.5,
        max_hold_hours=36,
    )


def test_candidate_production_matches_fixed_slice_census(monkeypatch) -> None:
    index = pd.date_range("2026-01-01", periods=20, freq="h", tz="UTC")
    frame = pd.DataFrame(index=index)
    frame["close"] = [99.0] * 12 + [101.0] * 8
    frame["ema50"] = 100.0
    frame["rsi"] = 50.0
    frame["adx"] = 20.0
    frame["volume_ratio"] = 1.0
    frame["cvd_24h"] = -1.0
    frame["close_4h"] = 110.0
    frame["ema50_4h"] = 100.0
    frame["close_1d"] = 110.0
    frame["ema200_1d"] = 100.0
    frame["atr"] = 1.0
    frame.loc[index[13:16], "adx"] = 30.0
    frame.loc[index[14:16], "volume_ratio"] = 1.4
    frame.loc[index[15], "cvd_24h"] = 1.0

    monkeypatch.setattr(
        "agent_shadow.candidates.scanner.build_merged_frame",
        lambda *args, **kwargs: (frame, None),
    )
    candidates = produce_candidates(["ZEC-USD"], index[12], index[15])

    assert len(candidates) == 4
    assert [item.n_met for item in candidates] == [2, 3, 4, 5]


def test_variant_registry_is_append_only(tmp_path: Path) -> None:
    path = tmp_path / "variants.jsonl"
    variant = _variant()
    append_variant(variant, path)
    original = path.read_bytes()

    changed = json.loads(json.dumps(variant))
    changed["models"]["orchestrator"] = "different-model"
    with pytest.raises(ValueError, match="changes require a new id"):
        append_variant(changed, path)

    assert path.read_bytes() == original
    assert load_variants(path) == [variant]


def test_seeded_variant_declares_required_scope() -> None:
    variant = _variant()
    assert variant["candidate_definition"]["stage"] == "wide"
    assert variant["assets"] == ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
    assert set(variant["agents"]) == {
        "technical", "macro", "sentiment", "whale", "risk", "news"
    }
    assert variant["models"] == {
        "subagents": "claude-haiku-4-5-20251001",
        "orchestrator": "claude-sonnet-4-6",
    }
    assert_variant_matches_runtime(variant)


def test_vote_logging_round_trips(tmp_path: Path) -> None:
    log = ShadowLog(tmp_path / "shadow.jsonl")
    record = {
        "record_type": "agent_vote",
        "candidate_id": "candidate-1",
        "variant_id": "variant-1",
        "reasoning": "full recorded reasoning",
        "reasoning_sha256": "abc",
        "error": None,
    }
    written = log.append(record)
    assert log.records() == [written]


def test_real_shadow_log_is_protected_by_test_guard() -> None:
    with pytest.raises(AssertionError, match="REAL logs"):
        ShadowLog(DEFAULT_LOG).append({"record_type": "canary"})


def test_price_attachment_is_idempotent(tmp_path: Path) -> None:
    log = ShadowLog(tmp_path / "shadow.jsonl")
    candidate = _candidate()
    log.append(
        {
            "record_type": "orchestrator_decision",
            "candidate_id": "candidate-1",
            "event_id": candidate.event_id,
            "variant_id": "agent-shadow-wide-v1",
            "asset": candidate.asset,
            "candle_time": candidate.candle_time,
            "entry_price": 100.0,
            "atr": 1.0,
            "atr_stop": 2.0,
            "atr_target": 3.5,
            "max_hold_hours": 36,
        }
    )
    index = pd.date_range(candidate.candle_time, periods=169, freq="h", tz="UTC")
    frame = pd.DataFrame(
        {"close": [100.0 + i / 100 for i in range(169)]}, index=index
    )
    frame["low"] = frame["close"] - 0.25
    frame["high"] = frame["close"] + 0.25
    frame.loc[index[2], "high"] = 104.0

    def builder(*args, **kwargs):
        return frame, None

    assert attach_price_paths(log, frame_builder=builder) == 1
    assert attach_price_paths(log, frame_builder=builder) == 0
    attachments = [
        item for item in log.records() if item["record_type"] == "price_attachment"
    ]
    assert len(attachments) == 1
    assert set(attachments[0]["price_path"]) == {"4h", "24h", "72h", "7d"}
    assert attachments[0]["atr_bracket"]["outcome"] == "TAKE_PROFIT"


def test_spend_ceiling_refuses_before_transport(tmp_path: Path) -> None:
    class Messages:
        called = False

        def create(self, **kwargs):
            self.called = True
            return SimpleNamespace()

    inner = SimpleNamespace(messages=Messages())
    telemetry = CallTelemetry()
    budget = MonthlyBudget(tmp_path / "spend.json", ceiling_usd=0.0)
    client = BudgetedClient(inner, budget, telemetry)

    with pytest.raises(SpendCeilingExceeded, match="SPEND CEILING"):
        client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": "fixture"}],
        )

    assert inner.messages.called is False
    assert telemetry.refusal is not None
    assert budget.state()["spent_usd"] == 0.0


def test_agent_failure_is_logged_and_does_not_abort(tmp_path: Path) -> None:
    fixture = _fixture()

    class FixtureAgent:
        def __init__(self, name: str, fail: bool = False) -> None:
            self.name = name
            self.fail = fail

        def run(self, asset: str) -> AgentSignal:
            if self.fail:
                raise RuntimeError("recorded fixture failure")
            return AgentSignal.model_validate(fixture["signals"][self.name])

    class FixtureOrchestrator:
        def decide(self, asset: str, signals: list[AgentSignal]) -> TradeDecision:
            assert len(signals) == 6
            return TradeDecision.model_validate(fixture["decision"])

    factories = {
        name: (lambda name=name: FixtureAgent(name, fail=name == "whale"))
        for name in _variant()["agents"]
    }
    log = ShadowLog(tmp_path / "shadow.jsonl")
    counts = run_candidates(
        [_candidate()],
        _variant(),
        log=log,
        budget=MonthlyBudget(tmp_path / "spend.json", ceiling_usd=5.0),
        candidate_cap=10,
        agent_factories=factories,
        orchestrator_factory=FixtureOrchestrator,
    )

    assert counts == {"candidates": 1, "votes": 6, "decisions": 1, "skipped": 0}
    records = log.records()
    assert len(records) == 7
    failed = next(item for item in records if item.get("agent_name") == "whale")
    assert failed["direction"] == "NEUTRAL"
    assert failed["error"] == "recorded fixture failure"
    assert records[-1]["record_type"] == "orchestrator_decision"


_BANNED_MODULES = {
    "pipeline.runner",
    "pipeline.limit_orders",
    "pipeline.position_tracker",
    "exchange.coinbase_client",
}
_BANNED_CALLS = {"place_limit_order", "open_position", "submit_order", "create_order"}


def _order_path_offenders(source: str) -> list[str]:
    tree = ast.parse(source)
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            offenders.extend(alias.name for alias in node.names if alias.name in _BANNED_MODULES)
        elif isinstance(node, ast.ImportFrom) and node.module in _BANNED_MODULES:
            offenders.append(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in _BANNED_CALLS:
                offenders.append(node.func.id)
            elif isinstance(node.func, ast.Attribute) and node.func.attr in _BANNED_CALLS:
                offenders.append(node.func.attr)
    return offenders


def test_shadow_package_cannot_reach_order_path() -> None:
    offenders = {}
    for path in SHADOW_PACKAGE.glob("*.py"):
        found = _order_path_offenders(path.read_text(encoding="utf-8"))
        if found:
            offenders[path.name] = found
    assert offenders == {}


def test_order_path_guard_rejects_a_removed_boundary_canary() -> None:
    canary = "from pipeline.limit_orders import place_limit_order\nplace_limit_order()\n"
    assert _order_path_offenders(canary) == [
        "pipeline.limit_orders", "place_limit_order"
    ]
