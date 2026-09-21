"""Run and log the declared shadow ensemble without exposing an order path."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Callable

from agent_shadow.budget import BudgetedClient, CallTelemetry, MonthlyBudget
from agent_shadow.candidates import Candidate, latest_candidates
from agent_shadow.log_store import DEFAULT_LOG, ShadowLog
from agent_shadow.variants import DEFAULT_REGISTRY, get_variant
from schemas.signals import AgentName, AgentSignal, SignalType

DEFAULT_CANDIDATE_CAP = 10
CANDIDATE_CAP_ENV = "AGENT_SHADOW_CANDIDATE_CAP"


def assert_variant_matches_runtime(variant: dict) -> None:
    """Refuse silent reuse of an id after the orchestrator weights drift."""
    from agents.orchestrator import _AGENT_WEIGHTS

    declared = {key: float(value) for key, value in variant["orchestrator_weighting"].items()}
    runtime = {key: float(value) for key, value in _AGENT_WEIGHTS.items()}
    if declared != runtime:
        raise ValueError(
            "declared orchestrator weighting no longer matches runtime; "
            "declare a new variant id before running"
        )


def _agent_factories() -> dict[str, Callable[[], object]]:
    from agents.asset_news_agent import AssetNewsAgent
    from agents.macro_agent import MacroAgent
    from agents.risk_agent import RiskAgent
    from agents.sentiment_agent import SentimentAgent
    from agents.technical_agent import TechnicalAgent
    from agents.whale_agent import WhaleAgent

    return {
        "technical": TechnicalAgent,
        "macro": MacroAgent,
        "sentiment": SentimentAgent,
        "whale": WhaleAgent,
        "risk": RiskAgent,
        "news": AssetNewsAgent,
    }


def _orchestrator_factory():
    from agents.orchestrator import OrchestratorAgent

    return OrchestratorAgent()


def _digest(reasoning: str) -> str:
    return hashlib.sha256(reasoning.encode("utf-8")).hexdigest()


def _decision_id(variant_id: str, event_id: str) -> str:
    return hashlib.sha256(f"{variant_id}|{event_id}".encode()).hexdigest()[:24]


def _fallback_signal(agent_name: str, asset: str, error: str) -> AgentSignal:
    return AgentSignal(
        agent=AgentName(agent_name),
        asset=asset,
        timestamp=datetime.now(timezone.utc),
        signal=SignalType.NEUTRAL,
        confidence=0.0,
        reasoning=f"Agent failed: {error}",
        ttl_minutes=0,
    )


def _tokens(telemetry: CallTelemetry) -> tuple[int | None, int | None]:
    if not telemetry.usage_available or telemetry.api_calls == 0:
        return None, None
    return telemetry.input_tokens, telemetry.output_tokens


def _base_record(candidate: Candidate, variant_id: str) -> dict:
    return {
        "candidate_id": _decision_id(variant_id, candidate.event_id),
        "event_id": candidate.event_id,
        "variant_id": variant_id,
        "asset": candidate.asset,
        "candle_time": candidate.candle_time,
        "n_met": candidate.n_met,
        "entry_price": candidate.entry_price,
        "atr": candidate.atr,
        "atr_stop": candidate.atr_stop,
        "atr_target": candidate.atr_target,
        "max_hold_hours": candidate.max_hold_hours,
        "data_providers": candidate.data_providers,
    }


def _signal_from_record(record: dict) -> AgentSignal:
    return AgentSignal.model_validate(record["signal_payload"])


def run_candidates(
    candidates: list[Candidate],
    variant: dict,
    *,
    log: ShadowLog,
    budget: MonthlyBudget,
    candidate_cap: int,
    agent_factories: dict[str, Callable[[], object]] | None = None,
    orchestrator_factory: Callable[[], object] | None = None,
) -> dict[str, int]:
    if candidate_cap <= 0:
        raise ValueError("candidate cap must be positive")
    if len(candidates) > candidate_cap:
        candidates = candidates[:candidate_cap]
        print(f"[agent-shadow] candidate cap applied: {candidate_cap}", file=sys.stderr)

    existing = log.records()
    votes_by_key = {
        (record.get("candidate_id"), record.get("agent_name")): record
        for record in existing
        if record.get("record_type") == "agent_vote"
    }
    completed = {
        record.get("candidate_id")
        for record in existing
        if record.get("record_type") == "orchestrator_decision"
    }
    factories = agent_factories or _agent_factories()
    make_orchestrator = orchestrator_factory or _orchestrator_factory
    variant_id = variant["variant_id"]
    subagent_model = variant["models"]["subagents"]
    orchestrator_model = variant["models"]["orchestrator"]
    counts = {"candidates": 0, "votes": 0, "decisions": 0, "skipped": 0}

    for candidate in candidates:
        base = _base_record(candidate, variant_id)
        candidate_id = base["candidate_id"]
        if candidate_id in completed:
            counts["skipped"] += 1
            continue
        counts["candidates"] += 1
        signals: list[AgentSignal] = []
        for agent_name in variant["agents"]:
            previous = votes_by_key.get((candidate_id, agent_name))
            if previous is not None:
                signals.append(_signal_from_record(previous))
                continue
            telemetry = CallTelemetry()
            started = perf_counter()
            error: str | None = None
            try:
                agent = factories[agent_name]()
                if hasattr(agent, "model"):
                    agent.model = subagent_model
                if hasattr(agent, "client"):
                    agent.client = BudgetedClient(agent.client, budget, telemetry)
                signal = agent.run(candidate.asset)
            except Exception as exc:
                error = str(exc)
                signal = _fallback_signal(agent_name, candidate.asset, error)
            if telemetry.refusal:
                error = telemetry.refusal
                print(f"[agent-shadow] {telemetry.refusal}", file=sys.stderr)
            elif telemetry.errors:
                error = "; ".join(telemetry.errors)
            elif signal.reasoning.startswith("Agent failed:"):
                error = signal.reasoning.removeprefix("Agent failed:").strip()
            latency_ms = round((perf_counter() - started) * 1000, 3)
            input_tokens, output_tokens = _tokens(telemetry)
            record = {
                **base,
                "record_type": "agent_vote",
                "agent_name": agent_name,
                "direction": signal.signal.value,
                "confidence": signal.confidence,
                "reasoning_sha256": _digest(signal.reasoning),
                "reasoning": signal.reasoning,
                "model_id": subagent_model,
                "latency_ms": latency_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "api_calls": telemetry.api_calls,
                "cost_usd": round(telemetry.cost_usd, 9),
                "error": error,
                "signal_payload": json.loads(signal.model_dump_json()),
            }
            log.append(record)
            votes_by_key[(candidate_id, agent_name)] = record
            signals.append(signal)
            counts["votes"] += 1

        telemetry = CallTelemetry()
        started = perf_counter()
        error = None
        decision_payload: dict
        try:
            orchestrator = make_orchestrator()
            if hasattr(orchestrator, "model"):
                orchestrator.model = orchestrator_model
            if hasattr(orchestrator, "client"):
                orchestrator.client = BudgetedClient(orchestrator.client, budget, telemetry)
            decision = orchestrator.decide(candidate.asset, signals)
            direction = decision.action.value
            confidence = decision.confidence
            reasoning = decision.reasoning
            decision_payload = json.loads(decision.model_dump_json())
        except Exception as exc:
            error = str(exc)
            direction = "HOLD"
            confidence = 0.0
            reasoning = f"Orchestrator failed closed: {exc}"
            decision_payload = {
                "asset": candidate.asset,
                "action": direction,
                "confidence": confidence,
                "reasoning": reasoning,
            }
        if telemetry.refusal:
            error = telemetry.refusal
            print(f"[agent-shadow] {telemetry.refusal}", file=sys.stderr)
        elif telemetry.errors:
            error = "; ".join(telemetry.errors)
        latency_ms = round((perf_counter() - started) * 1000, 3)
        input_tokens, output_tokens = _tokens(telemetry)
        log.append(
            {
                **base,
                "record_type": "orchestrator_decision",
                "agent_name": "orchestrator",
                "direction": direction,
                "confidence": confidence,
                "reasoning_sha256": _digest(reasoning),
                "reasoning": reasoning,
                "model_id": orchestrator_model,
                "latency_ms": latency_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "api_calls": telemetry.api_calls,
                "cost_usd": round(telemetry.cost_usd, 9),
                "error": error,
                "decision_payload": decision_payload,
            }
        )
        counts["decisions"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", default="agent-shadow-wide-v1")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--spend-file", type=Path, default=None)
    parser.add_argument("--candidate-cap", type=int, default=None)
    args = parser.parse_args(argv)

    variant = get_variant(args.variant, args.registry)
    assert_variant_matches_runtime(variant)
    cap = args.candidate_cap
    if cap is None:
        cap = int(os.getenv(CANDIDATE_CAP_ENV, str(DEFAULT_CANDIDATE_CAP)))
    budget_kwargs = {} if args.spend_file is None else {"path": args.spend_file}
    candidates = latest_candidates(variant["assets"])
    counts = run_candidates(
        candidates,
        variant,
        log=ShadowLog(args.log),
        budget=MonthlyBudget(**budget_kwargs),
        candidate_cap=cap,
    )
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
