"""Run and log the declared shadow ensemble without exposing an order path."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Callable, Iterable

import pandas as pd

from agent_shadow.budget import BudgetedClient, CallTelemetry, MonthlyBudget
from agent_shadow.candidates import (
    DEFAULT_LOOKBACK_HOURS,
    Candidate,
    ScanResult,
    scan_since,
)
from agent_shadow.log_store import DEFAULT_LOG, ShadowLog
from agent_shadow.variants import DEFAULT_REGISTRY, get_variant
from schemas.signals import AgentName, AgentSignal, SignalType

DEFAULT_CANDIDATE_CAP = 10
CANDIDATE_CAP_ENV = "AGENT_SHADOW_CANDIDATE_CAP"
LOOKBACK_HOURS_ENV = "AGENT_SHADOW_LOOKBACK_HOURS"

# A record is LIVE when it was decided within the hour after its bar closed —
# the regular hourly run for that bar — and BACKFILL otherwise. Derived from
# the measured lag on every record, never from how the run was invoked: a
# catch-up run still decides its newest bar LIVE and the older ones BACKFILL.
LIVE_WINDOW_HOURS = 1.0
BAR_HOURS = 1

DEFER_CANDIDATE_CAP = "candidate_cap"
DEFER_SPEND_CEILING = "spend_ceiling"


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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: str | datetime | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def timing_fields(candle_time: str, decided_at: datetime) -> dict:
    """
    When a record was decided, relative to the bar it is attached to.

    The lag is measured from the bar's CLOSE (candle_time + 1h), not its
    open: no decision can precede the close, so measuring from the open
    would put every live decision near one hour and blur the boundary.
    """
    bar_close = _as_utc(candle_time) + pd.Timedelta(hours=BAR_HOURS)
    decided = _as_utc(decided_at)
    lag_hours = (decided - bar_close) / pd.Timedelta(hours=1)
    return {
        "bar_close_time": bar_close.isoformat(),
        "decided_at": decided.isoformat(),
        "lag_hours_after_bar_close": round(float(lag_hours), 4),
        "timing": "LIVE" if lag_hours < LIVE_WINDOW_HOURS else "BACKFILL",
    }


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


@dataclass
class RunOutcome:
    """
    Counters plus the candidates this run deferred.

    ``skipped`` = already decided in an earlier run (dedup); nothing to do,
    ever. ``deferred`` = not decided YET because the candidate cap or the
    spend ceiling stopped this run; left undecided so the next run picks it
    up. The two are never merged: a skipped candidate is finished, a deferred
    one is owed.
    """

    counts: dict[str, int]
    deferred: list[tuple[Candidate, str]] = field(default_factory=list)


def run_candidates(
    candidates: list[Candidate],
    variant: dict,
    *,
    log: ShadowLog,
    budget: MonthlyBudget,
    candidate_cap: int,
    agent_factories: dict[str, Callable[[], object]] | None = None,
    orchestrator_factory: Callable[[], object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> RunOutcome:
    if candidate_cap <= 0:
        raise ValueError("candidate cap must be positive")
    now = clock or _utc_now

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
    counts = {"candidates": 0, "votes": 0, "decisions": 0, "skipped": 0, "deferred": 0}
    outcome = RunOutcome(counts=counts)

    # Dedup BEFORE the cap: an already-decided candidate costs nothing and
    # must not occupy a slot that an undecided one needs.
    pending: list[Candidate] = []
    for candidate in sorted(candidates, key=lambda item: (item.candle_time, item.asset)):
        if _decision_id(variant_id, candidate.event_id) in completed:
            counts["skipped"] += 1
        else:
            pending.append(candidate)

    def defer(rest: Iterable[Candidate], reason: str) -> None:
        rest = list(rest)
        for item in rest:
            outcome.deferred.append((item, reason))
        counts["deferred"] += len(rest)
        if rest:
            print(
                f"[agent-shadow] DEFERRED {len(rest)} candidate(s) ({reason}); "
                f"oldest {rest[0].asset} {rest[0].candle_time}. Left undecided for "
                "the next run — not dropped.",
                file=sys.stderr,
            )

    if len(pending) > candidate_cap:
        defer(pending[candidate_cap:], DEFER_CANDIDATE_CAP)
        pending = pending[:candidate_cap]

    for index, candidate in enumerate(pending):
        base = _base_record(candidate, variant_id)
        candidate_id = base["candidate_id"]
        signals: list[AgentSignal] = []
        vote_times: list[str] = []
        ceiling_hit = False
        for agent_name in variant["agents"]:
            previous = votes_by_key.get((candidate_id, agent_name))
            if previous is not None:
                signals.append(_signal_from_record(previous))
                if previous.get("decided_at"):
                    vote_times.append(previous["decided_at"])
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
                # A spend-ceiling refusal is not an observation of the agent;
                # recording it as a NEUTRAL vote would let the candidate be
                # "decided" on missing input and then skipped forever. Write
                # nothing for this agent and defer the candidate instead.
                print(f"[agent-shadow] {telemetry.refusal}", file=sys.stderr)
                ceiling_hit = True
                break
            if telemetry.errors:
                error = "; ".join(telemetry.errors)
            elif signal.reasoning.startswith("Agent failed:"):
                error = signal.reasoning.removeprefix("Agent failed:").strip()
            latency_ms = round((perf_counter() - started) * 1000, 3)
            input_tokens, output_tokens = _tokens(telemetry)
            timing = timing_fields(candidate.candle_time, now())
            record = {
                **base,
                **timing,
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
            vote_times.append(timing["decided_at"])
            counts["votes"] += 1

        if ceiling_hit:
            defer(pending[index:], DEFER_SPEND_CEILING)
            break

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
            # Votes already written are kept: they were paid for and the next
            # run reuses them. Only the decision is owed.
            print(f"[agent-shadow] {telemetry.refusal}", file=sys.stderr)
            defer(pending[index:], DEFER_SPEND_CEILING)
            break
        if telemetry.errors:
            error = "; ".join(telemetry.errors)
        latency_ms = round((perf_counter() - started) * 1000, 3)
        input_tokens, output_tokens = _tokens(telemetry)
        log.append(
            {
                **base,
                **timing_fields(candidate.candle_time, now()),
                # A decision can reuse votes recorded by an earlier, deferred
                # run; these bound when its inputs were actually gathered.
                "earliest_vote_at": min(vote_times) if vote_times else None,
                "latest_vote_at": max(vote_times) if vote_times else None,
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
        completed.add(candidate_id)
        counts["candidates"] += 1
        counts["decisions"] += 1
    return outcome


def resume_points(
    records: Iterable[dict], variant_id: str, assets: Iterable[str]
) -> dict[str, pd.Timestamp | None]:
    """
    The first bar each asset still needs examining, read from the log itself.

    Two sources, both specific to ``variant_id``: the newest ``scan_checkpoint``
    ``examined_through`` per asset (every bar up to it was examined and every
    candidate there decided), resumed one bar later; and the newest decided
    candidate's ``candle_time``, resumed at that same bar (dedup makes the
    overlap free). ``None`` means no record for the asset at all: cold start.
    """
    checkpoint: dict[str, pd.Timestamp] = {}
    decided: dict[str, pd.Timestamp] = {}
    for record in records:
        if record.get("variant_id") != variant_id:
            continue
        kind = record.get("record_type")
        if kind == "scan_checkpoint":
            for asset, stamp in (record.get("examined_through") or {}).items():
                value = pd.Timestamp(stamp)
                if asset not in checkpoint or value > checkpoint[asset]:
                    checkpoint[asset] = value
        elif kind == "orchestrator_decision":
            asset = record.get("asset")
            value = pd.Timestamp(record["candle_time"])
            if asset not in decided or value > decided[asset]:
                decided[asset] = value

    points: dict[str, pd.Timestamp | None] = {}
    for asset in assets:
        options = []
        if asset in checkpoint:
            options.append(checkpoint[asset] + pd.Timedelta(hours=BAR_HOURS))
        if asset in decided:
            options.append(decided[asset])
        points[asset] = max(options) if options else None
    return points


def checkpoint_record(
    variant_id: str,
    scan: ScanResult,
    outcome: RunOutcome,
    *,
    lookback_hours: int,
    run_at: datetime,
) -> dict:
    """
    Durable proof of what this run examined. Per asset, ``examined_through``
    stops one bar BEFORE that asset's oldest deferred candidate, so the next
    run re-examines — and decides — exactly what this one left owed.
    """
    examined = dict(scan.examined_through)
    for candidate, _reason in outcome.deferred:
        owed = pd.Timestamp(candidate.candle_time) - pd.Timedelta(hours=BAR_HOURS)
        current = examined.get(candidate.asset)
        if current is None or owed < pd.Timestamp(current):
            examined[candidate.asset] = owed.isoformat()
    return {
        "record_type": "scan_checkpoint",
        "variant_id": variant_id,
        "run_at": pd.Timestamp(run_at).tz_convert("UTC").isoformat(),
        "closed_cutoff": scan.closed_cutoff,
        "examined_through": examined,
        "bars_examined": scan.bars_examined,
        "cold_start": scan.cold_start,
        "lookback_hours": lookback_hours,
        "lookback_truncated": scan.truncated,
        "counts": outcome.counts,
        "deferred": [
            {"asset": c.asset, "candle_time": c.candle_time,
             "event_id": c.event_id, "reason": reason}
            for c, reason in outcome.deferred
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", default="agent-shadow-wide-v1")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--spend-file", type=Path, default=None)
    parser.add_argument("--candidate-cap", type=int, default=None)
    parser.add_argument("--lookback-hours", type=int, default=None)
    args = parser.parse_args(argv)

    variant = get_variant(args.variant, args.registry)
    assert_variant_matches_runtime(variant)
    cap = args.candidate_cap
    if cap is None:
        cap = int(os.getenv(CANDIDATE_CAP_ENV, str(DEFAULT_CANDIDATE_CAP)))
    lookback = args.lookback_hours
    if lookback is None:
        lookback = int(os.getenv(LOOKBACK_HOURS_ENV, str(DEFAULT_LOOKBACK_HOURS)))
    budget_kwargs = {} if args.spend_file is None else {"path": args.spend_file}

    log = ShadowLog(args.log)
    run_at = _utc_now()
    resume = resume_points(log.records(), variant["variant_id"], variant["assets"])
    scan = scan_since(variant["assets"], resume, now=run_at, lookback_hours=lookback)
    outcome = run_candidates(
        scan.candidates,
        variant,
        log=log,
        budget=MonthlyBudget(**budget_kwargs),
        candidate_cap=cap,
    )
    log.append(checkpoint_record(
        variant["variant_id"], scan, outcome, lookback_hours=lookback, run_at=run_at))
    summary = {
        "bars_examined": sum(scan.bars_examined.values()),
        "candidates_found": len(scan.candidates),
        **outcome.counts,
        "cold_start": scan.cold_start,
        "lookback_truncated": sorted(scan.truncated),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
