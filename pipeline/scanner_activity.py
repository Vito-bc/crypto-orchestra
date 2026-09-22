"""
Per-day, per-asset scanner-activity tally — the counter for pipeline
evaluations where agents were never called.

Why this module exists: `run_pipeline()` used to write an `agent_decisions.jsonl`
record every time the scanner gate found nothing to evaluate — no agents ran,
no vote happened, nothing was decided. That is not a decision; it is silence.
Recording it as one anyway meant the decisions log, and every note the vault
generator built from it, filled up with thousands of

    ETH-USD -> HOLD (conf: 0%)
    "Scanner gate: no breakout signal on last closed candle."

for the seven times an ensemble vote actually happened. This module replaces
that per-event record with a per-day counter: one row per (date, asset)
summarising how many hours were evaluated, how many the scanner was silent
on, and how many were blocked before agents ran, broken down by reason.

Design
------
`STATE_FILE` holds the IN-PROGRESS tally for whichever UTC date each asset
last reported against — a small JSON dict, updated in place, the same shape
as `open_positions.json` / `pending_orders.json` elsewhere in this project.
`DAILY_LOG` is the append-only history of CLOSED days: one JSON line per
(date, asset), written the moment a later event proves that day is over.

There is no clock-driven flush. A day is "closed" lazily, by the next event
for that asset landing on a later UTC date — so a multi-week pipeline outage
(as this project has had) does not lose or corrupt anything: whenever the
pipeline resumes, the stale in-progress tally is flushed as that day's
summary before today's tally starts. The vault generator additionally reads
the still-open STATE_FILE entry for "today so far", so a day in progress is
visible without waiting for the day to roll over.

Gate reasons
------------
`NO_SIGNAL` is the scanner-silent case — the common one, tallied in its own
field (`hours_scanner_silent`) because it answers "how often is there simply
nothing to evaluate," a different question from "how often did something
block a signal the scanner did find." The other declared reasons
(`IDEMPOTENCY_DUPLICATE`, `V3_ENFORCEMENT_BLOCK`) land in the `hours_blocked`
breakdown. All three share one property: agents were never called, so none of
them belongs in `agent_decisions.jsonl`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"

# The in-progress tally, one entry per asset, updated in place.
STATE_FILE = LOG_DIR / "scanner_activity_state.json"
# The closed-day history: one JSON line per (date, asset).
DAILY_LOG = LOG_DIR / "scanner_activity.jsonl"

# The scanner found nothing to evaluate at all -- tracked separately from the
# other gate reasons below (see module docstring).
NO_SIGNAL = "no_signal"

# Declared gate reasons under which agents are skipped despite the scanner
# having found a signal. Both currently short-circuit run_pipeline() before
# any sub-agent is called.
IDEMPOTENCY_DUPLICATE = "idempotency_duplicate"
V3_ENFORCEMENT_BLOCK = "v3_enforcement_block"

GATE_REASONS = (NO_SIGNAL, IDEMPOTENCY_DUPLICATE, V3_ENFORCEMENT_BLOCK)


def _today(now: Optional[datetime] = None) -> str:
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _empty_tally(date: str, asset: str) -> dict:
    return {
        "date": date,
        "asset": asset,
        "hours_evaluated": 0,
        "hours_scanner_silent": 0,
        "hours_blocked": {},
    }


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _append_daily_log(tally: dict) -> None:
    DAILY_LOG.parent.mkdir(parents=True, exist_ok=True)
    with DAILY_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(tally, sort_keys=True) + "\n")


def _tally_for(state: dict, asset: str, today: str) -> dict:
    """
    The in-progress tally for `asset`, flushing a stale one first.

    If the asset's open tally belongs to an earlier UTC date, that date is
    over: it is appended to DAILY_LOG as its closed summary, and a fresh
    tally for `today` replaces it in `state`. This is the only place a day
    ever closes — see the module docstring for why that is lazy rather than
    clock-driven.
    """
    existing = state.get(asset)
    if existing is not None and existing.get("date") != today:
        _append_daily_log(existing)
        existing = None
    if existing is None:
        existing = _empty_tally(today, asset)
        state[asset] = existing
    return existing


def record_evaluation(
    asset: str,
    gate_reason: Optional[str] = None,
    *,
    now: Optional[datetime] = None,
) -> None:
    """
    Record ONE `run_pipeline()` evaluation for `asset` — never a decision.

    `gate_reason=None` means agents WERE called (a real ensemble vote
    happened and is recorded separately, by `pipeline.runner._log_decision`,
    in `agent_decisions.jsonl`); this call only advances `hours_evaluated`,
    the denominator against which the silent/blocked counts below are read.

    `gate_reason` in `GATE_REASONS` means agents were skipped: NO_SIGNAL
    increments `hours_scanner_silent`; any other declared reason increments
    `hours_blocked[gate_reason]`. An undeclared reason raises rather than
    being silently absorbed into a made-up bucket -- the whole point of this
    module is that every skip is accounted for under a name someone chose on
    purpose.

    This function must never be allowed to affect trading: any failure here
    is caught and swallowed by the caller, the same convention
    `_settle_disposition()` in `pipeline/runner.py` already uses for its own
    best-effort journal writes.
    """
    if gate_reason is not None and gate_reason not in GATE_REASONS:
        raise ValueError(
            f"unknown scanner gate reason {gate_reason!r}; declared reasons: "
            f"{GATE_REASONS}")

    today = _today(now)
    state = _load_state()
    tally = _tally_for(state, asset, today)
    tally["hours_evaluated"] += 1
    if gate_reason == NO_SIGNAL:
        tally["hours_scanner_silent"] += 1
    elif gate_reason is not None:
        tally["hours_blocked"][gate_reason] = tally["hours_blocked"].get(gate_reason, 0) + 1
    _save_state(state)


def read_activity() -> list[dict]:
    """
    Every tally the vault generator (or any other reader) should show: the
    closed days from DAILY_LOG followed by whatever is still in progress in
    STATE_FILE, oldest first. A malformed line in DAILY_LOG is skipped, not
    fatal -- this is telemetry, not the record of trades.
    """
    closed: list[dict] = []
    if DAILY_LOG.exists():
        for line in DAILY_LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                closed.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    in_progress = sorted(_load_state().values(), key=lambda t: (t.get("date", ""), t.get("asset", "")))
    return closed + in_progress
