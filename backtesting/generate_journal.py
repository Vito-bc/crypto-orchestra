"""
Obsidian Journal Generator for CryptoOrchestra.

Converts all system data into Obsidian-compatible Markdown notes:
  - logs/trade_history.jsonl              → TradeJournal/
  - logs/agent_decisions.jsonl             → AgentOutputs/ (real decisions,
    one note each) + AgentOutputs/Scanner Activity.md (silent/blocked hours,
    one rolling note)
  - logs/scanner_activity.jsonl / _state.json → folded into the same
    Scanner Activity note
  - logs/agent_shadow.jsonl                → AgentShadow/
  - backtesting/monte_carlo_per_asset.json → Backtests/
  - Hardcoded strategy history             → Strategies/ and ErrorsAndFixes/

VAULT OWNERSHIP AND PRUNING
----------------------------
This generator OWNS every note under `_OWNED_FOLDERS` plus README.md: each
run is a full rebuild, not an append. `_write()` tags every note it produces
with a trailing `_MARKER` comment (invisible to Obsidian's renderer). At the
end of `main()`, `_prune_stale_notes()` deletes any marker-tagged note inside
an owned folder that this run did NOT rewrite — a note that stops being
produced (the old one-file-per-calendar-day AgentOutputs notes this run
replaced with one-file-per-decision, for instance) is gone by the next run
instead of sitting there forever.

A file WITHOUT the marker is never touched, in an owned folder or not — most
plausibly a `_Templates/` note a human copied in and filled by hand, per that
folder's own stated purpose. Ownership is proven positively (the marker), not
inferred from location, so pruning can never surprise a human-authored file.

Usage:
    python backtesting/generate_journal.py
    python backtesting/generate_journal.py --vault /path/to/vault
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT  = Path(__file__).resolve().parents[1]
VAULT = ROOT / "obsidian_vault"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Monkeypatchable independently of ROOT/VAULT: a test that redirects the
# decisions log to a scratch tmp_path must not also lose the real,
# committed agents/orchestrator.py that _orchestrator_action_threshold()
# parses — the two are unrelated inputs and vary independently in tests.
DECISIONS_LOG_PATH  = ROOT / "logs" / "agent_decisions.jsonl"
ORCHESTRATOR_PY_PATH = ROOT / "agents" / "orchestrator.py"

# ── Vault ownership & pruning ────────────────────────────────────────────────

_MARKER = "<!-- generated-by: crypto-orchestra-journal -->"

_OWNED_FOLDERS = (
    "TradeJournal", "AgentOutputs", "Backtests", "Strategies",
    "ErrorsAndFixes", "Research", "AgentShadow", "_Templates",
)

# Reset at the top of main() (or by a test that calls the generate_* functions
# directly) so a stale entry from an earlier run/test can never suppress a
# deletion in this one.
_written_paths: set[Path] = set()


def _prune_stale_notes() -> int:
    """
    Delete every marker-tagged note this generator owns but did not write
    this run. See the module docstring for the ownership rule.
    """
    removed = 0
    candidates = [VAULT / name for name in _OWNED_FOLDERS] + [VAULT / "README.md"]
    for root in candidates:
        if root.is_file():
            paths = [root]
        elif root.is_dir():
            paths = [p for p in root.rglob("*") if p.is_file()]
        else:
            continue
        for path in paths:
            if path.resolve() in _written_paths:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if _MARKER not in text:
                continue  # not ours — leave it alone, marker or no folder
            path.unlink()
            removed += 1
    return removed


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write(path: Path, content: str) -> None:
    if not content.endswith("\n"):
        content += "\n"
    content += _MARKER + "\n"
    path.write_text(content, encoding="utf-8")
    _written_paths.add(path.resolve())
    print(f"  ✓  {path.relative_to(VAULT)}")


def _fmt_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso


def _pnl_icon(pnl: float) -> str:
    return "🟢 WIN" if pnl > 0 else "🔴 LOSS"


# ── Trade Journal ─────────────────────────────────────────────────────────────

def generate_trade_notes() -> int:
    folder = _ensure(VAULT / "TradeJournal")
    path   = ROOT / "logs" / "trade_history.jsonl"
    if not path.exists():
        print("  No trade_history.jsonl found — skipping TradeJournal")
        return 0

    trades = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    for t in trades:
        asset      = t.get("asset", "UNKNOWN")
        reason     = t.get("reason", "UNKNOWN")
        pnl_pct    = t.get("pnl_pct", 0.0)
        pnl_usd    = t.get("pnl_usd", 0.0)
        entry_time = t.get("entry_time", "")
        exit_time  = t.get("closed_at_utc") or t.get("exit_time", "")
        entry_px   = t.get("entry_price", 0.0)
        exit_px    = t.get("exit_price", 0.0)
        hold_h     = t.get("hold_hours", 0.0)
        qty_usd    = t.get("qty_usd", 0.0)
        trade_id   = t.get("id", "unknown")

        date_str = entry_time[:10] if entry_time else "0000-00-00"
        result   = "WIN" if pnl_pct > 0 else "LOSS"
        icon     = _pnl_icon(pnl_pct)
        slug     = f"{date_str}_{asset.replace('-USD','')}_{reason}"
        fname    = folder / f"{slug}.md"

        tags = [
            result.lower(),
            asset.lower().replace("-", ""),
            reason.lower().replace("_", "-"),
        ]
        if hold_h < 12:
            tags.append("quick-exit")
        if abs(pnl_pct) > 5:
            tags.append("large-move")

        note = f"""---
date: {date_str}
asset: {asset}
type: trade
result: {result}
pnl_pct: {pnl_pct:+.2f}
pnl_usd: {pnl_usd:+.2f}
reason: {reason}
hold_hours: {hold_h:.1f}
position_usd: {qty_usd}
trade_id: {trade_id}
tags: [{", ".join(tags)}]
---

# {icon}  {asset} — {date_str}

## Entry / Exit
| Field       | Value |
|-------------|-------|
| Entry time  | {_fmt_dt(entry_time)} |
| Exit time   | {_fmt_dt(exit_time)} |
| Entry price | ${entry_px:,.2f} |
| Exit price  | ${exit_px:,.2f} |
| Hold        | {hold_h:.1f}h |
| Position    | ${qty_usd} |
| **Result**  | **{pnl_pct:+.2f}% ({pnl_usd:+.2f} USD)** |
| Reason      | {reason} |

## What happened
> *Fill in manually or via n8n auto-note*

## Agent consensus at entry
> *Check [[AgentOutputs]] for decisions around {date_str}*

## Lessons
- [ ] Was entry timing correct?
- [ ] Did the stop make sense for this volatility?
- [ ] Did any filter warn against this trade?

## Related
- [[{asset.replace("-USD", "")} Strategy]]
- [[ErrorsAndFixes/{date_str}_{asset.replace("-USD","")}]] *(if applicable)*
- [[Strategies/Per-Asset Parameters]]
"""

        _write(fname, note)

    print(f"  → {len(trades)} trade notes written")
    return len(trades)


# ── Agent Outputs ─────────────────────────────────────────────────────────────
#
# AgentOutputs/ shows decisions, not silence — and, as of 2026-09-22, not
# every decision either: a HOLD with routine confidence and no trade behind
# it is not information-dense enough to earn its own note among nearly two
# thousand of them. Three populations, each rendered differently:
#
#   1. SCANNER-GATE SKIPS — no agent was ever called. Structural test:
#      empty `votes` AND empty `agent_signals` (`_is_gate_skip_record`), not
#      a string match on `reasoning`. NEW records (2026-09-22 onward) are
#      never written this way at all — see pipeline/scanner_activity.py.
#      OLD records (~2,235 of them) still carry this shape and are excluded
#      at RENDER time, since the log itself is never rewritten. Folded into
#      Scanner Activity.md's "legacy silent hours" table.
#
#   2. REAL DECISIONS, NOT INDIVIDUALLY WORTHY — agents voted, but the
#      decision was HOLD, below the orchestrator's own documented
#      action-confidence threshold, and not tied to a placed order
#      (`_is_individual_worthy` below). The large majority of the ~1,995
#      real (agent-called) decisions in this log fall here. Folded into
#      Scanner Activity.md's "Agent-era decisions" section as a
#      distribution — per-day/asset counts, a confidence histogram, a
#      veto-reason breakdown — not one note per instance.
#
#   3. REAL DECISIONS, INDIVIDUALLY WORTHY — action != HOLD, or confidence
#      clears the threshold, or the decision is tied to an order/position
#      lifecycle event (see `_is_individual_worthy`). These get one note
#      each, with full agent-by-agent detail.
#
# Order-lifecycle events (LIMIT_ORDER_PLACED, POSITION_OPENED, ...) live in
# this same JSONL file but have no "action" field at all — a different
# record shape, already covered by TradeJournal/ via logs/trade_history.jsonl,
# excluded here rather than rendered as a malformed decision.

_LEGACY_AGENT_OUTPUTS_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}_decisions\.md$")


def _migrate_legacy_agent_outputs(folder: Path) -> int:
    """
    One-time cleanup (2026-09-22) of the OLD one-note-per-calendar-day
    AgentOutputs files, superseded by one note per real decision plus the
    single rolling Scanner Activity note.

    These predate `_MARKER`, so ordinary pruning (`_prune_stale_notes`)
    cannot identify them. They are identified here instead by BOTH their
    exact legacy filename shape (`YYYY-MM-DD_decisions.md`, which only the
    old `generate_agent_notes()` ever produced) AND their legacy heading
    (`# Agent Decisions — `) — a file that merely happens to share the name
    but was not actually written by this generator is left alone. Expected
    to become a no-op after it has run once against a given vault.
    """
    if not folder.is_dir():
        return 0
    removed = 0
    for path in folder.glob("*.md"):
        if not _LEGACY_AGENT_OUTPUTS_NAME.match(path.name):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if not text.lstrip().startswith("# Agent Decisions"):
            continue
        path.unlink()
        removed += 1
    return removed


def _is_gate_skip_record(record: dict) -> bool:
    """
    True for a HOLD record where the scanner gate skipped every agent.

    A structural test — empty `votes` AND empty `agent_signals` — not a text
    match on `reasoning`. That is the actual invariant ("no agent was ever
    called"), it is what every historical gate-skip record shares regardless
    of which of the three gate reasons produced it, and it needs no updating
    if runner.py's message text ever changes.
    """
    return not record.get("votes") and not record.get("agent_signals")


_ORDER_TIE_PREFIX = re.compile(r"^\[Limit\] Order #")


def _orchestrator_action_threshold() -> float:
    """
    The confidence floor above which a real decision counts as "actionable or
    nearly so" — see `_is_individual_worthy` below. Parsed from
    `agents/orchestrator.py`'s own module docstring ("Confidence < X always
    produces HOLD"), `_require()`-style, rather than hardcoded: the standing
    convention this generator uses everywhere else in this file (see
    `generate_research_notes`) is to derive a governing number from its
    source rather than retype it, so a change there is caught here (raise)
    instead of silently drifting.

    WHY THE DOCSTRING AND NOT A NUMERIC CONSTANT — documented here because it
    is not obvious: `agents/orchestrator.py` has exactly two ENFORCED
    thresholds, `_BUY_THRESHOLD` (0.45) and `_SELL_THRESHOLD` (-0.35), both
    applied to `composite_score` — a Python-local quantity computed inside
    `decide()` and never written to `agent_decisions.jsonl`. The only field
    the log actually persists is `confidence`, the LLM's own self-reported
    figure, which as of this writing is gated by no enforced code path at
    all. The docstring's "0.55" is the sole number in the module stated
    against `confidence` specifically — this generator trusts it as the
    intended selection threshold for that reason, not because it is
    currently enforced in code. If `_BUY_THRESHOLD` and this docstring line
    are ever reconciled to agree, this function keeps working unchanged.
    """
    text = ORCHESTRATOR_PY_PATH.read_text(encoding="utf-8")
    m = _require(r"Confidence < ([\d.]+) always produces HOLD", text,
                 ORCHESTRATOR_PY_PATH,
                 "the orchestrator's documented confidence-for-HOLD threshold")
    return float(m.group(1))


def _is_individual_worthy(record: dict, threshold: float) -> bool:
    """
    An individual AgentOutputs note only when the decision is actionable or
    nearly so:

      - `action != "HOLD"`, or
      - `confidence >= threshold` (the orchestrator's documented action
        threshold — see `_orchestrator_action_threshold`), or
      - the decision is tied to an order/position lifecycle event: runner.py
        stamps `"[Limit] Order #<id> placed..."` onto a HOLD record's
        `reasoning` the moment a BUY becomes a resting limit order (the LIVE
        action is the order, not the persisted HOLD label). Without this
        branch every one of this log's 7 real trades would be invisible as
        an individual note — 2 of the 7 carry confidence below even this
        threshold.

    Everything else — a routine-confidence HOLD with no trade behind it, the
    large majority of this log — folds into the rolling aggregate instead;
    the distribution is the information there, not each instance.
    """
    if record.get("action") != "HOLD":
        return True
    if (record.get("confidence") or 0.0) >= threshold:
        return True
    return bool(_ORDER_TIE_PREFIX.match(record.get("reasoning") or ""))


def _decision_slug(record: dict) -> str:
    """Filesystem-safe `{timestamp}_{asset}` — unique, sorts chronologically."""
    ts = record.get("logged_at_utc") or "unknown-time"
    safe_ts = ts.split("+")[0].replace(":", "-")
    if not safe_ts.endswith("Z"):
        safe_ts += "Z"
    asset = record.get("asset", "UNKNOWN")
    return f"{safe_ts}_{asset}"


def _render_decision_note(record: dict) -> str:
    """One note for one real ensemble decision — every agent's vote, the
    orchestrator's combination, and whatever the record shows of what the
    scanner gate saw."""
    asset      = record.get("asset", "UNKNOWN")
    action     = record.get("action", "?")
    conf       = record.get("confidence") or 0.0
    ts         = record.get("logged_at_utc", "")
    reasoning  = record.get("reasoning", "")
    veto       = record.get("veto_triggered", False)
    veto_reason = record.get("veto_reason")
    overrides  = record.get("overrides") or []
    votes      = record.get("votes") or []
    signals    = record.get("agent_signals") or []
    providers  = record.get("data_providers") or {}

    icon = {"BUY": "🟢", "SELL": "🔴"}.get(action, "⚪")

    if votes:
        vote_rows = "\n".join(
            f"| {v.get('agent', '?')} | {v.get('signal', '?')} | "
            f"{v.get('confidence', 0):.0%} | {v.get('weight_applied', 0):.2f} |"
            for v in votes
        )
        vote_table = f"| Agent | Vote | Confidence | Weight |\n|---|---|---:|---:|\n{vote_rows}"
    elif signals:
        # Checked against this log, not assumed: empty `votes` alongside
        # non-empty `agent_signals` is NOT the general shape of the
        # pre-scanner-gate era — 1982 of that era's 1995 real records DO
        # carry per-agent votes. It is specific to the orchestrator's
        # NewsVeto pre-check (agents/orchestrator.py): a critical-news veto
        # returns before per-agent votes are ever built, so `votes=[]`
        # while `agent_signals` (collected earlier, from the sub-agents
        # themselves) is still fully populated. Stated plainly, as a fact
        # about THIS record, not papered over as a generic gap.
        vote_table = (
            "*Per-agent vote weights were not persisted on this specific "
            "record — `votes` is empty. This orchestrator path (a critical "
            "news veto short-circuits before votes are built) always skips "
            "them; it is not a general gap in this era. See per-agent "
            "signals below instead, which were recorded in full.*"
        )
    else:
        vote_table = "*No per-agent vote weights or signals recorded on this record.*"

    if signals:
        signal_block = "\n\n".join(
            f"### {s.get('agent', '?')} — {s.get('signal', '?')} "
            f"({s.get('confidence', 0):.0%})\n\n"
            f"{s.get('reasoning') or '*no reasoning recorded*'}"
            for s in signals
        )
    else:
        signal_block = "*No per-agent signal detail recorded on this record.*"

    scanner_mentions = [
        text for text in ([reasoning] + list(overrides))
        if text and ("[Scanner]" in text or "Scanner gate" in text
                      or "Scanner override" in text)
    ]
    if scanner_mentions:
        scanner_block = "\n\n".join(f"> {m}" for m in scanner_mentions)
    else:
        scanner_block = (
            "*No scanner-gate detail is recorded on this decision — either "
            "it predates the scanner-gate architecture (records before "
            "2026-07-11 ran every agent every tick, with no gate at all), "
            "or the scanner fired without the orchestrator's reasoning "
            "mentioning it. `agent_decisions.jsonl` does not carry the raw "
            "scanner signal as its own field, so nothing further can be "
            "recovered here.*"
        )

    overrides_block = "\n".join(f"- {o}" for o in overrides) if overrides else "*none*"
    providers_block = (
        ", ".join(f"{k}={v}" for k, v in providers.items())
        if providers else "*not recorded*"
    )
    veto_line = f"{veto}" + (f" ({veto_reason})" if veto_reason else "")

    return f"""---
date: {ts[:10] if ts else "unknown"}
asset: {asset}
type: agent-decision
action: {action}
confidence: {conf:.2f}
veto_triggered: {veto}
tags: [agent-decision, {asset.lower().replace('-', '')}, {action.lower()}]
---

# {icon} {asset} → {action} ({conf:.0%}) — {ts or "unknown time"}

**Real ensemble decision** — every sub-agent ran; the orchestrator combined
their votes into this decision.

## Orchestrator's combined decision

- **Action:** {action}
- **Confidence:** {conf:.0%}
- **Veto triggered:** {veto_line}
- **Reasoning:** {reasoning or "*none recorded*"}

**Overrides applied:**
{overrides_block}

## Per-agent votes (as scored by the orchestrator)

{vote_table}

## Per-agent signals (raw sub-agent output)

{signal_block}

## What the scanner gate saw

{scanner_block}

## Data provenance

{providers_block}
"""


def _date_asset_table(records: list[dict], header: str) -> tuple[str, int]:
    """A `| Date | Asset | count |` table grouped from `logged_at_utc`/`asset`."""
    by_key: dict[tuple[str, str], int] = {}
    for r in records:
        date = (r.get("logged_at_utc") or "")[:10] or "unknown"
        key = (date, r.get("asset", "UNKNOWN"))
        by_key[key] = by_key.get(key, 0) + 1
    total = sum(by_key.values())
    if not by_key:
        return "*None on record.*", 0
    rows = "\n".join(f"| {date} | {asset} | {count} |"
                      for (date, asset), count in sorted(by_key.items()))
    return f"| Date | Asset | {header} |\n|---|---|---:|\n{rows}", total


def _confidence_bucket(confidence: float) -> str:
    clamped = max(0.0, min(confidence, 1.0))
    lo = min(int(clamped * 10), 9) / 10
    return f"{lo:.1f}–{lo + 0.1:.1f}"


def _confidence_histogram(records: list[dict]) -> str:
    counts: dict[str, int] = {}
    for r in records:
        bucket = _confidence_bucket(r.get("confidence") or 0.0)
        counts[bucket] = counts.get(bucket, 0) + 1
    if not counts:
        return "*None on record.*"
    ordered = sorted(counts.items(), key=lambda kv: kv[0])
    rows = "\n".join(f"| {bucket} | {count} |" for bucket, count in ordered)
    return f"| Confidence | Count |\n|---|---:|\n{rows}"


def _veto_category(record: dict) -> str:
    """
    A small, fixed set of veto CATEGORIES, not raw `veto_reason` text.

    `veto_reason`/`reasoning` on a vetoed decision is free-form LLM prose —
    on this log, 653 of 911 vetoed records have a distinct `veto_reason`
    string, so grouping on it verbatim would produce a table with almost as
    many rows as instances, defeating the entire point of an aggregate
    ("the distribution is the information, not each instance"). This buckets
    by the reliable structural signals actually present instead: the two
    hardcoded pre-check paths in `agents/orchestrator.py` stamp a
    `[NewsVeto]`/`[RiskVeto]` prefix onto `reasoning`, `pipeline/runner.py`'s
    circuit breaker stamps `[CircuitBreaker]`, and the free-text LLM vetoes
    overwhelmingly (880 of 911 here) mention "BEAR" (macro regime BEAR or
    FULL_BEAR) or "ok_to_trade"/"ATR volatility" (a risk-agent flag the LLM
    incorporated into its own veto, distinct from the hardcoded RiskVeto
    pre-check). Verified against this log: zero records fall through to
    "other veto (uncategorised)" — that bucket exists as an honest catch-all
    for whatever a future run's free text does not match, not because it is
    currently populated.
    """
    if not record.get("veto_triggered"):
        return "(no veto)"
    reasoning = record.get("reasoning") or ""
    if reasoning.startswith("[NewsVeto]"):
        return "NewsVeto (critical news)"
    if reasoning.startswith("[RiskVeto]"):
        return "RiskVeto (agent absent / ok_to_trade=false, hard pre-check)"
    if reasoning.startswith("[CircuitBreaker]"):
        return "CircuitBreaker (portfolio drawdown halt)"
    if "BEAR" in reasoning.upper():
        return "Macro regime veto (BEAR / FULL_BEAR)"
    if "ok_to_trade" in reasoning or "ATR volatility" in reasoning:
        return "Risk veto (ok_to_trade / ATR volatility, in LLM reasoning)"
    return "other veto (uncategorised)"


def _veto_reason_breakdown(records: list[dict]) -> str:
    counts: dict[str, int] = {}
    for r in records:
        key = _veto_category(r)
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return "*None on record.*"
    rows = "\n".join(f"| {reason} | {count} |"
                      for reason, count in sorted(counts.items(), key=lambda kv: -kv[1]))
    return f"| Veto category | Count |\n|---|---:|\n{rows}"


def _scanner_activity_note(
    current_activity: list[dict],
    legacy_skips: list[dict],
    agent_era_aggregate: list[dict],
    threshold: float,
) -> str:
    """
    The single rolling note for everything AgentOutputs/ does NOT render as
    an individual note: scanner-gate silence (current tally + legacy
    gate-skip records) and, separately, the real-but-routine agent-era
    decisions `_is_individual_worthy` excluded. Distribution, not instances —
    see the module header comment above `generate_agent_notes` for the full
    three-population split this note and AgentOutputs/ together cover.
    """
    legacy_table, legacy_total = _date_asset_table(legacy_skips, "Silent hours (legacy)")

    if current_activity:
        current_rows = "\n".join(
            f"| {row.get('date', '?')} | {row.get('asset', '?')} | "
            f"{row.get('hours_evaluated', 0)} | {row.get('hours_scanner_silent', 0)} | "
            + (", ".join(f"{k}={v}" for k, v in sorted((row.get('hours_blocked') or {}).items())) or "none")
            + " |"
            for row in current_activity
        )
        current_table = (
            "| Date | Asset | Hours evaluated | Hours silent | Hours blocked (by reason) |\n"
            "|---|---|---:|---:|---|\n" + current_rows
        )
    else:
        current_table = (
            "*No tallied activity yet. The pipeline has been disabled since "
            "2026-07-17; pipeline/scanner_activity.py has not recorded an "
            "hour of any kind since it was added.*"
        )

    era_table, era_total = _date_asset_table(agent_era_aggregate, "HOLDs")
    era_histogram = _confidence_histogram(agent_era_aggregate)
    era_vetoes = _veto_reason_breakdown(agent_era_aggregate)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"""---
date: {today}
type: scanner-activity
tags: [scanner-activity, derived, rolling]
---

# Scanner activity — rolling

> One note, not one per day. Rebuilt every run from
> `logs/scanner_activity.jsonl` / `logs/scanner_activity_state.json` (the
> per-day tally, in place since 2026-09-22 — see
> `pipeline/scanner_activity.py`), aggregated directly from
> `logs/agent_decisions.jsonl`'s legacy scanner-gate-silent records, and
> (below) from every real agent-era decision that did not clear
> `_is_individual_worthy` — see `docs/operations/agent_outputs_selection.md`.

This is **not** a decision log. Everything on this page is a distribution,
not a record of any one event. Individually worthy decisions — action !=
HOLD, confidence at or above the orchestrator's documented threshold
({threshold:.2f}), or tied to a placed order — are one note each, elsewhere
in this folder.

## Current tally (2026-09-22 onward)

{current_table}

## Legacy silent hours (pre-tally, from agent_decisions.jsonl)

{legacy_total} scanner-gate-silent record(s) were written before this tally
existed, every one with the identical reasoning "Scanner gate: no breakout
signal on last closed candle." and no agent ever called. `logs/` itself is
never rewritten — this table is derived at render time, on every run, and is
their only trace in the vault.

{legacy_table}

## Agent-era decisions (aggregate)

{era_total} real decision(s) — agents voted, the orchestrator decided — that
did not clear the individual-note bar: HOLD, below {threshold:.2f}
confidence, and not tied to a placed order. All predate the scanner-gate
architecture (2026-04-15 → 2026-07-11). The distribution is the information
here, not each instance.

**HOLDs per day/asset:**

{era_table}

**Confidence histogram:**

{era_histogram}

**Veto-reason breakdown:**

{era_vetoes}
"""


def generate_agent_notes() -> None:
    folder = _ensure(VAULT / "AgentOutputs")
    _migrate_legacy_agent_outputs(folder)

    from pipeline.scanner_activity import read_activity

    path = DECISIONS_LOG_PATH
    records: list[dict] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    else:
        print("  No agent_decisions.jsonl — writing placeholder Scanner Activity.md only")

    # Order-lifecycle events have no "action" field — a different record
    # shape, already covered by TradeJournal/ via trade_history.jsonl.
    decisions = [d for d in records if "action" in d]
    real = [d for d in decisions if not _is_gate_skip_record(d)]
    legacy_skips = [d for d in decisions if _is_gate_skip_record(d)]

    threshold = _orchestrator_action_threshold()
    individual = [r for r in real if _is_individual_worthy(r, threshold)]
    aggregate = [r for r in real if not _is_individual_worthy(r, threshold)]

    for record in individual:
        _write(folder / f"{_decision_slug(record)}.md", _render_decision_note(record))

    _write(folder / "Scanner Activity.md",
           _scanner_activity_note(read_activity(), legacy_skips, aggregate, threshold))

    print(f"  → {len(individual)} individual decision note(s), "
          f"{len(aggregate)} agent-era decision(s) + {len(legacy_skips)} "
          "legacy gate-skip record(s) folded into Scanner Activity.md")


# ── Agent Shadow ─────────────────────────────────────────────────────────────
#
# logs/agent_shadow.jsonl (schema: docs/operations/agent_shadow.md) is an
# engineering observation stream, wired 2026-09-20. It decides nothing, places nothing, and sits outside pipeline/, off every
# trading path. Every note below says so prominently and quotes the reading
# rule declared before the stream's first run: a candidate stream counts as a
# RESULT only if, over a stated period, its one-sided 95% bound clears zero
# AND its annualised Sharpe exceeds 1.645/√years — anything short of both is
# OBSERVATION, and selecting a subset for its observed outcome and moving it
# onto the trading gate is forbidden without a new pre-registered trial.
#
# Since schema 2 (2026-09-23) every vote and decision also carries a LIVE /
# BACKFILL flag and its lag after bar close: a catch-up run decides missed
# bars with agents reading CURRENT context, so those are a different
# observation from a live one. Every note and the index show the flag; the
# reading rule applies per population, never to the two pooled silently.

_SHADOW_OBSERVATION_NOTICE = (
    "**OBSERVATION ONLY — no trade resulted.** This is an engineering "
    "observation stream (`docs/operations/agent_shadow.md`), outside "
    "`pipeline/` and off every trading path. The declared reading rule "
    "applies: a candidate stream counts as a RESULT only if, over a stated "
    "period, its one-sided 95% bound clears zero AND its annualised Sharpe "
    "exceeds 1.645/√years. Anything short of both is OBSERVATION. Selecting "
    "a subset for its observed outcome and moving it onto the trading gate "
    "is forbidden without a new pre-registered trial."
)


def _shadow_slug(candle_time: object, asset: str, candidate_id: str) -> str:
    """Filesystem-safe, unique even if two variants share a candle+asset."""
    safe_ts = str(candle_time).split("+")[0].replace(":", "-").replace(" ", "T")
    if not safe_ts.endswith("Z"):
        safe_ts += "Z"
    return f"{safe_ts}_{asset}_{candidate_id}"


def _shadow_timing(record: dict | None) -> tuple[str, str]:
    """(flag, lag) for a vote or decision; schema-1 records predate both."""
    if not record or not record.get("timing"):
        return "NOT RECORDED", "n/a"
    lag = record.get("lag_hours_after_bar_close")
    return str(record["timing"]), ("n/a" if lag is None else f"{float(lag):.2f}")


def _shadow_call_footer(record: dict) -> str:
    cost = record.get("cost_usd")
    cost_str = f"${cost:.6f}" if cost is not None else "not recorded"
    flag, lag = _shadow_timing(record)
    return (
        f"*model={record.get('model_id', '?')}  "
        f"latency={record.get('latency_ms', '?')}ms  "
        f"api_calls={record.get('api_calls', '?')}  cost={cost_str}  "
        f"timing={flag}  lag={lag}h after bar close  "
        f"decided_at={record.get('decided_at', 'not recorded')}*"
    )


def _render_shadow_note(
    candidate_id: str,
    votes: list[dict],
    decision: dict | None,
    attachment: dict | None,
) -> str:
    """One note per shadow decision — every agent's vote and reasoning, the
    orchestrator's combination, the variant id, n_met, and — once attached —
    the subsequent price path."""
    base = decision or (votes[0] if votes else {})
    asset        = base.get("asset", "UNKNOWN")
    variant_id   = base.get("variant_id", "?")
    candle_time  = base.get("candle_time", "?")
    n_met        = base.get("n_met")
    entry_price  = base.get("entry_price")
    atr          = base.get("atr")
    atr_stop     = base.get("atr_stop")
    atr_target   = base.get("atr_target")
    max_hold     = base.get("max_hold_hours")
    # The decision's own timing classifies the observation: its time is at
    # or after every vote it used, so its lag bounds the whole input set.
    flag, lag = _shadow_timing(decision or (votes[0] if votes else None))
    if decision and decision.get("earliest_vote_at"):
        input_span = (f"{decision['earliest_vote_at']} → "
                      f"{decision.get('latest_vote_at', '?')}")
    else:
        input_span = "not recorded"

    if votes:
        vote_sections = []
        for v in sorted(votes, key=lambda r: r.get("agent_name", "")):
            err = f" — **error:** {v['error']}" if v.get("error") else ""
            vote_sections.append(
                f"### {v.get('agent_name', '?')} — {v.get('direction', '?')} "
                f"({v.get('confidence', 0):.0%}){err}\n\n"
                f"{v.get('reasoning') or '*no reasoning recorded*'}\n\n"
                f"{_shadow_call_footer(v)}"
            )
        vote_block = "\n\n".join(vote_sections)
    else:
        vote_block = "*No agent votes recorded yet for this candidate.*"

    if decision:
        err = f" — **error:** {decision['error']}" if decision.get("error") else ""
        decision_block = (
            f"**Direction:** {decision.get('direction', '?')}  "
            f"**Confidence:** {decision.get('confidence', 0):.0%}{err}\n\n"
            f"{decision.get('reasoning') or '*no reasoning recorded*'}\n\n"
            f"{_shadow_call_footer(decision)}"
        )
    else:
        decision_block = "*Orchestrator decision not yet recorded for this candidate.*"

    if attachment:
        path_rows = "\n".join(
            f"| {label} | {vals.get('close')} | {vals.get('return', 0):+.3%} |"
            for label, vals in sorted((attachment.get("price_path") or {}).items())
        )
        bracket = attachment.get("atr_bracket") or {}
        attachment_block = (
            f"| Horizon | Close | Return |\n|---|---:|---:|\n{path_rows}\n\n"
            f"**ATR bracket outcome:** {bracket.get('outcome', '?')} at "
            f"{bracket.get('hours', '?')}h — price {bracket.get('price', '?')} "
            f"(stop {bracket.get('stop_price', '?')}, target {bracket.get('target_price', '?')})"
        )
    else:
        attachment_block = (
            "*Not yet attached — price paths are added once the full "
            "seven-day window exists (`agent_shadow.attachments`).*"
        )

    return f"""---
date: {str(candle_time)[:10]}
asset: {asset}
type: agent-shadow
variant_id: {variant_id}
candidate_id: {candidate_id}
n_met: {n_met}
timing: {flag}
lag_hours_after_bar_close: {lag}
tags: [agent-shadow, observation-only, {flag.lower().replace(' ', '-')}, {asset.lower().replace('-', '')}]
---

# Agent shadow — {asset} — {candle_time}

> {_SHADOW_OBSERVATION_NOTICE}

## Candidate

- **Timing:** **{flag}** — decided {lag}h after the bar closed
  (a BACKFILL decision's agents read context from later than its candle; it
  is a different observation from a LIVE one — read the two separately)
- **Decided at:** {(decision or {}).get('decided_at', 'not recorded')}
- **Votes gathered:** {input_span}
- **Variant:** {variant_id}
- **Candle time:** {candle_time}
- **n_met:** {n_met}
- **Entry price:** {entry_price}
- **ATR:** {atr}  (stop {atr_stop}x / target {atr_target}x / max hold {max_hold}h)

## Agent votes

{vote_block}

## Orchestrator's combined decision

{decision_block}

## Subsequent price path

{attachment_block}
"""


def _render_shadow_index(entries: list[tuple[str, dict]]) -> str:
    """`entries`: (candidate_id, base_record), already ordered newest-first."""
    if entries:
        rows = "\n".join(
            f"| [[{_shadow_slug(base.get('candle_time', '?'), base.get('asset', 'UNKNOWN'), cid)}"
            f"|{base.get('candle_time', '?')}]] | {base.get('asset', '?')} | "
            f"{base.get('variant_id', '?')} | {base.get('n_met', '?')} | "
            f"{_shadow_timing(base)[0]} | {_shadow_timing(base)[1]} |"
            for cid, base in entries
        )
    else:
        rows = "*none yet*"
    populations: dict[str, int] = {}
    for _cid, base in entries:
        flag = _shadow_timing(base)[0]
        populations[flag] = populations.get(flag, 0) + 1
    population_line = ", ".join(
        f"{flag} {count}" for flag, count in sorted(populations.items())) or "none"

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"""---
date: {today}
type: agent-shadow-index
tags: [agent-shadow, index, observation-only]
---

# Agent shadow — index

> {_SHADOW_OBSERVATION_NOTICE}

Newest first. Populations: {population_line}. LIVE and BACKFILL are
different observations — any reading must separate them or justify pooling
them, and the reading rule's bound/Sharpe test applies to each on its own.

| Candle time | Asset | Variant | n_met | Timing | Lag after close (h) |
|---|---|---|---:|---|---:|
{rows}
"""


def _render_shadow_placeholder(checkpoint: dict | None = None) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if checkpoint:
        through = checkpoint.get("examined_through") or {}
        scanned = (
            f"\n\nThe scanner is running: last run {checkpoint.get('run_at', '?')} "
            "examined every closed bar through "
            + (", ".join(f"{a} {t}" for a, t in sorted(through.items())) or "—")
            + " and found no candidate."
        )
    else:
        scanned = ""
    return f"""---
date: {today}
type: agent-shadow-index
tags: [agent-shadow, index, observation-only, placeholder]
---

# Agent shadow — index

> {_SHADOW_OBSERVATION_NOTICE}

**Awaiting first candidate.** `logs/agent_shadow.jsonl` holds no candidate
yet — no WIDE candidate has occurred since the hourly task was registered on
2026-09-21 (`docs/operations/agent_shadow.md`). This is expected, not an
error: a successful no-event poll records zero candidates and costs nothing.
This page lists decisions from the moment the first one is recorded.{scanned}
"""


def generate_agent_shadow_notes() -> None:
    folder = _ensure(VAULT / "AgentShadow")
    path = ROOT / "logs" / "agent_shadow.jsonl"

    records: list[dict] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    # scan_checkpoint records (schema 2) carry no candidate_id: a log holding
    # only those is still "awaiting first candidate", not an empty index.
    if not any(r.get("candidate_id") for r in records):
        checkpoints = [r for r in records if r.get("record_type") == "scan_checkpoint"]
        _write(folder / "Index.md",
               _render_shadow_placeholder(checkpoints[-1] if checkpoints else None))
        print("  → 0 shadow notes (no candidate yet) — placeholder written")
        return

    votes_by_candidate: dict[str, list[dict]] = {}
    decision_by_candidate: dict[str, dict] = {}
    attachment_by_candidate: dict[str, dict] = {}
    for r in records:
        cid = r.get("candidate_id")
        if cid is None:
            continue
        record_type = r.get("record_type")
        if record_type == "agent_vote":
            votes_by_candidate.setdefault(cid, []).append(r)
        elif record_type == "orchestrator_decision":
            decision_by_candidate[cid] = r
        elif record_type == "price_attachment":
            attachment_by_candidate[cid] = r

    # Oldest first for a stable, deterministic write order; the index is
    # built from a reversed copy so IT reads newest first, as required.
    candidate_ids = sorted(
        set(votes_by_candidate) | set(decision_by_candidate),
        key=lambda cid: (
            decision_by_candidate.get(cid)
            or (votes_by_candidate.get(cid) or [{}])[0]
        ).get("candle_time", ""),
    )

    index_entries: list[tuple[str, dict]] = []
    for cid in candidate_ids:
        votes = votes_by_candidate.get(cid, [])
        decision = decision_by_candidate.get(cid)
        attachment = attachment_by_candidate.get(cid)
        base = decision or (votes[0] if votes else {})
        slug = _shadow_slug(base.get("candle_time", "unknown"),
                             base.get("asset", "UNKNOWN"), cid)
        _write(folder / f"{slug}.md",
               _render_shadow_note(cid, votes, decision, attachment))
        index_entries.append((cid, base))

    index_entries.reverse()
    _write(folder / "Index.md", _render_shadow_index(index_entries))

    print(f"  → {len(candidate_ids)} shadow decision note(s) + index written")


# ── Backtest Results ──────────────────────────────────────────────────────────

def generate_backtest_notes() -> None:
    folder = _ensure(VAULT / "Backtests")

    # Per-asset Monte Carlo results
    mc_path = ROOT / "backtesting" / "monte_carlo_per_asset.json"
    if mc_path.exists():
        mc = json.loads(mc_path.read_text(encoding="utf-8"))
        period = mc.get("period", "full_year")
        assets = mc.get("assets", {})

        rows = []
        for asset, data in assets.items():
            n      = data.get("n_trades", 0)
            stop   = data.get("atr_stop", 2.0)
            target = data.get("atr_target", 3.5)
            mc2    = data.get("mc_2pct") or {}
            wr     = mc2.get("win_rate", 0)
            exp    = mc2.get("expectancy_pct", 0)
            ruin   = mc2.get("ruin_pct", 0)
            rows.append(
                f"| {asset} | {n} | {wr:.1%} | {exp:+.3f}% | {stop}x / {target}x | {ruin:.1f}% |"
            )

        note = f"""---
date: {datetime.now(timezone.utc).strftime("%Y-%m-%d")}
type: backtest
period: {period}
tags: [backtest, monte-carlo, per-asset]
---

# Per-Asset Monte Carlo — {period}

Full year backtest (Aug 2024 – Jun 2025) covering 3 market regimes:
crash → +60% Trump rally → -28% Q1 bear

## Results (2% position sizing, 10,000 simulations)

| Asset | Trades | Win Rate | Expectancy | ATR Stop/Target | Ruin Risk |
|-------|--------|----------|------------|-----------------|-----------|
{chr(10).join(rows)}

## Key Findings
- **ZEC-USD**: Best performer. Near breakeven in multi-regime test. Profitable in bull-only.
- **ETH-USD**: Improved with wider stop (2.5x). Win rate 44% across all regimes.
- **BTC-USD**: Conservative mover, but low win rate (35%) dragged by bear periods.
- **SOL-USD**: High volatility, similar profile to ETH but less consistent.
- **Ruin risk = 0%** across all assets at both 2% and 5% sizing.

## Regime Breakdown
| Period | Win Rate | Avg P&L |
|--------|----------|---------|
| Trump Rally (Nov–Jan 2024) | 48% | -0.40% |
| Q1 2025 Bear (Jan–Apr 2025) | 32% | -1.39% |
| Full Year (Aug 2024–Jun 2025) | 42% | -0.71% |

## Next Steps
- [ ] Walk-forward validation (out-of-sample)
- [ ] Live Coinbase test with $100
- [ ] Monitor ZEC signals in next bull regime

## Related
- [[Strategies/Per-Asset Parameters]]
- [[Strategies/Changelog]]
"""
        _write(folder / "full_year_per_asset_MC.md", note)


# ── Strategy Notes ────────────────────────────────────────────────────────────

def generate_strategy_notes() -> None:
    folder = _ensure(VAULT / "Strategies")

    # Changelog
    changelog = """---
type: strategy
tags: [strategy, changelog, parameters]
---

# CryptoOrchestra — Strategy Changelog

## v4 — Per-Asset ATR Parameters (Jun 30 2026)
**Problem:** ETH and SOL had consistently negative P&L due to intraday wicks hitting 2.0x ATR stops.
**Fix:** ETH/SOL → stop=2.5x, target=4.5x (R:R=1.80). BTC/ZEC → keep stop=2.0x, target=3.5x.
**Result:** ETH win rate 44% across full year. Ruin risk stays 0%.
- [[Backtests/full_year_per_asset_MC]]

## v3 — Whipsaw Guard (Jun 29 2026)
**Problem:** 3 consecutive ZEC stop-losses on May 25-26 2026 (whipsaw cluster).
**Fix:** Block entry if 2+ stops in 96h on same asset (live). 48h window in scanner.
**Result:** 79 false signals blocked across full_year period.
- [[ErrorsAndFixes/ZEC_Whipsaw_May2026]]

## v2 — R:R Ratio Fix (Jun 28 2026)
**Problem:** ATR_STOP=2.5 / ATR_TARGET=2.0 → R:R=0.8. System was losing by math.
**Fix:** stop=2.0x, target=3.5x → R:R=1.75. ETH max_hold 24h→36h.
**Result:** Trump rally avg P&L improved from -0.64% to -0.40%. ZEC became profitable (+0.34%).

## v1 — Initial System (Apr 2026)
6-agent orchestration: breakout, macro, sentiment, fundamental, on-chain, risk.
Conservative DRY_RUN paper trading. First 7 live paper trades accumulated.
"""
    _write(folder / "Changelog.md", changelog)

    # Per-asset parameters
    params = """---
type: strategy
tags: [parameters, atr, per-asset]
---

# Per-Asset Trading Parameters

Last updated: Jun 30 2026

| Asset | ATR Stop | ATR Target | R:R | Max Hold | Notes |
|-------|----------|------------|-----|----------|-------|
| BTC-USD | 2.0x | 3.5x | 1.75 | 48h | Clean mover, no wick problem |
| ETH-USD | 2.5x | 4.5x | 1.80 | 36h | Wick-heavy — wider stop needed |
| SOL-USD | 2.5x | 4.5x | 1.80 | 36h | High volatility, same as ETH |
| ZEC-USD | 2.0x | 3.5x | 1.75 | 36h | Best performer, clean trends |

## Why ETH/SOL use wider stops
ETH and SOL have large intraday wicks — price spikes 2-4% below entry in 2-6h then
recovers. With 2.0x ATR stop, those wicks trigger stop-loss even when direction is correct.
At 2.5x ATR, wicks are absorbed. Target scaled up to 4.5x to maintain R:R > 1.75.

## Position Sizing
- Conservative: 2% per trade ($2 on $100 account — below minimums)
- Testing: 5% per trade ($5 on $100 — above Coinbase minimum ~$1)
- Recommended for $100 live test: 5-10%

## Related
- [[Strategies/Changelog]]
- [[Backtests/full_year_per_asset_MC]]
"""
    _write(folder / "Per-Asset Parameters.md", params)


# ── Errors and Fixes ──────────────────────────────────────────────────────────

def generate_error_notes() -> None:
    folder = _ensure(VAULT / "ErrorsAndFixes")

    zec_whipsaw = """---
date: 2026-05-25
type: error
asset: ZEC-USD
tags: [zec, whipsaw, consecutive-losses, fixed]
---

# ZEC Whipsaw — 3 Stops in 24h (May 25-26 2026)

## What happened
Three consecutive ZEC stop-losses fired within 24 hours:
- 2026-05-25 23:06 → STOP_LOSS (-5.12%, 7h hold)
- 2026-05-26 05:06 → STOP_LOSS (-5.52%, 14h hold)
- 2026-05-26 11:13 → STOP_LOSS (-5.82%, 12h hold)

**Total damage: -$32.92 from $600 position exposure**

## Root cause
Market was in a rapid downtrend (ZEC dropped ~10% in 24h). The EMA50 cross signal
kept firing as price bounced off declining levels, but each bounce failed immediately.
The 4h trend filter was borderline — price just above EMA50 on 4h but momentum was down.

## Fix applied
Whipsaw guard added to `pipeline/runner.py`:
- Block new entry if 2+ stops hit in 96h on same asset
- Same guard mirrored in `signal_scanner.py` (48h window)
- [[Strategies/Changelog#v3]]

## Lesson
In a rapidly declining market, the EMA50 cross on 1h can fire repeatedly as price
bounces during a downtrend. The 4h filter alone is not enough when 4h is borderline.
The whipsaw guard catches this case regardless of regime.
"""
    _write(folder / "ZEC_Whipsaw_May2026.md", zec_whipsaw)

    rr_fix = """---
date: 2026-06-28
type: error
tags: [rr-ratio, parameters, fixed, critical]
---

# R:R Ratio Bug — System Was Losing by Math

## What happened
Initial parameters: ATR_STOP=2.5x, ATR_TARGET=2.0x
This gives R:R = 2.0/2.5 = **0.80** — the system needs >50% win rate just to break even
at R:R=1.0. At R:R=0.8, breakeven requires 55.6% win rate. We had 35-48%.

## Fix applied
Changed to: ATR_STOP=2.0x, ATR_TARGET=3.5x → **R:R = 1.75**
At R:R=1.75, breakeven win rate = 1/(1+1.75) = **36.4%** — achievable.
- [[Strategies/Changelog#v2]]

## Lesson
Always verify R:R = target_mult / stop_mult before running any backtest.
A strategy can have a 48% win rate and still lose money if R:R < 1.0.
"""
    _write(folder / "RR_Ratio_Bug.md", rr_fix)


# ── Research (derived from committed artifacts — never hand-written) ──────────

def _require(pattern: str, text: str, source: Path, what: str) -> "re.Match[str]":
    """
    Regex-extract a fact from a committed doc, or fail loudly.

    generate_journal.py must not retype validation/fee/cost numbers as Python
    literals — that lets the vault silently drift from the documents that
    actually govern them. Deriving by regex means a source-format change is
    caught here (raise) instead of shipping a page with a blank or, worse, a
    stale number that happens to still parse.
    """
    m = re.search(pattern, text, re.DOTALL)
    if not m:
        try:
            shown = source.relative_to(ROOT)
        except ValueError:
            # `source` is not under the current ROOT — a test pointing this
            # parser at a scratch file is the normal way that happens, and
            # the fail-loud error below must report the path it actually
            # tried, not crash on its own formatting with a different,
            # more confusing exception.
            shown = source
        raise RuntimeError(
            f"generate_journal.py Research page: could not find {what} in "
            f"{shown}. The source format changed — fix the "
            "parser rather than emit a page with a blank or a stale number."
        )
    return m


def generate_research_notes() -> None:
    folder = _ensure(VAULT / "Research")

    from backtesting.cost_sensitivity import (
        _ARTIFACT_EXPECTANCY_PCT,
        _ARTIFACT_N_CLOSED,
        _ARTIFACT_PF,
        _CANDIDATE_MAKER_RATE,
        _CANDIDATE_TAKER_RATE,
    )
    from pipeline.fees import CURRENT_SCHEDULE

    # ── 1. Current validation status: docs/trial_registry.md + CLAUDE.md ──────
    registry_path = ROOT / "docs" / "trial_registry.md"
    claude_md_path = ROOT / "CLAUDE.md"
    for p in (registry_path, claude_md_path):
        if not p.exists():
            raise RuntimeError(f"Research page needs {p} — not found")
    registry_text = registry_path.read_text(encoding="utf-8")
    claude_text = claude_md_path.read_text(encoding="utf-8")

    # CLAUDE.md's V2 bullet leads with a STATUS in bold and carries the
    # PF/expectancy/n further into the same bullet:
    #
    #   - **V2 momentum (ZEC): RETIRED AS AN ACTIVATION CANDIDATE (2026-09-18,
    #     Closure 1 in `docs/trial_registry.md`), on EDGE.** PF 0.761
    #     (-0.62%/trade, n=114) on the continuous ... window.
    #
    # The status is parsed too, rather than described in a literal here, for
    # the same reason the numbers are: a bullet that changes from RETIRED to
    # anything else must not leave this page asserting the old disposition.
    # `[^*]` keeps the status capture inside its own bold run, so the match
    # cannot slide past it into a later bullet's numbers.
    m = _require(
        r"\*\*V2 momentum \(ZEC\): ([^*]+?)\*\*\s*"
        r"PF ([\d.]+) \((-?[\d.]+)%/trade, n=(\d+)\)",
        claude_text, claude_md_path,
        "the V2 ZEC headline status and PF/expectancy/n")
    v2_status = " ".join(m.group(1).split())
    v2_pf, v2_expectancy_pct, v2_n = m.group(2), m.group(3), m.group(4)

    _require(r"Do NOT switch `DRY_RUN=false` on current evidence\.",
              claude_text, claude_md_path, "the DRY_RUN refusal statement")
    _require(r"LIVE\s+\*?\*?NO-GO", registry_text, registry_path,
              "the LIVE NO-GO verdict")
    _require(r"`DRY_RUN=true`", registry_text, registry_path,
              "the DRY_RUN=true statement")

    # ── 2. Operational fee schedule: pipeline/fees.py + fee-tier evidence ──────
    #
    # The evidence filename is derived from CURRENT_SCHEDULE.source rather than
    # hardcoded to one date: this same page previously pinned
    # "fee_tier_2026-09-15.json" as a literal, and that literal silently
    # stopped matching CURRENT_SCHEDULE the moment the 2026-09-22 tier was
    # adopted — the mismatch below would have raised regardless of whether
    # anyone remembered to update this path by hand. Deriving it means the
    # NEXT tier change can't reintroduce the same failure mode.
    m = _require(r"^(docs/operations/fee_tier_[\d-]+\.json)",
                  CURRENT_SCHEDULE.source, ROOT / "pipeline" / "fees.py",
                  "CURRENT_SCHEDULE's evidence file path in its source field")
    # `m` is reassigned by every later _require() call in this function, so
    # the group is captured into its own name now rather than read back off
    # `m` inside the f-string built at the end — that read would silently
    # pick up whatever `m` was rebound to last, not this match.
    evidence_relpath = m.group(1)
    evidence_path = ROOT / evidence_relpath
    if not evidence_path.exists():
        raise RuntimeError(f"Research page needs {evidence_path} — not found")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    measured = evidence["measured_tier"]
    if (measured["maker_fee_rate"] != CURRENT_SCHEDULE.maker_rate
            or measured["taker_fee_rate"] != CURRENT_SCHEDULE.taker_rate):
        raise RuntimeError(
            "Research page: pipeline/fees.py CURRENT_SCHEDULE "
            f"({CURRENT_SCHEDULE.maker_rate}/{CURRENT_SCHEDULE.taker_rate}) "
            f"disagrees with {evidence_path.name}'s measured_tier "
            f"({measured['maker_fee_rate']}/{measured['taker_fee_rate']}) — "
            "these must describe the same adopted schedule"
        )
    # Whether the adopted cohort was independently audited is itself a fact
    # that changes between schedules (the 2026-09-15 cohort was; the
    # 2026-09-22 one was only reproduced from the local probe log, not
    # separately audited) — read from the evidence file rather than asserted
    # as a generator literal, so an unaudited adoption is never described as
    # audited.
    audited = bool(evidence.get("evidence", {}).get("independently_audited"))
    audited_clause = (
        "a measured, independently audited 4-reading cohort" if audited else
        "a measured 4-reading cohort, reproduced from the local probe log "
        "but not independently audited")

    cost_sensitivity_py = ROOT / "backtesting" / "cost_sensitivity.py"
    cs_source = cost_sensitivity_py.read_text(encoding="utf-8")
    m = _require(r'observed_at="([\d\-:T.+]+)"', cs_source, cost_sensitivity_py,
                  "the candidate reading's observed_at timestamp")
    candidate_observed_at = m.group(1)
    m = _require(r'"pricing_tier":\s*"([^"]+)"', cs_source, cost_sensitivity_py,
                  "the candidate reading's pricing_tier")
    candidate_pricing_tier = m.group(1)
    m = _require(r"4-reading cohort \((\d{4}-\d{2}-\d{2}) → (\d{4}-\d{2}-\d{2})\)",
                  registry_text, registry_path, "the candidate cohort window")
    cohort_start, cohort_end = m.group(1), m.group(2)

    # ── 3. Cost-sensitivity headline: docs/research/2026-09-cost-sensitivity.md ─
    cs_md_path = ROOT / "docs" / "research" / "2026-09-cost-sensitivity.md"
    if not cs_md_path.exists():
        raise RuntimeError(f"Research page needs {cs_md_path} — not found")
    cs_text = cs_md_path.read_text(encoding="utf-8")

    m = _require(
        r"\*\*ADOPTED \(all exit paths[^\n]*\*\*\s*\|\s*0\.6%\s*\|\s*1\.2%\s*\|\s*\*\*\+([\d.]+)%\*\*",
        cs_text, cs_md_path, "the ADOPTED break-even gross move")
    breakeven_adopted = m.group(1)
    m = _require(
        r"\*\*CANDIDATE, not adopted \(all exit paths\)\*\*\s*\|\s*0\.5%\s*\|\s*0\.9%\s*\|\s*\*\*\+([\d.]+)%\*\*",
        cs_text, cs_md_path, "the CANDIDATE break-even gross move")
    breakeven_candidate = m.group(1)

    m = _require(
        r"\*\*PROSPECTIVE SENSITIVITY — ADOPTED \(0\.6%/1\.2%\)\*\*\s*\|\s*114\s*\|\s*\*\*([\d.]+)\*\*\s*\|\s*\*\*(-?[\d.]+)%/trade\*\*",
        cs_text, cs_md_path, "the ADOPTED re-priced PF/expectancy")
    pf_adopted, expectancy_adopted = m.group(1), m.group(2)
    m = _require(
        r"\*\*PROSPECTIVE SENSITIVITY — CANDIDATE, not adopted \(0\.5%/0\.9%\)\*\*\s*\|\s*114\s*\|\s*\*\*([\d.]+)\*\*\s*\|\s*\*\*(-?[\d.]+)%/trade\*\*",
        cs_text, cs_md_path, "the CANDIDATE re-priced PF/expectancy")
    pf_candidate, expectancy_candidate = m.group(1), m.group(2)

    m = _require(r"\| Frozen 1\.0% model \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the frozen-model 95% upper bound")
    bound_frozen = float(m.group(1))
    m = _require(r"\| ADOPTED 0\.6%/1\.2% \(measured\) \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the ADOPTED 95% upper bound")
    bound_adopted = float(m.group(1))
    m = _require(r"\| CANDIDATE 0\.5%/0\.9% \(not adopted\) \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the CANDIDATE 95% upper bound")
    bound_candidate = float(m.group(1))

    if not (bound_frozen > 0 and bound_adopted < 0 and bound_candidate < 0):
        raise RuntimeError(
            "Research page: expected the frozen-model bound above zero and "
            "both operational bounds below zero, but parsed "
            f"frozen={bound_frozen}%, adopted={bound_adopted}%, "
            f"candidate={bound_candidate}% — the write-up's conclusion may "
            "have changed; re-check docs/research/2026-09-cost-sensitivity.md "
            "before regenerating this page"
        )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    note = f"""---
date: {today}
type: research
tags: [research, validation-status, fees, cost-sensitivity, derived]
---

# Research Status — derived, not hand-written

> Every figure on this page is parsed from a committed source file at
> generation time (`backtesting/generate_journal.py::generate_research_notes`).
> None of it is a literal typed into the generator. If a source file's format
> changes in a way that breaks parsing, vault regeneration fails loudly
> instead of publishing a blank or stale number.

## 1. Current validation status

**DRY_RUN / LIVE NO-GO.** This system trades in paper/shadow mode only. No
real-money trading is authorized.

- V2 momentum (ZEC-USD, frozen mechanism): **PF {v2_pf} ({v2_expectancy_pct}%/trade, n={v2_n})**
  — *{v2_status}*
- `docs/trial_registry.md` records the **LIVE NO-GO** verdict.
- `CLAUDE.md` is explicit: *"Do NOT switch `DRY_RUN=false` on current evidence."*

Source: [[../../docs/trial_registry.md|docs/trial_registry.md]], `CLAUDE.md`
("Validation Status").

## 2. Operational fee schedule

| | Maker | Taker | Tier |
|---|---:|---:|---|
| **ADOPTED** — `pipeline/fees.py` `CURRENT_SCHEDULE` | {CURRENT_SCHEDULE.maker_rate:.1%} | {CURRENT_SCHEDULE.taker_rate:.1%} | {CURRENT_SCHEDULE.tier_name} |
| **CANDIDATE — NOT ADOPTED** — single {candidate_observed_at} reading | {_CANDIDATE_MAKER_RATE:.1%} | {_CANDIDATE_TAKER_RATE:.1%} | {candidate_pricing_tier} |

The adopted schedule (`{CURRENT_SCHEDULE.schedule_id}`) is {audited_clause} —
see `{evidence_relpath}`. The candidate tier is a single {candidate_observed_at}
reading and is **not written into `pipeline/fees.py`**;
`active_schedule()` still returns only the adopted schedule. Formal adoption
waits for its own 4-reading cohort ({cohort_start} → {cohort_end}) before any
`FeeSchedule` entry is added for it.

Source: `pipeline/fees.py`, `{evidence_relpath}`,
`backtesting/cost_sensitivity.py`.

## 3. Cost-sensitivity headline (trial `2026-09-cost-sensitivity.v1`)

> Prospective cost sensitivity, not an edge test — see
> `docs/research/2026-09-cost-sensitivity.md` for the full write-up and every
> caveat on how these numbers may and may not be used.

**Break-even gross move** (the same across STOP_LOSS/MAX_HOLD/TAKE_PROFIT,
because `close_position()` prices every exit at the taker rate):

| Schedule | Break-even gross move |
|---|---:|
| ADOPTED (measured) | +{breakeven_adopted}% |
| CANDIDATE, not adopted | +{breakeven_candidate}% |

**The frozen V2 ZEC mechanism's own n={_ARTIFACT_N_CLOSED} trades, re-priced**
(same entries/exits, different fee assumption — not a restatement of
`docs/research/artifacts/results.json`):

| Fee model | PF | Expectancy |
|---|---:|---:|
| Frozen artifact (historical fee model) | {_ARTIFACT_PF:.5f} | {_ARTIFACT_EXPECTANCY_PCT * 100:+.2f}%/trade |
| PROSPECTIVE — ADOPTED | {pf_adopted} | {expectancy_adopted}%/trade |
| PROSPECTIVE — CANDIDATE, not adopted | {pf_candidate} | {expectancy_candidate}%/trade |

**One-sided 95% upper bound on the true per-trade edge:**

| Scenario | Upper bound | Side of zero |
|---|---:|---|
| Frozen 1.0% model | {bound_frozen:+.4f}% | **Above zero** |
| ADOPTED, measured | {bound_adopted:+.4f}% | **Below zero** |
| CANDIDATE, not adopted | {bound_candidate:+.4f}% | **Below zero** |

The frozen-model bound sits above zero while both operational-cost bounds sit
below zero: at either the adopted or the candidate operational fee schedule,
this sample rules out a profitable version of the frozen mechanism at 95%
one-sided confidence. This does not change `DRY_RUN`, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, V3 status, or Phase 7B status.

Source: [[../../docs/research/2026-09-cost-sensitivity.md|docs/research/2026-09-cost-sensitivity.md]].
"""
    _write(folder / "Research Status.md", note)


# ── README ────────────────────────────────────────────────────────────────────

def generate_readme() -> None:
    readme = """# CryptoOrchestra — Second Brain

This vault is the living knowledge base for the CryptoOrchestra multi-agent trading system.
It grows automatically: every trade, backtest, and agent decision is logged here.

## Vault Structure

| Folder | Contents |
|--------|----------|
| [[TradeJournal/]] | One note per closed trade — P&L, hold time, lessons |
| [[Backtests/]] | Scanner results, Monte Carlo outputs, period analyses |
| [[Research/]] | Validation status, fee schedule, and cost-sensitivity headline — derived from committed docs, regenerated each run |
| [[Strategies/]] | Parameters, changelog, approach decisions |
| [[ErrorsAndFixes/]] | Documented mistakes and how they were fixed |
| [[AgentOutputs/]] | One note per REAL ensemble decision, plus one rolling Scanner Activity note for silent/blocked hours |
| [[AgentShadow/]] | One note per shadow-mode decision (observation only — no trade resulted) |
| [[MarketNotes/]] | Market regime observations |
| [[_Templates/]] | Note templates for manual additions |

## Quick Stats
> Update after each weekly review

- **Total live trades:** 7
- **Live P&L:** -$46.62 (bear market period — all stops)
- **Best asset:** ZEC (48.6% win rate in full-year backtest)
- **System status:** DRY_RUN=true | Targeting live $100 test

## Weekly Review Prompt (copy to Claude)
```
Read the last 20 notes in TradeJournal/ and ErrorsAndFixes/.
Find: 1) most common reason for losses, 2) any repeating pattern,
3) one parameter experiment to try next week.
```

## Key Decisions
- [[Strategies/Changelog]] — full history of parameter changes
- [[Backtests/full_year_per_asset_MC]] — latest Monte Carlo results
- [[ErrorsAndFixes/ZEC_Whipsaw_May2026]] — most important lesson so far
"""
    _write(VAULT / "README.md", readme)


# ── Templates ─────────────────────────────────────────────────────────────────

def generate_templates() -> None:
    folder = _ensure(VAULT / "_Templates")

    trade_tpl = """---
date: {{date}}
asset: {{asset}}
type: trade
result: WIN|LOSS
pnl_pct:
reason: STOP_LOSS|TAKE_PROFIT|MAX_HOLD
hold_hours:
tags: []
---

# {{icon}} {{asset}} — {{date}}

## Entry / Exit
| Field | Value |
|-------|-------|
| Entry time | |
| Exit time  | |
| Entry price | |
| Exit price  | |
| P&L | |

## What happened

## Agent consensus

## Lessons
- [ ]
"""
    _write(folder / "Trade Note.md", trade_tpl)

    weekly_tpl = """---
date: {{week_start}}
type: weekly-review
tags: [weekly-review]
---

# Weekly Review — {{week_start}}

## Trades this week
| Asset | Result | P&L | Notes |
|-------|--------|-----|-------|
| | | | |

## Agent consensus quality
*Were agents aligned on winning trades? Did they disagree on losing ones?*

## Pattern spotted

## Parameter experiment for next week

## Overall mood
"""
    _write(folder / "Weekly Review.md", weekly_tpl)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global VAULT

    if "--vault" in sys.argv:
        idx  = sys.argv.index("--vault")
        VAULT = Path(sys.argv[idx + 1])

    _written_paths.clear()

    print(f"\nGenerating Obsidian vault at: {VAULT}\n")

    print("TradeJournal/")
    generate_trade_notes()

    print("\nAgentOutputs/")
    generate_agent_notes()

    print("\nAgentShadow/")
    generate_agent_shadow_notes()

    print("\nBacktests/")
    generate_backtest_notes()

    print("\nResearch/")
    generate_research_notes()

    print("\nStrategies/")
    generate_strategy_notes()

    print("\nErrorsAndFixes/")
    generate_error_notes()

    print("\n_Templates/")
    generate_templates()

    print("\nREADME.md")
    generate_readme()

    removed = _prune_stale_notes()
    print(f"\nPruned {removed} stale note(s) this generator previously wrote "
          "but did not produce this run.")

    print(f"\nDone. Open {VAULT} as an Obsidian vault.")
    print("Install Obsidian from https://obsidian.md (free) and open this folder.")


if __name__ == "__main__":
    main()
