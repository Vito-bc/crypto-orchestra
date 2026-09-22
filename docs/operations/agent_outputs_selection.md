# AgentOutputs/ individual-note selection rule

Declared 2026-09-22. This governs `backtesting/generate_journal.py`'s
`generate_agent_notes()` only — a vault-rendering rule, not a change to
what `pipeline/runner.py` decides, trades, or logs.

## The problem this rule exists for

`logs/agent_decisions.jsonl` holds three populations, in order of what the
vault should do with each:

1. **Scanner-gate skips** — no agent was ever called (`votes` and
   `agent_signals` both empty). Never individual notes; folded into
   `AgentOutputs/Scanner Activity.md`'s legacy table, or (2026-09-22 onward)
   never written at all — see `pipeline/scanner_activity.py`.
2. **Real decisions, not individually worthy** — agents voted, the
   orchestrator decided, and the decision was a routine-confidence HOLD with
   no trade behind it. Measured on the committed log (2026-09-22): **1915**
   of the log's 1995 real decisions land here — the overwhelming majority.
   Rendering one note each would recreate exactly the noise this vault
   rebuild exists to remove, just shifted from "silence" to "unremarkable
   HOLD." These fold into `Scanner Activity.md`'s new "Agent-era decisions"
   section as a **distribution** — per-day/asset counts, a confidence
   histogram, a veto-reason breakdown — not one note per instance.
3. **Real decisions, individually worthy** — one note each, full detail.

## The rule

A real decision (agents voted) earns an individual note when **any** of:

- `action != "HOLD"`, or
- `confidence >= threshold`, where `threshold` is parsed from
  `agents/orchestrator.py`'s own module docstring line — *"Confidence < X
  always produces HOLD"* — not hardcoded in the generator, so a change to
  that line moves the vault's selection with it; or
- the decision is **tied to an order/position lifecycle event**: when a BUY
  becomes a resting limit order, `pipeline/runner.py` stamps
  `"[Limit] Order #<id> placed at support $..."` onto the start of the
  now-HOLD record's `reasoning` (the live action is the order, not the
  persisted label). `generate_journal.py` matches this prefix exactly
  (`^\[Limit\] Order #`) rather than fuzzy-matching text.

The third branch is load-bearing, not decorative: of the log's 7 real
trades, 2 carry confidence below the threshold (0.46 and 0.41 against
0.55) and would be invisible as individual notes without it. Every one of
the 7 is caught by the order-tie match.

## Why the threshold is parsed from a docstring, not a numeric constant

`agents/orchestrator.py` has exactly two ENFORCED numeric gates,
`_BUY_THRESHOLD` (0.45) and `_SELL_THRESHOLD` (-0.35), both applied to
`composite_score` — a value computed locally inside `decide()` and never
written to `agent_decisions.jsonl`. The only field the log actually
persists is `confidence`, the LLM's own self-reported figure, which as of
this writing is gated by no enforced code path at all. The module's
docstring line — *"Confidence < 0.55 always produces HOLD"* — is the only
number in the file stated against `confidence` specifically, even though it
does not correspond to a live, enforced check. This is a known inconsistency
in `agents/orchestrator.py` itself (docstring vs. `_BUY_THRESHOLD`, and two
different quantities on two different scales), not resolved here — the
generator trusts the docstring's number because it is the one actually
about `confidence`, parses it with the same `_require()`-or-fail discipline
used throughout `generate_journal.py`, and will raise loudly rather than
silently drift if that line is ever rewritten or removed.

## Measured, as of 2026-09-22 (see the PR that declared this rule)

| | Count |
|---|---:|
| Real decisions total | 1995 |
| — individually worthy (one note each) | 80 |
| — agent-era aggregate (distribution only) | 1915 |
| Legacy scanner-gate-silent (aggregate only) | 240 |

## What this does not do

It does not change any trading decision, gate, or log write. It does not
retroactively edit `logs/agent_decisions.jsonl` — the log is the record;
the vault is the view, recomputed from it on every run.
