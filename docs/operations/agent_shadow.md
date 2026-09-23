# Log-only agent shadow

Declared 2026-09-20. This service is an engineering observation stream. It
decides nothing, places nothing, and is outside `pipeline/` so its output is not
on a trading path. It does not change `signal_scanner.py`, `ASSET_CONFIG`, the
entry filters, any frozen constant, `DRY_RUN`, or `LIVE_BALANCE_USD`.

## Reading rule (declared before the first run)

A shadow stream counts as a **RESULT** only if, over a stated period, its
one-sided 95% upper bound clears zero **and** its annualised Sharpe exceeds
`1.645 / sqrt(years)`: 1.65 at one year, 1.16 at two years, and 0.95 at three
years. Anything below both requirements is **OBSERVATION** and must be recorded
as such.

More candidates per year do not lower this threshold. Under the standing floor
identity, trade count cancels out: the annual floor depends on per-bar
dispersion and time in market, while more trades still multiply costs.

Selecting agents, votes, confidence bands, assets, or any other subset because
of its observed outcomes and moving that subset into the trading gate is
forbidden. Such a change requires a new trial in `docs/trial_registry.md`,
pre-registered before evaluation, with its own SESOI and floor gate.

### LIVE and BACKFILL are two populations (added 2026-09-23)

A decision made during catch-up (see "Catch-up after a gap" below) is made by
agents reading **current** news, sentiment and market context, not the context
as of the candle it is attached to. The price path attached later is
unaffected; the agents' inputs are not. A backfilled record is therefore a
different observation from a live one, and every vote and decision is labelled:

- `timing` = `LIVE` when the record was decided less than one hour after its
  bar closed (the regular hourly run for that bar), `BACKFILL` otherwise;
- `lag_hours_after_bar_close`, the measured lag the flag is derived from.

The flag is derived from the lag on each record, never from how the run was
invoked: a catch-up run still decides its newest bar `LIVE` and older bars
`BACKFILL`, and a slow regular run can produce a `BACKFILL` record.

**Any reading of this stream must either separate the two populations or
state a justification for pooling them.** The RESULT test above — the
one-sided bound and the Sharpe threshold — applies to each population on its
own. A RESULT in the pooled stream that is not a RESULT in the `LIVE`
population is not a RESULT.

## What is recorded

The candidate producer calls `signal_scanner.build_merged_frame` and
`signal_scanner._detect_breakout_signal` directly and unmodified. A WIDE
candidate is an EMA50 trigger that passes every hard gate declared for that
asset and reaches the scored conditions, before the `min_conditions` test. The
producer records `n_met`, including candidates below the live `n_met >= 4`
gate. The task runs hourly and examines every closed bar since the last one it
recorded (see "Catch-up after a gap"); LLM calls are made only for new
candidates.

`logs/agent_shadow.jsonl` is append-only and gitignored. Tests replace this path
with a temporary file; the repository-wide real-logs guard in
`tests/conftest.py` also rejects any test write anywhere under the real `logs/`
directory.

There are four JSONL record types (schema version 2; version 1 had no
`scan_checkpoint` and no timing fields):

- `agent_vote`: `candidate_id`, `event_id`, variant, asset, candle time,
  `n_met`, entry close and ATR bracket inputs; agent name, direction,
  confidence, full reasoning and its SHA-256 digest; model id, latency, SDK
  input/output token counts when available, call count, metered cost, error,
  and the complete structured signal payload. Since schema 2 also
  `decided_at`, `bar_close_time` (candle time + 1h),
  `lag_hours_after_bar_close` and `timing` (`LIVE`/`BACKFILL`).
- `orchestrator_decision`: the same candidate, metering and timing fields,
  plus the combined direction, confidence, full reasoning/digest, error, and
  complete structured decision payload, and `earliest_vote_at` /
  `latest_vote_at` — a decision can reuse votes recorded by an earlier,
  deferred run, and these bound when its inputs were actually gathered. A
  failed orchestrator is logged as HOLD with an error; it still cannot reach
  an order path.
- `scan_checkpoint`: one per run. What the run examined, not only what it
  found — per asset `examined_through` (the newest closed bar examined, held
  back before any deferred candidate), `bars_examined`, `cold_start`,
  `lookback_hours`, `lookback_truncated`, the run's counters, and every
  deferred candidate with its reason. It carries no `candidate_id` and is not
  an observation.
- `price_attachment`: one record per `candidate_id`, added only after the full
  seven-day path exists. It holds raw close-to-close returns at +4h, +24h,
  +72h, and +7d and the stop/target/max-hold outcome using that asset's recorded
  frozen ATR bracket. It computes no strategy P&L and no aggregate statistic.

The attachment key is deterministic. Re-running the attachment command sees an
existing `candidate_id` and does not append a second record. The attachment
has no timing flag of its own: the price path depends only on the candle, not
on when the agents were asked.

## Catch-up after a gap (added 2026-09-23)

Before this change each run examined exactly one closed bar per asset — the
newest. The task is hourly with `StartWhenAvailable`, so after a six-hour sleep
the catch-up run looked at the newest bar only and the five before it were
never examined. Nothing backfilled them.

**Resume point.** Each run reads the shadow log itself and, per asset, resumes
at the later of:

- one bar after the newest `scan_checkpoint.examined_through` for this
  `variant_id` — every bar up to it was examined and every candidate there
  decided;
- the newest decided candidate's `candle_time` for this `variant_id`,
  inclusive — which covers a run killed after deciding but before writing its
  checkpoint (the task has a 40-minute limit). The overlap is free: dedup
  skips it.

It then examines every closed bar from there to the newest closed bar.
Checkpoints are needed because candidate records alone cannot distinguish
"examined, nothing found" from "never examined": without them a log with no
candidate yet — as on 2026-09-23 — would never leave a resume point, and a
quiet stretch longer than the cap would read as an outage. A bar the data
source has not served yet is not marked examined; the next run picks it up.

**Cold start.** With no checkpoint and no decision for the variant (an empty
or missing log), a run examines only the newest closed bar per asset, exactly
as before, and does not replay the 120-day warm-up window as if it had been
missed. Its checkpoint makes every later run resume-aware.

**Look-back cap.** One run reaches at most `AGENT_SHADOW_LOOKBACK_HOURS`
(default **72**, `--lookback-hours` overrides it for a run) back from the
newest closed bar, so an unnoticed month-long outage cannot turn one hourly
invocation into a 700-bar replay. When the cap cuts a resume point short, the
run prints `LOOK-BACK CAP` to stderr, and the checkpoint records
`lookback_truncated` with the original resume point, where examination
started, and how many bars will never be examined. Those bars are lost, not
deferred. That includes deferred candidates older than the cap by the time the
next run happens (for example, a spend ceiling that stays hit for days).

**Deferred is not skipped.** The run's counters keep them apart:

- `skipped` — already decided in an earlier run; finished, never revisited;
- `deferred` — found this run but not decided yet, because the candidate cap
  or the spend ceiling stopped the run. No decision is written for them
  (votes already paid for before a ceiling refusal are kept and reused), the
  checkpoint stays one bar before the oldest one, and the next run decides
  them. Each is listed in the checkpoint with its reason (`candidate_cap` or
  `spend_ceiling`) and printed to stderr as `DEFERRED`.

The run's final stdout line is a JSON summary: `bars_examined`,
`candidates_found`, `candidates`, `votes`, `decisions`, `skipped`, `deferred`,
`cold_start`, `lookback_truncated`.

## Variants

`docs/operations/agent_shadow_variants.jsonl` is the append-only registry. Every
line is a declaration made before its first run and contains:

- `variant_id` and `declared_date`;
- asset list;
- candidate stage and definition;
- participating agents;
- full orchestrator weighting;
- exact sub-agent and orchestrator model ids.

Changing any of these fields requires a new `variant_id`. Do not edit an
existing line. Code exposes an append operation and rejects an existing id; it
has no update operation.

The seeded `agent-shadow-wide-v1` variant covers BTC, ETH, SOL, and ZEC at the
WIDE stage, with the six Claude-calling agents and the current Sonnet
orchestrator. The deterministic breakout agent is not a model call and is not a
participant; the WIDE event itself supplies the breakout condition. Its
declared mature-rate projection is about **$0.92/month**, based on the blind
candidate census's 54.47 candidates/month and its stated 6-Haiku + 1-Sonnet
call estimate. This is a budgeting projection, not an outcome claim.

To declare a variant, prepare a complete object with the fields above and use
`agent_shadow.variants.append_variant`. Review and commit the new line before
enabling its first run.

## Cost controls

`AGENT_SHADOW_MONTHLY_CEILING_USD` is a hard monthly ceiling; the default is
`5.00`. `logs/agent_shadow_spend.json` persists the UTC month, actual metered
spend, and conservative in-flight reservations. Before every SDK call, the
shadow reserves a deliberately high input bound plus the request's maximum
output. A call that would cross the ceiling is refused before transport and
printed prominently. Since 2026-09-23 a refusal **defers** the candidate and
every later one in the run instead of recording a NEUTRAL vote or a failed
HOLD in its place — which used to mark the candidate decided on missing input
and skip it forever. Votes already recorded for a deferred candidate are kept
(they were paid for) and reused when the next run decides it; the decision's
`earliest_vote_at`/`latest_vote_at` then show the span. If the SDK omits usage or the call fails after transport begins, the full
reservation is charged conservatively. A crash can leave a reservation in the
file; it remains budget-consuming (fail closed) until the owner audits it.

`AGENT_SHADOW_CANDIDATE_CAP` limits candidates decided in one invocation and
defaults to 10. `--candidate-cap` can set a smaller operational value for a
run. Already-decided candidates do not count against it. The oldest
candidates are decided first; the rest are deferred, never dropped.

## Run, attach, and verify

Do not run these commands until the owner intentionally enables API use:

```powershell
venv\Scripts\python.exe -m agent_shadow.runner --variant agent-shadow-wide-v1
venv\Scripts\python.exe -m agent_shadow.attachments
```

The second command reads prices only and is safe to repeat. To inspect recent
records without modifying the log:

```powershell
Get-Content logs\agent_shadow.jsonl -Tail 20
Get-Content logs\agent_shadow_spend.json
```

Register the scheduler only from the primary checkout after merge:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_agent_shadow_task.ps1
Get-ScheduledTaskInfo -TaskName CryptoOrchestra-AgentShadow
```

The registration script resolves the primary Git checkout (the first entry in
`git worktree list --porcelain`), uses its batch file and working directory,
sets an hourly :05 poll, `WakeToRun`, `StartWhenAvailable`, and disables both
battery restrictions. The batch file sets `PYTHONIOENCODING=utf-8`. Task
Scheduler ignores overlapping runs. A successful no-event invocation prints
zero candidates; that is expected and incurs no model cost. A catch-up run
deciding the full default cap of 10 candidates makes 70 model calls and can
approach the task's 40-minute limit (the script states the arithmetic:
10 candidates at the measured p99 is about 35 minutes); if it is killed, what it decided stays
decided, and the next run resumes from the log.
