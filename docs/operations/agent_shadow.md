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

## What is recorded

The candidate producer calls `signal_scanner.build_merged_frame` and
`signal_scanner._detect_breakout_signal` directly and unmodified. A WIDE
candidate is an EMA50 trigger that passes every hard gate declared for that
asset and reaches the scored conditions, before the `min_conditions` test. The
producer records `n_met`, including candidates below the live `n_met >= 4`
gate. The task polls once per closed hourly candle, but LLM calls are made only
for new candidates.

`logs/agent_shadow.jsonl` is append-only and gitignored. Tests replace this path
with a temporary file; the repository-wide real-logs guard in
`tests/conftest.py` also rejects any test write anywhere under the real `logs/`
directory.

There are three JSONL record types (schema version 1):

- `agent_vote`: `candidate_id`, `event_id`, variant, asset, candle time,
  `n_met`, entry close and ATR bracket inputs; agent name, direction,
  confidence, full reasoning and its SHA-256 digest; model id, latency, SDK
  input/output token counts when available, call count, metered cost, error,
  and the complete structured signal payload.
- `orchestrator_decision`: the same candidate and metering fields, plus the
  combined direction, confidence, full reasoning/digest, error, and complete
  structured decision payload. A failed orchestrator is logged as HOLD with an
  error; it still cannot reach an order path.
- `price_attachment`: one record per `candidate_id`, added only after the full
  seven-day path exists. It holds raw close-to-close returns at +4h, +24h,
  +72h, and +7d and the stop/target/max-hold outcome using that asset's recorded
  frozen ATR bracket. It computes no strategy P&L and no aggregate statistic.

The attachment key is deterministic. Re-running the attachment command sees an
existing `candidate_id` and does not append a second record.

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
output. A call that would cross the ceiling is refused before transport and the
refusal is written in the vote/decision error field and printed prominently.
If the SDK omits usage or the call fails after transport begins, the full
reservation is charged conservatively. A crash can leave a reservation in the
file; it remains budget-consuming (fail closed) until the owner audits it.

`AGENT_SHADOW_CANDIDATE_CAP` limits candidates in one invocation and defaults
to 10. `--candidate-cap` can set a smaller operational value for a run.

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
zero candidates; that is expected and incurs no model cost.
