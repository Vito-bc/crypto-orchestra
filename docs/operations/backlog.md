# Operational backlog

Recorded findings, not fixes. Each line names the defect and the evidence
behind it; none is addressed in the PR that adds this file. This is
operational housekeeping, separate from the research program in
[`../trial_registry.md`](../trial_registry.md) — nothing here bears on any
closure, on `DRY_RUN`, or on `LIVE_BALANCE_USD`.

- **(a) The STF cost probe's WakeToRun task does not wake the host from the
  sleep state.** Observed 2026-09-20: the scheduled reading was lost, and the
  catch-up attempt returned Windows Task Scheduler result `0x800710E0`
  ("the operator or administrator has refused the request") rather than
  running. Evidence: the missing entry in `logs/stf_cost_probe.jsonl` for the
  expected 2026-09-20 window, and the task result recorded against the
  Scheduled Tasks history for that run. This is why the fee-tier reading
  cohort below skips 09-20 — see (d).

- **(b) The live pipeline has used Yahoo data by accident when Coinbase
  candle fetching fails.** The scanner's `_fetch_ohlcv` silently fell back to
  `yfinance` after any Coinbase error. Reproduced on 2026-09-21 from main
  `7deb2ed`: a current ZEC cache made the next 1h and 4h fetch start in the
  future; Coinbase returned HTTP 400 (`start must not be in the future`),
  both frames came from Yahoo, and the 1d frame came from Coinbase. Earlier
  `logs/scheduler.log` entries ("Fetching 90 days of ZEC-USD data from Yahoo
  Finance") show this has occurred since at least July. `tools/price_data.py`
  is another explicit Yahoo price source, independent of this scanner defect.
  This PR prevents the future-start request and labels each served scanner
  frame and its written records, but **does not change the live pipeline's
  permitted fallback policy**. Whether live trading may ever use Yahoo data
  remains an owner decision; until then, its results are not directly
  comparable to Coinbase-only research.

- **(c) `check_and_fill` does not check `cancel_order`'s return value.**
  `pipeline/limit_orders.py:389` calls `cancel_order(exch_id)` and discards
  the boolean result. `cancel_order`'s own docstring
  (`exchange/coinbase_client.py:256`) states plainly: "Returns False for
  CANCEL_QUEUED, PENDING_CANCEL, already-filled, errors. Caller should treat
  False as UNRESOLVED and re-check on the next reconciliation run." The
  caller does neither — it proceeds as if the cancel succeeded. **Live-only**:
  the dry-run path returns `True` unconditionally before reaching the network
  call, so this has never been exercised in paper/shadow mode. Pre-existing;
  not introduced by any change in this PR.

- **(d) The fee-tier adoption PR is due after the 2026-09-21 reading
  completes the Intro cohort.** The account's fee tier changed to "Intro"
  (maker 0.5% / taker 0.9%) on 2026-09-16, one reading after
  `pipeline/fees.py` `CURRENT_SCHEDULE` was adopted from the prior "Intro 1"
  cohort (see `docs/operations/fee_tier_2026-09-15.json`). The same bar
  applies before adopting the new tier: four consecutive daily readings with
  no tier change. The cohort is **2026-09-17, 09-18, 09-19, 09-21** — not
  09-20, which (a) above lost — so the follow-up PR adding a new
  `FeeSchedule` (new id, old one kept in the registry) is due once the
  09-21 reading lands in `logs/stf_cost_probe.jsonl`, not before.

- **(e) The STF cost probe loses a missed day rather than delaying it — the
  same class of problem the agent shadow had, for a different reason.**
  The shadow's catch-up run used to examine only the newest bar, so a sleep
  window thinned its stream; that is fixed by examining every missed bar
  (see `agent_shadow.md`, "Catch-up after a gap"). The probe cannot be fixed
  the same way, by design: it is anchored to 00:05 UTC, inside a 90-minute
  execution window, with `StartWhenAvailable = $false`
  (`scripts/register_stf_cost_probe_task.ps1`), because a reading taken hours
  late samples a different market than the protocol specifies. So a day the
  host is asleep through the window is a **lost** reading, not a delayed one.
  Observed 2026-09-20 and 2026-09-21: no entry in `logs/stf_cost_probe.jsonl`
  for either window (the fee-tier cohort in `fee_tier_2026-09-22.json`
  records both as lost). Not changed here; recorded so the gap pattern is not
  mistaken for a probe fault. Any fix belongs to the host's wake behaviour
  ((a) above), not to the probe's schedule.
