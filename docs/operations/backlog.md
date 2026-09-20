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

- **(b) The agent pipeline's live price source and the research harness's
  source disagree.** `tools/price_data.py` pulls from `yfinance` (Yahoo
  Finance); every research consumer — `backtesting/hydrate_research_data.py`,
  `backtesting/hydrate_perps_proxy.py`, `backtesting/hydrate_carry_basis.py` —
  pulls from Coinbase's own public endpoints or Binance's public archive. A
  live run and a backtest of the same nominal mechanism are not reading the
  same tape. This has not mattered operationally because the agent pipeline
  does not currently run (see `CLAUDE.md` "Research program status"), but it
  would need resolving before any future live or shadow reactivation.

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
