# CFM funding monitor

This is a small, credential-free measuring instrument. It records the public
hourly funding snapshot for Coinbase's BTC and ETH CFM perpetual-style futures.
It is how the project's recorded condition for re-evaluating carry is
watched — it is not that condition, and it does not open a position,
recommend one, or change any trading decision. See "What a crossing is, and
is not" below.

## What it records

At minute `:02` of every hour, `backtesting/cfm_funding_monitor.py` calls
`get_public_products(product_type="FUTURE")` through the cost probe's existing
unauthenticated client. It selects:

- `BIP-20DEC30-CDE` (BTC)
- `ETP-20DEC30-CDE` (ETH)

For each product it appends the public `funding_rate`, `funding_time`, and
`funding_interval` to `logs/cfm_funding.jsonl`, along with observation time and
derived monitoring fields. A `(product, funding_time)` pair is written once, so
a repeated poll within one funding interval cannot overweight that hour.

The annualised decimal rate is:

```text
funding_rate × (365.25 × 24 × 3600 / funding_interval_seconds)
```

## Coverage — two checks, two questions

The reported statistic is a **trailing seven-day mean**, and the thresholds
below were derived for a seven-day mean. A mean over whatever observations
happen to exist is a different statistic with the same name, so two
independent requirements must both pass before the monitor publishes one:

| Check | Window | Minimum | Question it answers |
|---|---|---:|---|
| **Freshness** | last 24 h | 20 of 24 hourly slots | Is the monitor running *now*? |
| **Depth** | last 7 d | **135 of 168 hourly slots (80%)** | Is this actually a *seven-day* mean? |

Failing either reports `trailing_7d_status: UNAVAILABLE` with a null mean — the
same fail-closed contract the 24-hour check has always used — and the record
and alert name the window that failed. Neither check substitutes for the
other: a week that is 96% covered but stopped a day ago is stale, and a
perfectly fresh day is not a week.

**Why the depth check was added.** Until 2026-09-22 only freshness was
checked, and the mean was then taken over whatever lay inside the seven-day
window. Three days after the monitor was built that produced a "trailing
7-day mean" over 43 observations — about 1.8 days — compared against a
seven-day threshold. BTC read *above threshold* on 24 consecutive records on
that basis. The 24-hour check had passed the whole time, because it answers
the other question.

**Why 80%.** It is a declared convention, not an estimate, and no alternative
was evaluated. It tolerates an outage the size of a reboot plus a patch cycle
(up to 33 missing hours spread across the week) while still refusing to call
two days a week. A 100% rule would blind the monitor for a full week after a
single missed poll, and lost hours are permanent — CFM has no funding-history
endpoint, so nothing can be backfilled.

**Expect a warm-up.** From a cold start the mean is `UNAVAILABLE` for the
first ~5.6 days of continuous operation, and each product's first observation
emits one coverage alarm.

**The change applies going forward only.** Existing log lines were not
rewritten. Because the previous state is recomputed under the current rule,
the first poll after this change does not fire a transition alert — it simply
records `UNAVAILABLE` with the depth reason. Records written before
2026-09-22 carry a `trailing_7d_mean_annualized` computed under the old,
freshness-only rule; do not compare them against the thresholds below.

## What a crossing is, and is not

The condition recorded in `CLAUDE.md` for re-evaluating carry is funding
**sustained above break-even for longer than a typical cycle** — the median
cycle length from the carry document, **17.0 days (BTC)** and **21.3 days
(ETH)**. A threshold crossing is a notification that the recorded condition
may be starting to form. It is not the condition, and this monitor does not
evaluate the condition: it records and alerts, and decides nothing.

Every record carries `reevaluation_condition` and `reevaluation_sustain_days`
so the distinction survives the trip from this log to whoever reads it, and
every threshold alert restates it.

## Thresholds

The thresholds are fixed by the adopted-fee arithmetic in
[`../research/2026-09-19-carry-scoping.md`](../research/2026-09-19-carry-scoping.md):

| Product | Realised-rate break-even | Typical-cycle break-even | Median cycle |
|---|---:|---:|---:|
| BTC | 14.8%/yr | 45.7%/yr | 17.0 d |
| ETH | 14.4%/yr | 38.7%/yr | 21.3 d |

The first threshold amortises the adopted round-trip cost at the cycle rate
actually observed in the proxy history. The higher threshold asks what the
typical (median-length) cycle must earn to pay for itself. Both include the
scoping document's top-up friction allowance. They are monitoring boundaries,
not entry rules. They are compared against the trailing seven-day mean, which
is why that mean has to be a seven-day mean — see the coverage section above.
The median-cycle column is the "longer than a typical cycle" duration in the
recorded re-evaluation condition; it is reported, not computed against.

## Reading an alert

A threshold alert names the product, the trailing seven-day annualised mean,
the boundary crossed, and whether the crossing was above or below it. It means
that a covered seven-day mean moved across a monitoring boundary — nothing
more. It is not the recorded re-evaluation condition, not a signal, and not an
instruction to trade.

A threshold alert also restates the recorded re-evaluation condition and the
typical cycle length for that asset, and prints **both** coverage
denominators, so it cannot be read as a covered week when it is not one.

A coverage alert means one of the two checks above failed, and names which:
`freshness: n/24 ...` or `depth: n/168 ...`. While either persists the mean is
`UNAVAILABLE`. A recovery alert says both are satisfied again. The JSONL write
happens before any Telegram attempt; a missing Telegram configuration,
rejection, or network error cannot prevent the observation from being stored.

## Verify it is running

Register or refresh the task from an ordinary PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_cfm_funding_monitor_task.ps1
```

The registration script reads the task back and verifies an hourly trigger at
`:02`, `WakeToRun = True`, `StartWhenAvailable = True`, battery restrictions
off, and a five-minute execution limit. A late poll remains a valid current
snapshot here, unlike the execution-window-bound cost probe.

Inspect task state and the latest durable records:

```powershell
Get-ScheduledTask -TaskName CryptoOrchestra-CFM-FundingMonitor |
    Select-Object TaskName, State
Get-ScheduledTaskInfo -TaskName CryptoOrchestra-CFM-FundingMonitor |
    Select-Object LastRunTime, LastTaskResult, NextRunTime
Get-Content logs\cfm_funding.jsonl -Tail 2
```

`LastTaskResult` should be `0`. Each healthy, non-duplicate funding interval
adds one BTC line and one ETH line. A run that finds the same `funding_time`
again exits successfully and reports that there was nothing new to append.
