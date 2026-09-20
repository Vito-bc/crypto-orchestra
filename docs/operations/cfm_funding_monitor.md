# CFM funding monitor

This is a small, credential-free measuring instrument. It records the public
hourly funding snapshot for Coinbase's BTC and ETH CFM perpetual-style futures.
It is the project's recorded condition for re-evaluating carry; it does not
open a position, recommend one, or change any trading decision.

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

The trailing mean covers the prior seven days. It is reported as `UNAVAILABLE`
unless at least 20 distinct hourly observations exist in the last 24 hours; the
monitor never computes a reassuring mean across an operational gap. The first
day is therefore an expected warm-up period, and each product's first
observation emits one coverage alarm.

## Thresholds

The thresholds are fixed by the adopted-fee arithmetic in
[`../research/2026-09-19-carry-scoping.md`](../research/2026-09-19-carry-scoping.md):

| Product | Realised-rate break-even | Typical-cycle break-even |
|---|---:|---:|
| BTC | 14.8%/yr | 45.7%/yr |
| ETH | 14.4%/yr | 38.7%/yr |

The first threshold amortises the adopted round-trip cost at the cycle rate
actually observed in the proxy history. The higher threshold asks what the
typical (median-length) cycle must earn to pay for itself. Both include the
scoping document's top-up friction allowance. They are monitoring boundaries,
not entry rules.

## Reading an alert

A threshold alert names the product, the trailing seven-day annualised mean,
the boundary crossed, and whether the crossing was above or below it. It means
only that the recorded condition changed and the carry question may be reviewed
with the rest of the evidence. It is not a signal or an instruction to trade.

A coverage alert means fewer than 20 of the most recent 24 expected hourly
observations exist. While that persists the mean is `UNAVAILABLE`. A recovery
alert says coverage is sufficient again. The JSONL write happens before any
Telegram attempt; a missing Telegram configuration, rejection, or network error
cannot prevent the observation from being stored.

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
