# SUPERSEDED — `2026-08-evidence-hardening.v1`

Superseded on 2026-08-13 by `2026-08-warmup-semantics.v1`
(`docs/research/artifacts/`). Kept verbatim, not deleted: these files are the
record of what was published, and the size of the correction is only legible
against them.

## Why

The scanner that produced these numbers failed **open** on a missing indicator.
A hard gate whose operand was still NaN was skipped outright, so while an
indicator warmed up the bar was judged by a *weaker* mechanism than the config
declared — and nothing counted it.

This config compounded the defect by declaring each asset's evaluation start at
its **first candle**, which for two assets is months before the frozen
mechanism's 200-day daily EMA exists.

## What changed in the affected rows

| Row | This artifact | `2026-08-warmup-semantics.v1` |
|---|---|---|
| `V2-continuous` (ZEC) | n=133, PF 0.855, −0.366%/trade | n=114, PF 0.761, −0.623%/trade |
| `V2-registry:bull_2021` | n=25, PF 1.419, +0.871%/trade | see current artifact |
| `transfer-frozen-zec:SOL-USD` | n=118, PF 0.698, from 2021-06-17 | n=97, from 2022-01-03 |

The 19 excluded ZEC trades contributed **+22.28%** between them — the entire
apparent `bull_2021` edge came from the span where the declared daily-EMA veto
could not be computed.

## Status of these numbers

**Not comparable** to the current artifact and **not citable** in a decision.
They measure a different mechanism. The direction of the headline conclusion is
unchanged — V2 is unprofitable, integrated V3 is worse, no asset reaches PF 1.0
— and the correction makes it *more* negative, not less.
