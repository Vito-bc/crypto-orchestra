# Recovering a FEE_ERROR order

## What it means

A pending order (`logs/pending_orders.json`) can carry status `FEE_ERROR`.
This means the order **genuinely met its fill condition** — Coinbase filled
it, or the dry-run price crossed the limit — but `pipeline/fees.py` could not
resolve a trustworthy entry fee rate for it, so `pipeline/limit_orders.py`
refused to record the fill rather than guess a rate.

This is a **deliberate fail-closed state**, not a bug to route around
automatically. See `pipeline/fees.py` and `pipeline/limit_orders.py`
(`_fee_stamp_is_fillable`) for the invariants this protects: no fee rate is
ever defaulted, guessed, or inferred from an absent field, a familiar-looking
number, or a calendar date.

`FEE_ERROR` keeps blocking **new** entries for that asset for as long as it
is unresolved (`get_open_orders()` counts it as outstanding) — this is
intentional. A second order must not fill for the same asset while the first
sits unrecorded.

## Why it happens

The order's `fee_schedule_id` / `maker_fee_rate` fields fail one of the
checks in `pipeline.fees.entry_rate_for_record()`:

- `fee_schedule_id` is `None` (present-but-null, or was stripped after being
  stamped) — there is no automatic legacy inference for this. An order that
  structurally never had the field (predates fee stamping entirely) simply
  has no id either, by construction, and is refused the same way.
- `fee_schedule_id` is present but not a schedule registered in
  `pipeline.fees._SCHEDULES`.
- `fee_schedule_id` and `maker_fee_rate` are both present but disagree (the
  id names a schedule whose maker rate does not match the stamped rate).

A second, distinct condition can also block a fill without ever touching the
fee stamp: the exchange confirmed the order FILLED but reported no usable
`average_filled_price` (`exchange/coinbase_client.py:check_order_filled`
returning `(True, None)`). This code does **not** mark that case `FEE_ERROR`
— there is nothing to "repair" in the fee stamp — it leaves the order's
status untouched and retries the same check next cycle. If it persists,
investigate the exchange order directly; do not edit `fee_error_price` in to
force it through (see "What not to do" below).

## How to inspect

```powershell
venv\Scripts\python.exe -c "import json; [print(r) for r in json.load(open('logs/pending_orders.json')) if r['status']=='FEE_ERROR']"
```

Check `fee_schedule_id`, `maker_fee_rate`, and `fee_error_price` on the
row. Cross-reference `fee_schedule_id` against the registered schedules in
`pipeline/fees.py` (`CURRENT_SCHEDULE`, `LEGACY_SCHEDULE`) to see which check
failed.

## How to repair

1. Decide which registered schedule this order should have been stamped
   with. This is a judgment call about history — usually "whichever schedule
   was active when the order's `placed_at` timestamp says it was placed."
   Do not invent a new schedule for this; use an id already registered in
   `pipeline.fees._SCHEDULES`.
2. Edit `logs/pending_orders.json` directly and set **both**
   `fee_schedule_id` (to the chosen schedule's `schedule_id`) and
   `maker_fee_rate` (to that schedule's `maker_rate`, exactly — they are
   cross-checked and must agree) on the affected row.
3. Leave `fee_error_price` untouched. It already holds the price the order
   was blocked at; recovery fills at that price, not at whatever price is
   current when you repair it.
4. Re-run the pipeline for that asset (or wait for the next scheduler tick).
   `check_and_fill()` re-checks every `FEE_ERROR` row on each cycle; once the
   stamp resolves cleanly, it flips to `FILLED` and
   `pipeline.runner._check_pending_fills()` opens the position at
   `fee_error_price` on the very next call.

## What not to do

- Do not set `fee_error_price` to a value other than what the order was
  actually blocked at. That would silently change the position's quantity,
  stops, target, and P&L for a fill that already happened.
- Do not "fix" a stuck `FEE_ERROR` by cancelling or deleting the row to clear
  the new-entry block. That discards a real, observed fill with no P&L ever
  recorded for it. There is deliberately no automatic expiry, auto-cancel, or
  clearing tool for `FEE_ERROR` — every repair is an explicit, auditable edit
  to the stamp fields, made by someone who can state which schedule applies
  and why.
- Do not back-fill `fee_schedule_id` from `maker_fee_rate` (or vice versa) by
  guesswork. If you are not sure which schedule applies, that uncertainty is
  the point of `FEE_ERROR` — leave it blocked and investigate further rather
  than picking one.
