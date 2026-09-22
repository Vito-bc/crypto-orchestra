"""
Single source for PROSPECTIVE paper/shadow execution fee accounting.

Why this module exists: maker/taker rates were spelled independently in
`position_tracker.py` and `limit_orders.py`, and both had drifted away from the
fee tier the account is actually on. `close_position()` also recomputed the
ENTRY fee from whatever the module constant said at CLOSE time, so changing a
rate silently restated trades that had already been opened under the old one.
The rates now have one definition, and a trade carries the schedule it was
opened under.

Same shape as `pipeline.sizing`, and for the same reason: a number that decides
money must not be duplicated at several call sites.

THE BOUNDARY IS PER ORDER, NOT PER POSITION
-------------------------------------------
Coinbase prices each ORDER at the tier in force when that order is placed. A
position is two separate orders — a resting limit entry and, days later, a
market exit — so it can straddle a tier change and settle its two legs under
two different schedules.

An earlier version of this module stamped BOTH rates when the entry filled and
settled the exit from the entry's schedule. That is wrong in both directions:
a position opened before a tier change would have paid its exit at the old
rate (undercharging a market order that is actually priced at today's tier),
and a limit order placed before a change but filled after would have taken its
entry rate from the fill rather than the placement.

So:

  entry leg  stamped on the PendingOrder at PLACEMENT, carried into the
             Position at fill, and never recomputed afterwards;
  exit  leg  resolved from `active_schedule()` at the moment the exit order
             is sent, because that is a NEW order at today's tier.

Preserving an entry fee is history. Pricing a future exit at a historical rate
is not preservation — it is a different error wearing the same clothes.

THESE ARE NOT COINBASE CONSTANTS
--------------------------------
`CURRENT_SCHEDULE` is a MEASURED SNAPSHOT of one account's tier, taken in
September 2026 and audited. Coinbase tiers move with trailing volume, and the
tier names themselves have changed over the years. This value is therefore:

  - current, not permanent — it may change and the snapshot must be re-taken;
  - account-specific, not a published schedule;
  - PROSPECTIVE ONLY. It says nothing about what any account paid in
    2020-2026, and must never be back-projected into historical research.

Historical research fee assumptions live in `backtesting/signal_scanner.py`
(`_ENTRY_FEE` / `_TP_FEE` / `_SL_FEE`) and are deliberately untouched by this
module. Whether those historical assumptions were correct is a separate,
unstarted research question.

WHY A FROZEN SNAPSHOT AND NOT A LIVE API READ
---------------------------------------------
The view-only fee credential in `backtesting/stf_cost_probe.py` can read the
tier, but wiring it into per-trade accounting would make every paper-trade
calculation depend on an external service being up. An outage would then have
to either block accounting or fall back to a rate — and a fallback is exactly
the silent undercharge this module exists to prevent. A frozen snapshot with
explicit provenance and an effective date is the safer design: it is offline,
deterministic, reviewable in a diff, and it cannot quietly get cheaper.

Re-measuring is a deliberate act: run the probe, audit the reading, add a new
schedule with a new id and effective date, and point `CURRENT_SCHEDULE` at it.
The old schedule stays in the registry so trades opened under it keep
reconciling.

SCHEDULE HISTORY
-----------------
Each tier change gets its own dated, named constant, in adoption order:

  LEGACY_SCHEDULE          modeled, pre-2026-09-15, never verified
  SCHEDULE_INTRO_1_2026_09 measured, Intro 1, 0.6%/1.2% -- adopted 2026-09-15,
                            superseded 2026-09-22
  CURRENT_SCHEDULE          measured, Intro, 0.5%/0.9% -- adopted 2026-09-22

Every one of them stays in `_SCHEDULES` forever. `CURRENT_SCHEDULE` is a name
that moves; a `schedule_id` never does.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Optional

MAKER = "maker"
TAKER = "taker"

# A rate above this is a parsing accident, not a fee schedule — 0.6 where 0.006
# was meant. The highest Coinbase Advanced taker tier is far below 10%.
MAX_PLAUSIBLE_RATE = 0.10


class FeeConfigurationError(RuntimeError):
    """The operational fee schedule could not be determined. Never defaulted."""


def _validated_rate(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FeeConfigurationError(f"{field} is {value!r}, not a number")
    rate = float(value)
    if not isfinite(rate) or rate < 0.0 or rate > MAX_PLAUSIBLE_RATE:
        raise FeeConfigurationError(
            f"{field}={rate!r} is outside [0, {MAX_PLAUSIBLE_RATE}] — a decimal "
            "fraction is expected (0.006 means 0.6%), so this looks like a "
            "percent/fraction confusion")
    return rate


@dataclass(frozen=True)
class FeeSchedule:
    """One account fee schedule, with the provenance that makes it auditable."""

    schedule_id: str
    tier_name: str
    maker_rate: float
    taker_rate: float
    # A SELECTED ACCOUNTING TIMESTAMP the schedule is dated to for record-
    # keeping — e.g. "the day the measurement was confirmed" — not the
    # instant this code went live and not a live cutover. active_schedule()
    # never reads this field: it always returns CURRENT_SCHEDULE the moment
    # the code runs, so paper accounting never depends on wall-clock time.
    # Do not make it load-bearing in rate resolution for that reason.
    effective_from: Optional[str]
    source: str

    def __post_init__(self) -> None:
        _validated_rate(self.maker_rate, f"{self.schedule_id}.maker_rate")
        _validated_rate(self.taker_rate, f"{self.schedule_id}.taker_rate")
        # Taker is never cheaper than maker on Coinbase spot. If it is here,
        # the two have been transposed, and every exit would be undercharged.
        if self.taker_rate < self.maker_rate:
            raise FeeConfigurationError(
                f"{self.schedule_id}: taker_rate {self.taker_rate} is below "
                f"maker_rate {self.maker_rate} — the roles look transposed")

    def rate_for(self, role: str) -> float:
        """
        The rate for one execution role.

        An unrecognised role raises rather than picking a side: defaulting
        would silently charge the maker rate for a taker execution, which is
        the undercharge this module exists to prevent.
        """
        if role == MAKER:
            return self.maker_rate
        if role == TAKER:
            return self.taker_rate
        raise FeeConfigurationError(
            f"unknown execution role {role!r} — expected {MAKER!r} or {TAKER!r}")


# The September 2026 reading, superseded 2026-09-22. Tier "Intro 1", observed
# with the same rates on four separate dates (2026-09-11, 09-12, 09-14, 09-15)
# through a key-verified view-only credential, with no tier change across the
# cohort, and independently audited (docs/operations/fee_tier_2026-09-15.json).
# No longer what active_schedule() returns — kept so trades stamped under it
# keep reconciling under it.
SCHEDULE_INTRO_1_2026_09 = FeeSchedule(
    schedule_id="coinbase-intro-1-2026-09",
    tier_name="Intro 1",
    maker_rate=0.006,
    taker_rate=0.012,
    # A SELECTED accounting-boundary timestamp (00:00 UTC on the day the
    # measurement cohort completed) — NOT the instant this schedule went
    # operationally live, and it is not consulted by active_schedule() (see
    # the field comment on FeeSchedule). It even precedes the last supporting
    # reading, 2026-09-15T00:05:01Z, by five minutes: it marks the day the
    # tier was confirmed, not a to-the-minute cutover. Actual operational
    # adoption is the date this change merged to main — see the PR/commit
    # history for that date, not this field.
    effective_from="2026-09-15T00:00:00+00:00",
    # Points at the TRACKED evidence file. The probe's own report lives under
    # logs/, which .gitignore excludes, so naming it alone would leave a fresh
    # checkout unable to inspect the evidence behind a money constant. The
    # digest below pins that report for anyone who does have it.
    source=("docs/operations/fee_tier_2026-09-15.json — measured account tier, "
            "4 key-verified readings 2026-09-11..2026-09-15, "
            "tier_changed_during_the_probe=false; source report "
            "logs/stf_cost_report_2026-09-15.json sha256 "
            "7d7f203a2aae767fd2535ae4959d30df8daa2688bf343a7b1e1a905eb7020b02"),
)

# The September 2026 reading, adopted 2026-09-22. Tier "Intro", observed with
# the same rates on four dates (2026-09-17, 09-18, 09-19, 09-22) through the
# same key-verified view-only credential. 09-20 and 09-21 have no reading —
# lost to the daily probe's scheduled-task WakeToRun failure on both dates —
# but the adoption bar has always been four readings with no tier change
# between them, not four consecutive calendar days: the prior cohort above
# tolerated the same kind of gap on 2026-09-13. See
# docs/operations/fee_tier_2026-09-22.json for the full evidence, including
# why this cohort (unlike the one above) was not independently audited.
CURRENT_SCHEDULE = FeeSchedule(
    schedule_id="coinbase-intro-2026-09-22",
    tier_name="Intro",
    maker_rate=0.005,
    taker_rate=0.009,
    # Same convention as SCHEDULE_INTRO_1_2026_09.effective_from above: a
    # SELECTED accounting-boundary timestamp (00:00 UTC on the day the
    # cohort completed), not a live cutover and not consulted by
    # active_schedule(). It precedes the last supporting reading,
    # 2026-09-22T00:05:02Z, by five minutes. Actual operational adoption is
    # the date this change merges to main — see the PR/commit history for
    # that date, not this field.
    effective_from="2026-09-22T00:00:00+00:00",
    source=("docs/operations/fee_tier_2026-09-22.json — measured account tier, "
            "4 key-verified readings 2026-09-17, 09-18, 09-19, 09-22 "
            "(09-20/09-21 lost to a scheduled-task WakeToRun failure), "
            "tier_changed_during_the_cohort=false; source report "
            "logs/stf_cost_report_2026-09-22.json sha256 "
            "9be3873301e456626f6b07eea6ec15dce0f70de527e66c2fd4641de39fe39158"),
)

# What the operational paths charged BEFORE any measured schedule was adopted.
# It is kept so that trades opened under it keep reconciling under it. It is a
# historical operational assumption, not a measurement, and not a claim about
# what Coinbase actually charged at the time.
LEGACY_SCHEDULE = FeeSchedule(
    schedule_id="modeled-advanced-base-pre-2026-09-15",
    tier_name="Advanced base tier (modeled, unverified)",
    maker_rate=0.004,
    taker_rate=0.006,
    effective_from=None,
    source=("modeled assumption used by paper accounting before "
            "2026-09-15; never verified against the account"),
)

_SCHEDULES = {s.schedule_id: s for s in
              (CURRENT_SCHEDULE, SCHEDULE_INTRO_1_2026_09, LEGACY_SCHEDULE)}


def active_schedule() -> FeeSchedule:
    """The schedule a NEW paper/shadow execution is priced under."""
    return CURRENT_SCHEDULE


def schedule_by_id(schedule_id: str) -> FeeSchedule:
    """
    Look up a recorded schedule. An unknown id raises.

    Falling back to a default here would let an unrecognised record be priced
    under whatever happens to be current — restating history in one direction
    or undercharging in the other.
    """
    try:
        return _SCHEDULES[schedule_id]
    except KeyError:
        raise FeeConfigurationError(
            f"unknown fee schedule id {schedule_id!r}; known ids: "
            f"{sorted(_SCHEDULES)}") from None


def entry_rate_for_record(
    fee_schedule_id: Optional[str] = None,
    maker_fee_rate: Optional[float] = None,
) -> float:
    """
    The maker rate the ENTRY order was placed under.

    One leg, one rate. The exit is a separate order placed later and is priced
    by `active_schedule()` at the moment it is sent — deliberately not
    resolvable from here, so no caller can settle an exit at a historical tier.

    This function never infers a schedule from anything but an explicit,
    registered `fee_schedule_id` — not from an absent id, not from a rate
    that happens to look familiar, not from a calendar date. There is no
    load-time migration that stamps old records with a guessed schedule: a
    record that predates fee stamping simply has no id, and reaching this
    function without one is refused, exactly like corruption. (An operator
    who is certain a specific historical record should be treated as
    LEGACY_SCHEDULE can say so explicitly by writing that schedule_id onto
    the record — a deliberate, auditable edit, not something this module
    infers on its behalf.)

    Resolution:

      1. No `fee_schedule_id` at all is refused outright. There is no rate
         that can be assumed safe for an unstamped record.
      2. A stamped id must EXIST and, if a rate is also present, must AGREE
         with it. An unknown id, or an id whose schedule contradicts the rate
         beside it, raises rather than picking one of the two.

    Note what is absent: no branch returns the ACTIVE maker rate. A recorded
    entry is never repriced at today's tier.
    """
    if fee_schedule_id is None:
        raise FeeConfigurationError(
            "record has no fee_schedule_id — no rate can be assumed safe to "
            "charge it. This is never defaulted or inferred from an absent "
            "field, a familiar-looking rate, or a calendar date; a record "
            "reaching this function without a registered schedule id was "
            "either never stamped or has been corrupted")

    schedule = schedule_by_id(fee_schedule_id)

    if maker_fee_rate is None:
        return schedule.maker_rate

    rate = _validated_rate(maker_fee_rate, "record maker_fee_rate")
    if schedule.maker_rate != rate:
        raise FeeConfigurationError(
            f"record stamps fee_schedule_id={fee_schedule_id!r} (maker "
            f"{schedule.maker_rate}) but maker_fee_rate={rate}; the record "
            "contradicts itself and cannot be settled without choosing one")
    return rate
