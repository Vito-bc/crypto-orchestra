"""Credential-free hourly CFM funding monitor.

This instrument records public funding snapshots for the two CFM perpetual
products scoped in ``docs/research/2026-09-19-carry-scoping.md``.  It makes no
trading or carry decision.  A missing observation is never filled or averaged
through.

TWO COVERAGE QUESTIONS, TWO CHECKS
----------------------------------
The reported statistic is a **trailing seven-day mean**, and the thresholds it
is compared against (14.8%/yr BTC, 14.4%/yr ETH) were derived for a seven-day
mean in the carry-scoping document.  A mean over whatever happens to exist is
a different statistic wearing the same name.

The monitor originally checked coverage over the last 24 hours only, and then
averaged every observation inside the seven-day window however few there were.
Three days after it was built that produced a "trailing 7-day mean" computed
from 43 observations — 1.8 days — and compared it against a seven-day
threshold.  BTC read "above threshold" on 24 consecutive records on that
basis.  The 24-hour check had passed, because it answers a different
question.

So there are two independent requirements, and the mean is reported only when
BOTH are met:

  FRESHNESS   at least ``MIN_COVERAGE`` of the last 24 hourly slots.  Answers
              "is this monitor currently running?"  A stale mean over a fully
              covered week would otherwise pass silently.
  DEPTH       at least ``MIN_TRAILING_COVERAGE`` of the 168 hourly slots in
              the trailing seven days.  Answers "is this actually a seven-day
              mean?"  A freshly started monitor would otherwise pass its
              second day.

Failing either yields ``trailing_7d_status = "UNAVAILABLE"`` and a ``None``
mean — the same fail-closed contract the 24-hour check always used.  Every
record carries both denominators so a reader never has to infer which check
was the binding one.

A CROSSING IS A NOTIFICATION, NOT THE CONDITION
-----------------------------------------------
CLAUDE.md records the condition that would reopen the carry line as funding
"sustained above break-even for longer than a typical cycle" — the median
cycle length from the carry document, 17.0 days (BTC) and 21.3 days (ETH).
A single threshold crossing is nothing of the sort.  The condition is
recorded, per product, on every record and in every alert so the distinction
cannot be lost between here and the decision.  This module does not evaluate
that condition, and it decides nothing either way.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from math import ceil, isfinite
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.stf_cost_probe import _public_client  # noqa: E402
from notifications.telegram import send_telegram_message  # noqa: E402

OBSERVATIONS = ROOT / "logs" / "cfm_funding.jsonl"

# `typical_cycle_days` is the MEDIAN cycle length from
# `docs/research/2026-09-19-carry-scoping.md` §5 (BTC 17.0 d against a mean of
# 54.8; ETH 21.3 d against 66.5). It is carried here only to state the
# recorded re-evaluation condition on every record; nothing computes with it.
PRODUCTS = {
    "BIP-20DEC30-CDE": {
        "asset": "BTC",
        "realised_threshold": 0.148,
        "typical_cycle_threshold": 0.457,
        "typical_cycle_days": 17.0,
    },
    "ETP-20DEC30-CDE": {
        "asset": "ETH",
        "realised_threshold": 0.144,
        "typical_cycle_threshold": 0.387,
        "typical_cycle_days": 21.3,
    },
}

SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60
EXPECTED_FUNDING_INTERVAL_SECONDS = 3600.0
TRAILING_WINDOW = timedelta(days=7)
COVERAGE_WINDOW = timedelta(hours=24)

# FRESHNESS — "is the monitor running now?"  20 of the last 24 hourly slots.
MIN_COVERAGE = 20

# DEPTH — "is this a seven-day mean?"  The trailing window holds 168 hourly
# slots, and at least 80% of them must be present.
#
# Why 80%, and why declared rather than tuned: the figure is a convention, not
# an estimate, and no alternative was evaluated. It tolerates a gap the size
# of a Windows reboot plus a patch cycle (up to 33 missing hours spread across
# the week) without pretending that a monitor which has run for two days has
# produced a seven-day mean. Loosening it to admit the current 44-observation
# state would defeat the point of the check; tightening it to 100% would make
# one lost hour blind the monitor for a week, and the carry document is
# explicit that lost hours are permanent — CFM has no funding-history
# endpoint, so nothing can be backfilled.
#
# Consequence to expect, not a bug: from a cold start the mean is UNAVAILABLE
# for the first ~5.6 days of continuous operation.
TRAILING_SLOTS = 168
TRAILING_COVERAGE_FRACTION = 0.80
MIN_TRAILING_COVERAGE = ceil(TRAILING_SLOTS * TRAILING_COVERAGE_FRACTION)  # 135

# The recorded condition for re-evaluating carry, from CLAUDE.md's "Research
# program status" section. Stated on every record and in every alert so a
# crossing is never read as the condition itself.
REEVALUATION_CONDITION = (
    "CFM funding sustained above the realised-rate break-even for longer than "
    "a typical cycle (median cycle length, carry scoping §5). A threshold "
    "crossing is a notification, not this condition. This monitor does not "
    "evaluate it and decides nothing."
)


class FundingMonitorError(RuntimeError):
    """The monitor could not produce a trustworthy public observation."""


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _utc_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise FundingMonitorError(f"{field} is not a timestamp: {value!r}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FundingMonitorError(f"{field} is not a timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise FundingMonitorError(f"{field} has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _iso_utc(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise FundingMonitorError(f"{field} is not numeric: {value!r}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise FundingMonitorError(f"{field} is not numeric: {value!r}") from exc
    if not isfinite(number):
        raise FundingMonitorError(f"{field} is not finite: {value!r}")
    return number


def funding_interval_seconds(value: Any) -> float:
    """Parse the API's protobuf-duration spelling, currently ``3600s``."""
    if not isinstance(value, str):
        raise FundingMonitorError(f"funding_interval is not a duration: {value!r}")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)s", value)
    if not match:
        raise FundingMonitorError(f"funding_interval is not a duration: {value!r}")
    seconds = float(match.group(1))
    if not isfinite(seconds) or seconds <= 0:
        raise FundingMonitorError(f"funding_interval is not positive: {value!r}")
    return seconds


def annualise(rate: float, funding_interval: float) -> float:
    """Return an annual decimal rate from one funding interval's decimal rate."""
    rate = _finite_number(rate, "funding_rate")
    interval = _finite_number(funding_interval, "funding_interval_seconds")
    if interval <= 0:
        raise FundingMonitorError("funding_interval_seconds must be positive")
    return rate * (SECONDS_PER_YEAR / interval)


def parse_products(payload: Any, observed_at: datetime | None = None) -> list[dict]:
    """Extract the two target products from either SDK objects or dictionaries."""
    products = _field(payload, "products")
    if not isinstance(products, list):
        raise FundingMonitorError("public-products response has no products list")

    seen: dict[str, dict] = {}
    observed = observed_at or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise FundingMonitorError("observed_at must include a timezone")

    for product in products:
        product_id = _field(product, "product_id")
        if product_id not in PRODUCTS:
            continue
        if product_id in seen:
            raise FundingMonitorError(f"duplicate target product in response: {product_id}")

        details = _field(product, "future_product_details")
        rate = _finite_number(_field(details, "funding_rate"), "funding_rate")
        raw_interval = _field(details, "funding_interval")
        interval = funding_interval_seconds(raw_interval)
        if interval != EXPECTED_FUNDING_INTERVAL_SECONDS:
            raise FundingMonitorError(
                f"{product_id} funding_interval changed from hourly: {raw_interval!r}")
        funding_time = _utc_timestamp(_field(details, "funding_time"), "funding_time")
        seen[product_id] = {
            "observed_at": _iso_utc(observed),
            "product": product_id,
            "asset": PRODUCTS[product_id]["asset"],
            "funding_rate": rate,
            "funding_time": _iso_utc(funding_time),
            "funding_interval": raw_interval,
            "funding_interval_seconds": interval,
            "annualized_rate": round(annualise(rate, interval), 12),
        }

    missing = sorted(set(PRODUCTS) - set(seen))
    if missing:
        raise FundingMonitorError(f"target products missing from response: {', '.join(missing)}")
    return [seen[product_id] for product_id in PRODUCTS]


def _read_observations(path: Path = OBSERVATIONS) -> list[dict]:
    if not path.exists():
        return []
    observations = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FundingMonitorError(
                f"invalid JSON in {path} line {line_number}: {exc.msg}") from exc
        if not isinstance(value, dict):
            raise FundingMonitorError(f"non-object in {path} line {line_number}")
        observations.append(value)
    return observations


def _product_observations(observations: list[dict], product: str) -> list[dict]:
    unique: dict[str, dict] = {}
    for row in observations:
        if row.get("product") != product:
            continue
        funding_time = _utc_timestamp(row.get("funding_time"), "funding_time")
        key = _iso_utc(funding_time)
        if key in unique:
            raise FundingMonitorError(
                f"duplicate ({product}, {key}) already exists in the observation log")
        _finite_number(row.get("annualized_rate"), "annualized_rate")
        unique[key] = row
    return list(unique.values())


def trailing_status(
    observations: list[dict], product: str, as_of: datetime
) -> dict[str, Any]:
    """
    Compute both coverage checks and, only when both pass, the seven-day mean.

    Freshness (24h) and depth (7d) answer different questions and neither
    substitutes for the other — see the module docstring. A failure of either
    reports ``UNAVAILABLE`` with a ``None`` mean and names itself in
    ``unavailable_reasons``, so a reader never has to work out which check was
    binding.
    """
    if as_of.tzinfo is None:
        raise FundingMonitorError("as_of must include a timezone")
    as_of = as_of.astimezone(timezone.utc)
    rows = _product_observations(observations, product)

    recent_day = []
    trailing = []
    for row in rows:
        moment = _utc_timestamp(row["funding_time"], "funding_time")
        if as_of - COVERAGE_WINDOW < moment <= as_of:
            recent_day.append(row)
        if as_of - TRAILING_WINDOW < moment <= as_of:
            trailing.append(row)

    coverage = len(recent_day)
    depth = len(trailing)
    fresh = coverage >= MIN_COVERAGE
    deep = depth >= MIN_TRAILING_COVERAGE

    reasons = []
    if not fresh:
        reasons.append(
            f"freshness: {coverage}/{TRAILING_SLOTS // 7} hourly observations "
            f"in the last 24h, minimum {MIN_COVERAGE}")
    if not deep:
        reasons.append(
            f"depth: {depth}/{TRAILING_SLOTS} hourly observations in the "
            f"trailing 7 days, minimum {MIN_TRAILING_COVERAGE} "
            f"({TRAILING_COVERAGE_FRACTION:.0%}) — a mean over less than that "
            "is not a seven-day mean and is not comparable to a seven-day "
            "threshold")

    available = fresh and deep
    mean = None
    if available:
        # `trailing` is non-empty whenever `deep` holds; MIN_TRAILING_COVERAGE
        # is far above zero, so this cannot divide by zero.
        mean = round(
            sum(float(row["annualized_rate"]) for row in trailing) / depth, 12)

    return {
        "status": "AVAILABLE" if available else "UNAVAILABLE",
        "coverage_last_24h": coverage,
        "coverage_required": MIN_COVERAGE,
        "freshness_24h_status": "OK" if fresh else "INSUFFICIENT",
        "observations_in_trailing_7d": depth,
        "trailing_7d_slots": TRAILING_SLOTS,
        "trailing_7d_coverage_required": MIN_TRAILING_COVERAGE,
        "trailing_7d_coverage_status": "OK" if deep else "INSUFFICIENT",
        "unavailable_reasons": reasons,
        "mean_annualized_rate": mean,
    }


def _coverage_line(status: dict) -> str:
    """Both denominators, always — either one of them can be the binding check."""
    return (f"Coverage: {status['coverage_last_24h']}/24 in the last 24h "
            f"(minimum {MIN_COVERAGE}), "
            f"{status['observations_in_trailing_7d']}/{TRAILING_SLOTS} in the "
            f"trailing 7 days (minimum {MIN_TRAILING_COVERAGE}).")


def _condition_line(product: str) -> str:
    """The recorded re-evaluation condition, restated wherever a level is."""
    days = PRODUCTS[product]["typical_cycle_days"]
    return (f"Recorded re-evaluation condition: sustained above break-even for "
            f"longer than a typical cycle ({days:.1f} days, median "
            f"{PRODUCTS[product]['asset']} cycle). A crossing is a "
            f"notification, not that condition.")


def _threshold_alerts(product: str, previous: dict, current: dict) -> list[str]:
    asset = PRODUCTS[product]["asset"]
    previous_mean = previous["mean_annualized_rate"]
    current_mean = current["mean_annualized_rate"]
    messages = []

    if current["status"] == "UNAVAILABLE" and previous["status"] != "UNAVAILABLE":
        reasons = "; ".join(current.get("unavailable_reasons") or ["unknown"])
        messages.append(
            f"CFM funding monitor coverage alarm — {asset} ({product})\n"
            f"Trailing 7-day mean: UNAVAILABLE ({reasons})\n"
            f"{_coverage_line(current)}\n"
            "This monitor decides nothing itself.")
    elif current["status"] == "AVAILABLE" and previous["status"] == "UNAVAILABLE":
        messages.append(
            f"CFM funding monitor coverage restored — {asset} ({product})\n"
            f"{_coverage_line(current)}\n"
            f"Trailing 7-day mean: {current_mean * 100:.2f}%/yr.\n"
            "This monitor decides nothing itself.")

    if current_mean is None:
        return messages

    thresholds = (
        ("realised-rate break-even", PRODUCTS[product]["realised_threshold"]),
        ("typical-cycle break-even", PRODUCTS[product]["typical_cycle_threshold"]),
    )
    for label, threshold in thresholds:
        crossed_up = previous_mean is None or previous_mean < threshold <= current_mean
        crossed_down = previous_mean is not None and previous_mean >= threshold > current_mean
        if crossed_up or crossed_down:
            direction = "ABOVE" if crossed_up else "BELOW"
            messages.append(
                f"CFM funding monitor threshold crossing — {asset} ({product})\n"
                f"Trailing 7-day mean: {current_mean * 100:.2f}%/yr, {direction} "
                f"the {label} threshold ({threshold * 100:.1f}%/yr).\n"
                f"{_coverage_line(current)}\n"
                f"{_condition_line(product)}")
    return messages


def _safe_alert(message: str) -> None:
    """Best-effort notification. Observation durability never depends on Telegram."""
    try:
        delivered = send_telegram_message(message)
    except Exception as exc:
        print(f"warning: Telegram alert raised {type(exc).__name__}: {exc}", file=sys.stderr)
        return
    if not delivered:
        print("warning: Telegram alert was not delivered", file=sys.stderr)


def poll(client: Any | None = None) -> list[dict]:
    """Fetch, deduplicate and append one durable line per target product."""
    destination = OBSERVATIONS
    public_client = client or _public_client()
    payload = public_client.get_public_products(product_type="FUTURE")
    parsed = parse_products(payload)
    existing = _read_observations(destination)
    existing_keys = {
        (row.get("product"), _iso_utc(_utc_timestamp(row.get("funding_time"), "funding_time")))
        for row in existing
        if row.get("product") in PRODUCTS
    }

    written = []
    for observation in parsed:
        key = (observation["product"], observation["funding_time"])
        if key in existing_keys:
            continue

        product_rows = existing + [observation]
        as_of = _utc_timestamp(observation["funding_time"], "funding_time")
        current = trailing_status(product_rows, observation["product"], as_of)

        earlier = _product_observations(existing, observation["product"])
        if earlier:
            prior_time = max(
                _utc_timestamp(row["funding_time"], "funding_time") for row in earlier)
            previous = trailing_status(existing, observation["product"], prior_time)
        else:
            previous = {
                "status": "NOT_YET_OBSERVED",
                "coverage_last_24h": 0,
                "coverage_required": MIN_COVERAGE,
                "freshness_24h_status": "INSUFFICIENT",
                "observations_in_trailing_7d": 0,
                "trailing_7d_slots": TRAILING_SLOTS,
                "trailing_7d_coverage_required": MIN_TRAILING_COVERAGE,
                "trailing_7d_coverage_status": "INSUFFICIENT",
                "unavailable_reasons": ["no observation recorded yet"],
                "mean_annualized_rate": None,
            }

        # Both denominators and both verdicts go on the record. A reader of
        # one line must be able to tell a covered mean from an uncovered one
        # without re-deriving either window, and must be able to tell WHICH
        # check was binding.
        observation["trailing_7d_status"] = current["status"]
        observation["coverage_last_24h"] = current["coverage_last_24h"]
        observation["coverage_required"] = current["coverage_required"]
        observation["freshness_24h_status"] = current["freshness_24h_status"]
        observation["observations_in_trailing_7d"] = current[
            "observations_in_trailing_7d"
        ]
        observation["trailing_7d_slots"] = current["trailing_7d_slots"]
        observation["trailing_7d_coverage_required"] = current[
            "trailing_7d_coverage_required"
        ]
        observation["trailing_7d_coverage_status"] = current[
            "trailing_7d_coverage_status"
        ]
        observation["trailing_7d_unavailable_reasons"] = current[
            "unavailable_reasons"
        ]
        observation["trailing_7d_mean_annualized"] = current["mean_annualized_rate"]
        # Declared, not computed: the condition a level crossing does not meet.
        observation["reevaluation_condition"] = REEVALUATION_CONDITION
        observation["reevaluation_sustain_days"] = PRODUCTS[
            observation["product"]
        ]["typical_cycle_days"]

        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(observation, sort_keys=True) + "\n")

        # The durable write is deliberately before every best-effort send.
        for message in _threshold_alerts(observation["product"], previous, current):
            _safe_alert(message)

        existing.append(observation)
        existing_keys.add(key)
        written.append(observation)
    return written


def _display(observation: dict) -> str:
    mean = observation["trailing_7d_mean_annualized"]
    trailing = "UNAVAILABLE" if mean is None else f"{mean * 100:.2f}%/yr"
    return (
        f"recorded {observation['product']} funding_time={observation['funding_time']} "
        f"rate={observation['funding_rate']:.8g} "
        f"annualized={observation['annualized_rate'] * 100:.2f}%/yr "
        f"trailing_7d={trailing} "
        f"coverage_24h={observation['coverage_last_24h']}/24 "
        f"coverage_7d={observation['observations_in_trailing_7d']}"
        f"/{observation['trailing_7d_slots']}"
    )


def main() -> None:
    try:
        observations = poll()
        if not observations:
            print("no new funding observations (product/funding_time already recorded)")
            return
        for observation in observations:
            print(_display(observation))
    except FundingMonitorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except Exception as exc:
        print(f"error: public-products request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
