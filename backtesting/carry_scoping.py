"""
Carry scoping — long spot + short perpetual, as a NON-DIRECTIONAL income source.

WHAT THIS IS
------------
Read-only arithmetic that prices funding carry on THIS account's venue. It
answers four questions and designs a monitor for a fifth:

  1. what a hedged unit earns gross, and what it keeps after this venue's fees;
  2. how much capital one unit ties up, and whether it is expressible here;
  3. how often the short leg would need a margin top-up, from price history;
  4. how far the perp trades from its index, as a caveat with a magnitude;
  5. what funding level would make this worth watching, and what a monitor
     would have to record.

WHAT IT IS NOT
--------------
No strategy is backtested. No parameter is searched. No trial is registered.
It computes no profit factor, expectancy, Sharpe, equity curve, drawdown or
directional return of any kind, and it ranks nothing. The conclusion section of
the accompanying document is titled "numbers", not "verdict" — the owner
decides.

WHY THE FOUR CLOSURES DO NOT COVER THIS, AND WHY THAT IS NOT RE-ARGUED HERE
----------------------------------------------------------------------------
Closures 1-3 and the floor-gate identity bound DIRECTIONAL programs, whose
per-trade variance is the underlying's. A hedged carry position cancels price
exposure; its income is the funding series, which this project has so far
measured only as a long's COST. That is a different variance regime, and this
task takes that as given rather than re-deriving it. What it adds is the price
tag on THIS venue.

THE DECLARED HARVEST RULE — one rule, fixed, no variation
----------------------------------------------------------
Long 1 unit spot + short 1 contract-equivalent perp. Opened when the trailing
7-day mean annualised funding is > 0, held while it stays > 0, closed when it
is <= 0, re-opened on the next crossing.

This is an ACCOUNTING CONVENTION that makes cycle counting deterministic, not
a strategy to optimise. The window is not a parameter to try alternatives for,
and no alternative was tried. `tests/test_carry_scoping.py` asserts there is
exactly one window constant and that the rule reads funding only.

THE BLIND
---------
The harvest rule is a function of the FUNDING SERIES ALONE — it never sees a
price, which is what makes it non-directional by construction rather than by
assertion. Prices enter in exactly two places, both of which emit counts or
scalars and neither of which produces a return:

    margin_event_counts  -> integer counts of threshold breaches (cash-flow
                            risk, NOT P&L — the excursion magnitudes never
                            leave the function)
    basis_stats          -> two scalars in basis points

VENUE MISMATCH
--------------
Funding levels, price history and the premium index are BINANCE USDT-margined
perps, used as proxy history for Coinbase CFM. Binance funds every 8h; CFM
funds hourly. What transfers is the annualised LEVEL, not the payment cadence,
and the basis, leverage population, cap/floor rules and liquidation engine all
differ. CFM has ~14 months of price history and NO funding history at all
(`docs/research/2026-09-18-perps-scoping.md` §4-5), which is why a proxy is
used. Fee rates, contract sizes and margin fractions are CFM's own.

INPUTS
------
4h klines and funding history come from `backtesting/hydrate_perps_proxy.py`,
which arrives with the perps-gate branch (PR #24). The premium index comes from
`backtesting/hydrate_carry_basis.py` in this branch. If the kline or funding
files are absent this module fails closed with that instruction rather than
computing on partial inputs.

USAGE
    python backtesting/carry_scoping.py            # print the report, write CSVs
    python backtesting/carry_scoping.py --verify   # recompute, compare, write nothing
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.hydrate_carry_basis import (  # noqa: E402
    DATA_DIR,
    HydrationError,
)
from backtesting.hydrate_carry_basis import digests as basis_digests  # noqa: E402
from pipeline.fees import CURRENT_SCHEDULE  # noqa: E402

# ── Declared constants. None is searched, tuned or varied. ──────────────────

SYMBOLS = ("BTCUSDT", "ETHUSDT")
BAR = "4h"
BARS_PER_DAY = 6
RUN_DATE = "2026-09-19"

ANALYSIS_START = pd.Timestamp("2020-01-01", tz="UTC")
ANALYSIS_END = pd.Timestamp("2026-09-18", tz="UTC")     # exclusive
FUNDING_END = pd.Timestamp("2026-09-01", tz="UTC")      # exclusive; see module doc

# THE declared harvest window. One constant, one value. A test asserts that no
# second window constant exists, because a second one is the beginning of a
# search this task exists to avoid.
HARVEST_WINDOW_DAYS = 7

# Coinbase CFM product facts, from `docs/research/2026-09-18-perps-scoping.md`
# §2-3 (public product records and an authenticated read of this account on
# 2026-09-18). Notionals are that date's snapshot and move with spot.
CFM_PRODUCTS = {
    "BTCUSDT": {
        "product_id": "BIP-20DEC30-CDE",
        "contract_size": 0.01,           # BTC per contract
        "unit_label": "0.01 BTC",
        "notional_usd": 808.30,
        "overnight_margin_short": 0.3064,
        "overnight_margin_long": 0.2456,
    },
    "ETHUSDT": {
        "product_id": "ETP-20DEC30-CDE",
        "contract_size": 0.1,            # ETH per contract
        "unit_label": "0.1 ETH",
        "notional_usd": 258.40,
        "overnight_margin_short": 0.3348,
        "overnight_margin_long": 0.2453,
    },
}

# CFM futures fees, authenticated read 2026-09-18. The short leg pays maker on
# the way in and taker on the way out, same as the long leg would.
PERP_MAKER = 0.00095
PERP_TAKER = 0.00100

# Spot schedules. ADOPTED is `pipeline/fees.py` CURRENT_SCHEDULE, which the
# standing policy names as the schedule to price against. CANDIDATE is the
# 2026-09-18 account reading that has NOT cleared its reading cohort and is
# therefore reported alongside, never instead — the same two-scenario treatment
# `docs/research/2026-09-cost-sensitivity.md` uses.
SPOT_SCHEDULES = {
    "adopted": {"maker": CURRENT_SCHEDULE.maker_rate,
                "taker": CURRENT_SCHEDULE.taker_rate,
                "label": f"ADOPTED ({CURRENT_SCHEDULE.tier_name}, pipeline/fees.py)"},
    "candidate": {"maker": 0.005, "taker": 0.009,
                  "label": "CANDIDATE (Intro, read 2026-09-18, not adopted)"},
}

# Declared liquidity reserve, as a fraction of posted short margin. A declared
# convention so the capital figure is not silently optimistic; it is not tuned
# and no alternative was evaluated.
RESERVE_FRACTION_OF_MARGIN = 0.25

# Margin-event geometry. Windows in days; thresholds as fractions of the short
# margin, so 1.0 is the move that consumes the whole posted margin.
MARGIN_WINDOWS_DAYS = (1, 3, 7)
MARGIN_THRESHOLDS = (0.50, 0.75, 1.00)

# Declared top-up friction: a margin event at the 50% threshold is charged one
# SPOT round trip on the amount topped up, which is 50% of the margin fraction
# of notional. Arithmetic and declared. It does not price the directional risk
# of being partially unhedged while the top-up is raised, which is unpriced
# here and noted in the document.
TOPUP_THRESHOLD = 0.50

PORTFOLIO_BUDGETS_USD = (10_000, 20_000)

OUT_DIR = ROOT / "docs" / "research" / "data"
CSV_CARRY = OUT_DIR / f"carry_net_by_year_{RUN_DATE}.csv"
CSV_CAPITAL = OUT_DIR / f"carry_capital_{RUN_DATE}.csv"
CSV_MARGIN = OUT_DIR / f"carry_margin_events_{RUN_DATE}.csv"
CSV_BASIS = OUT_DIR / f"carry_basis_{RUN_DATE}.csv"
CSV_REGIME = OUT_DIR / f"carry_regime_map_{RUN_DATE}.csv"
CSV_DIGESTS = OUT_DIR / f"carry_inputs_{RUN_DATE}.csv"


class CarryError(RuntimeError):
    pass


# ══ LOADERS ══════════════════════════════════════════════════════════════════

def _read_kline_frame(symbol: str, prefix: str, columns: dict[int, str]
                      ) -> pd.DataFrame:
    paths = sorted(DATA_DIR.glob(f"{prefix}_{symbol}_{BAR}_*.csv"))
    if not paths:
        raise CarryError(
            f"no {prefix}_{symbol}_{BAR}_*.csv under {DATA_DIR}. Klines and "
            "funding come from backtesting/hydrate_perps_proxy.py (perps-gate "
            "branch, PR #24); the premium index comes from "
            "backtesting/hydrate_carry_basis.py in this branch.")
    frames = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        frame = pd.read_csv(io.StringIO(text), header=None,
                            usecols=list(columns), names=list(columns.values()))
        frame = frame[pd.to_numeric(frame["open_time"], errors="coerce").notna()]
        frames.append(frame)
    out = pd.concat(frames, ignore_index=True)
    stamps = pd.to_numeric(out["open_time"])
    unit = "us" if stamps.max() > 2_000_000_000_000_0 else "ms"
    out.index = pd.to_datetime(stamps, unit=unit, utc=True)
    out = out.drop(columns=["open_time"]).sort_index()
    out = out[~out.index.duplicated()]
    out = out[(out.index >= ANALYSIS_START) & (out.index < ANALYSIS_END)]
    if out.empty:
        raise CarryError(f"{symbol} {prefix}: no bars inside the declared window")
    return out.apply(pd.to_numeric, errors="coerce").dropna()


def load_prices(symbol: str) -> pd.DataFrame:
    """4h close and high. READS ONLY — no arithmetic on prices happens here."""
    return _read_kline_frame(symbol, "kl", {0: "open_time", 2: "high", 4: "close"})


def load_premium_index(symbol: str) -> pd.Series:
    """
    The perp's published premium index at each 4h close, as a fraction.

    This is the venue's own measure of how far the contract trades from its
    underlying index — the basis quantity, published rather than reconstructed.
    """
    return _read_kline_frame(symbol, "prem", {0: "open_time", 4: "close"})["close"]


def load_funding(symbol: str) -> pd.DataFrame:
    """Published funding history: timestamp, interval hours, realised rate."""
    paths = sorted(DATA_DIR.glob(f"fund_{symbol}_*.csv"))
    if not paths:
        raise CarryError(
            f"no funding history for {symbol} under {DATA_DIR} — see "
            "backtesting/hydrate_perps_proxy.py (perps-gate branch, PR #24)")
    frames = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        frame = pd.read_csv(io.StringIO(text))
        if "calc_time" not in frame.columns:
            frame = pd.read_csv(io.StringIO(text), header=None,
                                names=["calc_time", "funding_interval_hours",
                                       "last_funding_rate"])
        frames.append(frame)
    out = pd.concat(frames, ignore_index=True)
    out = out[pd.to_numeric(out["calc_time"], errors="coerce").notna()]
    out["ts"] = pd.to_datetime(pd.to_numeric(out["calc_time"]), unit="ms", utc=True)
    out["rate"] = pd.to_numeric(out["last_funding_rate"], errors="coerce")
    out["interval_h"] = pd.to_numeric(out["funding_interval_hours"],
                                      errors="coerce").fillna(8.0)
    out = out.dropna(subset=["rate"]).sort_values("ts").drop_duplicates("ts")
    out = out[(out["ts"] >= ANALYSIS_START) & (out["ts"] < FUNDING_END)]
    if out.empty:
        raise CarryError(f"{symbol}: no funding events inside the declared window")
    out["annualised_pct"] = out["rate"] * (24.0 / out["interval_h"]) * 365.0 * 100.0
    return out[["ts", "rate", "interval_h", "annualised_pct"]].reset_index(drop=True)


# ══ THE DECLARED HARVEST RULE ════════════════════════════════════════════════
# A function of the FUNDING SERIES ALONE. It takes no price and can see none,
# which is what makes the position non-directional by construction.

def trailing_mean_annualised(funding: pd.DataFrame) -> pd.Series:
    """
    Trailing HARVEST_WINDOW_DAYS mean of the annualised funding rate, in
    percent, indexed by funding event. Time-based and trailing-inclusive: the
    value at event i averages every event in (t_i - window, t_i].
    """
    series = pd.Series(funding["annualised_pct"].to_numpy(),
                       index=pd.DatetimeIndex(funding["ts"]))
    return series.rolling(f"{HARVEST_WINDOW_DAYS}D").mean()


def harvest_flags(funding: pd.DataFrame) -> pd.Series:
    """
    The declared rule, as a per-funding-event occupancy flag in {0, 1}.

    1 means a hedged unit is open across that funding event and the short leg
    is settled at that event's rate. The signal computed at event i governs
    holding from event i+1 onward, so a rate is never harvested on the strength
    of knowing it — the shift is the whole of the look-ahead discipline here.

    THE INPUT IS THE CONSTRAINT: this function receives a funding frame and
    nothing else. There is no price in scope, so the rule cannot be
    directional, and `tests/test_carry_scoping.py` asserts it structurally.
    """
    signal = (trailing_mean_annualised(funding) > 0.0).astype("int8")
    return signal.shift(1).fillna(0).astype("int8")


def cycles(flags: pd.Series) -> list[dict]:
    """
    Open/close cycles under the declared rule, as timestamps and lengths.

    Returns the opening and closing event times and the length in days (the
    fields are `opened_at` and `closed_at` — deliberately not "open"/"close",
    which in a market module read as prices). No
    rate and no price crosses this boundary — a caller gets a calendar, which
    is all cycle counting needs.
    """
    values = flags.to_numpy()
    index = flags.index
    out: list[dict] = []
    start = None
    for i, v in enumerate(values):
        if v == 1 and start is None:
            start = i
        elif v == 0 and start is not None:
            out.append({"opened_at": index[start], "closed_at": index[i],
                        "days": (index[i] - index[start]).total_seconds() / 86400.0})
            start = None
    if start is not None:
        out.append({"opened_at": index[start], "closed_at": index[-1],
                    "days": (index[-1] - index[start]).total_seconds() / 86400.0,
                    "open_at_window_end": True})
    return out


# ══ COST ARITHMETIC ══════════════════════════════════════════════════════════

def spot_round_trip(schedule: dict) -> float:
    """
    Fees paid on a spot round trip, as a fraction of notional.

    Entry fee on notional at the maker rate, exit fee on proceeds at the taker
    rate, as `pipeline/fees.py` prices the two legs. The position is hedged, so
    proceeds equal notional to first order and the round trip is maker + taker.
    """
    return schedule["maker"] + schedule["taker"]


def perp_round_trip() -> float:
    """Fees on the short perp leg's round trip, as a fraction of notional."""
    return PERP_MAKER + PERP_TAKER


def cycle_cost(schedule: dict) -> float:
    """Both legs, one open and one close, as a fraction of spot notional."""
    return spot_round_trip(schedule) + perp_round_trip()


def committed_capital_multiple(symbol: str) -> float:
    """
    Capital tied up per unit of spot notional: the spot itself, the short leg's
    overnight margin, and the declared liquidity reserve on top of that margin.
    """
    margin = CFM_PRODUCTS[symbol]["overnight_margin_short"]
    return 1.0 + margin + RESERVE_FRACTION_OF_MARGIN * margin


# ══ DELIVERABLE 3: margin events — CASH-FLOW RISK, NOT P&L ══════════════════

def margin_event_counts(prices: pd.DataFrame, margin_fraction: float,
                        ) -> list[dict]:
    """
    How often the short leg's adverse excursion would demand a top-up.

    For every 4h bar treated as an entry at its close, the adverse excursion is
    the highest HIGH over the following window divided by that close. A short
    loses when price rises, so this is the loss side. It is counted against
    {50%, 75%, 100%} of the posted short margin.

    THE RETURN TYPE IS THE CONSTRAINT: integer counts and fractions of bars.
    The excursion magnitudes are local to this function and never leave it, so
    nothing downstream can assemble them into a return series. What is counted
    is a CASH-FLOW event — a top-up would be demanded, or the position would be
    liquidated without one — not a profit or a loss. The hedge means the spot
    leg gains what the short leg loses; that is why this is a funding and
    settlement question rather than a P&L one.

    TWO COUNTS, because one alone misleads. `breach_bars` treats every bar as
    an entry, so consecutive bars describe the SAME rally from different start
    points — it is a per-entry PROBABILITY, not a count of events. `episodes`
    walks forward and skips a full window after each breach, so one rally is
    counted once; it is the count the cost arithmetic uses. Reporting only the
    first would overstate event frequency by up to the window length in bars.
    """
    close = prices["close"].to_numpy()
    high = prices["high"].to_numpy()
    index = prices.index
    years = sorted({int(y) for y in index.year})
    rows: list[dict] = []

    for window_days in MARGIN_WINDOWS_DAYS:
        span = window_days * BARS_PER_DAY
        # Forward max high over the next `span` bars, excluding the entry bar.
        forward_max = (pd.Series(high, index=index)
                       .iloc[::-1].rolling(span, min_periods=1).max()
                       .iloc[::-1].shift(-1).to_numpy())
        excursion = np.divide(forward_max, close,
                              out=np.full_like(close, np.nan),
                              where=close > 0) - 1.0
        for threshold in MARGIN_THRESHOLDS:
            level = threshold * margin_fraction
            breach = excursion > level
            valid = ~np.isnan(excursion)
            # Non-overlapping episodes: after a breach, skip the whole window
            # before another may be counted, so one rally counts once.
            episode_at = np.zeros(len(breach), dtype=bool)
            i = 0
            while i < len(breach):
                if valid[i] and breach[i]:
                    episode_at[i] = True
                    i += span
                else:
                    i += 1
            for year in years:
                mask = (index.year == year) & valid
                n = int(mask.sum())
                rows.append({
                    "year": year,
                    "window_days": window_days,
                    "threshold_of_margin": threshold,
                    "price_move_required_pct": round(level * 100.0, 4),
                    "entry_bars": n,
                    "breach_bars": int((breach & mask).sum()),
                    "breach_fraction": (round(float((breach & mask).sum()) / n, 5)
                                        if n else None),
                    "episodes": int((episode_at & mask).sum()),
                })
            n_all = int(valid.sum())
            rows.append({
                "year": "full_window",
                "window_days": window_days,
                "threshold_of_margin": threshold,
                "price_move_required_pct": round(level * 100.0, 4),
                "entry_bars": n_all,
                "breach_bars": int((breach & valid).sum()),
                "breach_fraction": (round(float((breach & valid).sum()) / n_all, 5)
                                    if n_all else None),
                "episodes": int((episode_at & valid).sum()),
            })
    return rows


def topup_friction_pct_yr(margin_rows: list[dict], margin_fraction: float,
                          schedule: dict, years: float) -> float:
    """
    Declared allowance for margin-event cost, in %/yr of notional.

    Convention, declared and not tuned: each non-overlapping EPISODE in which
    the adverse move exceeds the 50%-of-margin level within 7 days is charged
    ONE SPOT ROUND TRIP on the amount topped up, which is 50% of the margin
    fraction of notional. Episodes, not breach bars: counting bars would credit
    one rally up to 42 times.

    It does NOT price the directional risk of running partially unhedged while
    cash is raised. That is unpriced here and named in the document.
    """
    row = next((r for r in margin_rows
                if r["year"] == "full_window"
                and r["window_days"] == max(MARGIN_WINDOWS_DAYS)
                and r["threshold_of_margin"] == TOPUP_THRESHOLD), None)
    if row is None or not years:
        return 0.0
    events_per_year = row["episodes"] / years
    cost_per_event = TOPUP_THRESHOLD * margin_fraction * spot_round_trip(schedule)
    return events_per_year * cost_per_event * 100.0


# ══ DELIVERABLE 4: basis, as a caveat with a magnitude ══════════════════════

def basis_stats(premium: pd.Series) -> dict:
    """
    Dispersion of the published premium index, in basis points.

    THE RETURN TYPE IS THE CONSTRAINT: two scalars and a count. No basis series
    and no per-period value leaves this function, so no basis P&L can be built
    from it — which is deliberate, because this task prices basis as a caveat
    with a magnitude and does not model carrying it.
    """
    bps = premium * 10_000.0
    daily = bps.diff(BARS_PER_DAY).dropna()
    return {
        "observations": int(len(bps)),
        "sd_bps": round(float(bps.std(ddof=1)), 2),
        "mean_bps": round(float(bps.mean()), 2),
        "worst_1d_change_bps": round(float(daily.abs().max()), 2),
        "p99_abs_1d_change_bps": round(float(daily.abs().quantile(0.99)), 2),
    }


# ══ ASSEMBLY ════════════════════════════════════════════════════════════════

def _year_coverage_days(stamps: pd.Series) -> float:
    return float((stamps.iloc[-1] - stamps.iloc[0]).total_seconds() / 86400.0) + 1.0


def analyse_symbol(symbol: str, schedule_key: str) -> dict:
    schedule = SPOT_SCHEDULES[schedule_key]
    funding = load_funding(symbol)
    prices = load_prices(symbol)
    flags = harvest_flags(funding)
    held = flags.to_numpy().astype(bool)
    rates = funding["rate"].to_numpy()
    stamps = pd.DatetimeIndex(funding["ts"])

    all_cycles = cycles(flags)
    cost_per_cycle = cycle_cost(schedule) * 100.0
    capital_multiple = committed_capital_multiple(symbol)

    total_days = _year_coverage_days(funding["ts"])
    total_years = total_days / 365.25

    rows = []
    for year in sorted({int(y) for y in stamps.year}) + ["full_window"]:
        if year == "full_window":
            mask = np.ones(len(rates), dtype=bool)
            days = total_days
        else:
            mask = np.asarray(stamps.year == year)
            days = _year_coverage_days(funding.loc[mask, "ts"])
        scale = 365.25 / days
        gross_sum = float(rates[mask & held].sum()) * 100.0
        always_sum = float(rates[mask].sum()) * 100.0
        in_year = [c for c in all_cycles
                   if year == "full_window" or c["opened_at"].year == year]
        n_cycles = len(in_year)
        median_days = (round(float(np.median([c["days"] for c in in_year])), 2)
                       if in_year else None)
        fee_pct_yr = n_cycles * scale * cost_per_cycle
        gross_pct_yr = gross_sum * scale
        net_pct_yr = gross_pct_yr - fee_pct_yr
        rows.append({
            "symbol": symbol,
            "spot_schedule": schedule_key,
            "year": year,
            "coverage_days": round(days, 1),
            "gross_always_on_pct_yr": round(always_sum * scale, 4),
            "gross_harvest_rule_pct_yr": round(gross_pct_yr, 4),
            "cycles": n_cycles,
            "cycles_per_year": round(n_cycles * scale, 3),
            "median_cycle_days": median_days,
            "fraction_of_events_held": round(float((mask & held).sum() / mask.sum()), 4),
            "fee_pct_yr": round(fee_pct_yr, 4),
            "net_on_notional_pct_yr": round(net_pct_yr, 4),
            "net_on_committed_capital_pct_yr": round(net_pct_yr / capital_multiple, 4),
        })

    margin_rows = margin_event_counts(
        prices, CFM_PRODUCTS[symbol]["overnight_margin_short"])
    friction = topup_friction_pct_yr(
        margin_rows, CFM_PRODUCTS[symbol]["overnight_margin_short"],
        schedule, total_years)

    full = rows[-1]
    # TWO break-evens, because the cycle-length distribution is heavily skewed
    # and the two answer different questions.
    #
    #   MEDIAN-CYCLE: what annualised funding a TYPICAL cycle must earn to pay
    #   for its own round trip. It is the conservative reading, and it is the
    #   one the task specifies.
    #
    #   REALISED-RATE: what average funding the program AS RUN must earn, using
    #   the cycles actually observed per year rather than the median length. It
    #   is lower, because a few long cycles carry most of the time in position
    #   while the median cycle is short.
    #
    # Quoting only the first overstates the requirement for a program that
    # spends most of its time inside long cycles; quoting only the second hides
    # how often a short cycle fails to cover its own costs. Both are reported.
    median_cycle = full["median_cycle_days"] or 1.0
    mean_cycle = (round(float(np.mean([c["days"] for c in all_cycles])), 2)
                  if all_cycles else None)
    break_even = cost_per_cycle * (365.25 / median_cycle) + friction
    break_even_realised = full["fee_pct_yr"] + friction

    trailing = trailing_mean_annualised(funding)
    regime = []
    for year in sorted({int(y) for y in stamps.year}) + ["full_window"]:
        sub = (trailing if year == "full_window"
               else trailing[trailing.index.year == year])
        sub = sub.dropna()
        if sub.empty:
            continue
        row = {"symbol": symbol, "spot_schedule": schedule_key, "year": year,
               "observations": int(len(sub)),
               "break_even_median_cycle_pct_yr": round(break_even, 4),
               "break_even_realised_rate_pct_yr": round(break_even_realised, 4)}
        for mult in (1, 2, 3):
            row[f"frac_above_{mult}x_break_even"] = round(
                float((sub > mult * break_even).mean()), 4)
        row["frac_above_realised_break_even"] = round(
            float((sub > break_even_realised).mean()), 4)
        row["frac_above_zero"] = round(float((sub > 0).mean()), 4)
        row["median_trailing_pct_yr"] = round(float(sub.median()), 4)
        regime.append(row)

    return {
        "symbol": symbol,
        "schedule_key": schedule_key,
        "rows": rows,
        "cycles": all_cycles,
        "cost_per_cycle_pct": round(cost_per_cycle, 4),
        "capital_multiple": round(capital_multiple, 4),
        "margin_rows": margin_rows,
        "topup_friction_pct_yr": round(friction, 4),
        "break_even_pct_yr": round(break_even, 4),
        "break_even_realised_pct_yr": round(break_even_realised, 4),
        "median_cycle_days": median_cycle,
        "mean_cycle_days": mean_cycle,
        "regime": regime,
        "total_years": round(total_years, 3),
    }


def capital_rows() -> list[dict]:
    rows = []
    for symbol, spec in CFM_PRODUCTS.items():
        notional = spec["notional_usd"]
        margin = notional * spec["overnight_margin_short"]
        reserve = margin * RESERVE_FRACTION_OF_MARGIN
        total = notional + margin + reserve
        row = {
            "symbol": symbol,
            "product_id": spec["product_id"],
            "unit": spec["unit_label"],
            "spot_notional_usd": round(notional, 2),
            "short_margin_usd": round(margin, 2),
            "reserve_usd": round(reserve, 2),
            "total_per_unit_usd": round(total, 2),
        }
        for budget in PORTFOLIO_BUDGETS_USD:
            row[f"units_at_{budget}"] = int(budget // total)
        row["units_at_live_balance_100"] = int(100 // total)
        rows.append(row)
    return rows


def analyse() -> dict:
    out = {"symbols": {}, "capital": capital_rows(), "basis": {}}
    for schedule_key in SPOT_SCHEDULES:
        for symbol in SYMBOLS:
            out["symbols"][(symbol, schedule_key)] = analyse_symbol(symbol, schedule_key)
    for symbol in SYMBOLS:
        out["basis"][symbol] = basis_stats(load_premium_index(symbol))
    return out


# ══ CSV EMISSION ════════════════════════════════════════════════════════════

def _rows_carry(result: dict) -> list[dict]:
    rows = []
    for key in sorted(result["symbols"], key=lambda k: (k[1], k[0])):
        rows.extend(result["symbols"][key]["rows"])
    return rows


def _rows_capital(result: dict) -> list[dict]:
    return result["capital"]


def _rows_margin(result: dict) -> list[dict]:
    rows = []
    for symbol in SYMBOLS:
        for row in result["symbols"][(symbol, "adopted")]["margin_rows"]:
            rows.append({"symbol": symbol, **row})
    return rows


def _rows_basis(result: dict) -> list[dict]:
    return [{"symbol": s, **result["basis"][s]} for s in SYMBOLS]


def _rows_regime(result: dict) -> list[dict]:
    rows = []
    for key in sorted(result["symbols"], key=lambda k: (k[1], k[0])):
        rows.extend(result["symbols"][key]["regime"])
    return rows


def _write_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()),
                            lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


_EMITTERS = (
    (CSV_CARRY, _rows_carry),
    (CSV_CAPITAL, _rows_capital),
    (CSV_MARGIN, _rows_margin),
    (CSV_BASIS, _rows_basis),
    (CSV_REGIME, _rows_regime),
)


def write_outputs(result: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for path, builder in _EMITTERS:
        path.write_text(_write_csv(builder(result)), encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")
    rows = [{"file": r["file"], "sha256": r["sha256"], "lines": r["lines"]}
            for r in basis_digests()]
    CSV_DIGESTS.write_text(_write_csv(rows), encoding="utf-8")
    print(f"wrote {CSV_DIGESTS.relative_to(ROOT)} ({len(rows)} premium-index files)")


def verify_outputs(result: dict) -> bool:
    ok = True
    for path, builder in _EMITTERS:
        if not path.exists():
            print(f"MISSING {path.relative_to(ROOT)}")
            ok = False
            continue
        if path.read_text(encoding="utf-8") != _write_csv(builder(result)):
            print(f"MISMATCH {path.relative_to(ROOT)}")
            ok = False
        else:
            print(f"ok {path.relative_to(ROOT)}")
    return ok


def _print_report(result: dict) -> None:
    for schedule_key, schedule in SPOT_SCHEDULES.items():
        print(f"\n===== spot schedule: {schedule['label']} "
              f"({schedule['maker']:.3%} / {schedule['taker']:.3%}) =====")
        for symbol in SYMBOLS:
            block = result["symbols"][(symbol, schedule_key)]
            print(f"\n-- {symbol}  cost/cycle {block['cost_per_cycle_pct']:.4f}% "
                  f" capital x{block['capital_multiple']:.4f}"
                  f"  break-even {block['break_even_pct_yr']:.2f}%/yr (median cycle "
                  f"{block['median_cycle_days']:.1f}d) | "
                  f"{block['break_even_realised_pct_yr']:.2f}%/yr (realised rate, "
                  f"mean cycle {block['mean_cycle_days']:.1f}d) | "
                  f"top-up friction {block['topup_friction_pct_yr']:.2f}%/yr")
            print(f"{'year':>12} {'gross':>9} {'cycles':>7} {'fee':>9} "
                  f"{'net/notional':>13} {'net/capital':>12}")
            for row in block["rows"]:
                print(f"{str(row['year']):>12} {row['gross_harvest_rule_pct_yr']:9.3f} "
                      f"{row['cycles']:7d} {row['fee_pct_yr']:9.3f} "
                      f"{row['net_on_notional_pct_yr']:13.3f} "
                      f"{row['net_on_committed_capital_pct_yr']:12.3f}")
    print("\n-- capital per unit --")
    for row in result["capital"]:
        print(f"  {row['product_id']:18s} {row['unit']:9s} "
              f"spot {row['spot_notional_usd']:8.2f} margin {row['short_margin_usd']:7.2f} "
              f"reserve {row['reserve_usd']:6.2f} total {row['total_per_unit_usd']:8.2f} "
              f"| 10k {row['units_at_10000']:3d}  20k {row['units_at_20000']:3d} "
              f" $100 cap {row['units_at_live_balance_100']}")
    print("\n-- basis (published premium index) --")
    for symbol, stats in result["basis"].items():
        print(f"  {symbol:9s} SD {stats['sd_bps']:7.2f} bps  mean {stats['mean_bps']:7.2f} "
              f" worst 1d change {stats['worst_1d_change_bps']:8.2f} bps")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true",
                        help="recompute and compare against committed CSVs")
    args = parser.parse_args()
    try:
        result = analyse()
    except (CarryError, HydrationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _print_report(result)
    if args.verify:
        return 0 if verify_outputs(result) else 1
    write_outputs(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
