"""
BLIND feasibility gate for a possible 4h trial on BTC/ETH perpetuals.

WHAT THIS IS
------------
The standing policy in `docs/trial_registry.md` refuses to start a trial whose
decidable-edge floor exceeds its own SESOI. This module applies that gate EX
ANTE to two mechanisms declared before the module was written, and answers one
question: could either of them decide its own question at all?

It reports event counts, dispersion, funding exposure and floors. It computes
NO P&L, profit factor, expectancy, Sharpe, drawdown or equity curve, it ranks
neither mechanism against the other nor either asset against the other, and it
simulates no return of any kind. Every number here is a property of a PRICE
SERIES, a FUNDING SERIES or a POSITION FLAG — never of an outcome.

It registers nothing, pre-registers nothing and authorizes nothing. No trial ID
is claimed. `DRY_RUN`, `LIVE_BALANCE_USD`, `ASSET_CONFIG` and every standing
determination are untouched.

THE TWO DECLARED MECHANISMS — fixed before this module touched data
-------------------------------------------------------------------
No parameter below was searched, tuned, varied or compared on any outcome.
Both are canonical forms carrying literature-conventional parameters, chosen
for STRUCTURAL reasons only.

  M1 "breakout, rare" — Donchian on 4h bars. Enter long when the close exceeds
     the prior 55-bar high; exit when the close falls below the prior 20-bar
     low. Long only, one unit, at most one position per asset. It is the frozen
     STF-CLOSE-55-20 rule on a faster clock, so nothing about it was chosen
     here: `entry_exit_events` and both lookbacks are IMPORTED from the frozen
     protocol and this module cannot vary them.

  M2 "time-series momentum, every bar" — at each 4h close, position =
     sign(close_t - close_{t-30}) in {-1, +1}, held for the NEXT bar, both
     assets, always in a position. 30 bars (5 days) is a declared convention.
     Rationale: the only class whose power is structurally reachable at this
     venue's cost is one that trades every bar.

HOW THE BLIND IS ENFORCED
-------------------------
Exactly three functions in this module may see price LEVELS or differences:

    log_return_dispersion   -> scalars only (SD, by year and overall)
    m2_position_flags       -> a Series of SIGNS in {-1, +1}, never magnitudes
    load_closes             -> reads the price series off disk, does no arithmetic

plus two estimators imported UNMODIFIED from code that is already guarded:
`entry_exit_events` (timestamps and labels only) and
`mean_pairwise_log_return_correlation` (a scalar rho_bar).

Everything downstream operates on timestamps, integer bar counts, position
signs and published funding rates. There is no code path from a price to a
reported return, so P&L is not withheld here — it is absent.
`tests/test_perps_gate.py` asserts this structurally, by walking the AST.

VENUE MISMATCH — READ BEFORE USING ANY NUMBER
---------------------------------------------
The price and funding history below is BINANCE USDT-margined perps, used as
proxy history for Coinbase CFM. They are different venues: Binance funds every
8h and CFM hourly; basis, leverage population, cap/floor rules, fee schedule
and liquidation engine all differ. A proxy is used because CFM has ~14 months
of price history and NO funding history at all, not because the two are
interchangeable. See `docs/research/2026-09-18-perps-scoping.md` sections 4-5.
Any pre-registration leaning on these numbers must carry the mismatch as a
stated limitation.

USAGE
    python backtesting/perps_gate.py            # print the report, write CSVs
    python backtesting/perps_gate.py --verify   # recompute, compare, write nothing
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

from backtesting.hydrate_perps_proxy import (  # noqa: E402
    DATA_DIR,
    HydrationError,
    digests,
)
from backtesting.stf_feasibility import entry_exit_events  # noqa: E402
# M1's two lookbacks are RE-EXPORTED, not used directly: `entry_exit_events`
# reads them from the frozen protocol itself. They are bound here so
# `tests/test_perps_gate.py` can assert identity with the frozen values, which
# is what makes "this module cannot vary M1's parameters" a checked claim
# rather than a promise. Removing them would silently weaken that test.
from backtesting.stf_protocol import ENTRY_LOOKBACK, EXIT_LOOKBACK  # noqa: E402,F401
from backtesting.universe_inventory import (  # noqa: E402
    common_overlap_window,
    effective_assets,
    mean_pairwise_log_return_correlation,
)

# ── Declared constants. None of these is searched, tuned or varied. ──────────

SYMBOLS = ("BTCUSDT", "ETHUSDT")
BAR = "4h"
BARS_PER_DAY = 6
RUN_DATE = "2026-09-19"

# Window. The last bar the proxy archive published when this gate was built.
ANALYSIS_START = pd.Timestamp("2020-01-01", tz="UTC")
ANALYSIS_END = pd.Timestamp("2026-09-18", tz="UTC")     # exclusive
# The archive publishes NO daily funding files, so funding stops at the last
# closed month. The gate declares the boundary rather than filling it.
FUNDING_END = pd.Timestamp("2026-09-01", tz="UTC")      # exclusive

# M2's declared lookback, in bars. 30 bars = 5 days at 4h.
M2_LOOKBACK = 30

# Coinbase CFM futures fee rates, authenticated read of this account on
# 2026-09-18 (docs/research/2026-09-18-perps-scoping.md section 3).
CFM_MAKER = 0.00095
CFM_TAKER = 0.00100
# Round trip priced maker-in / taker-out. THIS IS THE OPTIMISTIC ASSIGNMENT:
# it assumes every entry rests and is filled as a maker, which no mechanism
# here is built to guarantee. All-taker is the honest upper bound.
ROUND_TRIP_OPTIMISTIC = (1 + CFM_MAKER) / (1 - CFM_TAKER) - 1
ROUND_TRIP_ALL_TAKER = (1 + CFM_TAKER) / (1 - CFM_TAKER) - 1

# Standing-policy gate constants.
Z_95_ONE_SIDED = 1.645
SESOI_ANNUAL_PCT = 10.0     # declared by the task, in NET annual percent
DECLARED_YEARS = 6.7
SHADOW_YEARS = (1, 2, 3)

OUT_DIR = ROOT / "docs" / "research" / "data"
CSV_DISPERSION = OUT_DIR / f"perps_gate_dispersion_{RUN_DATE}.csv"
CSV_EVENTS = OUT_DIR / f"perps_gate_events_{RUN_DATE}.csv"
CSV_COST = OUT_DIR / f"perps_gate_cost_{RUN_DATE}.csv"
CSV_FLOORS = OUT_DIR / f"perps_gate_floors_{RUN_DATE}.csv"
CSV_CONTROLS = OUT_DIR / f"perps_gate_control_spec_{RUN_DATE}.csv"
CSV_DIGESTS = OUT_DIR / f"perps_gate_inputs_{RUN_DATE}.csv"


class GateError(RuntimeError):
    pass


# ══ PRICE BOUNDARY ═══════════════════════════════════════════════════════════
# The three functions below are the only ones in this module that may see a
# price level or a price difference. Two of them hand back scalars; the third
# hands back signs. Nothing else in the file touches a price.

def load_closes(symbol: str) -> pd.Series:
    """
    The 4h close series off disk, UTC-indexed, deduplicated, truncated to the
    declared window. READS ONLY — it performs no arithmetic on prices, so no
    quantity derived from a price can originate here.
    """
    paths = sorted(DATA_DIR.glob(f"kl_{symbol}_{BAR}_*.csv"))
    if not paths:
        raise GateError(f"no {symbol} {BAR} klines under {DATA_DIR} — "
                        "run backtesting/hydrate_perps_proxy.py first")
    frames = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        frame = pd.read_csv(io.StringIO(text), header=None, usecols=[0, 4],
                            names=["open_time", "close"])
        frame = frame[pd.to_numeric(frame["open_time"], errors="coerce").notna()]
        frames.append(frame)
    out = pd.concat(frames, ignore_index=True)
    stamps = pd.to_numeric(out["open_time"])
    # Binance switched open_time from ms to microseconds in some 2025 archives.
    unit = "us" if stamps.max() > 2_000_000_000_000_0 else "ms"
    index = pd.to_datetime(stamps, unit=unit, utc=True)
    series = pd.Series(pd.to_numeric(out["close"], errors="coerce").values,
                       index=index).dropna().sort_index()
    series = series[~series.index.duplicated()]
    series = series[(series.index >= ANALYSIS_START) & (series.index < ANALYSIS_END)]
    if series.empty:
        raise GateError(f"{symbol}: no bars inside the declared window")
    return series


def log_return_dispersion(closes: pd.Series) -> dict:
    """
    Per-bar log-return standard deviation, overall and by calendar year.

    THE RETURN TYPE IS THE CONSTRAINT. This hands back scalars keyed by year —
    a dispersion, never a level, never a mean, never a sign, never a series. A
    caller cannot reconstruct a return path, an equity curve or a P&L from a
    standard deviation, which is why dispersion is the one distributional
    quantity the blind allows out. Sample SD (ddof=1), expressed in percent.
    """
    r = np.log(closes).diff().dropna()
    overall = float(r.std(ddof=1) * 100.0)
    by_year = {
        int(year): {"sd_pct": round(float(group.std(ddof=1) * 100.0), 4),
                    "bars": int(len(group))}
        for year, group in r.groupby(r.index.year)
    }
    return {"sd_pct": round(overall, 4), "bars": int(len(r)), "by_year": by_year}


def m2_position_flags(closes: pd.Series) -> pd.Series:
    """
    M2's position flag series: +1 long, -1 short, one value per bar.

    THE RETURN TYPE IS THE CONSTRAINT. `np.sign` discards magnitude entirely,
    so what leaves this function is a direction and nothing else. The flag for
    bar b is sign(close_{b-1} - close_{b-1-30}): the signal is formed on the
    close of the PRIOR bar and held for this one, exactly as declared, so no
    value known only at bar b's close can influence bar b's position.

    Bars whose lookback is not yet available carry 0 (flat, warming up) rather
    than a guess. A real trial starts flat.
    """
    signal = np.sign(closes - closes.shift(M2_LOOKBACK))
    held = signal.shift(1)
    return held.fillna(0.0).astype("int8")


# ══ END PRICE BOUNDARY ═══════════════════════════════════════════════════════
# Everything below operates on timestamps, integer bar counts, position signs
# and published funding rates.


def load_funding(symbol: str) -> pd.DataFrame:
    """Published funding history: timestamp, interval hours, realised rate."""
    paths = sorted(DATA_DIR.glob(f"fund_{symbol}_*.csv"))
    if not paths:
        raise GateError(f"no {symbol} funding history under {DATA_DIR}")
    frames = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        frame = pd.read_csv(io.StringIO(text))
        if "calc_time" not in frame.columns:      # some months ship headerless
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
    return out[["ts", "rate", "interval_h"]].reset_index(drop=True)


# ── M1: events -> flags, in bar space ───────────────────────────────────────

def m1_position_flags(closes: pd.Series) -> pd.Series:
    """
    M1's position flag series, built from `entry_exit_events` ALONE.

    The events carry timestamps and labels; this function turns them into a
    per-bar 0/+1 occupancy flag by index position. It never reads a price: the
    only argument it takes from `closes` is its INDEX, and the events it
    replays came out of the guarded frozen counter. Long only, so the flag is
    0 or +1 and never -1.

    Convention, declared: a position is counted as held from its ENTRY bar
    through the bar BEFORE its exit bar, matching `_in_position_days` in the
    frozen feasibility audit. This charges funding from the entry bar onward,
    which is the conservative side of a half-bar ambiguity.
    """
    index = closes.index
    flags = pd.Series(0, index=index, dtype="int8")
    in_position = False
    start = None
    for event in entry_exit_events(closes):
        ts = pd.Timestamp(event["ts"])
        if event["event"] == "ENTRY" and not in_position:
            in_position, start = True, ts
        elif event["event"] == "EXIT" and in_position:
            flags.loc[(index >= start) & (index < ts)] = 1
            in_position = False
    if in_position:
        flags.loc[index >= start] = 1
    return flags


def _runs(flags: pd.Series) -> list[dict]:
    """
    Maximal runs of a constant NON-ZERO flag value, as index positions.

    This is the event decomposition both mechanisms share: for M1 a run is a
    held position, for M2 a run is one sign of the momentum position. Returns
    integer positions and lengths in bars only.
    """
    values = flags.to_numpy()
    runs: list[dict] = []
    start = None
    for i, v in enumerate(values):
        if v != 0 and (start is None or values[start] != v):
            if start is not None:
                runs.append({"start": start, "end": i, "sign": int(values[start])})
            start = i
        elif v == 0 and start is not None:
            runs.append({"start": start, "end": i, "sign": int(values[start])})
            start = None
    if start is not None:
        runs.append({"start": start, "end": len(values), "sign": int(values[start])})
    for run in runs:
        run["bars"] = int(run["end"] - run["start"])
    return runs


def event_statistics(flags: pd.Series, mechanism: str) -> dict:
    """
    Blind event statistics: counts, holds in bars, occupancy fractions.

    Operates on the flag series alone. No price, no return, no rate. The only
    inputs are signs and the calendar the flags are indexed by.
    """
    index = flags.index
    runs = _runs(flags)
    holds = [run["bars"] for run in runs]
    years = sorted({int(y) for y in index.year})

    by_year = []
    for year in years:
        mask = index.year == year
        year_flags = flags[mask]
        year_runs = [r for r in runs if index[r["start"]].year == year]
        row = {
            "year": year,
            "bars": int(mask.sum()),
            "starts": len(year_runs),
            "bars_in_position": int((year_flags != 0).sum()),
            "fraction_in_position": round(float((year_flags != 0).mean()), 4),
        }
        if mechanism == "M2":
            row["fraction_long"] = round(float((year_flags == 1).mean()), 4)
            row["fraction_short"] = round(float((year_flags == -1).mean()), 4)
            row["mean_run_bars"] = (round(float(np.mean([r["bars"] for r in year_runs])), 2)
                                    if year_runs else None)
        else:
            row["mean_hold_bars"] = (round(float(np.mean([r["bars"] for r in year_runs])), 2)
                                     if year_runs else None)
            row["median_hold_bars"] = (round(float(np.median([r["bars"] for r in year_runs])), 2)
                                       if year_runs else None)
        by_year.append(row)

    return {
        "mechanism": mechanism,
        "bars": int(len(flags)),
        "runs": len(runs),
        "mean_hold_bars": round(float(np.mean(holds)), 2) if holds else None,
        "median_hold_bars": round(float(np.median(holds)), 2) if holds else None,
        "max_hold_bars": int(np.max(holds)) if holds else None,
        "bars_in_position": int((flags != 0).sum()),
        "fraction_in_position": round(float((flags != 0).mean()), 4),
        "fraction_long": round(float((flags == 1).mean()), 4),
        "fraction_short": round(float((flags == -1).mean()), 4),
        "by_year": by_year,
    }


# ── Cost side: fees and funding. Arithmetic on flags and published rates. ────

def funding_drag(flags: pd.Series, funding: pd.DataFrame) -> dict:
    """
    Funding paid, as a percentage of position notional, by year and overall.

    Arithmetic, not P&L: for each published funding event, look up the bar it
    falls in and add `position sign x published rate`. A positive published
    rate is paid by longs and received by shorts, so a long leg contributes a
    cost and a short leg a credit, and the two are reported separately so the
    netting is visible rather than assumed.

    It takes a FLAG SERIES and a RATE SERIES. It never sees a price, and it
    produces a cost, not a return: nothing here knows whether the position
    made money.
    """
    index = flags.index
    positions = np.searchsorted(index.to_numpy(), funding["ts"].to_numpy(),
                                side="right") - 1
    rows = []
    for pos, ts, rate in zip(positions, funding["ts"], funding["rate"]):
        if pos < 0 or pos >= len(flags):
            continue
        sign = int(flags.iloc[pos])
        if sign == 0:
            continue
        rows.append({"year": int(ts.year), "sign": sign,
                     "paid_pct": sign * float(rate) * 100.0})
    if not rows:
        return {"total_pct": 0.0, "events": 0, "long_pct": 0.0,
                "short_pct": 0.0, "by_year": []}

    frame = pd.DataFrame(rows)
    by_year = []
    for year, group in frame.groupby("year"):
        longs = group[group["sign"] == 1]["paid_pct"]
        shorts = group[group["sign"] == -1]["paid_pct"]
        by_year.append({
            "year": int(year),
            "events": int(len(group)),
            "total_pct": round(float(group["paid_pct"].sum()), 4),
            "long_pct": round(float(longs.sum()), 4),
            "short_pct": round(float(shorts.sum()), 4),
        })
    return {
        "total_pct": round(float(frame["paid_pct"].sum()), 4),
        "events": int(len(frame)),
        "long_pct": round(float(frame[frame["sign"] == 1]["paid_pct"].sum()), 4),
        "short_pct": round(float(frame[frame["sign"] == -1]["paid_pct"].sum()), 4),
        "by_year": by_year,
    }


def unconditional_funding_pct_yr(funding: pd.DataFrame) -> float:
    """
    What a position that was ALWAYS long would have paid, %/yr of notional.

    A benchmark, not a mechanism: it is the published rate series summed and
    annualised, with no position flag involved at all. It exists so the
    conditional drag can be compared against the occupancy-weighted share of
    it, which is how a mechanism's funding SELECTION becomes visible.
    """
    years = float((funding["ts"].iloc[-1] - funding["ts"].iloc[0]).days + 1) / 365.25
    return float(funding["rate"].sum() * 100.0 / years)


def cost_exposure(stats: dict, drag: dict, funding_years: float,
                  event_years: float) -> dict:
    """
    Round trips per year, fee drag, funding drag and their total — all in
    percent of notional per year. Cost side only; there is no revenue term
    anywhere in this function and none is implied by it.

    A round trip is one entry plus one exit. For M1 that is one held position;
    for M2 a sign reversal closes one position and opens the next, so the run
    count is the round-trip count to within the single open run at the window
    edge.
    """
    round_trips_per_year = stats["runs"] / event_years
    fee_optimistic = round_trips_per_year * ROUND_TRIP_OPTIMISTIC * 100.0
    fee_all_taker = round_trips_per_year * ROUND_TRIP_ALL_TAKER * 100.0
    funding_per_year = drag["total_pct"] / funding_years
    return {
        "round_trips_per_year": round(round_trips_per_year, 3),
        "fee_drag_pct_yr_optimistic": round(fee_optimistic, 4),
        "fee_drag_pct_yr_all_taker": round(fee_all_taker, 4),
        "funding_drag_pct_yr": round(funding_per_year, 4),
        "funding_long_pct_yr": round(drag["long_pct"] / funding_years, 4),
        "funding_short_pct_yr": round(drag["short_pct"] / funding_years, 4),
        "total_drag_pct_yr_optimistic": round(fee_optimistic + funding_per_year, 4),
        "total_drag_pct_yr_all_taker": round(fee_all_taker + funding_per_year, 4),
    }


# ── The floor and the gate ──────────────────────────────────────────────────

def sigma_trade_pct(sd_bar_pct: float, mean_hold_bars: float) -> float:
    """
    Per-trade dispersion from per-bar dispersion: SD_bar * sqrt(hold in bars).

    A LOWER BOUND, stated as one. It assumes returns compound independently
    across bars inside a position; a trend-following position is selected for
    autocorrelation, and a real per-trade series also carries entry and exit
    timing dispersion this ignores. True per-trade SD is therefore larger and
    every floor below is correspondingly OPTIMISTIC — the same direction of
    error Closure 2 recorded for its own floors.
    """
    return sd_bar_pct * (mean_hold_bars ** 0.5)


def annual_floor_pct(sigma_pct: float, n_per_year: float, years: float) -> float:
    """
    floor_annual = 1.645 * sigma_trade * sqrt(n / Y)

    The smallest ANNUAL edge a sample of `n * Y` trades could separate from
    zero at one-sided 95%, given per-trade dispersion `sigma_pct`. It is the
    per-trade floor `1.645 * sigma / sqrt(nY)` multiplied by the `n` trades a
    year carries.

    AN IDENTITY WORTH SEEING. Substituting the declared sigma construction
    `sigma = SD_bar * sqrt(h)` gives

        floor_annual = 1.645 * SD_bar * sqrt(n * h / Y)

    and `n * h` is simply the number of BARS IN POSITION per year. The annual
    floor therefore depends only on per-bar dispersion and on time in market —
    how that time is cut into trades cancels out exactly. Trading the same
    exposure more often does not lower the floor. It only raises the fee bill,
    which scales with `n` and not with its square root.
    """
    if n_per_year <= 0 or years <= 0:
        raise GateError("floor needs a positive event rate and horizon")
    return Z_95_ONE_SIDED * sigma_pct * ((n_per_year / years) ** 0.5)


def gate(floor_pct: float) -> str:
    """PASS when the floor is at or below the declared SESOI, FAIL otherwise."""
    return "PASS" if floor_pct <= SESOI_ANNUAL_PCT else "FAIL"


def years_to_reach_sesoi(floor_pct: float, years: float) -> float:
    """
    The horizon at which this mechanism's floor would fall to the SESOI, at an
    unchanged event rate. Since the floor scales as 1/sqrt(Y) for a fixed rate,
    Y* = Y * (floor / SESOI)^2. It is arithmetic on the floor, not a forecast:
    it assumes the dispersion and the event rate hold, and neither is promised.
    """
    return years * (floor_pct / SESOI_ANNUAL_PCT) ** 2


# ── Deliverable 5: the positive-control spec. DESIGN ONLY — NOT RUN. ────────

# Declared here, before any return is computed, so the magnitudes and seeds are
# on record rather than chosen once a result is visible.
CONTROL_SEED_INJECTION = 20260919
CONTROL_SEED_SHUFFLE = 20260920
CONTROL_REPLICATIONS = 1000

# Multiples of each mechanism's own annual floor. The expectations follow from
# the floor's definition and are not tuned: a true edge equal to 1.0x the floor
# puts the one-sided lower bound at zero IN EXPECTATION, so it clears about
# half the time; 2.0x the floor is the 1.645 + 1.645 case, about 95%; and the
# SESOI row is below the floor by construction, so it should mostly NOT clear.
CONTROL_MULTIPLES = (
    ("A_floor_1x", 1.0, "~50% of replications clear zero"),
    ("B_floor_2x", 2.0, "~95% of replications clear zero"),
    ("C_sesoi", None, "well under 50% — the floor is above the SESOI"),
    ("D_null_shuffled", 0.0, "~5% clear zero — the nominal false-positive rate"),
)


def control_spec(result: dict) -> list[dict]:
    """
    Per-bar drift magnitudes for the return trial's positive and negative
    controls. ARITHMETIC ON THE FLOORS, not a simulation: nothing here injects
    anything, runs anything or observes any return. It converts a target
    ANNUAL edge into the per-bar drift that would produce it, given the bars a
    mechanism spends in position each year.
    """
    rows = []
    for mech in ("M1", "M2"):
        for sym in SYMBOLS:
            f = result["floors"][mech][sym]
            bars = f["bars_in_position_per_year"]
            for name, multiple, expectation in CONTROL_MULTIPLES:
                if name == "C_sesoi":
                    target = SESOI_ANNUAL_PCT
                elif multiple == 0.0:
                    target = 0.0
                else:
                    target = f["floor_declared_Y"] * multiple
                rows.append({
                    "control": name,
                    "mechanism": mech,
                    "symbol": sym,
                    "target_annual_edge_pct": f"{target:.4f}",
                    "per_bar_drift_pct": f"{target / bars:.6f}",
                    "bars_in_position_per_year": f"{bars:.1f}",
                    "seed": (CONTROL_SEED_SHUFFLE if name == "D_null_shuffled"
                             else CONTROL_SEED_INJECTION),
                    "replications": CONTROL_REPLICATIONS,
                    "expectation": expectation,
                })
    return rows


# ── Assembly ────────────────────────────────────────────────────────────────

def _years_spanned(index: pd.DatetimeIndex) -> float:
    return float(((index[-1] - index[0]).total_seconds() / 86400.0 + 1) / 365.25)


def analyse() -> dict:
    closes = {s: load_closes(s) for s in SYMBOLS}
    funding = {s: load_funding(s) for s in SYMBOLS}

    window = common_overlap_window(closes, list(SYMBOLS))
    correlation = mean_pairwise_log_return_correlation(closes, list(SYMBOLS), window)
    n_eff = effective_assets(len(SYMBOLS), correlation["rho_bar"])

    dispersion = {s: log_return_dispersion(closes[s]) for s in SYMBOLS}
    flags = {
        "M1": {s: m1_position_flags(closes[s]) for s in SYMBOLS},
        "M2": {s: m2_position_flags(closes[s]) for s in SYMBOLS},
    }
    stats = {m: {s: event_statistics(flags[m][s], m) for s in SYMBOLS}
             for m in ("M1", "M2")}

    event_years = {s: _years_spanned(closes[s].index) for s in SYMBOLS}
    funding_years = {
        s: float((funding[s]["ts"].iloc[-1] - funding[s]["ts"].iloc[0]).days + 1) / 365.25
        for s in SYMBOLS
    }
    drag = {m: {s: funding_drag(flags[m][s], funding[s]) for s in SYMBOLS}
            for m in ("M1", "M2")}
    unconditional = {s: round(unconditional_funding_pct_yr(funding[s]), 4)
                     for s in SYMBOLS}
    cost = {
        m: {s: cost_exposure(stats[m][s], drag[m][s], funding_years[s], event_years[s])
            for s in SYMBOLS}
        for m in ("M1", "M2")
    }

    floors = {}
    for mech in ("M1", "M2"):
        floors[mech] = {}
        for sym in SYMBOLS:
            sd_bar = dispersion[sym]["sd_pct"]
            hold = stats[mech][sym]["mean_hold_bars"]
            sigma = sigma_trade_pct(sd_bar, hold)
            n_year = stats[mech][sym]["runs"] / event_years[sym]
            row = {
                "sd_bar_pct": sd_bar,
                "mean_hold_bars": hold,
                "sigma_trade_pct": round(sigma, 4),
                "n_per_year": round(n_year, 3),
                "floor_declared_Y": round(annual_floor_pct(sigma, n_year, DECLARED_YEARS), 4),
            }
            for y in SHADOW_YEARS:
                row[f"floor_Y{y}"] = round(annual_floor_pct(sigma, n_year, float(y)), 4)
            row["bars_in_position_per_year"] = round(
                stats[mech][sym]["bars_in_position"] / event_years[sym], 1)
            row["gate_declared_Y"] = gate(row["floor_declared_Y"])
            row["years_to_sesoi"] = round(
                years_to_reach_sesoi(row["floor_declared_Y"], DECLARED_YEARS), 1)
            row["cost_bar_pct_yr"] = round(
                cost[mech][sym]["total_drag_pct_yr_optimistic"] + SESOI_ANNUAL_PCT, 4)
            floors[mech][sym] = row

        # Correlation-adjusted pooled variant, the same construction Closure 2
        # used: N_eff independent assets each firing at the mean per-asset rate.
        mean_rate = float(np.mean([floors[mech][s]["n_per_year"] for s in SYMBOLS]))
        mean_sigma = float(np.mean([floors[mech][s]["sigma_trade_pct"] for s in SYMBOLS]))
        pooled_rate = mean_rate * n_eff
        pooled = {
            "sigma_trade_pct": round(mean_sigma, 4),
            "n_per_year": round(pooled_rate, 3),
            "n_eff": n_eff,
            "floor_declared_Y": round(
                annual_floor_pct(mean_sigma, pooled_rate, DECLARED_YEARS), 4),
        }
        for y in SHADOW_YEARS:
            pooled[f"floor_Y{y}"] = round(
                annual_floor_pct(mean_sigma, pooled_rate, float(y)), 4)
        pooled["bars_in_position_per_year"] = round(float(np.mean(
            [stats[mech][s]["bars_in_position"] / event_years[s] for s in SYMBOLS]))
            * n_eff, 1)
        pooled["gate_declared_Y"] = gate(pooled["floor_declared_Y"])
        pooled["years_to_sesoi"] = round(
            years_to_reach_sesoi(pooled["floor_declared_Y"], DECLARED_YEARS), 1)
        floors[mech]["POOLED"] = pooled

    return {
        "window": window,
        "analysis_start": ANALYSIS_START.isoformat(),
        "analysis_end": ANALYSIS_END.isoformat(),
        "funding_end": FUNDING_END.isoformat(),
        "event_years": {s: round(event_years[s], 3) for s in SYMBOLS},
        "funding_years": {s: round(funding_years[s], 3) for s in SYMBOLS},
        "correlation": correlation,
        "n_eff": n_eff,
        "dispersion": dispersion,
        "stats": stats,
        "drag": drag,
        "cost": cost,
        "unconditional_funding_pct_yr": unconditional,
        "floors": floors,
    }


# ── CSV emission ────────────────────────────────────────────────────────────

def _rows_dispersion(result: dict) -> list[dict]:
    rows = []
    for sym in SYMBOLS:
        d = result["dispersion"][sym]
        rows.append({"symbol": sym, "scope": "full_window", "bars": d["bars"],
                     "sd_4h_log_return_pct": f"{d['sd_pct']:.4f}"})
        for year, cell in sorted(d["by_year"].items()):
            rows.append({"symbol": sym, "scope": str(year), "bars": cell["bars"],
                         "sd_4h_log_return_pct": f"{cell['sd_pct']:.4f}"})
    rows.append({"symbol": "BTCUSDT|ETHUSDT", "scope": "rho_bar_4h_log_returns",
                 "bars": result["correlation"]["observations"],
                 "sd_4h_log_return_pct": f"{result['correlation']['rho_bar']:.4f}"})
    rows.append({"symbol": "BTCUSDT|ETHUSDT", "scope": "n_eff",
                 "bars": result["correlation"]["pairs"],
                 "sd_4h_log_return_pct": f"{result['n_eff']:.4f}"})
    return rows


def _rows_events(result: dict) -> list[dict]:
    rows = []
    for mech in ("M1", "M2"):
        for sym in SYMBOLS:
            s = result["stats"][mech][sym]
            rows.append({
                "mechanism": mech, "symbol": sym, "scope": "full_window",
                "bars": s["bars"], "starts": s["runs"],
                "mean_hold_bars": s["mean_hold_bars"],
                "median_hold_bars": s["median_hold_bars"],
                "fraction_in_position": f"{s['fraction_in_position']:.4f}",
                "fraction_long": f"{s['fraction_long']:.4f}",
                "fraction_short": f"{s['fraction_short']:.4f}",
            })
            for row in s["by_year"]:
                rows.append({
                    "mechanism": mech, "symbol": sym, "scope": str(row["year"]),
                    "bars": row["bars"], "starts": row["starts"],
                    "mean_hold_bars": row.get("mean_hold_bars", row.get("mean_run_bars")),
                    "median_hold_bars": row.get("median_hold_bars", ""),
                    "fraction_in_position": f"{row['fraction_in_position']:.4f}",
                    "fraction_long": (f"{row['fraction_long']:.4f}"
                                      if "fraction_long" in row else ""),
                    "fraction_short": (f"{row['fraction_short']:.4f}"
                                       if "fraction_short" in row else ""),
                })
    return rows


def _rows_cost(result: dict) -> list[dict]:
    rows = []
    for mech in ("M1", "M2"):
        for sym in SYMBOLS:
            c = result["cost"][mech][sym]
            d = result["drag"][mech][sym]
            rows.append({
                "mechanism": mech, "symbol": sym, "scope": "full_window",
                "round_trips_per_year": f"{c['round_trips_per_year']:.3f}",
                "fee_drag_pct_yr": f"{c['fee_drag_pct_yr_optimistic']:.4f}",
                "fee_drag_pct_yr_all_taker": f"{c['fee_drag_pct_yr_all_taker']:.4f}",
                "funding_drag_pct_yr": f"{c['funding_drag_pct_yr']:.4f}",
                "funding_long_pct_yr": f"{c['funding_long_pct_yr']:.4f}",
                "funding_short_pct_yr": f"{c['funding_short_pct_yr']:.4f}",
                "total_drag_pct_yr": f"{c['total_drag_pct_yr_optimistic']:.4f}",
                "funding_always_in_position_pct_yr":
                    f"{result['unconditional_funding_pct_yr'][sym]:.4f}",
                "funding_occupancy_weighted_pct_yr": f"{
                    result['unconditional_funding_pct_yr'][sym]
                    * result['stats'][mech][sym]['fraction_in_position']:.4f}",
            })
            for row in d["by_year"]:
                rows.append({
                    "mechanism": mech, "symbol": sym, "scope": str(row["year"]),
                    "round_trips_per_year": "", "fee_drag_pct_yr": "",
                    "fee_drag_pct_yr_all_taker": "",
                    "funding_drag_pct_yr": f"{row['total_pct']:.4f}",
                    "funding_long_pct_yr": f"{row['long_pct']:.4f}",
                    "funding_short_pct_yr": f"{row['short_pct']:.4f}",
                    "total_drag_pct_yr": "",
                    "funding_always_in_position_pct_yr": "",
                    "funding_occupancy_weighted_pct_yr": "",
                })
    return rows


def _rows_floors(result: dict) -> list[dict]:
    rows = []
    for mech in ("M1", "M2"):
        for key in (*SYMBOLS, "POOLED"):
            f = result["floors"][mech][key]
            rows.append({
                "mechanism": mech, "basis": key,
                "sigma_trade_pct": f"{f['sigma_trade_pct']:.4f}",
                "n_per_year": f"{f['n_per_year']:.3f}",
                "floor_annual_pct_Y6.7": f"{f['floor_declared_Y']:.4f}",
                "floor_annual_pct_Y1": f"{f['floor_Y1']:.4f}",
                "floor_annual_pct_Y2": f"{f['floor_Y2']:.4f}",
                "floor_annual_pct_Y3": f"{f['floor_Y3']:.4f}",
                "bars_in_position_per_year": f"{f['bars_in_position_per_year']:.1f}",
                "cost_bar_gross_pct_yr": (f"{f['cost_bar_pct_yr']:.4f}"
                                          if "cost_bar_pct_yr" in f else ""),
                "sesoi_annual_pct": f"{SESOI_ANNUAL_PCT:.1f}",
                "gate": f["gate_declared_Y"],
                "years_to_reach_sesoi": f"{f['years_to_sesoi']:.1f}",
            })
    return rows


def _write_csv(path: Path, rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()),
                            lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


_EMITTERS = (
    (CSV_DISPERSION, _rows_dispersion),
    (CSV_EVENTS, _rows_events),
    (CSV_COST, _rows_cost),
    (CSV_FLOORS, _rows_floors),
    (CSV_CONTROLS, control_spec),
)


def write_outputs(result: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for path, builder in _EMITTERS:
        path.write_text(_write_csv(path, builder(result)), encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")
    rows = [{"file": r["file"], "sha256": r["sha256"], "lines": r["lines"]}
            for r in digests()]
    CSV_DIGESTS.write_text(_write_csv(CSV_DIGESTS, rows), encoding="utf-8")
    print(f"wrote {CSV_DIGESTS.relative_to(ROOT)} ({len(rows)} input files)")


def verify_outputs(result: dict) -> bool:
    ok = True
    for path, builder in _EMITTERS:
        if not path.exists():
            print(f"MISSING {path.relative_to(ROOT)}")
            ok = False
            continue
        fresh = _write_csv(path, builder(result))
        if path.read_text(encoding="utf-8") != fresh:
            print(f"MISMATCH {path.relative_to(ROOT)}")
            ok = False
        else:
            print(f"ok {path.relative_to(ROOT)}")
    return ok


def _print_report(result: dict) -> None:
    w = result["window"]
    print(f"\nwindow {w['start']} -> {w['end']}  ({w['years']} y)   "
          f"funding ends {result['funding_end']}")
    print(f"rho_bar (4h log returns) {result['correlation']['rho_bar']}   "
          f"N_eff {result['n_eff']}")
    print("\n-- dispersion (4h log-return SD, %) --")
    for sym in SYMBOLS:
        print(f"  {sym:9s} full {result['dispersion'][sym]['sd_pct']:.4f}")
    print("\n-- events --")
    for mech in ("M1", "M2"):
        for sym in SYMBOLS:
            s = result["stats"][mech][sym]
            print(f"  {mech} {sym:9s} starts {s['runs']:5d}  mean hold "
                  f"{s['mean_hold_bars']:7.2f} bars  in-position "
                  f"{s['fraction_in_position']:.4f}")
    print("\n-- cost (% of notional per year) --")
    for mech in ("M1", "M2"):
        for sym in SYMBOLS:
            c = result["cost"][mech][sym]
            print(f"  {mech} {sym:9s} rt/yr {c['round_trips_per_year']:8.3f}  "
                  f"fee {c['fee_drag_pct_yr_optimistic']:8.4f}  funding "
                  f"{c['funding_drag_pct_yr']:8.4f}  total "
                  f"{c['total_drag_pct_yr_optimistic']:8.4f}")
    print("\n-- floor and gate (SESOI = 10.0% net annual) --")
    for mech in ("M1", "M2"):
        for key in (*SYMBOLS, "POOLED"):
            f = result["floors"][mech][key]
            print(f"  {mech} {key:9s} sigma {f['sigma_trade_pct']:8.4f}  n/yr "
                  f"{f['n_per_year']:8.3f}  floor@6.7y "
                  f"{f['floor_declared_Y']:9.4f}%  {f['gate_declared_Y']}"
                  f"  (SESOI at Y={f['years_to_sesoi']:.1f} y)")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true",
                        help="recompute and compare against committed CSVs")
    args = parser.parse_args()
    try:
        result = analyse()
    except (GateError, HydrationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _print_report(result)
    if args.verify:
        return 0 if verify_outputs(result) else 1
    write_outputs(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
