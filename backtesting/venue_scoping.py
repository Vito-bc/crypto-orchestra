"""
Venue scoping — Bullish US, CME micro/spot-quoted BTC futures, Bitstamp.

READ-ONLY. No account, no credentials, no authenticated call, no P&L, no
trial ID, no recommendation. See `docs/research/2026-09-20-venue-scoping.md`
for the fact sheet this script supports.

WHAT THIS COMPUTES
------------------
Three things a web page cannot give us because they need the project's own
data or its own declared mechanisms:

  1. The Friday-close -> Sunday-open BTC gap distribution on the Binance 4h
     proxy (2020-2026), and what fraction of M1's (frozen Donchian 55/20)
     entries and exits would have fallen inside CME's declared closed hours.
     This is a STRUCTURAL COUNT of a price/calendar relationship — no return,
     no strategy outcome, no P&L.
  2. CME futures cost and expressibility arithmetic (contracts, margin,
     commission) at declared BTC price points. Pure arithmetic on declared
     public contract specs and one secondary-sourced IBKR fee figure (cited).
  3. A re-derivation of the carry break-even from
     `docs/research/2026-09-19-carry-scoping.md`, swapping the spot leg's fee
     schedule for Bullish's and Bitstamp's published rates while holding the
     perp leg (Coinbase CFM) and the funding/cycle data fixed. Reuses
     `carry_scoping.analyse_symbol` unmodified, via an additional schedule
     entry, so the break-even arithmetic cannot drift from the one the carry
     document already published and had reviewed.

VENUE MISMATCH, AGAIN
----------------------
The weekend-gap and CME-closed-hours counts use the Binance USDT-margined
perp proxy the whole research branch has been using since
`docs/research/2026-09-19-perps-gate.md` — not CME's own price history, which
is 14 months deep and does not cover 2020-2026. The proxy is a stand-in for
BTC's price PATH, not for CME's own microstructure; CME does not trade this
instrument. Declared as a limitation, not corrected for.

USAGE
    python backtesting/venue_scoping.py            # print report, write CSVs
    python backtesting/venue_scoping.py --verify   # recompute, compare, write nothing
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting import carry_scoping  # noqa: E402
from backtesting.hydrate_perps_proxy import HydrationError  # noqa: E402
from backtesting.perps_gate import GateError, load_closes  # noqa: E402
from backtesting.stf_feasibility import entry_exit_events  # noqa: E402

RUN_DATE = "2026-09-20"
NY = ZoneInfo("America/New_York")

OUT_DIR = ROOT / "docs" / "research" / "data"
CSV_WEEKEND_GAPS = OUT_DIR / f"venue_weekend_gaps_{RUN_DATE}.csv"
CSV_CME_CLOSED = OUT_DIR / f"venue_cme_closed_hours_{RUN_DATE}.csv"
CSV_CME_COST = OUT_DIR / f"venue_cme_cost_{RUN_DATE}.csv"
CSV_EXPRESSIBILITY = OUT_DIR / f"venue_expressibility_{RUN_DATE}.csv"
CSV_SPOT_COMPARE = OUT_DIR / f"venue_spot_compare_{RUN_DATE}.csv"
CSV_CARRY_REDERIVED = OUT_DIR / f"venue_carry_rederived_{RUN_DATE}.csv"

_EMITTERS_REGISTERED = (
    CSV_WEEKEND_GAPS, CSV_CME_CLOSED, CSV_CME_COST,
    CSV_EXPRESSIBILITY, CSV_SPOT_COMPARE, CSV_CARRY_REDERIVED,
)


class VenueScopingError(RuntimeError):
    pass


# ══ DECLARED FACTS — public, cited in the accompanying document ═════════════

# Bullish US, "Standard markets" (BTC/ETH/SOL), Individual schedule, from the
# help-centre fee schedule article (docs/research/2026-09-20-venue-scoping.md
# Part A). Individual: 0 maker / 0.5bps taker. Worst case both legs taker.
BULLISH_STANDARD_MAKER = 0.0000
BULLISH_STANDARD_TAKER = 0.00005

# Bitstamp official schedule, bitstamp.net/fee-schedule/ (2026-09-20 read).
BITSTAMP_TIERS = {
    "bitstamp_0_1k": {"maker": 0.0000, "taker": 0.0000,
                       "label": "Bitstamp $0-1k/30d (0.00%/0.00%)"},
    "bitstamp_1_10k": {"maker": 0.0030, "taker": 0.0040,
                        "label": "Bitstamp $1k-10k/30d (0.30%/0.40%)"},
    "bitstamp_10_100k": {"maker": 0.0020, "taker": 0.0030,
                         "label": "Bitstamp $10k-100k/30d (0.20%/0.30%)"},
}

# CME contract specs, public (cmegroup.com contract-spec pages, secondary
# sources cited in the document where the primary page 403'd this tooling).
CME_CONTRACTS = {
    "MBT": {"underlying": "BTC", "contract_size_btc": 0.10,
            "label": "Micro Bitcoin futures (MBT)"},
    "BTC_SPOT_QUOTED": {"underlying": "BTC", "contract_size_btc": 0.01,
                        "label": "Spot-Quoted Bitcoin futures"},
}

# IBKR benchmark cost per MBT contract, ONE SIDE. Commission $2.25 (IBKR's own
# published tiered range is $0.25-$0.85; $2.25 is brokerchooser.com's quoted
# benchmark and matches the brief's reference point) plus ~$1.20 CME exchange
# + clearing + NFA per contract (secondary-sourced; see document Part B).
IBKR_MBT_COMMISSION_PER_SIDE = 2.25
IBKR_MBT_EXCHANGE_CLEARING_PER_SIDE = 1.20
# Spot-quoted contract is 1/10th MBT's notional per contract at the same BTC
# price; no independent IBKR fee schedule for it was found (undocumented at
# this venue as of 2026-09-20), so it is NOT included in the per-contract
# dollar cost table -- only in the expressibility table, where contract count
# alone (not fees) is what is being computed.

# Declared round-trip counts per year, imported as facts from the already-
# reviewed perps-gate document (2026-09-19), not re-derived here.
M1_ROUND_TRIPS_PER_YEAR_BTC = 19.06
M2_ROUND_TRIPS_PER_YEAR_BTC = 191.80

BTC_PRICE_POINTS = (60_000, 70_000, 80_000)
ACCOUNT_SIZES_USD = (10_000, 20_000)
SIZING_FRACTIONS = (0.02, 0.05, 0.10)
CIRCUIT_BREAKER_FRACTIONS = (0.50, 0.25)  # of the sizing fraction, per runner.py


# ══ SECTION 1 — weekend gap + CME-closed-hours structural count ═════════════

def _cme_closed(ts_utc: pd.Timestamp) -> bool:
    """
    True if `ts_utc` falls inside CME Globex's declared closed window for its
    BTC-linked futures: the daily halt (5:00pm-6:00pm ET, Mon-Thu) or the
    weekend closure (Friday 5:00pm ET -> Sunday 6:00pm ET). Public contract-
    hours information; independent of any account or data source.
    """
    local = ts_utc.tz_convert(NY)
    dow = local.dayofweek  # Mon=0 ... Sun=6
    t = local.hour + local.minute / 60.0
    if dow == 5:                       # Saturday: always closed
        return True
    if dow == 4 and t >= 17.0:         # Friday from 5pm ET: weekend closure starts
        return True
    if dow == 6 and t < 18.0:          # Sunday before 6pm ET: still closed
        return True
    if 17.0 <= t < 18.0:               # Mon-Thu daily halt, 5-6pm ET
        return True
    return False


def weekend_gaps(closes: pd.Series) -> pd.DataFrame:
    """
    One row per ISO week with both a pre-5pm-ET Friday bar and a post-6pm-ET
    Sunday bar: the log return between them, in percent.
    """
    local = closes.index.tz_convert(NY)
    iso = local.isocalendar()
    frame = pd.DataFrame({
        "close": closes.to_numpy(),
        "dow": local.dayofweek,
        "hour": local.hour + local.minute / 60.0,
        "iso_year": iso.year.to_numpy(),
        "iso_week": iso.week.to_numpy(),
    }, index=closes.index)

    rows = []
    for (yr, wk), grp in frame.groupby(["iso_year", "iso_week"]):
        fri = grp[(grp["dow"] == 4) & (grp["hour"] < 17.0)]
        sun = grp[(grp["dow"] == 6) & (grp["hour"] >= 18.0)]
        if fri.empty or sun.empty:
            continue
        fri_row, sun_row = fri.iloc[-1], sun.iloc[0]
        gap_pct = float(np.log(sun_row["close"] / fri_row["close"]) * 100.0)
        rows.append({
            "iso_year": int(yr), "iso_week": int(wk),
            "friday_close_utc": fri_row.name.isoformat(),
            "sunday_open_utc": sun_row.name.isoformat(),
            "friday_close_px": round(float(fri_row["close"]), 2),
            "sunday_open_px": round(float(sun_row["close"]), 2),
            "gap_log_return_pct": round(gap_pct, 4),
        })
    return pd.DataFrame(rows)


def weekend_gap_summary(gaps: pd.DataFrame) -> dict:
    s = gaps["gap_log_return_pct"]
    return {
        "weeks": int(len(s)),
        "mean_pct": round(float(s.mean()), 4),
        "sd_pct": round(float(s.std(ddof=1)), 4),
        "min_pct": round(float(s.min()), 4),
        "max_pct": round(float(s.max()), 4),
        "p05_pct": round(float(s.quantile(0.05)), 4),
        "p25_pct": round(float(s.quantile(0.25)), 4),
        "median_pct": round(float(s.median()), 4),
        "p75_pct": round(float(s.quantile(0.75)), 4),
        "p95_pct": round(float(s.quantile(0.95)), 4),
        "frac_abs_gt_1pct": round(float((s.abs() > 1.0).mean()), 4),
        "frac_abs_gt_3pct": round(float((s.abs() > 3.0).mean()), 4),
        "frac_abs_gt_5pct": round(float((s.abs() > 5.0).mean()), 4),
    }


def m1_closed_hours_fraction(symbol: str) -> dict:
    """
    Of M1's (frozen Donchian 55/20 on 4h) entry and exit event timestamps, the
    fraction that fall inside CME's declared closed hours. Structural count
    only: `entry_exit_events` hands back timestamps and labels, never a price
    or a return (see `backtesting/stf_feasibility.py`).
    """
    closes = load_closes(symbol)
    events = entry_exit_events(closes)
    entries = [e for e in events if e["event"] == "ENTRY"]
    exits = [e for e in events if e["event"] == "EXIT"]

    def frac_closed(evs: list[dict]) -> float | None:
        if not evs:
            return None
        closed = sum(1 for e in evs if _cme_closed(pd.Timestamp(e["ts"])))
        return round(closed / len(evs), 4)

    return {
        "symbol": symbol,
        "total_events": len(events),
        "entries": len(entries),
        "exits": len(exits),
        "frac_entries_cme_closed": frac_closed(entries),
        "frac_exits_cme_closed": frac_closed(exits),
        "frac_all_events_cme_closed": frac_closed(events),
    }


# ══ SECTION 2 — CME cost and expressibility arithmetic ══════════════════════

def cme_cost_table() -> list[dict]:
    """
    Per-side and round-trip dollar and percent cost for MBT at three BTC
    prices. Percent is of the CONTRACT's notional (0.10 BTC), which is what a
    trader sizing to a contract count actually pays relative to.
    """
    rows = []
    for price in BTC_PRICE_POINTS:
        notional = CME_CONTRACTS["MBT"]["contract_size_btc"] * price
        per_side_usd = IBKR_MBT_COMMISSION_PER_SIDE + IBKR_MBT_EXCHANGE_CLEARING_PER_SIDE
        round_trip_usd = per_side_usd * 2.0
        per_side_pct = per_side_usd / notional * 100.0
        round_trip_pct = round_trip_usd / notional * 100.0
        rows.append({
            "btc_price_usd": price,
            "contract_notional_usd": round(notional, 2),
            "commission_per_side_usd": IBKR_MBT_COMMISSION_PER_SIDE,
            "exchange_clearing_per_side_usd": IBKR_MBT_EXCHANGE_CLEARING_PER_SIDE,
            "per_side_usd": round(per_side_usd, 2),
            "round_trip_usd": round(round_trip_usd, 2),
            "per_side_pct_notional": round(per_side_pct, 4),
            "round_trip_pct_notional": round(round_trip_pct, 4),
            "m1_fee_drag_pct_yr": round(round_trip_pct * M1_ROUND_TRIPS_PER_YEAR_BTC, 4),
            "m2_fee_drag_pct_yr": round(round_trip_pct * M2_ROUND_TRIPS_PER_YEAR_BTC, 4),
        })
    return rows


def expressibility_table() -> list[dict]:
    """
    Contracts per position at 2%/5%/10% sizing, both MBT and spot-quoted BTC,
    at $10k/$20k accounts and three BTC prices. Also the circuit-breaker sizes
    (50%/25% of the base sizing fraction) and the residual directional
    exposure integer rounding leaves behind.
    """
    rows = []
    for account in ACCOUNT_SIZES_USD:
        for price in BTC_PRICE_POINTS:
            for product_key, product in CME_CONTRACTS.items():
                unit_notional = product["contract_size_btc"] * price
                for frac in SIZING_FRACTIONS:
                    target_usd = account * frac
                    contracts_exact = target_usd / unit_notional
                    contracts_int = int(round(contracts_exact))
                    residual_usd = (contracts_exact - contracts_int) * unit_notional
                    residual_pct_of_target = (
                        round(abs(residual_usd) / target_usd * 100.0, 2)
                        if target_usd else None)
                    cb_rows = []
                    for cb in CIRCUIT_BREAKER_FRACTIONS:
                        cb_target = target_usd * cb
                        cb_contracts = int(round(cb_target / unit_notional))
                        cb_rows.append((cb, cb_contracts))
                    rows.append({
                        "account_usd": account,
                        "btc_price_usd": price,
                        "product": product_key,
                        "contract_notional_usd": round(unit_notional, 2),
                        "sizing_fraction": frac,
                        "target_usd": round(target_usd, 2),
                        "contracts_exact": round(contracts_exact, 4),
                        "contracts_expressible": contracts_int,
                        "residual_directional_usd": round(residual_usd, 2),
                        "residual_pct_of_target": residual_pct_of_target,
                        "cb_50pct_contracts": cb_rows[0][1],
                        "cb_25pct_contracts": cb_rows[1][1],
                        "cb_50pct_expressible": cb_rows[0][1] > 0 or cb_target == 0,
                        "cb_25pct_expressible": cb_rows[1][1] > 0 or cb_rows[1][0] == 0,
                    })
    return rows


# ══ SECTION 3 — spot venue round-trip / fee-drag comparison ═════════════════

SPOT_VENUES_FOR_COMPARE = {
    "coinbase_adopted": {"maker": 0.006, "taker": 0.012,
                         "label": "Coinbase (ADOPTED, pipeline/fees.py)"},
    "bullish_standard_individual": {
        "maker": BULLISH_STANDARD_MAKER, "taker": BULLISH_STANDARD_TAKER,
        "label": "Bullish US Standard markets, Individual (worst case, both taker)"},
    **{k: {"maker": v["maker"], "taker": v["taker"], "label": v["label"]}
       for k, v in BITSTAMP_TIERS.items()},
}


def spot_compare_table() -> list[dict]:
    rows = []
    for key, venue in SPOT_VENUES_FOR_COMPARE.items():
        round_trip_pct = (venue["maker"] + venue["taker"]) * 100.0
        rows.append({
            "venue": key,
            "label": venue["label"],
            "maker_pct": round(venue["maker"] * 100.0, 4),
            "taker_pct": round(venue["taker"] * 100.0, 4),
            "round_trip_pct": round(round_trip_pct, 4),
            "m1_fee_drag_pct_yr": round(round_trip_pct * M1_ROUND_TRIPS_PER_YEAR_BTC, 4),
            "m2_fee_drag_pct_yr": round(round_trip_pct * M2_ROUND_TRIPS_PER_YEAR_BTC, 4),
        })
    return rows


# ══ SECTION 4 — carry break-even re-derivation ══════════════════════════════
# Reuses `carry_scoping.analyse_symbol` UNCHANGED: it reads whichever schedule
# key is asked for out of `carry_scoping.SPOT_SCHEDULES`, so registering new
# entries there and calling the same function cannot diverge from the already-
# reviewed carry document's arithmetic for cycles, margin events or funding.

_NEW_CARRY_SCHEDULES = {
    "bullish_standard_individual": {
        "maker": BULLISH_STANDARD_MAKER, "taker": BULLISH_STANDARD_TAKER,
        "label": "Bullish US Standard, Individual (0/0.5bps, worst case both taker)"},
    "bitstamp_10_100k": {**BITSTAMP_TIERS["bitstamp_10_100k"]},
    "bitstamp_1_10k": {**BITSTAMP_TIERS["bitstamp_1_10k"]},
}

# CROSS-VENUE TRANSFER FRICTION. Every pairing above puts the spot leg on a
# DIFFERENT company than the perp leg (Coinbase CFM), so a margin top-up needs
# an explicit transfer -- a cost the topup_friction_pct_yr term above does NOT
# price (that term is the spot-fee cost of the top-up TRADE, not the cost of
# MOVING the cash between the two companies). Two declared rails, both
# sourced from Part A's own fee-schedule facts and applied uniformly to every
# pairing (a Bitstamp-specific figure was not independently gathered for this
# task):
#
#   wire:        $30 flat -- Bullish's own published USD Fedwire / International
#                Wire / CHATS withdrawal fee (docs/research/2026-09-20-venue-
#                scoping.md Part A).
#   onchain_btc: a BTC on-chain transfer, priced at Bullish's own published BTC
#                withdrawal fee (0.00006 BTC) and the $70,000 BTC price point
#                -- the middle of Part B's own three price points. Used as ONE
#                rail for BOTH the BTC and ETH pairings: a real top-up would
#                typically convert through BTC regardless of which asset the
#                carry position itself holds, since BTC is the more liquid
#                on-chain settlement asset at both venues.
#
# The cost is FLAT PER TRANSFER, independent of position size, so it is
# amortised over the ACCOUNT's total capital (a single wire covers however
# many hedged units that account runs, since all units are triggered by the
# same underlying price move at once) -- not over one CFM contract's own
# notional, which is the denominator every other column in this table uses.
# The two are therefore NOT the same basis, and the "incl. transfer" columns
# below combine them on the explicit, declared assumption that the account
# deploys roughly its full capital into the hedge (spot notional roughly
# equal to account size) -- the same order-of-magnitude reading this
# correction's own brief uses.
TRANSFER_RAIL_WIRE_USD = 30.0
TRANSFER_RAIL_ONCHAIN_BTC_FEE_BTC = 0.00006        # Bullish's published rate
TRANSFER_RAIL_ONCHAIN_REFERENCE_PRICE_USD = 70_000  # mid of BTC_PRICE_POINTS
TRANSFER_RAILS_USD = {
    "wire": TRANSFER_RAIL_WIRE_USD,
    "onchain_btc": round(TRANSFER_RAIL_ONCHAIN_BTC_FEE_BTC
                         * TRANSFER_RAIL_ONCHAIN_REFERENCE_PRICE_USD, 2),
}

# The margin-event window/threshold the carry document's own top-up
# convention uses (7-day window, 50%-of-margin threshold), so the transfer
# count matches the event count `topup_friction_pct_yr` already prices.
_MARGIN_EVENT_WINDOW_DAYS = 7
_MARGIN_EVENT_THRESHOLD = 0.50


def _margin_events_per_year(margin_rows: list[dict], total_years: float) -> float:
    row = next((r for r in margin_rows
                if r["year"] == "full_window"
                and r["window_days"] == _MARGIN_EVENT_WINDOW_DAYS
                and r["threshold_of_margin"] == _MARGIN_EVENT_THRESHOLD), None)
    if row is None or not total_years:
        raise VenueScopingError(
            "margin-event row not found for cross-venue transfer friction")
    return row["episodes"] / total_years


def carry_rederived_rows() -> list[dict]:
    for key, sched in _NEW_CARRY_SCHEDULES.items():
        carry_scoping.SPOT_SCHEDULES.setdefault(key, sched)

    rows = []
    for symbol in carry_scoping.SYMBOLS:
        for key in _NEW_CARRY_SCHEDULES:
            result = carry_scoping.analyse_symbol(symbol, key)
            full = result["rows"][-1]
            events_per_year = _margin_events_per_year(
                result["margin_rows"], result["total_years"])
            row = {
                "symbol": symbol,
                "spot_schedule": key,
                "spot_round_trip_pct": round(
                    carry_scoping.spot_round_trip(carry_scoping.SPOT_SCHEDULES[key])
                    * 100.0, 4),
                "cost_per_cycle_pct": result["cost_per_cycle_pct"],
                "cycles_full_window": full["cycles"],
                "median_cycle_days": result["median_cycle_days"],
                "margin_events_per_year_7d_50pct": round(events_per_year, 4),
                "topup_friction_pct_yr": result["topup_friction_pct_yr"],
                "break_even_median_cycle_pct_yr": result["break_even_pct_yr"],
                "break_even_realised_rate_pct_yr": result["break_even_realised_pct_yr"],
            }
            for account in ACCOUNT_SIZES_USD:
                for rail_key, rail_cost_usd in TRANSFER_RAILS_USD.items():
                    friction_pct = events_per_year * rail_cost_usd / account * 100.0
                    incl = result["break_even_realised_pct_yr"] + friction_pct
                    row[f"transfer_friction_pct_yr_{rail_key}_{account}"] = (
                        round(friction_pct, 4))
                    row[f"break_even_realised_incl_transfer_{rail_key}_{account}"] = (
                        round(incl, 4))
            rows.append(row)
    return rows


# ══ CSV I/O, report, main ════════════════════════════════════════════════════

def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise VenueScopingError(f"no rows to write for {path}")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buf.getvalue(), encoding="utf-8")


def _csv_text(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def analyse() -> dict:
    btc_closes = load_closes("BTCUSDT")
    gaps = weekend_gaps(btc_closes)
    gap_summary = weekend_gap_summary(gaps)
    closed_hours = [m1_closed_hours_fraction(sym) for sym in ("BTCUSDT", "ETHUSDT")]
    cme_cost = cme_cost_table()
    expressibility = expressibility_table()
    spot_compare = spot_compare_table()
    carry_rows = carry_rederived_rows()
    return {
        "gaps": gaps,
        "gap_summary": gap_summary,
        "closed_hours": closed_hours,
        "cme_cost": cme_cost,
        "expressibility": expressibility,
        "spot_compare": spot_compare,
        "carry": carry_rows,
    }


def write_outputs(result: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    gap_rows = result["gaps"].to_dict("records")
    _write_csv(CSV_WEEKEND_GAPS, gap_rows)
    _write_csv(CSV_CME_CLOSED, result["closed_hours"])
    _write_csv(CSV_CME_COST, result["cme_cost"])
    _write_csv(CSV_EXPRESSIBILITY, result["expressibility"])
    _write_csv(CSV_SPOT_COMPARE, result["spot_compare"])
    _write_csv(CSV_CARRY_REDERIVED, result["carry"])
    for path in _EMITTERS_REGISTERED:
        print(f"wrote {path.relative_to(ROOT)}")


def verify_outputs(result: dict) -> bool:
    ok = True
    pairs = (
        (CSV_WEEKEND_GAPS, result["gaps"].to_dict("records")),
        (CSV_CME_CLOSED, result["closed_hours"]),
        (CSV_CME_COST, result["cme_cost"]),
        (CSV_EXPRESSIBILITY, result["expressibility"]),
        (CSV_SPOT_COMPARE, result["spot_compare"]),
        (CSV_CARRY_REDERIVED, result["carry"]),
    )
    for path, rows in pairs:
        if not path.exists():
            print(f"MISSING {path.relative_to(ROOT)}")
            ok = False
            continue
        if path.read_text(encoding="utf-8") != _csv_text(rows):
            print(f"MISMATCH {path.relative_to(ROOT)}")
            ok = False
        else:
            print(f"ok {path.relative_to(ROOT)}")
    return ok


def _print_report(result: dict) -> None:
    print("== weekend gap summary (BTC, Binance 4h proxy, 2020-2026) ==")
    for k, v in result["gap_summary"].items():
        print(f"  {k}: {v}")
    print("\n== M1 (Donchian 55/20, 4h) events inside CME closed hours ==")
    for row in result["closed_hours"]:
        print(f"  {row}")
    print("\n== CME MBT cost table ==")
    for row in result["cme_cost"]:
        print(f"  {row}")
    print("\n== spot venue round-trip comparison ==")
    for row in result["spot_compare"]:
        print(f"  {row['venue']:30s} RT {row['round_trip_pct']:8.4f}%  "
              f"M1 drag {row['m1_fee_drag_pct_yr']:8.3f}%/yr  "
              f"M2 drag {row['m2_fee_drag_pct_yr']:9.3f}%/yr")
    print("\n== carry break-even re-derivation ==")
    for row in result["carry"]:
        print(f"  {row['symbol']:9s} {row['spot_schedule']:26s} "
              f"median-cycle BE {row['break_even_median_cycle_pct_yr']:8.3f}%/yr  "
              f"realised-rate BE {row['break_even_realised_rate_pct_yr']:8.3f}%/yr")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true",
                        help="recompute and compare against committed CSVs")
    args = parser.parse_args()
    try:
        result = analyse()
    except (VenueScopingError, HydrationError, GateError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _print_report(result)
    if args.verify:
        return 0 if verify_outputs(result) else 1
    write_outputs(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
