"""
Option C scoping instrument — CFM perpetual-style futures. READ ONLY, NO ORDERS.

WHAT THIS IS
------------
A measuring tool for a venue the project does not trade and has not registered
a trial on. It answers two arithmetic questions and nothing else:

  (a) FUNDING DRAG — what a continuously-long perp position pays in funding,
      from a proxy venue's published funding history.
  (b) POWER — how many independent observations a signal at a given bar
      frequency generates on this venue, and the smallest per-trade edge that
      many observations could distinguish from zero.

WHAT IT IS NOT
--------------
It computes NO strategy return, P&L, profit factor, expectancy, equity curve,
Sharpe or asset ranking, and it simulates no rule. Every number it emits is a
property of a PRICE SERIES or a FUNDING SERIES, not of a strategy. If a
question here needed a rule to be simulated, it is not answered here.

It registers nothing, pre-registers nothing and authorizes nothing.

VENUE MISMATCH — READ BEFORE USING ANY NUMBER
---------------------------------------------
The funding and price history used below is **Binance USDT-margined perps**,
not Coinbase CFM. They are different venues:

  - Binance funds every 8h; Coinbase CFM funds every 3600s (hourly), per the
    product record's own `funding_interval` field.
  - Binance's funding level reflects Binance's basis, its own leverage
    population and its cap/floor rules. Coinbase CFM's does not have to match,
    and CFM's own history is only ~14 months deep (launch 2025-07-18), which is
    why a proxy is used at all.
  - Contract specifications, fee schedules and liquidation mechanics differ.

A proxy is used because no deeper alternative exists, not because it is
equivalent. Any pre-registration that leans on these numbers must declare them
as proxy-venue history and carry that as a stated limitation.

ESTIMATOR REUSE
---------------
rho_bar and N_eff come from `backtesting.universe_inventory` unmodified, so
this file cannot drift from the estimator the spot universe inventory used.

USAGE
    python backtesting/perps_scoping.py --funding      # funding drag table
    python backtesting/perps_scoping.py --power        # power table
    python backtesting/perps_scoping.py --products     # CFM product inventory
    python backtesting/perps_scoping.py --all          # all three, write CSVs
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.universe_inventory import (  # noqa: E402
    effective_assets,
    mean_pairwise_log_return_correlation,
)

# ── Frozen scoping parameters ────────────────────────────────────────────────
#
# Declared here rather than passed in, so re-running cannot quietly re-scope the
# measurement to a friendlier window or asset set.

PROXY_VENUE = "binance-um-perp"
PROXY_BASE = "https://data.binance.vision/data/futures/um/monthly"
PROXY_SYMBOLS = ("BTCUSDT", "ETHUSDT")

# Binance USDT-margined perps begin 2019-09; funding history published from
# 2020-01. The window starts where BOTH series exist.
HISTORY_START = "2020-01"

# One-sided 95%, the same constant the universe inventory and the standing
# policy's decidable-edge floor use.
Z_95_ONE_SIDED = 1.645

# In-position fractions the drag is reported at. Not a strategy parameter: a
# multiplier applied to a measured annual rate.
IN_POSITION_FRACTIONS = (0.25, 0.50, 0.75)

# Bars per year at each signal frequency. This is the bar count, i.e. the
# MAXIMUM number of non-overlapping observations a signal evaluated at that
# frequency can produce in a year — one per bar. A real rule is in a position
# on a minority of bars and holds for more than one, so its realised event
# count is far lower and its floor correspondingly higher. See the module
# docstring in the report for why this is an upper bound.
BARS_PER_YEAR = {"daily": 365.0, "4h": 365.0 * 6.0}

CACHE = ROOT / ".perps_scoping_cache"
OUT_CSV = ROOT / "docs" / "research" / "data" / "perps_funding_2026-09-18.csv"
OUT_PRODUCTS = (ROOT / "docs" / "research" / "data"
                / "perps_products_2026-09-18.csv")


class ScopingError(RuntimeError):
    """A measurement could not be taken."""


# ── Proxy archive access (credential-free, read-only HTTP GET) ───────────────

def _months(start: str, end_dt: datetime) -> list[str]:
    y, m = (int(x) for x in start.split("-"))
    out = []
    while (y, m) <= (end_dt.year, end_dt.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _fetch_zip_csv(url: str, cache_key: str) -> str | None:
    """
    GET one monthly archive member, cached on disk. Returns None for 404.

    404 is an expected answer here — the archive simply has no file for a month
    that has not closed yet — so it is distinguished from every other failure
    rather than swallowed with them.
    """
    CACHE.mkdir(exist_ok=True)
    cached = CACHE / f"{cache_key}.csv"
    if cached.exists():
        return cached.read_text(encoding="utf-8")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            blob = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise ScopingError(f"HTTP {exc.code} for {url}") from exc
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        text = z.read(z.namelist()[0]).decode("utf-8")
    cached.write_text(text, encoding="utf-8")
    return text


def load_funding(symbol: str, end_dt: datetime) -> pd.DataFrame:
    """Published funding history: calc_time, interval hours, realised rate."""
    frames = []
    for mon in _months(HISTORY_START, end_dt):
        url = (f"{PROXY_BASE}/fundingRate/{symbol}/"
               f"{symbol}-fundingRate-{mon}.zip")
        text = _fetch_zip_csv(url, f"fund_{symbol}_{mon}")
        if text is None:
            continue
        df = pd.read_csv(io.StringIO(text))
        if "calc_time" not in df.columns:      # some months ship headerless
            df = pd.read_csv(io.StringIO(text), header=None,
                             names=["calc_time", "funding_interval_hours",
                                    "last_funding_rate"])
        frames.append(df)
    if not frames:
        raise ScopingError(f"no funding history retrieved for {symbol}")
    out = pd.concat(frames, ignore_index=True)
    out = out[pd.to_numeric(out["calc_time"], errors="coerce").notna()]
    out["calc_time"] = pd.to_numeric(out["calc_time"])
    out["last_funding_rate"] = pd.to_numeric(out["last_funding_rate"],
                                             errors="coerce")
    out = out.dropna(subset=["last_funding_rate"])
    out["ts"] = pd.to_datetime(out["calc_time"], unit="ms", utc=True)
    out["interval_h"] = pd.to_numeric(out["funding_interval_hours"],
                                      errors="coerce").fillna(8.0)
    return out.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)


def load_klines(symbol: str, interval: str, end_dt: datetime) -> pd.Series:
    """Close series at `interval`, UTC-indexed."""
    frames = []
    for mon in _months(HISTORY_START, end_dt):
        url = (f"{PROXY_BASE}/klines/{symbol}/{interval}/"
               f"{symbol}-{interval}-{mon}.zip")
        text = _fetch_zip_csv(url, f"kl_{symbol}_{interval}_{mon}")
        if text is None:
            continue
        df = pd.read_csv(io.StringIO(text), header=None, usecols=[0, 4],
                         names=["open_time", "close"])
        df = df[pd.to_numeric(df["open_time"], errors="coerce").notna()]
        frames.append(df)
    if not frames:
        raise ScopingError(f"no {interval} klines retrieved for {symbol}")
    out = pd.concat(frames, ignore_index=True)
    out["open_time"] = pd.to_numeric(out["open_time"])
    # Binance switched open_time from ms to us in some 2025 archives.
    unit = "us" if out["open_time"].max() > 2_000_000_000_000_0 else "ms"
    idx = pd.to_datetime(out["open_time"], unit=unit, utc=True)
    s = pd.Series(pd.to_numeric(out["close"], errors="coerce").values, index=idx)
    return s.dropna().sort_index()[lambda x: ~x.index.duplicated()]


# ── (a) Funding drag ─────────────────────────────────────────────────────────

def funding_stats(df: pd.DataFrame) -> dict:
    """
    Annualised funding paid by a CONTINUOUSLY LONG position, as a percentage of
    POSITION NOTIONAL.

    Sign convention, stated because it is the whole meaning of the number: a
    positive published rate is paid BY longs TO shorts. A negative rate is
    received by the long. `mean_annualised_pct` is therefore the net cost of
    being long through every interval in the sample, and it is already net of
    the intervals the long was paid.

    Percentiles are of the ANNUALISED rate of individual intervals — the cost
    of being long through a typical interval, extrapolated. They are not a
    distribution of annual outcomes, and the high percentiles must not be read
    as "what a bad year costs".
    """
    rate = df["last_funding_rate"].to_numpy(float)
    per_year = 24.0 / df["interval_h"].to_numpy(float)   # intervals per day...
    per_year = per_year * 365.0
    ann = rate * per_year                                 # ...annualised
    return {
        "intervals": int(len(rate)),
        "first": str(df["ts"].iloc[0].date()),
        "last": str(df["ts"].iloc[-1].date()),
        "pct_intervals_long_pays": round(100.0 * float((rate > 0).mean()), 2),
        "mean_annualised_pct": round(100.0 * float(ann.mean()), 4),
        "median_annualised_pct": round(100.0 * float(np.median(ann)), 4),
        "p10_annualised_pct": round(100.0 * float(np.percentile(ann, 10)), 4),
        "p25_annualised_pct": round(100.0 * float(np.percentile(ann, 25)), 4),
        "p75_annualised_pct": round(100.0 * float(np.percentile(ann, 75)), 4),
        "p90_annualised_pct": round(100.0 * float(np.percentile(ann, 90)), 4),
        "p99_annualised_pct": round(100.0 * float(np.percentile(ann, 99)), 4),
    }


def funding_by_year(df: pd.DataFrame) -> list[dict]:
    rows = []
    for year, grp in df.groupby(df["ts"].dt.year):
        st = funding_stats(grp)
        st["year"] = int(year)
        rows.append(st)
    return rows


# ── (b) Power ────────────────────────────────────────────────────────────────

def per_bar_sd_pct(close: pd.Series) -> float:
    """
    Sample SD of one-bar log returns, in percent.

    THE DERIVATION, stated because the number is only meaningful with it: a
    "trade" here is modelled as entering at one bar's open and leaving at the
    next — the shortest hold a signal at that frequency can express. Its return
    dispersion is then exactly the dispersion of one-bar returns, which is a
    property of the price series and requires no rule.

    A real rule holds for MORE than one bar, and dispersion grows with holding
    time (roughly as sqrt(t) absent autocorrelation). So this SD is a LOWER
    bound on per-trade dispersion, every floor computed from it is a LOWER
    bound on the true floor, and the power conclusions below are therefore the
    optimistic end.
    """
    r = np.log(close).diff().dropna()
    return float(r.std(ddof=1) * 100.0)


def decidable_floor_pct(sd_pct: float, n: float) -> float:
    """1.645 * SD / sqrt(n) — the standing policy's floor, unchanged."""
    return Z_95_ONE_SIDED * sd_pct / np.sqrt(n)


# ── CFM product inventory (credential-free) ──────────────────────────────────

def cfm_perp_inventory() -> list[dict]:
    """
    Every CFM perpetual-style product, from the PUBLIC products endpoint.

    Credential-free on purpose: the contract specs, margin rates and current
    funding rate are all on the public record, so this inventory is
    reproducible by anyone without touching an account. The authenticated
    endpoints add nothing to it.

    `holdable_overnight_at_cap` answers one arithmetic question and no other:
    is one contract's overnight margin requirement within the cap the spot
    pipeline enforces? It sizes nothing and recommends nothing.
    """
    from coinbase.rest import RESTClient
    from pipeline.sizing import live_balance_usd

    cap = live_balance_usd()
    pub = RESTClient()
    resp = pub.get_public_products(product_type="FUTURE")
    data = resp.to_dict() if hasattr(resp, "to_dict") else dict(vars(resp))

    rows = []
    for prod in data.get("products", []):
        det = prod.get("future_product_details") or {}
        if "perp" not in (det.get("group_description") or "").lower():
            continue
        price = float(prod.get("price") or 0)
        size = float(det.get("contract_size") or 0)
        notional = price * size
        om = det.get("overnight_margin_rate") or {}
        im = det.get("intraday_margin_rate") or {}
        om_long = float(om.get("long_margin_rate") or 0) or None
        im_long = float(im.get("long_margin_rate") or 0) or None
        fr = det.get("funding_rate")
        interval_s = float((det.get("funding_interval") or "0s").rstrip("s") or 0)
        ann = (float(fr) * (31_536_000.0 / interval_s) * 100.0
               if fr not in (None, "") and interval_s else None)
        rows.append({
            "product_id": prod["product_id"],
            "display_name": prod.get("display_name"),
            "underlying": det.get("contract_root_unit"),
            "contract_size": det.get("contract_size"),
            "price": prod.get("price"),
            "notional_per_contract_usd": round(notional, 2),
            "tick_price_increment": prod.get("price_increment"),
            "quote_increment": prod.get("quote_increment"),
            "min_order_contracts": prod.get("base_min_size"),
            "max_order_contracts": prod.get("base_max_size"),
            "contract_expiry": det.get("contract_expiry"),
            "contract_expiry_type": det.get("contract_expiry_type"),
            "funding_interval": det.get("funding_interval"),
            "funding_rate_now": fr,
            "funding_rate_now_annualised_pct": None if ann is None else round(ann, 3),
            "intraday_long_margin_rate": im.get("long_margin_rate"),
            "intraday_max_leverage": None if not im_long else round(1 / im_long, 2),
            "overnight_long_margin_rate": om.get("long_margin_rate"),
            "overnight_short_margin_rate": om.get("short_margin_rate"),
            "overnight_max_leverage_long": None if not om_long else round(1 / om_long, 2),
            "overnight_long_margin_usd_per_contract":
                None if not om_long else round(notional * om_long, 2),
            "holdable_overnight_at_cap":
                None if not om_long else bool(notional * om_long <= cap),
            "cap_usd": cap,
            "open_interest_contracts": det.get("open_interest"),
            "approx_quote_24h_volume_usd": prod.get("approximate_quote_24h_volume"),
            "twenty_four_by_seven": det.get("twenty_four_by_seven"),
            "venue": det.get("venue"),
            "risk_managed_by": det.get("risk_managed_by"),
        })
    rows.sort(key=lambda r: -(float(r["approx_quote_24h_volume_usd"] or 0)))
    return rows


# ── Reporting ────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--funding", action="store_true")
    ap.add_argument("--power", action="store_true")
    ap.add_argument("--products", action="store_true")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if not (args.funding or args.power or args.products or args.all):
        args.all = True

    end_dt = datetime.now(timezone.utc) - timedelta(days=1)
    print(f"PROXY VENUE: {PROXY_VENUE} (NOT Coinbase CFM — see module docstring)")
    print(f"symbols: {', '.join(PROXY_SYMBOLS)}   history from {HISTORY_START}\n")

    csv_rows = []

    if args.products or args.all:
        rows = cfm_perp_inventory()
        cap = rows[0]["cap_usd"] if rows else None
        holdable = [r for r in rows if r["holdable_overnight_at_cap"]]
        print("=" * 72)
        print(f"CFM PERPETUAL-STYLE PRODUCTS (public endpoint) — {len(rows)} listed")
        print(f"holdable overnight, 1 contract, within the ${cap:g} cap: "
              f"{len(holdable)} of {len(rows)}")
        print("=" * 72)
        print(f"{'product':<20}{'name':<16}{'notional$':>11}{'ovn marg$':>11}"
              f"{'lev':>6}{'fund%/yr':>10}  fits")
        for r in rows:
            print(f"{r['product_id']:<20}{str(r['display_name']):<16}"
                  f"{r['notional_per_contract_usd']:>11,.2f}"
                  f"{(r['overnight_long_margin_usd_per_contract'] or 0):>11,.2f}"
                  f"{(r['overnight_max_leverage_long'] or 0):>6.2f}"
                  f"{(r['funding_rate_now_annualised_pct'] or 0):>10.2f}"
                  f"  {'yes' if r['holdable_overnight_at_cap'] else 'NO'}")
        OUT_PRODUCTS.parent.mkdir(parents=True, exist_ok=True)
        with OUT_PRODUCTS.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {OUT_PRODUCTS.relative_to(ROOT)}")

    if args.funding or args.all:
        print("=" * 72)
        print("(a) FUNDING DRAG — annualised, % of POSITION NOTIONAL, long side")
        print("=" * 72)
        for sym in PROXY_SYMBOLS:
            df = load_funding(sym, end_dt)
            overall = funding_stats(df)
            print(f"\n{sym}  n={overall['intervals']} intervals  "
                  f"{overall['first']} -> {overall['last']}")
            print(f"  long pays in {overall['pct_intervals_long_pays']}% of intervals")
            print(f"  mean   {overall['mean_annualised_pct']:>8.3f}%/yr")
            print(f"  median {overall['median_annualised_pct']:>8.3f}%/yr")
            print(f"  p10 {overall['p10_annualised_pct']:.3f}  "
                  f"p25 {overall['p25_annualised_pct']:.3f}  "
                  f"p75 {overall['p75_annualised_pct']:.3f}  "
                  f"p90 {overall['p90_annualised_pct']:.3f}  "
                  f"p99 {overall['p99_annualised_pct']:.3f}")
            row = dict(overall); row["symbol"] = sym; row["scope"] = "overall"
            csv_rows.append(row)
            print("  by year:")
            for yr in funding_by_year(df):
                print(f"    {yr['year']}  n={yr['intervals']:>4}  "
                      f"mean {yr['mean_annualised_pct']:>8.3f}%/yr  "
                      f"median {yr['median_annualised_pct']:>8.3f}%/yr  "
                      f"long pays {yr['pct_intervals_long_pays']:>5.1f}% of intervals")
                r = dict(yr); r["symbol"] = sym; r["scope"] = f"year-{yr['year']}"
                csv_rows.append(r)
            print("  drag at in-position fraction X (mean rate x X):")
            for x in IN_POSITION_FRACTIONS:
                print(f"    X={int(x*100)}%  -> "
                      f"{overall['mean_annualised_pct'] * x:.3f}%/yr of notional")

    if args.power or args.all:
        print("\n" + "=" * 72)
        print("(b) POWER — structural, no rule simulated")
        print("=" * 72)
        closes_d = {s: load_klines(s, "1d", end_dt) for s in PROXY_SYMBOLS}
        lo = max(s.index.min() for s in closes_d.values())
        hi = min(s.index.max() for s in closes_d.values())
        window = {"start": lo, "end": hi,
                  "years": (hi - lo).days / 365.25}
        corr = mean_pairwise_log_return_correlation(closes_d,
                                                    list(PROXY_SYMBOLS), window)
        n_eff = effective_assets(len(PROXY_SYMBOLS), corr["rho_bar"])
        print(f"overlap {lo.date()} -> {hi.date()}  ({window['years']:.2f} y)")
        print(f"rho_bar (daily log returns, BTC vs ETH perp) = {corr['rho_bar']}")
        print(f"N_eff = {n_eff}   (N={len(PROXY_SYMBOLS)}, "
              f"estimator reused from universe_inventory)")

        sds = {"daily": np.mean([per_bar_sd_pct(s) for s in closes_d.values()])}
        closes_4h = {s: load_klines(s, "4h", end_dt) for s in PROXY_SYMBOLS}
        sds["4h"] = float(np.mean([per_bar_sd_pct(s) for s in closes_4h.values()]))
        print("\nper-bar SD of log returns (mean of the two symbols):")
        for k, v in sds.items():
            print(f"  {k:<6} {v:.4f}%")

        print("\nfloors  = 1.645 * SD / sqrt(n),  n = N_eff * bars_per_year * years")
        print(f"{'freq':<6} {'years':>6} {'n':>12} {'floor %/trade':>15}")
        for freq, bpy in BARS_PER_YEAR.items():
            for years in (1, 2, 3):
                n = n_eff * bpy * years
                f = decidable_floor_pct(sds[freq], n)
                print(f"{freq:<6} {years:>6} {n:>12.0f} {f:>15.4f}")
                csv_rows.append({
                    "symbol": "BTC+ETH",
                    "scope": f"power-{freq}-{years}y",
                    "observations_n": round(n),
                    "per_bar_sd_pct": round(sds[freq], 4),
                    "rho_bar": corr["rho_bar"],
                    "n_eff": n_eff,
                    "decidable_edge_floor_pct": round(f, 4),
                })

    if args.all and csv_rows:
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        cols = sorted({k for r in csv_rows for k in r})
        with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["symbol", "scope"] +
                               [c for c in cols if c not in ("symbol", "scope")])
            w.writeheader()
            w.writerows(csv_rows)
        print(f"\nwrote {OUT_CSV.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
