"""
Phase 7R data/power inventory — Coinbase spot universe, public API only.

WHAT THIS IS
------------
A data-availability and statistical-power inventory for a *possible* future
broad-universe trend pre-registration. It answers two questions only:

  1. What USD spot universe does Coinbase actually list, and how much history
     / liquidity does each pair have?
  2. If STF-CLOSE-55-20 (the frozen 7B rule in `stf_protocol.py`) were run
     across a candidate universe, how many EXPOSURE CLUSTERS would it
     produce, how many EFFECTIVE independent assets does the universe hold
     once cross-asset correlation is accounted for, and what per-trade edge
     could a sample that size distinguish from zero at one-sided 95%?

WHAT THIS IS NOT
----------------
This script computes NO strategy return, profit factor, expectancy, Sharpe,
equity curve, or per-asset performance ranking. Event counting reuses
`backtesting.stf_feasibility.entry_exit_events`, which is structurally blind
(prices go in, timestamps and ENTRY/EXIT labels come out — see that module's
docstring and `tests/test_stf_feasibility.py::test_events_carry_no_price_information`).

Exactly one function here consults price MOVEMENT rather than price levels:
`mean_pairwise_log_return_correlation`. Cross-asset correlation is a
structural property of the universe — it says how much the names repeat each
other, not how much any rule earns — and it cannot rank assets or select a
strategy. The function is written so that it cannot do either: it takes
closes and returns one mean scalar plus the window it was measured on. No
per-asset correlation and no return series of any kind leaves it.
`tests/test_universe_inventory.py` asserts that structurally, and asserts
that the module names no P&L, profit-factor, expectancy or Sharpe quantity
anywhere.

The "decidable edge floor" in DELIVERABLE 3 is NOT a measurement of this
universe. It borrows the per-trade dispersion (SD) already published in
`docs/research/2026-09-cost-sensitivity.md` section 6 — measured on the
FROZEN, SINGLE-ASSET V2 ZEC mechanism, a different rule with a hard stop —
and asks a purely arithmetic question: "if a future sample here had that much
scatter, how small an edge could a sample this size bound?" That is a
statement about statistical resolving power, not a claim about what this
universe would earn.

WHAT THE CLUSTER COUNT CAN AND CANNOT SAY
------------------------------------------
A cluster here is a span of portfolio exposure separated from the next by at
least CLUSTER_GAP_DAYS on which EVERY asset in the universe is flat. On a
wide universe that condition is nearly unreachable, so the count saturates BY
CONSTRUCTION: it measures how often the whole book happened to go quiet, not
how many independent episodes the data holds. It is reported as one end of a
bracket, never as evidence that breadth does or does not add independent
events. The other end is the correlation-adjusted pooled count built from
`effective_assets`, and the truth sits between them.

FROZEN INPUTS (imported, never redefined)
------------------------------------------
`ENTRY_LOOKBACK`, `EXIT_LOOKBACK`, `CLUSTER_GAP_DAYS` come from
`backtesting.stf_protocol`, unmodified. `entry_exit_events` comes from
`backtesting.stf_feasibility`, unmodified.

DEVIATION FROM THE 7R AUDIT'S CLUSTERING, AND WHY
--------------------------------------------------
`stf_feasibility.portfolio_structure` requires a single COMMON start date
(the latest first-eligible date across its fixed 4-asset universe), because
that frozen trial design holds all 4 assets from day one. A broad candidate
universe has staggered listing dates by construction (that is the entire
reason breadth is being considered) — forcing a common start across 50-150
assets would collapse the usable window to whatever the newest listing
allows, which is not a property of the universe, it is an artifact of
imposing the wrong join. This script instead computes, PER ASSET, the
in-position flag from that asset's OWN first-eligible date (first candle +
ENTRY_LOOKBACK days) to the end of history, and unions those flags across all
qualifying assets before clustering. This is a universe-inventory modeling
choice, not a change to the frozen trial design, and it is not a trial
registration — see `docs/trial_registry.md` for what pre-registration means
here.

NETWORK
-------
Public, unauthenticated Coinbase endpoints only (`RESTClient()` with no key
file — same contract as `hydrate_research_data.py`). No credentials, no
Anthropic API calls. This script does not touch `data/candles/`, the research
manifest, or any committed artifact; it holds candle data in memory only for
the duration of the run.

Usage:
    python backtesting/universe_inventory.py                  # full pull + analysis
    python backtesting/universe_inventory.py --analysis-only  # reuse committed CSV

`--analysis-only` re-reads the committed inventory CSV instead of re-walking
all 407 pairs, and fetches daily closes only for the pairs the candidate
rules select. Every series is truncated at ANALYSIS_END, the as-of date the
committed CSV was collected on, so the analysis does not drift as the
exchange appends new bars.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.stf_feasibility import entry_exit_events  # noqa: E402
from backtesting.stf_protocol import (  # noqa: E402
    CLUSTER_GAP_DAYS,
    ENTRY_LOOKBACK,
)

OUT_DIR = ROOT / "docs" / "research" / "data"
RUN_DATE = "2026-09-17"
CSV_PATH = OUT_DIR / f"universe_inventory_{RUN_DATE}.csv"
MD_PATH = OUT_DIR / f"universe_inventory_{RUN_DATE}.md"
PROGRESS_LOG = ROOT / "backtesting" / "_universe_inventory_progress.log"

_WINDOW_DAYS = 300  # stays under Coinbase's 350-candle cap with margin
_FLOOR_TS = 1420070400  # 2015-01-01 UTC — well before Coinbase's earliest USD spot listing
_SLEEP_S = 0.12

# The as-of date the committed CSV was collected on. Every close series is
# truncated here, so re-running the analysis tomorrow does not silently
# restate the numbers against a longer window than the CSV describes.
ANALYSIS_END = pd.Timestamp("2026-09-18", tz="UTC")

# Borrowed dispersion (NOT measured here) — docs/research/2026-09-cost-sensitivity.md
# section 6, per-trade net-return SD (% of position) on the frozen, single-asset
# V2 ZEC mechanism, n=114. A different rule; used only to size resolving power.
_BORROWED_SD_PCT = {
    "frozen_1.0pct_model": 5.1095,
    "adopted_0.6_1.2pct_measured": 4.9969,
    "candidate_0.5_0.9pct": 5.0121,
}
_Z_95_ONE_SIDED = 1.645


def _public_client():
    """Unauthenticated client — no key file, same contract as hydrate_research_data.py."""
    from coinbase.rest import RESTClient
    return RESTClient()


def fetch_products(client) -> pd.DataFrame:
    """All SPOT products quoted in USD, exactly as the public endpoint returns them."""
    resp = client.get_public_products(product_type="SPOT", get_all_products=True)
    rows = []
    for p in resp.products:
        if p.quote_currency_id != "USD":
            continue
        rows.append({
            "product_id": p.product_id,
            "status": p.status,
            "trading_disabled": bool(p.trading_disabled),
            "quote_increment": p.quote_increment,
            "base_increment": p.base_increment,
            "base_min_size": p.base_min_size,
            "min_market_funds_usd": p.quote_min_size,  # Advanced Trade's quote_min_size
                                                         # is the direct successor of the
                                                         # old Coinbase Pro "min_market_funds"
                                                         # field: minimum order size in the
                                                         # quote currency, which is USD here.
        })
    return pd.DataFrame(rows).sort_values("product_id").reset_index(drop=True)


def walk_back_daily_closes(client, product_id: str) -> tuple[pd.Series, int]:
    """
    Fetch the FULL daily close history of one product via public candles,
    walking backward in <350-day windows (Coinbase's per-request cap).

    Returns (close_series_indexed_by_utc_date, api_call_count). The same
    fetch serves two purposes: DELIVERABLE 1's "first daily candle" /
    "days of history", and DELIVERABLE 3's event counting — one pass of
    requests, not two.
    """
    now = int(time.time())
    end = now
    start = end - _WINDOW_DAYS * 86400
    all_rows: list[dict] = []
    n_calls = 0

    while True:
        resp = client.get_public_candles(
            product_id=product_id, start=str(start), end=str(end),
            granularity="ONE_DAY",
        )
        n_calls += 1
        candles = resp.candles or []
        if not candles:
            break
        rows = sorted(candles, key=lambda c: int(c.start))
        all_rows = rows + all_rows
        # A response shorter than the requested window means the window's
        # start preceded the asset's listing — the earliest candle in THIS
        # response is the true first daily candle. No bisection needed.
        if len(candles) < _WINDOW_DAYS - 2:
            break
        if start <= _FLOOR_TS:
            break
        end = start - 86400
        start = end - _WINDOW_DAYS * 86400
        time.sleep(_SLEEP_S)

    if not all_rows:
        return pd.Series(dtype="float64"), n_calls

    idx = pd.to_datetime([int(r.start) for r in all_rows], unit="s", utc=True)
    closes = pd.Series([float(r.close) for r in all_rows], index=idx)
    closes = closes[~closes.index.duplicated(keep="first")].sort_index()
    # Freeze the right edge at the CSV's as-of date (see ANALYSIS_END).
    return closes[closes.index <= ANALYSIS_END], n_calls


def median_volume_usd_30d(client, product_id: str) -> float | None:
    """
    Median USD volume over the trailing 30 daily bars. Fetched separately from
    `walk_back_daily_closes` (which only returns close, not volume) so the
    event-counting path stays minimal and auditable.
    """
    now = int(time.time())
    resp = client.get_public_candles(
        product_id=product_id, start=str(now - 30 * 86400), end=str(now),
        granularity="ONE_DAY",
    )
    candles = resp.candles or []
    if not candles:
        return None
    usd = [float(c.volume) * float(c.close) for c in candles]
    if not usd:
        return None
    return float(pd.Series(usd).median())


def build_inventory() -> pd.DataFrame:
    client = _public_client()
    products = fetch_products(client)
    as_of = pd.Timestamp.now(tz="UTC").normalize()

    records = []
    series_by_asset: dict[str, pd.Series] = {}
    total = len(products)

    with open(PROGRESS_LOG, "w", encoding="utf-8") as log:
        for i, row in products.iterrows():
            pid = row["product_id"]
            t0 = time.time()
            closes, n_calls = walk_back_daily_closes(client, pid)
            vol = median_volume_usd_30d(client, pid)
            time.sleep(_SLEEP_S)

            if closes.empty:
                first_candle = None
                days_history = 0
            else:
                first_candle = closes.index[0]
                days_history = int((as_of - first_candle).days)
                series_by_asset[pid] = closes

            records.append({
                "product_id": pid,
                "status": row["status"],
                "trading_disabled": row["trading_disabled"],
                "first_daily_candle": first_candle.date().isoformat() if first_candle is not None else None,
                "days_history": days_history,
                "median_30d_volume_usd": round(vol, 2) if vol is not None else None,
                "quote_increment": row["quote_increment"],
                "min_market_funds_usd": row["min_market_funds_usd"],
            })
            msg = (f"[{i+1}/{total}] {pid}: {days_history}d history, "
                   f"{n_calls} calls, {round(time.time()-t0, 1)}s")
            print(msg)
            log.write(msg + "\n")
            log.flush()

    df = pd.DataFrame(records)
    return df, series_by_asset, as_of


# ── Candidate universe rules (proposed, NOT selected — no price behaviour used) ──

def candidate_rules(df: pd.DataFrame) -> dict[str, pd.Series]:
    """
    Boolean masks over the inventory only (history length + liquidity).
    No return, no momentum, no price-behaviour signal is read here.
    """
    listed = (df["status"] == "online") & (~df["trading_disabled"])
    years = df["days_history"] / 365.25
    vol = df["median_30d_volume_usd"].fillna(0.0)

    return {
        "A: >=3y history AND median 30d vol >= $1,000,000": listed & (years >= 3.0) & (vol >= 1_000_000),
        "B: >=2y history AND median 30d vol >= $250,000": listed & (years >= 2.0) & (vol >= 250_000),
        "C: >=1y history AND median 30d vol >= $5,000,000": listed & (years >= 1.0) & (vol >= 5_000_000),
    }


def load_inventory() -> pd.DataFrame:
    """Re-read the committed CSV instead of re-walking all 407 pairs."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"no committed inventory at {CSV_PATH}; run without --analysis-only")
    return pd.read_csv(CSV_PATH)


def fetch_closes_for(assets: list[str]) -> dict[str, pd.Series]:
    """Daily closes for just the pairs the candidate rules select."""
    client = _public_client()
    out: dict[str, pd.Series] = {}
    for i, asset in enumerate(sorted(assets), start=1):
        closes, n_calls = walk_back_daily_closes(client, asset)
        if not closes.empty:
            out[asset] = closes
        print(f"  [{i}/{len(assets)}] {asset}: {len(closes)} bars, {n_calls} calls")
        time.sleep(_SLEEP_S)
    return out


# ── Deliverable 3: mechanism-agnostic event/cluster counting ──────────────────

def clusters_for_universe(series_by_asset: dict[str, pd.Series],
                          asset_ids: list[str]) -> list[dict]:
    """
    Union, across all qualifying assets, of each asset's own in-position flag
    (from ITS OWN first-eligible date, not a universe-wide common start), then
    cluster the union with gap >= CLUSTER_GAP_DAYS. Reuses
    `entry_exit_events` — the same blind function `stf_feasibility.py` uses —
    unmodified. Returns cluster spans only: no P&L, no trade valuation.

    READ THE MODULE DOCSTRING BEFORE QUOTING THIS COUNT. A gap requires EVERY
    asset in the universe to be flat for CLUSTER_GAP_DAYS at once, so on a
    wide universe the count saturates by construction and says nothing on its
    own about how many independent episodes the data holds.
    """
    if not asset_ids:
        return []

    held: dict[str, pd.Series] = {}
    for asset in asset_ids:
        closes = series_by_asset.get(asset)
        if closes is None or len(closes) <= ENTRY_LOOKBACK:
            continue
        first_eligible = closes.index[ENTRY_LOOKBACK]
        events = entry_exit_events(closes, evaluation_start=first_eligible)
        # events -> spans -> a boolean "in position" series over this asset's
        # own index from first_eligible onward. Mirrors
        # stf_feasibility._spans / _in_position_days; no price logic is
        # reimplemented, only the timestamps are walked.
        spans: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        open_entry = None
        for ev in events:
            ts = pd.Timestamp(ev["ts"])
            if ev["event"] == "ENTRY":
                open_entry = ts
            elif open_entry is not None:
                spans.append((open_entry, ts))
                open_entry = None
        if open_entry is not None:
            spans.append((open_entry, closes.index[-1]))

        idx = closes.index[closes.index >= first_eligible]
        flag = pd.Series(False, index=idx)
        for s, e in spans:
            flag.loc[(flag.index >= s) & (flag.index <= e)] = True
        held[asset] = flag

    if not held:
        return []

    full_index = pd.DatetimeIndex(sorted(set().union(*[h.index for h in held.values()])))
    any_open = pd.Series(False, index=full_index)
    for flag in held.values():
        any_open.loc[flag.index[flag]] = True

    exposed_days = any_open[any_open].index
    if len(exposed_days) == 0:
        return []

    out, start, prev = [], exposed_days[0], exposed_days[0]
    for day in exposed_days[1:]:
        if (day - prev).days > CLUSTER_GAP_DAYS:
            out.append({"start": start.isoformat(), "end": prev.isoformat()})
            start = day
        prev = day
    out.append({"start": start.isoformat(), "end": prev.isoformat()})
    return out


def common_overlap_window(series_by_asset: dict[str, pd.Series],
                          asset_ids: list[str]) -> dict:
    """
    The span on which EVERY asset in the rule has data: latest first bar to
    earliest last bar. Timestamps and lengths only.
    """
    present = [series_by_asset[a] for a in asset_ids if a in series_by_asset]
    if not present:
        return {"start": None, "end": None, "days": 0, "years": 0.0}
    start = max(s.index[0] for s in present)
    end = min(s.index[-1] for s in present)
    days = int((end - start).days) + 1
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": days,
        "years": round(days / 365.25, 2),
    }


def mean_pairwise_log_return_correlation(series_by_asset: dict[str, pd.Series],
                                         asset_ids: list[str],
                                         window: dict) -> dict:
    """
    rho_bar: the mean pairwise Pearson correlation of DAILY LOG RETURNS across
    the rule's assets, over their common overlap window.

    THE RETURN TYPE IS THE CONSTRAINT. This is the only function in the module
    that consults price movement, and it hands back scalars: the mean
    correlation, the pair count, and the window it was measured on. No
    per-asset correlation, no per-asset return series, and no return series of
    any kind crosses this boundary, so nothing downstream can rank assets,
    build an equity curve, or compute a P&L from what it returns. Correlation
    is reported because it is a structural property of the UNIVERSE — how much
    the names repeat one another — not a property of any strategy.
    """
    present = [a for a in asset_ids if a in series_by_asset]
    if len(present) < 2 or window["start"] is None:
        return {"rho_bar": None, "pairs": 0, "assets": len(present),
                "observations": 0}

    lo, hi = pd.Timestamp(window["start"]), pd.Timestamp(window["end"])
    frame = pd.DataFrame({
        a: series_by_asset[a][(series_by_asset[a].index >= lo)
                              & (series_by_asset[a].index <= hi)]
        for a in present
    }).sort_index()

    # np.log on a price frame, differenced: daily log returns, used ONLY to
    # form the correlation matrix below. They are local to this function.
    log_returns = np.log(frame).diff().dropna(how="all")
    corr = log_returns.corr(min_periods=30)

    upper = corr.where(np.triu(np.ones(corr.shape, dtype=bool), k=1))
    values = upper.stack()
    if values.empty:
        return {"rho_bar": None, "pairs": 0, "assets": len(present),
                "observations": int(len(log_returns))}

    return {
        "rho_bar": round(float(values.mean()), 4),
        "pairs": int(len(values)),
        "assets": len(present),
        "observations": int(len(log_returns)),
    }


def effective_assets(n_assets: int, rho_bar: float | None) -> float | None:
    """
    N_eff = N / (1 + (N - 1) * rho_bar) — the usual correlated-average
    effective sample size. At rho_bar = 0 it returns N; as rho_bar -> 1 it
    collapses toward 1, which is the point: correlated names are not
    independent names.
    """
    if rho_bar is None or n_assets <= 0:
        return None
    denom = 1.0 + (n_assets - 1) * rho_bar
    if denom <= 0:
        return None
    return round(n_assets / denom, 3)


def mean_entry_events_per_asset_year(series_by_asset: dict[str, pd.Series],
                                     asset_ids: list[str],
                                     window: dict) -> float | None:
    """
    Mean number of 55/20 ENTRY events per asset per year over the common
    overlap window, from the same blind `entry_exit_events`. Counts labels;
    reads no price out of them, and reports only the mean across assets — not
    a per-asset table, which could be read as a ranking.
    """
    present = [a for a in asset_ids if a in series_by_asset]
    if not present or window["start"] is None or window["years"] <= 0:
        return None

    lo = pd.Timestamp(window["start"])
    rates = []
    for asset in present:
        closes = series_by_asset[asset]
        # Rolling levels warm up on each asset's own earlier history where it
        # has any; the state machine still starts flat at `lo`.
        events = entry_exit_events(closes, evaluation_start=lo)
        entries = sum(1 for e in events if e["event"] == "ENTRY")
        rates.append(entries / window["years"])
    return round(float(sum(rates) / len(rates)), 3)


def decidable_edge_floor(n_events: float | None) -> dict[str, float | None]:
    """
    1.645 * SD / sqrt(n) for each borrowed SD scenario — the smallest
    per-trade edge a sample of `n_events` could separate from zero at
    one-sided 95%, IF it scattered like the borrowed SD.
    """
    if not n_events or n_events <= 0:
        return {k: None for k in _BORROWED_SD_PCT}
    return {
        k: round(_Z_95_ONE_SIDED * sd / (n_events ** 0.5), 4)
        for k, sd in _BORROWED_SD_PCT.items()
    }


def analyse(df: pd.DataFrame, series_by_asset: dict[str, pd.Series]) -> list[dict]:
    """Everything Deliverable 3 reports, per candidate rule."""
    out = []
    for name, mask in candidate_rules(df).items():
        assets = df.loc[mask, "product_id"].tolist()
        clusters = clusters_for_universe(series_by_asset, assets)
        window = common_overlap_window(series_by_asset, assets)
        corr = mean_pairwise_log_return_correlation(series_by_asset, assets, window)
        n_eff = effective_assets(len(assets), corr["rho_bar"])
        rate = mean_entry_events_per_asset_year(series_by_asset, assets, window)

        pooled = (n_eff * rate * window["years"]
                  if (n_eff and rate and window["years"]) else None)

        out.append({
            "rule": name,
            "pairs": len(assets),
            "clusters": len(clusters),
            "overlap": window,
            "rho_bar": corr["rho_bar"],
            "corr_pairs": corr["pairs"],
            "corr_observations": corr["observations"],
            "n_eff": n_eff,
            "entries_per_asset_year": rate,
            "pooled_n": round(pooled, 2) if pooled else None,
            "cluster_floor": decidable_edge_floor(len(clusters)),
            "pooled_floor": decidable_edge_floor(pooled),
        })
    return out


def _floor_range(floor: dict[str, float | None]) -> str:
    vals = [v for v in floor.values() if v is not None]
    if not vals:
        return "n/a"
    return f"{min(vals):.2f}–{max(vals):.2f}%"


def write_markdown(df: pd.DataFrame, summary: list[dict], as_of: pd.Timestamp) -> None:
    delisted = df[(df["status"] == "delisted") | (df["trading_disabled"])]
    L: list[str] = []

    L.append(f"# Coinbase spot universe inventory — {RUN_DATE}\n")
    L.append(
        "READ-ONLY inventory. No strategy return, PF, Sharpe, expectancy, "
        "equity curve, or asset performance ranking is computed anywhere in "
        "this document or its source script "
        "(`backtesting/universe_inventory.py`). This is not a "
        "pre-registration and selects no universe, trial ID, or parameter.\n")

    L.append("## Deliverable 1 — universe inventory\n")
    L.append(f"- As-of date: {as_of.date().isoformat()}")
    L.append("- Source: Coinbase public REST API (`get_public_products`, "
             "`get_public_candles`), no credentials")
    L.append(f"- CSV: `{CSV_PATH.relative_to(ROOT).as_posix()}`")
    L.append(f"- Total USD spot pairs returned by the public endpoint: {len(df)}")
    L.append(f"  - status=online, trading enabled: "
             f"{int(((df['status']=='online') & ~df['trading_disabled']).sum())}")
    L.append(f"  - status=delisted or trading_disabled (still visible on this "
             f"endpoint, excluded from candidate rules below): {len(delisted)} "
             f"({', '.join(sorted(delisted['product_id'].tolist()))})\n")
    L.append("`min_market_funds_usd` = Advanced Trade's `quote_min_size`, the "
             "direct successor of the old Coinbase Pro `min_market_funds` field "
             "(minimum order size in the quote currency; quote is USD for every "
             "row here).\n")

    L.append("### Candidate universe rules (proposed, not selected)\n")
    L.append("No price behaviour was consulted to BUILD these rules — only "
             "listing history length and liquidity. (Cross-asset return "
             "correlation is measured further down, on the rules' members, "
             "and is a structural property of the universe rather than a "
             "selection criterion.)\n")

    L.append("| Rule | Pairs (N) | rho_bar | N_eff | Exposure clusters | "
             "Cluster floor @95% | Correlation-adjusted pooled floor @95% |")
    L.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in summary:
        L.append(
            f"| {r['rule']} | {r['pairs']} | "
            f"{r['rho_bar'] if r['rho_bar'] is not None else 'n/a'} | "
            f"{r['n_eff'] if r['n_eff'] is not None else 'n/a'} | "
            f"{r['clusters']} | {_floor_range(r['cluster_floor'])} | "
            f"{_floor_range(r['pooled_floor'])} |")

    L.append(
        "\n**What the cluster column measures, and what it does not.** A "
        "cluster here is a maximal span of portfolio exposure, and two spans "
        f"are counted separately only when the WHOLE portfolio is flat for at "
        f"least `CLUSTER_GAP_DAYS` = {CLUSTER_GAP_DAYS} days between them — "
        "every asset in the rule out of position simultaneously. With 20-61 "
        "assets that condition is nearly unreachable: something in the book "
        "is almost always in a position, so the count saturates BY "
        "CONSTRUCTION at a handful of spans and stops responding to universe "
        "size. It is a count of portfolio-wide quiet periods, and it cannot "
        "distinguish \"8 independent episodes\" from \"8 flat gaps that "
        "happened to occur.\" **This table therefore establishes neither that "
        "breadth adds independent events nor that it fails to.** Both the "
        "saturation and the 31-pair and 61-pair rules landing on the same "
        "count are properties of the gap definition on a wide universe, not "
        "findings about independence. The correlation-adjusted column is the "
        "other end of the bracket; see below.\n")

    L.append("### Effective sample size behind the two floors\n")
    L.append("| Rule | N | rho_bar | N_eff | Common overlap window | Overlap "
             "(days / years) | Mean 55/20 entries per asset per year | "
             "Correlation-adjusted pooled n |")
    L.append("|---|---:|---:|---:|---|---:|---:|---:|")
    for r in summary:
        w = r["overlap"]
        span = (f"{w['start'][:10]} → {w['end'][:10]}"
                if w["start"] else "n/a")
        L.append(
            f"| {r['rule'].split(':')[0]} | {r['pairs']} | "
            f"{r['rho_bar'] if r['rho_bar'] is not None else 'n/a'} | "
            f"{r['n_eff'] if r['n_eff'] is not None else 'n/a'} | {span} | "
            f"{w['days']} / {w['years']} | "
            f"{r['entries_per_asset_year'] if r['entries_per_asset_year'] is not None else 'n/a'} | "
            f"{r['pooled_n'] if r['pooled_n'] is not None else 'n/a'} |")

    L.append(
        "\nEstimator, fixed in advance and not searched over alternatives:\n\n"
        "```\n"
        "rho_bar = mean pairwise Pearson correlation of DAILY LOG RETURNS\n"
        "          across the rule's assets over their common overlap window\n"
        "N_eff   = N / (1 + (N - 1) * rho_bar)\n"
        "pooled n = N_eff * entries_per_asset_per_year * years_of_overlap\n"
        "```\n")
    L.append(
        "rho_bar on daily returns is itself regime-dependent — report 03 of "
        "the literature review records BTC-alt R² swinging from 0.89 to "
        "roughly 0 within 24 months — so N_eff here is a full-window average "
        "and is not a guarantee for any sub-period.\n")
    rho_vals = [r["rho_bar"] for r in summary if r["rho_bar"] is not None]
    eff_vals = [r["n_eff"] for r in summary if r["n_eff"] is not None]
    if rho_vals and eff_vals:
        L.append(
            f"**What N_eff says about breadth on this venue.** All three rules "
            f"land in a narrow band — rho_bar {min(rho_vals):.2f}–"
            f"{max(rho_vals):.2f}, N_eff {min(eff_vals):.2f}–"
            f"{max(eff_vals):.2f}. On daily log returns these 20–61 USD pairs "
            f"carry roughly the information of fewer than two independent "
            f"assets, so pair count is a poor proxy for sample size here. The "
            f"pooled n is driven more by the LENGTH of the common overlap "
            f"window than by how many pairs a rule admits: rule B admits about "
            f"twice rule A's pairs, but admitting 2-year-old listings shortens "
            f"the window every member must share, and it ends with the smaller "
            f"pooled n. This is a structural property of the universe measured "
            f"on returns; it says nothing about what any rule would earn.\n")

    L.append(
        "**The two floors bracket the decidable edge, and neither is a "
        "result.** They rest on different, non-nested assumptions:\n")
    L.append(
        "- The **cluster floor** counts each portfolio-wide exposure span as "
        "ONE observation. It discards every trade inside a span, and it "
        "saturates on a wide universe as described above.\n"
        "- The **correlation-adjusted pooled floor** counts N_eff independent "
        "assets, each firing at its measured event rate for the length of the "
        "common overlap window. It ignores that one asset's events cluster in "
        "time, and that a single trend regime moves many names together beyond "
        "what a daily-return rho_bar captures.\n")

    flipped = [r for r in summary
               if r["pooled_n"] is not None and r["pooled_n"] < r["clusters"]]
    order_note = ""
    if flipped:
        names = ", ".join(r["rule"].split(":")[0] for r in flipped)
        order_note = (
            f" Neither end dominates. For rule {names} the pooled count falls "
            f"BELOW the cluster count — a short common overlap window costs "
            f"more than N_eff above 1 recovers — so there the pooled floor is "
            f"the wider of the two.")
    spans = []
    for r in summary:
        vals = [v for f in (r["cluster_floor"], r["pooled_floor"])
                for v in f.values() if v is not None]
        if vals:
            spans.append(f"{r['rule'].split(':')[0]} {min(vals):.2f}–{max(vals):.2f}%")
    L.append(
        f"Read each rule's pair as a range, not as two rival estimates."
        f"{order_note} Across both bases and all three borrowed SD scenarios: "
        f"{'; '.join(spans)}.\n")

    L.append("### Exact floors per borrowed SD scenario\n")
    L.append("| Rule | Basis | frozen 1.0% model | adopted 0.6/1.2% measured | "
             "candidate 0.5/0.9% |")
    L.append("|---|---|---:|---:|---:|")
    for r in summary:
        tag = r["rule"].split(":")[0]
        for basis, floor in (("cluster floor", r["cluster_floor"]),
                             ("correlation-adjusted pooled", r["pooled_floor"])):
            cells = " | ".join(
                f"{floor[k]:.4f}%" if floor[k] is not None else "n/a"
                for k in ("frozen_1.0pct_model", "adopted_0.6_1.2pct_measured",
                          "candidate_0.5_0.9pct"))
            L.append(f"| {tag} | {basis} | {cells} |")

    L.append("\n## Deliverable 2 — survivorship note\n")
    L.append(
        "The public `get_public_products` endpoint is a snapshot of pairs "
        "Coinbase lists TODAY. It is not purely survivors-only in the "
        "strictest sense — 4 pairs above carry `status=delisted` and are "
        "still returned — but Coinbase does not guarantee that delisted "
        "symbols stay discoverable here indefinitely, and pairs delisted long "
        "enough ago are simply absent with no trace. Any inventory or "
        "universe built from this endpoint alone is biased UPWARD for a "
        "trend/momentum result: assets that delisted because they collapsed, "
        "were hacked, or were abandoned are underrepresented or missing, "
        "while assets that survived to today (by definition including the "
        "eventual winners) dominate. A future universe test on this venue "
        "would need a delisted-pair source to bound this bias.\n")
    L.append(
        "A candidate delisted-pair source: **Binance** (`data.binance.vision`) "
        "publishes historical daily/monthly klines for symbols it has since "
        "delisted, without requiring credentials. It is a DIFFERENT VENUE — "
        "different listing calendar, different liquidity profile, different "
        "fee schedule, and Binance-delisted is not the same event as "
        "Coinbase-would-have-delisted. Using it to estimate a Coinbase "
        "survivorship correction would be a venue-mismatch approximation, not "
        "a direct measurement, and would need to be flagged as such. No data "
        "was pulled from Binance in this task — this is a documentation-only "
        "note, per the task's explicit constraint.\n")

    L.append("## Deliverable 3 — method notes\n")
    L.append(
        f"Event counting reuses `entry_exit_events` from "
        f"`backtesting/stf_feasibility.py` unmodified (the same function "
        f"`tests/test_stf_feasibility.py::test_events_carry_no_price_information` "
        f"guards). Rule constants (`ENTRY_LOOKBACK={ENTRY_LOOKBACK}`, "
        f"`EXIT_LOOKBACK` from `stf_protocol.py`, "
        f"`CLUSTER_GAP_DAYS={CLUSTER_GAP_DAYS}`) are imported from "
        f"`backtesting/stf_protocol.py`, also unmodified.\n")
    L.append(
        "Unlike the frozen 7B audit's `portfolio_structure` (which requires "
        "one common start date across its fixed 4-asset universe), each asset "
        "here is evaluated from ITS OWN first-eligible date (first candle + "
        "ENTRY_LOOKBACK days) for clustering. The union of all qualifying "
        "assets' in-position flags is then clustered with the same "
        "`CLUSTER_GAP_DAYS` gap rule. This is a universe-inventory modeling "
        "choice appropriate to staggered listing dates, not a change to the "
        "frozen trial design, and it registers nothing.\n")
    L.append(
        "**Correlation is the one price-movement quantity computed here.** It "
        "enters through a single function, "
        "`mean_pairwise_log_return_correlation`, which returns a mean scalar "
        "and the window it was measured on — no per-asset correlation and no "
        "return series of any kind leaves it, so nothing downstream can rank "
        "assets or build a P&L from it. "
        "`tests/test_universe_inventory.py` asserts this structurally.\n")
    L.append(
        "**Decidable edge floor** = `1.645 * SD / sqrt(n)`, where SD is NOT "
        "measured on this universe — it is borrowed from "
        "`docs/research/2026-09-cost-sensitivity.md` section 6 (per-trade net "
        "return SD, in percent of position, n=114, on the FROZEN SINGLE-ASSET "
        "V2 ZEC mechanism — a mechanism with a hard stop, unlike "
        "STF-CLOSE-55-20). It answers one arithmetic question: *if a future "
        "sample here scattered like that sample did, how small a true "
        "per-trade edge could n observations distinguish from zero at "
        "one-sided 95%?* It is not, and must not be read as, a prediction of "
        "this universe's dispersion or edge. Two values of n are reported — "
        "the saturating cluster count and the correlation-adjusted pooled "
        "count — because neither is defensible alone.\n")
    L.append(
        f"Analysis window is frozen at `ANALYSIS_END` = "
        f"{ANALYSIS_END.date().isoformat()}, the as-of date of the committed "
        f"CSV, so re-running `--analysis-only` does not restate these numbers "
        f"against a longer window than the inventory describes.\n")

    MD_PATH.write_text("\n".join(L) + "\n", encoding="utf-8")


def _print_report(summary: list[dict]) -> None:
    print("\n" + "=" * 78)
    print("CANDIDATE UNIVERSE RULES — capacity only, no performance")
    print("=" * 78)
    for r in summary:
        print(f"\n{r['rule']}")
        print(f"  pairs (N)                 : {r['pairs']}")
        print(f"  rho_bar (daily log ret)   : {r['rho_bar']}")
        print(f"  N_eff                     : {r['n_eff']}")
        print(f"  common overlap            : {r['overlap']['start']} -> "
              f"{r['overlap']['end']} ({r['overlap']['years']}y)")
        print(f"  entries per asset per year: {r['entries_per_asset_year']}")
        print(f"  exposure clusters         : {r['clusters']} (saturating)")
        print(f"  pooled n (corr-adjusted)  : {r['pooled_n']}")
        print(f"  cluster floor             : {_floor_range(r['cluster_floor'])}")
        print(f"  corr-adjusted pooled floor: {_floor_range(r['pooled_floor'])}")


def main() -> None:
    analysis_only = "--analysis-only" in sys.argv
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if analysis_only:
        print(f"Re-using committed inventory {CSV_PATH.name}; fetching closes "
              "for rule members only...")
        df = load_inventory()
        needed = sorted({a for mask in candidate_rules(df).values()
                         for a in df.loc[mask, "product_id"].tolist()})
        series_by_asset = fetch_closes_for(needed)
        as_of = ANALYSIS_END
    else:
        print("Fetching Coinbase public product list and per-asset daily history...")
        df, series_by_asset, as_of = build_inventory()
        df.to_csv(CSV_PATH, index=False)
        print(f"\nwrote {CSV_PATH.relative_to(ROOT)} ({len(df)} rows)")

    summary = analyse(df, series_by_asset)
    _print_report(summary)
    write_markdown(df, summary, as_of)
    print(f"\nwrote {MD_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
