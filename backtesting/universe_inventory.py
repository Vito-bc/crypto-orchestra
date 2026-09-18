"""
Phase 7R data/power inventory — Coinbase spot universe, public API only.

WHAT THIS IS
------------
A data-availability and statistical-power inventory for a *possible* future
broad-universe trend pre-registration. It answers two questions only:

  1. What USD spot universe does Coinbase actually list, and how much history
     / liquidity does each pair have?
  2. If STF-CLOSE-55-20 (the frozen 7B rule in `stf_protocol.py`) were run
     across a candidate universe, how many independent EXPOSURE CLUSTERS
     would it produce, and what per-trade edge could that many clusters
     distinguish from zero at one-sided 95%?

WHAT THIS IS NOT
----------------
This script computes NO strategy return, profit factor, expectancy, equity
curve, or per-asset performance ranking. Event counting reuses
`backtesting.stf_feasibility.entry_exit_events`, which is structurally blind
(prices go in, timestamps and ENTRY/EXIT labels come out — see that module's
docstring and `tests/test_stf_feasibility.py::test_events_carry_no_price_information`).
No other function in this script touches a price for any purpose other than
building that same blind event stream or a liquidity figure (USD volume is
descriptive of the market, not of the strategy).

The "decidable edge floor" in DELIVERABLE 3 is NOT a measurement of this
universe. It borrows the per-trade dispersion (SD) already published in
`docs/research/2026-09-cost-sensitivity.md` section 6 — measured on the
FROZEN, SINGLE-ASSET V2 ZEC mechanism, a different rule with a hard stop —
and asks a purely arithmetic question: "if a future sample here had that much
scatter, how small an edge could N clusters bound?" That is a statement about
statistical resolving power, not a claim about what this universe would earn.

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
    python backtesting/universe_inventory.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

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
    return closes, n_calls


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


# ── Deliverable 3: mechanism-agnostic event/cluster counting ──────────────────

def clusters_for_universe(series_by_asset: dict[str, pd.Series],
                          asset_ids: list[str]) -> list[dict]:
    """
    Union, across all qualifying assets, of each asset's own in-position flag
    (from ITS OWN first-eligible date, not a universe-wide common start), then
    cluster the union with gap >= CLUSTER_GAP_DAYS. Reuses
    `entry_exit_events` — the same blind function `stf_feasibility.py` uses —
    unmodified. Returns cluster spans only: no P&L, no trade count valuation.
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
        # stf_feasibility._spans / _in_position_days exactly (imported types
        # only; no reimplementation of price logic).
        spans: list[tuple[pd.Timestamp, pd.Timestamp | None]] = []
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


def decidable_edge_floor(n_clusters: int) -> dict[str, float | None]:
    """
    1.645 * SD / sqrt(n) for each borrowed SD scenario. n_clusters is treated
    as the independent-unit count (clusters, not pooled trades) — the more
    conservative choice, consistent with 7R's own position that trades within
    a cluster are not independent (`stf_feasibility.py`:
    "cluster, not independent episode").
    """
    if n_clusters <= 0:
        return {k: None for k in _BORROWED_SD_PCT}
    return {
        k: round(_Z_95_ONE_SIDED * sd / (n_clusters ** 0.5), 4)
        for k, sd in _BORROWED_SD_PCT.items()
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching Coinbase public product list and per-asset daily history...")
    df, series_by_asset, as_of = build_inventory()
    df.to_csv(CSV_PATH, index=False)
    print(f"\nwrote {CSV_PATH.relative_to(ROOT)} ({len(df)} rows)")

    rules = candidate_rules(df)
    rule_summary = []
    for name, mask in rules.items():
        asset_ids = df.loc[mask, "product_id"].tolist()
        n_pairs = len(asset_ids)
        clusters = clusters_for_universe(series_by_asset, asset_ids)
        n_clusters = len(clusters)
        floor = decidable_edge_floor(n_clusters)
        rule_summary.append({
            "rule": name, "pairs": n_pairs, "clusters": n_clusters,
            "decidable_edge_floor_pct": floor,
        })
        print(f"\n{name}\n  pairs={n_pairs} clusters={n_clusters} "
              f"decidable_edge_floor={floor}")

    delisted = df[(df["status"] == "delisted") | (df["trading_disabled"])]

    lines = []
    lines.append(f"# Coinbase spot universe inventory — {RUN_DATE}\n")
    lines.append(
        "READ-ONLY inventory. No strategy return, PF, Sharpe, expectancy, "
        "equity curve, or asset performance ranking is computed anywhere in "
        "this document or its source script "
        "(`backtesting/universe_inventory.py`). This is not a "
        "pre-registration and selects no universe, trial ID, or parameter.\n"
    )
    lines.append("## Deliverable 1 — universe inventory\n")
    lines.append(f"- As-of date: {as_of.date().isoformat()}")
    lines.append("- Source: Coinbase public REST API (`get_public_products`, "
                  "`get_public_candles`), no credentials")
    lines.append(f"- CSV: `{CSV_PATH.relative_to(ROOT).as_posix()}`")
    lines.append(f"- Total USD spot pairs returned by the public endpoint: {len(df)}")
    lines.append(f"  - status=online, trading enabled: "
                  f"{int(((df['status']=='online') & ~df['trading_disabled']).sum())}")
    lines.append(f"  - status=delisted or trading_disabled (still visible on this "
                  f"endpoint, excluded from candidate rules below): {len(delisted)} "
                  f"({', '.join(sorted(delisted['product_id'].tolist()))})\n")
    lines.append("`min_market_funds_usd` = Advanced Trade's `quote_min_size`, the "
                  "direct successor of the old Coinbase Pro `min_market_funds` field "
                  "(minimum order size in the quote currency; quote is USD for every "
                  "row here).\n")

    lines.append("### Candidate universe rules (proposed, not selected)\n")
    lines.append("No price behaviour (return, momentum, volatility) was consulted "
                  "to build these — only listing history length and liquidity.\n")
    lines.append("| Rule | Pairs | Exposure clusters (STF-CLOSE-55-20, 7R method) | "
                  "Decidable edge floor @95% (frozen-model SD / adopted SD / candidate SD) |")
    lines.append("|---|---:|---:|---|")
    for r in rule_summary:
        f = r["decidable_edge_floor_pct"]
        f_str = (f"{f['frozen_1.0pct_model']}% / {f['adopted_0.6_1.2pct_measured']}% / "
                 f"{f['candidate_0.5_0.9pct']}%") if f["frozen_1.0pct_model"] is not None else "n/a (0 clusters)"
        lines.append(f"| {r['rule']} | {r['pairs']} | {r['clusters']} | {f_str} |")

    lines.append("\n## Deliverable 2 — survivorship note\n")
    lines.append(
        "The public `get_public_products` endpoint is a snapshot of pairs Coinbase "
        "lists TODAY. It is not purely survivors-only in the strictest sense — 4 "
        "pairs above carry `status=delisted` and are still returned — but Coinbase "
        "clearly does not guarantee delisted symbols stay discoverable here "
        "indefinitely, and pairs that were delisted long enough ago are simply "
        "absent with no trace. Any inventory or universe built from this endpoint "
        "alone is biased UPWARD for a trend/momentum result: assets that "
        "delisted because they collapsed, were hacked, or were abandoned are "
        "underrepresented or missing, while assets that survived to today "
        "(by definition including the eventual winners) dominate. A future "
        "universe test on this venue would need a delisted-pair source to bound "
        "this bias.\n")
    lines.append(
        "A candidate delisted-pair source: **Binance** (`data.binance.vision`) "
        "publishes historical daily/monthly klines for symbols it has since "
        "delisted, without requiring credentials. It is a DIFFERENT VENUE — "
        "different listing calendar, different liquidity profile, different fee "
        "schedule, and Binance-delisted is not the same event as "
        "Coinbase-would-have-delisted. Using it to estimate a Coinbase "
        "survivorship correction would be a venue-mismatch approximation, not a "
        "direct measurement, and would need to be flagged as such. No data was "
        "pulled from Binance in this task — this is a documentation-only note, "
        "per the task's explicit constraint.\n")

    lines.append("## Deliverable 3 — method notes\n")
    lines.append(
        f"Event counting reuses `entry_exit_events` from `backtesting/"
        f"stf_feasibility.py` unmodified (the same function `tests/"
        f"test_stf_feasibility.py::test_events_carry_no_price_information` "
        f"guards). Rule constants (`ENTRY_LOOKBACK={ENTRY_LOOKBACK}`, "
        f"`EXIT_LOOKBACK` from `stf_protocol.py`, "
        f"`CLUSTER_GAP_DAYS={CLUSTER_GAP_DAYS}`) are imported from "
        f"`backtesting/stf_protocol.py`, also unmodified.\n")
    lines.append(
        "Unlike the frozen 7B audit's `portfolio_structure` (which requires "
        "one common start date across its fixed 4-asset universe), each asset "
        "here is evaluated from ITS OWN first-eligible date "
        "(first candle + ENTRY_LOOKBACK days). The union of all qualifying "
        "assets' in-position flags is then clustered with the same "
        "`CLUSTER_GAP_DAYS` gap rule. This is a universe-inventory modeling "
        "choice appropriate to staggered listing dates, not a change to the "
        "frozen trial design, and it registers nothing.\n")
    lines.append(
        "**Decidable edge floor** = `1.645 * SD / sqrt(n_clusters)`, where SD "
        "is NOT measured on this universe — it is borrowed from "
        "`docs/research/2026-09-cost-sensitivity.md` section 6 (per-trade net "
        "return SD, in percent of position, n=114, on the FROZEN SINGLE-ASSET "
        "V2 ZEC mechanism — a mechanism with a hard stop, unlike "
        "STF-CLOSE-55-20). It is reported as a resolving-power arithmetic "
        "exercise only: 'if a future sample here scattered like that sample "
        "did, how small a true per-trade edge could n_clusters distinguish "
        "from zero at one-sided 95%?' It is not, and must not be read as, a "
        "prediction of this universe's actual dispersion or edge. n_clusters, "
        "not pooled trade count, is used as the sample size — the more "
        "conservative choice, consistent with 7R's finding that trades within "
        "a cluster are not independent.\n")

    MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nwrote {MD_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
