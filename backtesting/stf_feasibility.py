"""
Phase 7R-1 — BLINDED feasibility audit for STF-CLOSE-55-20.

This answers "could a forward trial of this rule ever accumulate enough
observations, and how independent would they be?" It does NOT answer "would it
have made money", and it is built so that it cannot.

WHY BLINDED
-----------
The whole point of a pre-registered forward trial is that the parameters are
chosen before anyone sees what they earn. If a feasibility audit reported P&L on
pre-cutoff data, the choice to proceed — or to nudge 55/20 — would be
contaminated by exactly the selection bias the trial registry exists to prevent.

HOW THE BLIND IS ENFORCED
-------------------------
Prices enter one function, `entry_exit_events`, which returns TIMESTAMPS AND
EVENT LABELS ONLY. Every downstream computation operates on those timestamps.
There is no code path from a price to an output, so P&L is not withheld — it is
absent. `tests/test_stf_feasibility.py` asserts this structurally.

WHAT IT REPORTS
---------------
  * entry and exit counts, and open positions at the end
  * holding-period distribution
  * exposure clusters (portfolio-level, 60-day flat separation)
  * concurrency and pairwise overlap between assets
  * observed event rate and the implied time to 20 / 30 closed trades
  * data gaps and required-field availability

WHAT IT MUST NEVER REPORT
-------------------------
P&L, profit factor, expectancy, win rate, per-asset ranking, or any comparison
between parameter values. A single parameter set is replayed. There is no grid.

Usage:
    python backtesting/stf_feasibility.py            # write the audit
    python backtesting/stf_feasibility.py --verify   # reproduce, do not write
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

TRIAL_ID = "2026-08-stf-feasibility.7R1"
ARTIFACT_DIR = ROOT / "docs" / "research" / "artifacts" / "stf_feasibility"
ARTIFACT = ARTIFACT_DIR / "audit.json"

CANDLE_DIR = ROOT / "data" / "candles"

# The single rule under audit. Not a grid; not tunable from the CLI.
ENTRY_LOOKBACK = 55
EXIT_LOOKBACK = 20
UNIVERSE = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# A portfolio is "flat" when no asset holds a position. Two spans of exposure
# separated by at least this many flat days are counted as separate clusters.
# NOTE the term: cluster, not "independent episode". Sixty flat days does not
# make two crypto trend episodes statistically independent — it only makes them
# non-contiguous.
CLUSTER_GAP_DAYS = 60

# Targets from the draft protocol, quoted here only to express the observed
# event rate as an implied duration.
TARGET_TRADES = (20, 30)


class FeasibilityError(RuntimeError):
    """The audit cannot be produced as specified."""


# ── The ONLY function that touches prices ────────────────────────────────────

def entry_exit_events(closes: pd.Series) -> list[dict]:
    """
    Replay the rule and return EVENTS ONLY — no prices, no returns.

    Entry: close > max of the previous ENTRY_LOOKBACK closes (current excluded).
    Exit:  close < min of the previous EXIT_LOOKBACK closes (current excluded).
    Long only, one position at a time, evaluated on completed daily bars.

    The return type is the blind: timestamps and labels. A caller cannot
    reconstruct P&L from this, because the prices never leave the function.
    """
    closes = closes.sort_index()
    # shift(1) so the current bar is excluded from its own window — the same
    # look-ahead discipline the scanner uses for daily context.
    entry_level = closes.shift(1).rolling(ENTRY_LOOKBACK).max()
    exit_level = closes.shift(1).rolling(EXIT_LOOKBACK).min()

    events: list[dict] = []
    in_position = False
    for ts, close in closes.items():
        hi, lo = entry_level.get(ts), exit_level.get(ts)
        if not in_position:
            if pd.notna(hi) and close > hi:
                events.append({"ts": ts.isoformat(), "event": "ENTRY"})
                in_position = True
        else:
            if pd.notna(lo) and close < lo:
                events.append({"ts": ts.isoformat(), "event": "EXIT"})
                in_position = False
    return events


# ── Everything below operates on timestamps only ─────────────────────────────

def _spans(events: list[dict]) -> tuple[list[tuple[pd.Timestamp, pd.Timestamp]], int]:
    """Closed (entry, exit) spans, plus the count still open at the end."""
    spans, open_entry = [], None
    for ev in events:
        ts = pd.Timestamp(ev["ts"])
        if ev["event"] == "ENTRY":
            open_entry = ts
        elif open_entry is not None:
            spans.append((open_entry, ts))
            open_entry = None
    return spans, (1 if open_entry is not None else 0)


def _held_days(spans) -> list[int]:
    return [int((exit_ts - entry_ts).days) for entry_ts, exit_ts in spans]


def _quantiles(values: list[int]) -> dict:
    if not values:
        return {"n": 0}
    s = pd.Series(values)
    return {
        "n": int(len(s)),
        "min": int(s.min()),
        "p25": float(round(s.quantile(0.25), 1)),
        "median": float(round(s.median(), 1)),
        "p75": float(round(s.quantile(0.75), 1)),
        "max": int(s.max()),
        "mean": float(round(s.mean(), 1)),
    }


def _in_position_days(spans, index: pd.DatetimeIndex) -> pd.Series:
    """Boolean series: was a position open on this day?"""
    held = pd.Series(False, index=index)
    for entry_ts, exit_ts in spans:
        held.loc[(held.index >= entry_ts) & (held.index < exit_ts)] = True
    return held


def _clusters(any_open: pd.Series) -> list[dict]:
    """
    Portfolio exposure clusters: maximal spans of exposure separated by at
    least CLUSTER_GAP_DAYS of complete flatness.
    """
    days = any_open[any_open].index
    if len(days) == 0:
        return []
    out, start, prev = [], days[0], days[0]
    for day in days[1:]:
        if (day - prev).days > CLUSTER_GAP_DAYS:
            out.append({"start": start.isoformat(), "end": prev.isoformat(),
                        "exposed_days": int((prev - start).days) + 1})
            start = day
        prev = day
    out.append({"start": start.isoformat(), "end": prev.isoformat(),
                "exposed_days": int((prev - start).days) + 1})
    return out


def _data_quality(df: pd.DataFrame) -> dict:
    ts = pd.to_datetime(df["time"], utc=True).sort_values()
    gaps = ts.diff().dropna()
    missing = gaps[gaps > pd.Timedelta(days=1)]
    required = ["time", "open", "high", "low", "close", "volume"]
    return {
        "bars": int(len(df)),
        "first": ts.iloc[0].isoformat(),
        "last": ts.iloc[-1].isoformat(),
        "calendar_days_spanned": int((ts.iloc[-1] - ts.iloc[0]).days) + 1,
        "gap_count": int(len(missing)),
        "missing_days_est": int(sum(g.days - 1 for g in missing)),
        "required_fields_present": sorted(c for c in required if c in df.columns),
        "required_fields_missing": sorted(c for c in required if c not in df.columns),
        "close_non_finite": int((~pd.to_numeric(df["close"], errors="coerce")
                                 .apply(lambda v: v == v and abs(v) != float("inf"))).sum()),
    }


def build_audit() -> dict:
    per_asset: dict = {}
    frames: dict = {}

    for asset in UNIVERSE:
        path = CANDLE_DIR / f"{asset.replace('-', '_')}_1d.parquet"
        if not path.exists():
            raise FeasibilityError(f"missing daily data for {asset}: {path}")
        df = pd.read_parquet(path)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.sort_values("time").reset_index(drop=True)

        closes = pd.Series(df["close"].astype("float64").values, index=df["time"])
        events = entry_exit_events(closes)          # <- prices stop here
        spans, still_open = _spans(events)
        held = _held_days(spans)

        years = ((closes.index[-1] - closes.index[0]).days + 1) / 365.25
        # The first ENTRY_LOOKBACK bars cannot produce a signal.
        eligible_years = max(years - ENTRY_LOOKBACK / 365.25, 0.0)

        per_asset[asset] = {
            "entries": sum(1 for e in events if e["event"] == "ENTRY"),
            "closed_trades": len(spans),
            "open_at_end": still_open,
            "holding_days": _quantiles(held),
            "eligible_years": round(eligible_years, 2),
            "closed_trades_per_year": (round(len(spans) / eligible_years, 2)
                                       if eligible_years > 0 else None),
            "data_quality": _data_quality(df),
        }
        frames[asset] = (spans, closes.index)

    # ── Portfolio structure ──────────────────────────────────────────────────
    all_days = sorted(set().union(*[set(idx) for _, idx in frames.values()]))
    index = pd.DatetimeIndex(all_days)
    held_by_asset = {a: _in_position_days(spans, index) for a, (spans, _) in frames.items()}
    concurrent = sum(held_by_asset.values())
    any_open = concurrent > 0

    clusters = _clusters(any_open)
    pooled_closed = sum(v["closed_trades"] for v in per_asset.values())
    pooled_years = max(v["eligible_years"] for v in per_asset.values())
    pooled_rate = round(pooled_closed / pooled_years, 2) if pooled_years else None

    overlap = {}
    for i, a in enumerate(UNIVERSE):
        for b in UNIVERSE[i + 1:]:
            both = int((held_by_asset[a] & held_by_asset[b]).sum())
            either = int((held_by_asset[a] | held_by_asset[b]).sum())
            overlap[f"{a}|{b}"] = {
                "days_both": both,
                "days_either": either,
                "jaccard": round(both / either, 3) if either else None,
            }

    concurrency_hist = {str(k): int(v) for k, v in
                        concurrent.value_counts().sort_index().items()}

    return {
        "trial_id": TRIAL_ID,
        "purpose": ("BLINDED feasibility audit — event rates, exposure structure "
                    "and data quality only"),
        "blinding": ("prices are consumed exclusively by entry_exit_events(), "
                     "which returns timestamps and labels; no P&L, profit "
                     "factor, expectancy, win rate, asset ranking or parameter "
                     "comparison is computed anywhere in this module"),
        "not_evidence": ("this is PRE-CUTOFF data used to size a future trial. "
                         "It is not an evaluation and cannot support or oppose "
                         "activation."),
        "rule": {
            "name": "STF-CLOSE-55-20",
            "entry": f"daily close > max of prior {ENTRY_LOOKBACK} closes (current excluded)",
            "exit": f"daily close < min of prior {EXIT_LOOKBACK} closes (current excluded)",
            "note": ("levels are built from CLOSES, not highs/lows — this is not "
                     "the classical Donchian channel and is named accordingly"),
            "direction": "long only",
            "universe": UNIVERSE,
        },
        "per_asset": per_asset,
        "portfolio": {
            "pooled_closed_trades": pooled_closed,
            "pooled_years": round(pooled_years, 2),
            "pooled_closed_trades_per_year": pooled_rate,
            "implied_years_to_target": {
                str(t): (round(t / pooled_rate, 2) if pooled_rate else None)
                for t in TARGET_TRADES
            },
            "cluster_gap_days": CLUSTER_GAP_DAYS,
            "exposure_clusters": len(clusters),
            "clusters": clusters,
            "clusters_per_year": (round(len(clusters) / pooled_years, 2)
                                  if pooled_years else None),
            "days_with_any_exposure": int(any_open.sum()),
            "days_total": int(len(index)),
            "exposure_fraction": round(float(any_open.mean()), 3),
            "concurrency_days": concurrency_hist,
            "mean_concurrent_when_exposed": round(
                float(concurrent[any_open].mean()), 2) if any_open.any() else None,
            "pairwise_overlap": overlap,
        },
    }


def _serialise(audit: dict) -> str:
    return json.dumps(audit, indent=2, sort_keys=True) + "\n"


def verify() -> bool:
    if not ARTIFACT.exists():
        raise FeasibilityError(f"no committed audit at {ARTIFACT}")
    fresh = _serialise(build_audit())
    if fresh == ARTIFACT.read_text(encoding="utf-8"):
        return True
    print("MISMATCH: feasibility audit differs from a fresh run", file=sys.stderr)
    return False


def _report(a: dict) -> None:
    p = a["portfolio"]
    print("\n" + "=" * 74)
    print(f"STF-CLOSE-55-20 — BLINDED FEASIBILITY AUDIT  ({a['trial_id']})")
    print("No P&L, no profit factor, no ranking. Event structure only.")
    print("=" * 74)

    print(f"\n{'asset':10s} {'closed':>7s} {'open':>5s} {'per yr':>7s} "
          f"{'median hold':>12s} {'gaps':>5s}")
    for asset, v in a["per_asset"].items():
        h = v["holding_days"]
        print(f"{asset:10s} {v['closed_trades']:7d} {v['open_at_end']:5d} "
              f"{v['closed_trades_per_year']:7.2f} "
              f"{h.get('median', 0):12.1f} "
              f"{v['data_quality']['gap_count']:5d}")

    print(f"\npooled closed trades   : {p['pooled_closed_trades']} over "
          f"{p['pooled_years']} years  ({p['pooled_closed_trades_per_year']}/yr)")
    for target, years in p["implied_years_to_target"].items():
        print(f"  implied years to {target:>2s} : {years}")
    print(f"\nexposure clusters      : {p['exposure_clusters']} "
          f"({p['clusters_per_year']}/yr, {p['cluster_gap_days']}-day flat separation)")
    print(f"time with any exposure : {p['exposure_fraction']:.1%}")
    print(f"mean concurrent assets : {p['mean_concurrent_when_exposed']} when exposed")
    print("concurrency (days)     : " + ", ".join(
        f"{k}:{v}" for k, v in p["concurrency_days"].items()))
    print("\npairwise overlap (Jaccard on in-position days):")
    for pair, v in p["pairwise_overlap"].items():
        print(f"  {pair:22s} {v['jaccard']}")
    print("\n" + "=" * 74)
    print("Pre-cutoff data, used only to size a future trial. Not an evaluation.")
    print("=" * 74)


def main() -> None:
    try:
        if "--verify" in sys.argv:
            if verify():
                print("OK: feasibility audit reproduces byte-for-byte.")
                return
            sys.exit(1)
        audit = build_audit()
        _report(audit)
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        ARTIFACT.write_text(_serialise(audit), encoding="utf-8")
        print(f"\nwrote {ARTIFACT.relative_to(ROOT)}")
    except FeasibilityError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
