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
absent. `tests/test_stf_feasibility.py` asserts this on the executable tokens.

WHAT IT REPORTS
---------------
  * entry and exit counts, holding-period distribution, data quality per asset
  * portfolio exposure structure over the COMMON-UNIVERSE window: clusters,
    UNIQUE assets per cluster, trades per cluster, concurrency, overlap
  * the observed event rate and the implied time to 20 / 30 closed trades

WHAT IT MUST NEVER REPORT
-------------------------
P&L, profit factor, expectancy, win rate, per-asset ranking, or any comparison
between parameter values. A single parameter set is replayed. There is no grid.

Usage:
    python backtesting/stf_feasibility.py            # write the audit
    python backtesting/stf_feasibility.py --verify   # reproduce, do not write
"""

from __future__ import annotations

import hashlib
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

# Frozen audit window. Without a fixed end the artifact would change every time
# the candle cache grows, and it would describe data the committed research
# manifest does not cover. Both ends match the main research scope, so the same
# hydrated inputs serve both artifacts and CI needs no extra data.
from backtesting.stf_protocol import (  # noqa: E402
    AUDIT_END,
    AUDIT_START,
    CLUSTER_GAP_DAYS,
    ENTRY_LOOKBACK,
    EXIT_LOOKBACK,
    UNIVERSE,
)

# Files whose content determines this audit.
_CODE_PATHS = [
    "backtesting/research_runner.py",
    "backtesting/stf_feasibility.py",
    "backtesting/stf_protocol.py",
]

# Targets from the draft protocol, quoted here only to express the observed
# event rate as an implied duration.
TARGET_TRADES = (20, 30)


class FeasibilityError(RuntimeError):
    """The audit cannot be produced as specified."""


# ── The ONLY function that touches prices ────────────────────────────────────

def entry_exit_events(closes: pd.Series,
                      evaluation_start: pd.Timestamp | None = None) -> list[dict]:
    """
    Replay the rule and return EVENTS ONLY — no prices, no returns.

    Entry: close > max of the previous ENTRY_LOOKBACK closes (current excluded).
    Exit:  close < min of the previous EXIT_LOOKBACK closes (current excluded).
    Long only, one position at a time, evaluated on completed daily bars.

    `evaluation_start` separates WARM-UP from EVALUATION. The rolling levels are
    built from the whole series, because a 55-day window needs history; but the
    state machine starts FLAT there, because a forward trial starts flat.
    Replaying through the earlier span and discarding its trades afterwards left
    the machine "in position" across the boundary, which suppressed the first
    post-start entry — a trial would have taken it.

    The return type is the blind: timestamps and labels. A caller cannot
    reconstruct P&L from this, because the prices never leave the function.
    """
    closes = closes.sort_index()
    # shift(1) so the current bar is excluded from its own window — the same
    # look-ahead discipline the scanner uses for daily context.
    entry_level = closes.shift(1).rolling(ENTRY_LOOKBACK).max()
    exit_level = closes.shift(1).rolling(EXIT_LOOKBACK).min()
    if evaluation_start is not None:
        closes = closes[closes.index >= evaluation_start]

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
    }


def portfolio_structure(frames: dict, window_start: pd.Timestamp) -> dict:
    """
    Exposure structure over ONE window, for the fixed four-asset universe.

    `window_start` matters. The proposed trial holds four assets from day one,
    so calibrating on a span in which SOL did not yet exist would describe a
    different portfolio from the one being proposed.
    """
    index = pd.DatetimeIndex(
        sorted({d for _, idx in frames.values() for d in idx if d >= window_start}))
    # `frames` already holds spans replayed FROM window_start with a flat start.
    # Post-hoc filtering is what produced the suppressed-first-entry error.
    held = {a: _in_position_days(spans, index) for a, (spans, _) in frames.items()}
    concurrent = sum(held.values())
    any_open = concurrent > 0
    clusters = _clusters(any_open)

    # UNIQUE assets touched per cluster. This is NOT the mean concurrent count:
    # a cluster can hold 2.3 assets on an average day while touching all four
    # over its life. Calibrating a simulation with the concurrent figure invents
    # too few assets and too many repeat trades per asset, which then misstates
    # correlation, leave-one-out and concentration.
    per_cluster = []
    for cl in clusters:
        lo, hi = pd.Timestamp(cl["start"]), pd.Timestamp(cl["end"])
        uniq = [a for a in UNIVERSE if held[a].loc[lo:hi].any()]
        started = sum(1 for a in UNIVERSE
                      for (s, _e) in frames[a][0] if lo <= s <= hi)
        per_cluster.append({**cl, "unique_assets": len(uniq),
                            "closed_trades_started": started})

    years = ((index[-1] - index[0]).days + 1) / 365.25 if len(index) else 0.0
    pooled = sum(len(frames[a][0]) for a in UNIVERSE)
    mean_unique = (sum(c["unique_assets"] for c in per_cluster) / len(per_cluster)
                   if per_cluster else None)
    per_cluster_trades = (pooled / len(per_cluster)) if per_cluster else None
    rate = (pooled / years) if years else None

    overlap = {}
    for i, a in enumerate(UNIVERSE):
        for b in UNIVERSE[i + 1:]:
            both = int((held[a] & held[b]).sum())
            either = int((held[a] | held[b]).sum())
            overlap[f"{a}|{b}"] = {
                "days_both": both, "days_either": either,
                "jaccard": round(both / either, 3) if either else None,
            }

    return {
        "window_start": window_start.isoformat(),
        "window_end": AUDIT_END,
        "years": round(years, 2),
        "pooled_closed_trades": pooled,
        "pooled_closed_trades_per_year": round(rate, 2) if rate else None,
        "cluster_gap_days": CLUSTER_GAP_DAYS,
        "exposure_clusters": len(per_cluster),
        "clusters_per_year": round(len(per_cluster) / years, 2) if years else None,
        "clusters": per_cluster,
        "mean_unique_assets_per_cluster": round(mean_unique, 3) if mean_unique else None,
        "mean_concurrent_when_exposed": (round(float(concurrent[any_open].mean()), 3)
                                         if bool(any_open.any()) else None),
        "mean_closed_trades_per_cluster": (round(per_cluster_trades, 3)
                                           if per_cluster_trades else None),
        "trades_per_asset_per_cluster": (round(per_cluster_trades / mean_unique, 3)
                                         if per_cluster_trades and mean_unique else None),
        "implied_years_to_target": {
            str(t): (round(t / rate, 2) if rate else None) for t in TARGET_TRADES
        },
        "days_with_any_exposure": int(any_open.sum()),
        "days_total": int(len(index)),
        "exposure_fraction": round(float(any_open.mean()), 3) if len(index) else None,
        "concurrency_days": {str(k): int(v) for k, v in
                             concurrent.value_counts().sort_index().items()},
        "pairwise_overlap": overlap,
    }


def build_audit() -> dict:
    from backtesting.research_runner import (
        _HASH_SCHEME,
        environment_fingerprint,
        logical_sha256,
        sha256_source,
    )

    per_asset: dict = {}
    series: dict = {}
    inputs: list = []
    audit_end = pd.Timestamp(AUDIT_END)

    for asset in UNIVERSE:
        path = CANDLE_DIR / f"{asset.replace('-', '_')}_1d.parquet"
        if not path.exists():
            raise FeasibilityError(f"missing daily data for {asset}: {path}")
        raw = pd.read_parquet(path)
        inputs.append({
            "file": path.name,
            "hash_scheme": _HASH_SCHEME,
            "scope_start_inclusive": AUDIT_START,
            "scope_end_inclusive": AUDIT_END,
            "logical_sha256": logical_sha256(raw, AUDIT_START, AUDIT_END),
        })

        df = raw.copy()
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df[df["time"] <= audit_end].sort_values("time").reset_index(drop=True)
        if df.empty:
            raise FeasibilityError(f"{asset}: no data inside the audit window")

        closes = pd.Series(df["close"].astype("float64").values, index=df["time"])
        events = entry_exit_events(closes)          # <- prices stop here
        spans, still_open = _spans(events)
        series[asset] = closes

        years = ((closes.index[-1] - closes.index[0]).days + 1) / 365.25
        eligible_years = max(years - ENTRY_LOOKBACK / 365.25, 0.0)
        first_eligible = closes.index[min(ENTRY_LOOKBACK, len(closes) - 1)]

        per_asset[asset] = {
            "entries": sum(1 for e in events if e["event"] == "ENTRY"),
            "closed_trades": len(spans),
            "open_at_end": still_open,
            "holding_days": _quantiles(_held_days(spans)),
            "first_eligible": first_eligible.isoformat(),
            "eligible_years": round(eligible_years, 2),
            "closed_trades_per_year": (round(len(spans) / eligible_years, 2)
                                       if eligible_years > 0 else None),
            "data_quality": _data_quality(df),
        }

    # The trial holds all four assets from day one, so calibration starts where
    # the LAST of them becomes signal-eligible.
    common_start = max(pd.Timestamp(v["first_eligible"]) for v in per_asset.values())

    # SECOND replay, starting FLAT at common_start. The first replay above spans
    # each asset's own history and is informational; a forward trial does not
    # inherit a position from before it began.
    frames = {}
    for asset, closes in series.items():
        spans, _open = _spans(entry_exit_events(closes, evaluation_start=common_start))
        frames[asset] = (spans, closes.index)

    files = sorted(({"file": rel, "sha256": sha256_source(ROOT / rel)}
                    for rel in _CODE_PATHS), key=lambda d: d["file"])
    agg = hashlib.sha256()
    for entry in files:
        agg.update(f"{entry['file']}:{entry['sha256']}\n".encode())

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
        "code": {"files": files, "code_sha256": agg.hexdigest()},
        "environment": environment_fingerprint(),
        "inputs": sorted(inputs, key=lambda d: d["file"]),
        "audit_window": {"start_inclusive": AUDIT_START, "end_inclusive": AUDIT_END},
        "rule": {
            "name": "STF-CLOSE-55-20",
            "entry": f"daily close > max of prior {ENTRY_LOOKBACK} closes (current excluded)",
            "exit": f"daily close < min of prior {EXIT_LOOKBACK} closes (current excluded)",
            "note": ("levels are built from CLOSES, not highs/lows — this is not "
                     "the classical Donchian channel and is named accordingly"),
            "direction": "long only",
            "universe": UNIVERSE,
        },
        "per_asset_full_history": per_asset,
        # THE calibration basis: the fixed four-asset universe the trial would
        # actually hold. Per-asset figures above span each asset's own history
        # and are informational only.
        "common_universe": portfolio_structure(frames, common_start),
    }


def _serialise(audit: dict) -> str:
    return json.dumps(audit, indent=2, sort_keys=True) + "\n"

def _atomic_write(path: Path, text: str) -> None:
    """
    Write via a temporary file and replace.

    A direct write_text can leave a truncated artifact if the process dies
    mid-write, and a half-written JSON that still parses is worse than none.
    """
    tmp = path.with_suffix(path.suffix + ".partial")
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _assert_writable() -> None:
    """Same write discipline as the research runner: canonical env, clean tree."""
    from backtesting.research_runner import (
        assert_canonical_python,
        assert_code_is_committed,
    )

    assert_canonical_python()
    assert_code_is_committed(_CODE_PATHS)


def write_artifact() -> tuple[Path, dict]:
    """Guard once, build once, write, and return what was written."""
    _assert_writable()
    audit = build_audit()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write(ARTIFACT, _serialise(audit))
    return ARTIFACT, audit


def verify() -> bool:
    if not ARTIFACT.exists():
        raise FeasibilityError(f"no committed audit at {ARTIFACT}")
    if _serialise(build_audit()) == ARTIFACT.read_text(encoding="utf-8"):
        return True
    print("MISMATCH: feasibility audit differs from a fresh run", file=sys.stderr)
    return False


def _report(a: dict) -> None:
    c = a["common_universe"]
    print("\n" + "=" * 74)
    print(f"STF-CLOSE-55-20 — BLINDED FEASIBILITY AUDIT  ({a['trial_id']})")
    print("No P&L, no profit factor, no ranking. Event structure only.")
    print("=" * 74)

    print("\nper asset, own history (informational)")
    print(f"{'asset':10s} {'closed':>7s} {'open':>5s} {'per yr':>7s} {'median hold':>12s}")
    for asset, v in a["per_asset_full_history"].items():
        print(f"{asset:10s} {v['closed_trades']:7d} {v['open_at_end']:5d} "
              f"{v['closed_trades_per_year']:7.2f} "
              f"{v['holding_days'].get('median', 0):12.1f}")

    print(f"\nCOMMON UNIVERSE (the calibration basis) "
          f"{c['window_start'][:10]} -> {c['window_end'][:10]}, {c['years']} years")
    print(f"  pooled closed trades      : {c['pooled_closed_trades']} "
          f"({c['pooled_closed_trades_per_year']}/yr)")
    for t, y in c["implied_years_to_target"].items():
        print(f"    implied years to {t:>2s}     : {y}")
    print(f"  exposure clusters         : {c['exposure_clusters']} "
          f"({c['clusters_per_year']}/yr)")
    print(f"  UNIQUE assets per cluster : {c['mean_unique_assets_per_cluster']}")
    print(f"  mean concurrent when open : {c['mean_concurrent_when_exposed']}")
    print(f"  trades per cluster        : {c['mean_closed_trades_per_cluster']}")
    print(f"  trades per asset/cluster  : {c['trades_per_asset_per_cluster']}")
    print(f"  time with any exposure    : {c['exposure_fraction']:.1%}")
    print("  pairwise overlap (Jaccard):")
    for pair, v in c["pairwise_overlap"].items():
        print(f"    {pair:22s} {v['jaccard']}")
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
        path, audit = write_artifact()
        _report(audit)
        shown = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"\nwrote {shown}")
    except FeasibilityError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
