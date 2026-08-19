"""
Walk-forward validation of the per-asset ATR parameters.

Trial `2026-08-walkforward-repair.v1` (registered in docs/trial_registry.md
BEFORE any result was produced).

WHAT THIS IS FOR
----------------
One narrow question: does the stop multiplier selected on a training slice still
behave on the slice immediately after it, under the mechanism the config
actually declares?

WHAT IT IS NOT
--------------
NOT clean out-of-sample evidence, and it cannot authorise anything. The three
windows below were inspected repeatedly during the original development and are
counted in the trial registry's multiple-testing budget. Re-running them on a
corrected tool yields HISTORICAL DIAGNOSTICS. A real OOS claim needs a new
pre-registered hypothesis and forward data nobody has looked at.

Every number this tool produced before the repair is VOID. The defects were:

  1. `_load_asset` never attached the daily frame, so `close_1d` / `ema*_1d`
     were absent from every row and the declared daily-EMA trend gate was
     skipped for every signal on every asset.
  2. The scan loop enumerated only three blocked reasons, so `daily_trend` and
     `btc_regime` blocks fell through and were TRADED.
  3. Fees were `FEE_RATE = 0.006` on entry AND exit, while the registered
     mechanism is maker 0.4% entry / 0.4% take-profit / taker 0.6% stop.
  4. An unfilled max-hold horizon at the right edge of the data was reported as
     a completed MAX_HOLD trade — an invented outcome.
  5. An unknown asset silently fell back to the ETH strategy config and an empty
     ASSET_CONFIG, i.e. a different mechanism under the asset's own name.

The frame is now built by `signal_scanner.build_merged_frame` and trades are
simulated by `signal_scanner._simulate_trade` — the same assembly and the same
simulator the registered scanner uses — so this tool cannot drift away from the
mechanism it claims to validate.

Usage:
    python backtesting/walk_forward.py                    # run + write artifact
    python backtesting/walk_forward.py --asset ZEC-USD    # single asset
    python backtesting/walk_forward.py --verify           # reproduce, do not write
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from backtesting.backtest import STRATEGY_CONFIG
from backtesting.signal_scanner import (
    ASSET_CONFIG,
    _WHIPSAW_MAX_STOPS,
    _WHIPSAW_WINDOW_H,
    _detect_breakout_signal,
    _simulate_trade,
    build_merged_frame,
)

TRIAL_ID = "2026-08-walkforward-repair.v1"
ARTIFACT_DIR = ROOT / "docs" / "research" / "artifacts" / "walk_forward"
ARTIFACT = ARTIFACT_DIR / "results.json"

# Files whose content determines these results.
_CODE_PATHS = [
    "backtesting/walk_forward.py",
    "backtesting/signal_scanner.py",
    "backtesting/backtest.py",
    "exchange/coinbase_candles.py",
    # Supplies the hashing and environment primitives this artifact is built
    # from, so its content determines these results too.
    "backtesting/research_runner.py",
]


class WalkForwardError(RuntimeError):
    """The run cannot proceed under the registered protocol."""


# ── Frozen protocol — nothing here may be widened ────────────────────────────
#
# Rolling windows, unchanged from the original tool so the repair stays
# comparable to what it replaces. Train and test are half-open and do not
# overlap WITHIN a window; across windows they deliberately do, which is what
# "rolling" means and is why no single window is independent evidence.
WINDOWS = [
    {"label": "Window 1", "train_start": "2024-09-01",
     "test_start": "2024-11-15", "test_end": "2025-01-20",
     "train_regime": "Aug crash recovery", "test_regime": "Trump rally"},
    {"label": "Window 2", "train_start": "2024-11-01",
     "test_start": "2025-01-20", "test_end": "2025-04-01",
     "train_regime": "Trump rally", "test_regime": "Q1 2025 bear"},
    {"label": "Window 3", "train_start": "2025-01-20",
     "test_start": "2025-04-01", "test_end": "2025-06-28",
     "train_regime": "Q1 2025 bear", "test_regime": "Current bear"},
]

# 120 days before the earliest train start, matching the research runner's
# warm-up convention. The daily frame reaches further back on its own.
_WARMUP = "2024-05-04"
_END = "2025-06-28"

# The registered universe: the four assets the research covers. Deliberately
# NOT signal_scanner.ASSETS, which also lists LINK/ATOM/AVAX/DOT — disabled
# expansion candidates with no candle cache. Inheriting that list made the run
# depend on which assets happened to be downloaded.
ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# The spans this tool actually READS, declared explicitly rather than borrowed.
#
# Reusing the research runner's scope would hash candles through 2026-07-12,
# so a 2026 revision could invalidate a walk-forward that stops in June 2025 and
# whose numbers cannot possibly change — the same over-broad-hash defect Phase
# 6.9 removed. The 1h span is what build_merged_frame loads; the 1d span starts
# at the scanner's own daily history start because the daily EMAs need it.
from backtesting.signal_scanner import _DAILY_HISTORY_START  # noqa: E402

INPUT_SCOPES = {
    "1h": {"start": _WARMUP, "end": _END},
    "1d": {"start": _DAILY_HISTORY_START, "end": _END},
}
# coinbase_candles.download slices `time >= start & time <= end`, so both ends
# are inclusive and the boundary bar is read.
SCOPE_BOUNDS = "both inclusive"

# FROZEN. Extending this list turns a repair into a parameter search.
STOP_CANDIDATES = [1.5, 2.0, 2.5, 3.0]
RR_RATIO = 1.75
_MIN_TRAIN_TRADES = 3


def _asset_config(asset: str) -> dict:
    """
    The asset's own declared config. No fallback.

    `ASSET_CONFIG.get(asset, {})` used to hand an unknown asset an empty dict,
    which silently became min_conditions=3 and a 200-day daily EMA — a different
    mechanism reported under the asset's name.
    """
    cfg = ASSET_CONFIG.get(asset)
    if cfg is None:
        raise WalkForwardError(
            f"{asset} has no ASSET_CONFIG entry. Refusing to substitute another "
            "asset's parameters; declare it or drop it from the run.")
    return cfg


def _max_hold(asset: str) -> int:
    """
    The asset's own max-hold. No fallback.

    `STRATEGY_CONFIG.get(asset, STRATEGY_CONFIG["ETH-USD"])` gave any unknown
    asset ETH's hold window.
    """
    cfg = STRATEGY_CONFIG.get(asset)
    if cfg is None:
        raise WalkForwardError(
            f"{asset} has no STRATEGY_CONFIG entry, so its max-hold is unknown. "
            "Refusing to borrow another asset's.")
    hold = cfg.get("max_hold_hours")
    if hold is None:
        raise WalkForwardError(f"{asset} declares no max_hold_hours")
    return int(hold)


def _btc_regime_applicable(asset: str, cfg: dict) -> bool:
    """BTC-USD never receives its own regime column, so it is not applicable."""
    return asset != "BTC-USD" and bool(cfg.get("btc_regime_filter", False))


def load_asset(asset: str) -> pd.DataFrame:
    """
    Build the merged frame exactly as the registered scanner does.

    Uses build_merged_frame rather than reassembling 1h + 4h here: the previous
    hand-rolled assembly is precisely what omitted the daily frame. Anything the
    scanner attaches — look-ahead-safe 4h, +1d-shifted daily, BTC regime where
    applicable — is attached here by construction.
    """
    import backtesting.signal_scanner as scanner

    cfg = _asset_config(asset)
    # STRICT: a cache miss must raise, never fall back to yfinance. A registered
    # run whose inputs were hashed from parquet must not quietly compute on data
    # from a different provider.
    prev = scanner.STRICT_COINBASE_ONLY
    scanner.STRICT_COINBASE_ONLY = True
    try:
        df, _ = build_merged_frame(
            asset, _WARMUP, _END, cfg,
            btc_regime_applicable=_btc_regime_applicable(asset, cfg))
    finally:
        scanner.STRICT_COINBASE_ONLY = prev
    if df is None or df.empty:
        raise WalkForwardError(f"{asset}: no data for {_WARMUP}..{_END}")
    return df


def run_scan(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp,
             asset: str, atr_stop: float, atr_target: float) -> dict:
    """
    Scan the half-open interval [start_ts, end_ts) with explicit ATR parameters.

    Every blocked reason means no trade. Unresolved trades are PENDING and enter
    no statistic.

    The frame is TRUNCATED at end_ts before anything is simulated. Bounding only
    the entry time was a look-ahead leak: the simulator received the whole
    dataframe, so a train trade could close on test candles and that outcome
    then chose the stop multiplier, while test trades in Windows 1 and 2 could
    resolve on candles past their own test_end. PENDING was likewise measured
    against the end of ALL data rather than the end of this slice, so a trade
    entered hours before a boundary was reported as fully resolved.

    Bars BEFORE start_ts are kept: signal detection needs its lookback, and it
    only ever looks backwards.
    """
    if start_ts >= end_ts:
        raise WalkForwardError(f"empty interval {start_ts} .. {end_ts}")

    cfg = _asset_config(asset)
    max_hold = _max_hold(asset)
    btc_applicable = _btc_regime_applicable(asset, cfg)

    # Half-open: the bar AT end_ts belongs to the next slice and must be
    # invisible here, to detection and simulation alike.
    df = df[df.index < end_ts]
    if df.empty:
        raise WalkForwardError(f"no rows before {end_ts}")

    signals: list[dict] = []
    pending = 0
    blocked: dict[str, int] = {}
    skip_until = -1
    recent_stop_ts: list[pd.Timestamp] = []

    start_idx = int(df.index.searchsorted(start_ts))
    if start_idx >= len(df):
        # Candles exist before the slice but none inside it. Returning n=0
        # here reads as "the mechanism found nothing", which is a different
        # claim from "there was nothing to look at".
        raise WalkForwardError(
            f"no candles inside [{start_ts.isoformat()}, {end_ts.isoformat()})")
    for i in range(start_idx, len(df)):
        ts = df.index[i]
        if i < skip_until:
            continue

        result = _detect_breakout_signal(
            df, i, cfg, btc_regime_applicable=btc_applicable)
        if result is None:
            continue
        # ANY blocked reason means no trade. Enumerating a subset is how
        # daily_trend and btc_regime blocks used to be traded.
        reason = result.get("blocked")
        if reason:
            blocked[reason] = blocked.get(reason, 0) + 1
            continue

        cutoff = ts - pd.Timedelta(hours=_WHIPSAW_WINDOW_H)
        recent_stop_ts = [t for t in recent_stop_ts if t >= cutoff]
        if len(recent_stop_ts) >= _WHIPSAW_MAX_STOPS:
            blocked["whipsaw"] = blocked.get("whipsaw", 0) + 1
            continue

        price = float(df.iloc[i]["close"])
        # The registered simulator, on the TRUNCATED frame: it cannot see a
        # single candle beyond this slice, so an outcome that would need one is
        # censored rather than borrowed from the future.
        trade = _simulate_trade(df, i, price, max_hold, atr_stop, atr_target)

        if not trade.get("resolved", True):
            # The horizon runs past the available data. Counting it as a
            # completed MAX_HOLD invented an outcome at the right edge of every
            # slice — and every slice has a right edge.
            pending += 1
            skip_until = i + trade["hold_h"] + 1
            continue

        if trade["reason"] == "STOP_LOSS":
            recent_stop_ts.append(ts)
        exit_ts = df.index[min(i + trade["hold_h"], len(df) - 1)]
        signals.append({"ts": ts.isoformat(), "exit_ts": exit_ts.isoformat(),
                        "price": price, "trade": trade})
        skip_until = i + trade["hold_h"] + 1

    returns = np.array([s["trade"]["pnl_pct"] for s in signals], dtype="float64")
    n = len(signals)
    wins = int((returns > 0).sum()) if n else 0
    gross_win = float(returns[returns > 0].sum()) if n else 0.0
    gross_loss = float(-returns[returns < 0].sum()) if n else 0.0
    return {
        "n": n,
        "n_pending": pending,
        "wins": wins,
        "win_rate": round(wins / n, 6) if n else None,
        "avg_pnl": round(float(returns.mean()), 6) if n else None,
        "total_pnl": round(float(returns.sum()), 6),
        "profit_factor": (round(gross_win / gross_loss, 6) if gross_loss > 0
                          else ("inf" if gross_win > 0 else None)),
        "blocked": dict(sorted(blocked.items())),
        # Which bars actually traded. Cheap, and it makes the half-open window
        # boundary auditable instead of a claim in a comment.
        "signal_ts": [s["ts"] for s in signals],
        # Every resolved exit is strictly inside the slice. Recorded so the
        # boundary property is auditable from the artifact, not just asserted.
        "last_exit_ts": max((s["exit_ts"] for s in signals), default=None),
        "atr_stop": atr_stop,
        "atr_target": atr_target,
        "max_hold_hours": max_hold,
    }


def _select_on_train(train_scores: list[dict]) -> dict:
    """
    Registered selection rule: highest train avg P&L among candidates with at
    least _MIN_TRAIN_TRADES RESOLVED trades.

    Deterministic on ties: the lowest stop multiplier wins, so two runs of the
    same data can never disagree.
    """
    eligible = [s for s in train_scores if s["n"] >= _MIN_TRAIN_TRADES]
    if not eligible:
        return {}
    best = max(eligible, key=lambda s: (s["avg_pnl"], -s["atr_stop"]))
    return best


def run_windows(assets: list[str]) -> list[dict]:
    """Every window x asset cell. Deterministic; no wall-clock, no RNG."""
    frames = {a: load_asset(a) for a in assets}
    rows: list[dict] = []

    for win in WINDOWS:
        train_start = pd.Timestamp(win["train_start"], tz="UTC")
        test_start = pd.Timestamp(win["test_start"], tz="UTC")
        test_end = pd.Timestamp(win["test_end"], tz="UTC")
        # Half-open and non-overlapping within the window, asserted rather than
        # assumed: a train slice that reached into its own test slice would make
        # the whole exercise circular.
        if not (train_start < test_start < test_end):
            raise WalkForwardError(
                f"{win['label']}: boundaries must satisfy "
                f"train_start < test_start < test_end")

        for asset in sorted(assets):
            df = frames[asset]
            train_scores = [
                run_scan(df, train_start, test_start, asset,
                         stop, round(stop * RR_RATIO, 4))
                for stop in STOP_CANDIDATES
            ]
            best = _select_on_train(train_scores)
            row = {
                "trial": f"walkforward:{asset}:{win['label']}",
                "asset": asset,
                "window": win["label"],
                "train_start": win["train_start"],
                "test_start": win["test_start"],
                "test_end": win["test_end"],
                "train_regime": win["train_regime"],
                "test_regime": win["test_regime"],
                "interval_semantics": "[train_start, test_start) / [test_start, test_end)",
                "train_candidates": train_scores,
                "selection_rule": (
                    f"max avg_pnl among candidates with n >= {_MIN_TRAIN_TRADES} "
                    "resolved trades; ties -> lowest stop"),
            }
            if not best:
                row["selected"] = None
                row["test"] = None
                row["note"] = (
                    f"no candidate reached {_MIN_TRAIN_TRADES} resolved train "
                    "trades; nothing selected, so nothing is tested")
            else:
                row["selected"] = {"atr_stop": best["atr_stop"],
                                   "atr_target": best["atr_target"]}
                row["train"] = best
                row["test"] = run_scan(df, test_start, test_end, asset,
                                       best["atr_stop"], best["atr_target"])
            rows.append(row)
    return rows


# ── Provenance ───────────────────────────────────────────────────────────────

def _input_fingerprints(assets: list[str]) -> list[dict]:
    """
    Logical hash of every input, over the span THIS tool reads.

    Scopes are declared in INPUT_SCOPES rather than inherited from the research
    runner, whose window ends in 2026 — a candle this tool never loads must not
    be able to invalidate it.
    """
    from backtesting.research_runner import _HASH_SCHEME, logical_sha256

    out = []
    for asset in sorted(assets):
        stem = asset.replace("-", "_")
        for interval in ("1h", "1d"):
            path = ROOT / "data" / "candles" / f"{stem}_{interval}.parquet"
            if not path.exists():
                raise WalkForwardError(f"input dataset missing: {path}")
            scope = INPUT_SCOPES[interval]
            out.append({
                "file": path.name,
                "hash_scheme": _HASH_SCHEME,
                "scope_start_inclusive": scope["start"],
                "scope_end_inclusive": scope["end"],
                "scope_bounds": SCOPE_BOUNDS,
                "logical_sha256": logical_sha256(
                    pd.read_parquet(path), scope["start"], scope["end"]),
            })
    return out


def build_artifact(assets: list[str]) -> dict:
    """
    Deterministic, provenance-carrying result set.

    Reuses the research runner's primitives so this artifact means the same
    thing as the main one: content-addressed code, the pinned environment, and
    a logical input hash — here over this tool's own spans.
    """
    import hashlib

    from backtesting.research_runner import environment_fingerprint, sha256_source

    files = sorted(
        ({"file": rel, "sha256": sha256_source(ROOT / rel)} for rel in _CODE_PATHS),
        key=lambda d: d["file"])
    agg = hashlib.sha256()
    for entry in files:
        agg.update(f"{entry['file']}:{entry['sha256']}\n".encode())

    # Hash the inputs BEFORE and AFTER the scan. The shared loader appends to the
    # parquet cache when asked for a range it does not hold, so an artifact built
    # from pre-run hashes could describe inputs the run itself then changed.
    before = _input_fingerprints(assets)
    rows = run_windows(assets)
    after = _input_fingerprints(assets)
    if before != after:
        raise WalkForwardError(
            "input datasets changed during the run — the artifact would "
            "describe data that no longer exists. Re-hydrate deliberately, "
            "then re-run.")

    return {
        "trial_id": TRIAL_ID,
        "status": "HISTORICAL DIAGNOSTIC — not clean OOS, not evidence of edge",
        "code": {"files": files, "code_sha256": agg.hexdigest()},
        "environment": environment_fingerprint(),
        "protocol": {
            "assets": sorted(assets),
            "windows": WINDOWS,
            "warmup": _WARMUP,
            "end": _END,
            "input_scopes": INPUT_SCOPES,
            "scope_bounds": SCOPE_BOUNDS,
            "stop_candidates": STOP_CANDIDATES,
            "rr_ratio": RR_RATIO,
            "min_train_trades": _MIN_TRAIN_TRADES,
            "fees": "signal_scanner: entry 0.4%, take-profit 0.4%, stop/max-hold 0.6%",
            "censoring": ("each slice is truncated at its own end before "
                          "simulation; an outcome needing a later candle is "
                          "PENDING and enters no statistic"),
            "data_source": "local Coinbase parquet cache, STRICT (no fallback)",
        },
        "inputs": after,
        "rows": rows,
    }


def _serialise(artifact: dict) -> str:
    return json.dumps(artifact, indent=2, sort_keys=True) + "\n"


def _assert_writable(assets: list[str]) -> None:
    """
    The canonical artifact may only be written by a full, registered run.

    Previously `--asset ZEC-USD` overwrote the four-asset artifact with a
    one-asset one, and neither the interpreter nor the working tree was checked
    — so a partial run on an unregistered Python from uncommitted code could
    silently become the committed record.
    """
    from backtesting.research_runner import (
        assert_canonical_python,
        assert_code_is_committed,
    )

    if sorted(assets) != sorted(ASSETS):
        raise WalkForwardError(
            f"the canonical artifact covers {sorted(ASSETS)}; refusing to write "
            f"a run of {sorted(assets)}. Use --asset for a report only.")
    assert_canonical_python()
    # THIS tool's paths. The shared default covers only the main runner's
    # _CODE_PATHS, which omits walk_forward.py itself.
    assert_code_is_committed(_CODE_PATHS)


def write_artifact(assets: list[str]) -> Path:
    _assert_writable(assets)
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(_serialise(build_artifact(assets)), encoding="utf-8")
    return ARTIFACT


def verify_artifact(assets: list[str]) -> bool:
    if not ARTIFACT.exists():
        raise WalkForwardError(f"no committed artifact at {ARTIFACT}")
    fresh = _serialise(build_artifact(assets))
    if fresh == ARTIFACT.read_text(encoding="utf-8"):
        return True
    print("MISMATCH: walk-forward artifact differs from a fresh run",
          file=sys.stderr)
    return False


# ── Reporting ────────────────────────────────────────────────────────────────

def _print_report(artifact: dict) -> None:
    print("\n" + "=" * 74)
    print(f"WALK-FORWARD — trial {artifact['trial_id']}")
    print(artifact["status"])
    print(f"Stop candidates: {STOP_CANDIDATES}  |  R:R = {RR_RATIO}  (frozen)")
    print("=" * 74)

    for row in artifact["rows"]:
        print(f"\n{row['window']}  {row['asset']}")
        print(f"  train [{row['train_start']} .. {row['test_start']})"
              f"   {row['train_regime']}")
        for c in row["train_candidates"]:
            avg = f"{c['avg_pnl']:+.2f}%" if c["avg_pnl"] is not None else "   n/a"
            mark = ""
            if row["selected"] and c["atr_stop"] == row["selected"]["atr_stop"]:
                mark = "  <- selected"
            print(f"    stop={c['atr_stop']}x  n={c['n']:2d}  "
                  f"pending={c['n_pending']:2d}  avg={avg}{mark}")
        if not row["selected"]:
            print(f"  {row['note']}")
            continue
        t = row["test"]
        avg = f"{t['avg_pnl']:+.2f}%" if t["avg_pnl"] is not None else "n/a"
        print(f"  test  [{row['test_start']} .. {row['test_end']})"
              f"   {row['test_regime']}")
        print(f"    n={t['n']}  pending={t['n_pending']}  avg={avg}  "
              f"PF={t['profit_factor']}")
        if t["blocked"]:
            print(f"    blocked: {t['blocked']}")

    print("\n" + "=" * 74)
    print("These windows were inspected during the original development and are")
    print("counted in the registry's multiple-testing budget. This is a tool")
    print("correctness check, NOT out-of-sample evidence, and it cannot support")
    print("activation in either direction. LIVE NO-GO is unaffected.")
    print("=" * 74)


def main() -> None:
    asset_arg = next((sys.argv[i + 1] for i, a in enumerate(sys.argv)
                      if a == "--asset" and i + 1 < len(sys.argv)), None)
    assets = [asset_arg] if asset_arg else list(ASSETS)

    try:
        if "--verify" in sys.argv:
            if verify_artifact(assets):
                print("OK: walk-forward artifact reproduces byte-for-byte.")
                return
            sys.exit(1)
        if asset_arg:
            # REPORT ONLY. A partial run is a debugging aid, never the
            # record: `--asset ZEC-USD` used to overwrite the four-asset
            # canonical artifact with a one-asset one.
            _print_report(build_artifact(assets))
            print("\n--asset is REPORT ONLY; the canonical artifact "
                  "was not written. Run without --asset to regenerate it.")
            return
        # The ONLY write path. main() used to serialise the artifact itself,
        # which bypassed every guard in write_artifact() - universe,
        # interpreter and clean working tree alike.
        _print_report(build_artifact(assets))
        path = write_artifact(assets)
        # Reporting a path must never be able to fail a completed write.
        shown = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"\nwrote {shown}")
    except WalkForwardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
