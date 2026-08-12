"""
Deterministic research runner — immutable provenance for headline results.

Why this exists: the 2026-08 review could not verify the registry's headline
numbers, because they were produced by ad-hoc CLI invocations whose thresholds,
windows and input data were not recorded. Prose and registry claims must be
reproducible from committed code plus a committed manifest.

Contract:

  * Configuration is FROZEN in this file (RESEARCH_CONFIG). Nothing is selected
    from the command line — a CLI-tunable threshold is how period-selected
    results get published as if pre-registered.
  * Every input dataset is hashed (SHA-256) and its row count, min/max timestamp
    and detected gaps are recorded.
  * Output is diffable JSON with NO wall-clock field, so two runs over the same
    code/config/data are BYTE-IDENTICAL. Run time belongs in the log, not in a
    reproducibility artifact.
  * Coverage is checked and the run FAILS CLOSED on missing or short input. A
    registered run never silently falls back to a different data provider.

Usage:
    python backtesting/research_runner.py                 # write artifacts
    python backtesting/research_runner.py --check         # verify, do not write

Artifacts (committed; raw parquet is NOT):
    docs/research/artifacts/manifest.json    inputs + hashes + coverage
    docs/research/artifacts/results.json     result rows
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CANDLE_DIR = ROOT / "data" / "candles"
ARTIFACT_DIR = ROOT / "docs" / "research" / "artifacts"

# ── FROZEN configuration — do not parameterise from the CLI ──────────────────
RESEARCH_CONFIG: dict = {
    "config_id": "2026-08-evidence-hardening.v1",
    "trial_ids": ["V2-continuous", "V2-registry-windows", "V3-ER30-integrated"],
    "primary_asset": "ZEC-USD",
    "assets": ["ZEC-USD", "BTC-USD", "ETH-USD", "SOL-USD"],
    "continuous_window": {"start": "2021-03-01", "end": "2026-07-12"},
    # Per-asset evaluation starts, declared explicitly because listing dates
    # differ. SOL-USD has no Coinbase candles before 2021-06-17, so a row
    # labelled "2021-03-01" for SOL would misstate its own window — the earlier
    # artifact did exactly that. Declaring the boundary here keeps it frozen and
    # visible instead of silently truncated at scan time.
    "asset_eval_start": {
        "ZEC-USD": "2021-03-01",
        "BTC-USD": "2021-03-01",
        "ETH-USD": "2021-03-01",
        "SOL-USD": "2021-06-17",   # first Coinbase candle
    },
    "oos_freeze": "2026-07-12",
    # Referenced BY NAME from signal_scanner.PERIODS so the dates cannot drift
    # from the registry. Hardcoding them here previously produced three wrong
    # windows (bull_2021 ended 11-30 not 11-10; mid_year_holdout and recent_year
    # were both wrong), which silently changed the reported trade counts.
    "registry_window_names": [
        "bull_2021", "bear_2022", "mid_year_holdout", "recent_year",
    ],
    # Regime cells for the asset x regime matrix. These ARE the named regimes.
    "regime_names": ["bull_2021", "bear_2022", "mid_year_holdout", "recent_year"],
    # Fee/slippage assumptions must travel with the results.
    "costs": {
        "entry_fee_pct": 0.4,
        "take_profit_fee_pct": 0.4,
        "stop_loss_fee_pct": 0.6,
        "extra_friction_stress_pct": 0.25,
        "slippage_model": "none (limit entry at level, stop/target at price)",
    },
    "v3": {
        "status": "RETIRED / REJECTED FOR ACTIVATION (2026-08-09)",
        "threshold_historical_only": 0.20,
        "enforcement_enabled": False,
    },
    # The frozen ZEC mechanism, applied UNCHANGED to every asset for the
    # transfer test. Using each asset's own ASSET_CONFIG would measure
    # "per-asset tuned strategies", not "does the ZEC mechanism transfer".
    "frozen_mechanism": {
        "source": "ZEC-USD (git tag v2-adx25-frozen)",
        "atr_stop": 2.0,
        "atr_target": 3.5,
        "min_conditions": 4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 200,
        "btc_regime_filter": False,
        "v3_candidate_threshold": 0.20,
        "v3_enforcement_enabled": False,
        "enabled": True,
    },
    # Probes named in the task whose original implementations are NOT in this
    # repository. They are recorded as non-reproducible rather than recreated
    # from memory — inventing numbers to fill a manifest is the exact failure
    # mode this provenance work exists to prevent.
    "non_reproducible_probes": [
        "mean_reversion", "slow_trend", "agent_vote_ic", "drawdown_buying",
    ],
}

# Any hourly gap strictly larger than one bar is a real gap. The previous 6h
# threshold hid 2-5h holes entirely, which is exactly the size of gap that
# silently removes signals without looking like missing data.
_GAP_THRESHOLD_H = 1


class ProvenanceError(RuntimeError):
    """Input data is missing, short, or otherwise unfit for a registered run."""


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str:
    """Code commit the artifact was produced from; 'unknown' outside a repo."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT),
            capture_output=True, text=True, timeout=30, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def describe_dataset(path: Path) -> dict:
    """Hash + coverage description for one candle file. Fails closed."""
    if not path.exists():
        raise ProvenanceError(f"input dataset missing: {path}")
    df = pd.read_parquet(path)
    if df.empty:
        raise ProvenanceError(f"input dataset is empty: {path}")
    if "time" not in df.columns:
        raise ProvenanceError(f"input dataset has no 'time' column: {path}")

    ts = pd.to_datetime(df["time"], utc=True).sort_values().reset_index(drop=True)
    gaps: list[dict] = []
    total_missing_h = 0.0
    if len(ts) > 1 and path.stem.endswith("_1h"):
        deltas = ts.diff().dropna()
        big = deltas[deltas > pd.Timedelta(hours=_GAP_THRESHOLD_H)]
        for idx, delta in big.items():
            gap_h = delta.total_seconds() / 3600
            total_missing_h += gap_h - 1          # one bar is the normal step
            gaps.append({
                "after": ts.iloc[idx - 1].isoformat(),
                "gap_hours": round(gap_h, 2),
            })
    return {
        "file": path.name,
        "sha256": sha256_file(path),
        "rows": int(len(df)),
        "min_ts": ts.iloc[0].isoformat(),
        "max_ts": ts.iloc[-1].isoformat(),
        "gap_threshold_hours": _GAP_THRESHOLD_H,
        "n_gaps": len(gaps),
        "missing_bars_est": int(round(total_missing_h)),
        # Cap the listing so the artifact stays diffable.
        "gaps_sample": gaps[:10],
    }


def assert_covers(inputs: list[dict], asset: str, interval: str,
                  start: str, end: str) -> None:
    """
    Fail closed when a requested evaluation window is not fully covered.

    Without this, a window extending past the cache silently produced a shorter
    scan and the artifact reported it as a complete result.
    """
    stem = asset.replace("-", "_")
    fname = f"{stem}_{interval}.parquet"
    rec = next((i for i in inputs if i["file"] == fname), None)
    if rec is None:
        raise ProvenanceError(f"no manifest entry for {fname}")
    # Compare calendar dates: the first candle of a day carries a time-of-day
    # component (e.g. 17:00 on a listing day), which must not read as "starts
    # after" a midnight boundary on that same date.
    lo = pd.Timestamp(rec["min_ts"]).normalize()
    hi = pd.Timestamp(rec["max_ts"]).normalize()
    want_lo = pd.Timestamp(start, tz="UTC").normalize()
    want_hi = pd.Timestamp(end, tz="UTC").normalize()
    if lo > want_lo:
        raise ProvenanceError(
            f"{fname} starts {lo.date()}, after requested {want_lo.date()} — "
            "window is not fully covered"
        )
    if hi < want_hi:
        raise ProvenanceError(
            f"{fname} ends {hi.date()}, before requested {want_hi.date()} — "
            "window is not fully covered"
        )


def build_manifest(assets: Optional[list[str]] = None) -> dict:
    """Manifest of every input the registered run depends on."""
    assets = assets or RESEARCH_CONFIG["assets"]
    inputs = []
    for asset in assets:
        stem = asset.replace("-", "_")
        for interval in ("1h", "1d"):
            inputs.append(describe_dataset(CANDLE_DIR / f"{stem}_{interval}.parquet"))
    return {
        "config_id": RESEARCH_CONFIG["config_id"],
        "code_commit": _git_commit(),
        "config": RESEARCH_CONFIG,
        "inputs": sorted(inputs, key=lambda d: d["file"]),
    }


def _round(value, digits: int = 6):
    """Stable rounding so float noise cannot make two runs differ."""
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:            # nan
            return None
        if value in (float("inf"), float("-inf")):
            return str(value)
        return round(value, digits)
    return value


def _scan_window(asset: str, start: str, end: str, *, v3_enforcement: bool,
                 warmup: Optional[str] = None,
                 config_override: Optional[dict] = None) -> dict:
    from backtesting.equity_report import summary
    from backtesting.signal_scanner import scan_asset

    warmup = warmup or (pd.Timestamp(start) - pd.Timedelta(days=120)).date().isoformat()
    period = {"label": f"{asset} {start}->{end}", "btc_move": "",
              "warmup": warmup, "start": start, "end": end}
    res = scan_asset(asset, period, v3_enforcement=v3_enforcement,
                     config_override=config_override)
    sigs = res.get("signals", [])
    # Right-censored trades carry no realised outcome (Phase 2).
    closed = [s for s in sigs if s["trade"].get("resolved", True)]
    trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
               "hold_h": s["trade"]["hold_h"]} for s in closed]
    m = summary(trades, start=start, end=end)
    return {
        "asset": asset, "start": start, "end": end,
        "v3_enforcement": v3_enforcement,
        "mechanism": res.get("mechanism"),
        "n_signals": len(sigs),
        "n_closed": len(closed),
        "n_pending": len(sigs) - len(closed),
        "blocked_v3": int(res.get("blocked_v3", 0)),
        "pf": _round(m.get("pf")),
        "expectancy_pct": _round(m.get("expectancy")),
        "win_rate": _round(m.get("win_rate")),
        "max_dd": _round(m.get("max_dd")),
        "time_underwater": _round(m.get("time_underwater")),
        "longest_dd_days": _round(m.get("longest_dd_days"), 2),
        "total_ret": _round(m.get("total_ret")),
    }


def run_results(manifest: Optional[dict] = None) -> dict:
    """
    Produce every registered result row. Deterministic, no wall-clock.

    Runs with STRICT_COINBASE_ONLY so a cache miss raises instead of silently
    switching provider — the manifest hashes must describe the data actually used.
    """
    import backtesting.signal_scanner as scanner
    from backtesting.signal_scanner import PERIODS

    cfg = RESEARCH_CONFIG
    asset = cfg["primary_asset"]
    cw = cfg["continuous_window"]
    frozen = cfg["frozen_mechanism"]
    inputs = (manifest or build_manifest())["inputs"]
    rows: list[dict] = []

    # Coverage must be proven BEFORE any scan runs, against each asset's
    # DECLARED evaluation start (see asset_eval_start).
    for a in cfg["assets"]:
        a_start = cfg["asset_eval_start"][a]
        for interval in ("1h", "1d"):
            assert_covers(inputs, a, interval, a_start, cw["end"])

    prev_strict = scanner.STRICT_COINBASE_ONLY
    scanner.STRICT_COINBASE_ONLY = True
    try:
        # 1. Continuous-window ZEC (the falsification result), own mechanism.
        rows.append({"trial": "V2-continuous", **_scan_window(
            asset, cw["start"], cw["end"], v3_enforcement=False)})

        # 2. Registry windows — the period-selected view, for contrast.
        #    Dates and warmups come from PERIODS so they cannot drift.
        for name in cfg["registry_window_names"]:
            p = PERIODS[name]
            rows.append({"trial": f"V2-registry:{name}", **_scan_window(
                asset, p["start"], p["end"], v3_enforcement=False,
                warmup=p["warmup"])})

        # 3. Integrated V3 comparison on the continuous window.
        rows.append({"trial": "V3-ER30-integrated", **_scan_window(
            asset, cw["start"], cw["end"], v3_enforcement=True)})

        # 4. Zero-tuning transfer: the FROZEN ZEC mechanism applied unchanged to
        #    every asset. Using each asset's own tuned config would answer a
        #    different question.
        for a in cfg["assets"]:
            rows.append({"trial": f"transfer-frozen-zec:{a}", **_scan_window(
                a, cfg["asset_eval_start"][a], cw["end"], v3_enforcement=False,
                config_override=frozen)})

        # 5. Asset x REGIME matrix — actual regime cells, not a single aggregate.
        for a in cfg["assets"]:
            for name in cfg["regime_names"]:
                p = PERIODS[name]
                rows.append({"trial": f"regime:{a}:{name}", **_scan_window(
                    a, p["start"], p["end"], v3_enforcement=False,
                    warmup=p["warmup"], config_override=frozen)})
    finally:
        scanner.STRICT_COINBASE_ONLY = prev_strict

    return {
        "config_id": cfg["config_id"],
        "rows": rows,
        # Recorded explicitly so the artifact states what it does NOT cover.
        "non_reproducible": [
            {
                "probe": name,
                "reason": "no implementation found in this repository; the "
                          "published figures cannot be regenerated from "
                          "committed code and are NOT reproduced here",
            }
            for name in cfg["non_reproducible_probes"]
        ],
    }


def write_artifacts(out_dir: Optional[Path] = None) -> tuple[Path, Path]:
    out_dir = out_dir or ARTIFACT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest()
    results = run_results(manifest)
    m_path = out_dir / "manifest.json"
    r_path = out_dir / "results.json"
    # sort_keys + fixed indent + trailing newline => byte-stable output.
    m_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                      encoding="utf-8")
    r_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n",
                      encoding="utf-8")
    return m_path, r_path


def _cli() -> None:
    check_only = "--check" in sys.argv
    try:
        if check_only:
            manifest = build_manifest()
            print(json.dumps(manifest, indent=2, sort_keys=True))
            print(f"\nOK: {len(manifest['inputs'])} input datasets hashed.")
            return
        m_path, r_path = write_artifacts()
        print(f"wrote {m_path.relative_to(ROOT)}")
        print(f"wrote {r_path.relative_to(ROOT)}")
    except ProvenanceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    _cli()
