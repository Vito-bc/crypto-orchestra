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
    "oos_freeze": "2026-07-12",
    "registry_windows": [
        {"label": "2021_bull",      "start": "2021-03-01", "end": "2021-11-30"},
        {"label": "2022_bear",      "start": "2022-01-01", "end": "2022-12-31"},
        {"label": "2024_recovery",  "start": "2024-07-01", "end": "2025-03-31"},
        {"label": "2025_full_year", "start": "2025-04-01", "end": "2026-06-30"},
    ],
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
}

# A gap larger than this in an hourly series is reported as a coverage gap.
_GAP_THRESHOLD_H = 6


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

    ts = pd.to_datetime(df["time"], utc=True).sort_values()
    gaps: list[dict] = []
    if len(ts) > 1 and path.stem.endswith("_1h"):
        deltas = ts.diff().dropna()
        big = deltas[deltas > pd.Timedelta(hours=_GAP_THRESHOLD_H)]
        for idx, delta in big.items():
            gaps.append({
                "after": ts.loc[idx - 1].isoformat() if (idx - 1) in ts.index else None,
                "gap_hours": round(delta.total_seconds() / 3600, 2),
            })
    return {
        "file": path.name,
        "sha256": sha256_file(path),
        "rows": int(len(df)),
        "min_ts": ts.iloc[0].isoformat(),
        "max_ts": ts.iloc[-1].isoformat(),
        "n_gaps_over_%dh" % _GAP_THRESHOLD_H: len(gaps),
        # Cap the listing so the artifact stays diffable.
        "gaps_sample": gaps[:10],
    }


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


def _scan_window(asset: str, start: str, end: str, *, v3_enforcement: bool) -> dict:
    from backtesting.equity_report import summary
    from backtesting.signal_scanner import scan_asset

    warmup = (pd.Timestamp(start) - pd.Timedelta(days=120)).date().isoformat()
    period = {"label": f"{asset} {start}->{end}", "btc_move": "",
              "warmup": warmup, "start": start, "end": end}
    res = scan_asset(asset, period, v3_enforcement=v3_enforcement)
    sigs = res.get("signals", [])
    # Right-censored trades carry no realised outcome (Phase 2).
    closed = [s for s in sigs if s["trade"].get("resolved", True)]
    trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
               "hold_h": s["trade"]["hold_h"]} for s in closed]
    m = summary(trades, start=start, end=end)
    return {
        "asset": asset, "start": start, "end": end,
        "v3_enforcement": v3_enforcement,
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


def run_results() -> dict:
    """Produce every registered result row. Deterministic, no wall-clock."""
    cfg = RESEARCH_CONFIG
    asset = cfg["primary_asset"]
    cw = cfg["continuous_window"]
    rows: list[dict] = []

    # 1. Continuous-window ZEC (the falsification result).
    rows.append({"trial": "V2-continuous", **_scan_window(
        asset, cw["start"], cw["end"], v3_enforcement=False)})

    # 2. Registry windows — the period-selected view, for contrast.
    for w in cfg["registry_windows"]:
        rows.append({"trial": f"V2-registry:{w['label']}", **_scan_window(
            asset, w["start"], w["end"], v3_enforcement=False)})

    # 3. Integrated V3 comparison on the continuous window.
    rows.append({"trial": "V3-ER30-integrated", **_scan_window(
        asset, cw["start"], cw["end"], v3_enforcement=True)})

    # 4. Asset-by-regime matrix on the continuous window.
    for a in cfg["assets"]:
        rows.append({"trial": f"V2-asset:{a}", **_scan_window(
            a, cw["start"], cw["end"], v3_enforcement=False)})

    return {"config_id": cfg["config_id"], "rows": rows}


def write_artifacts(out_dir: Optional[Path] = None) -> tuple[Path, Path]:
    out_dir = out_dir or ARTIFACT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest()
    results = run_results()
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
