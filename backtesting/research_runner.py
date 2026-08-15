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

  * Identity of the CODE is content-addressed (SHA-256 per file plus an
    aggregate). `code_commit` is retained as an informational label only: it
    goes stale whenever history is rewritten, whereas the hashes survive any
    squash or rebase that does not change file contents.
  * The computational environment travels with the results. numpy/pandas/ta/
    pyarrow compute the indicators, so their versions determine the numbers as
    surely as the source does.

Usage:
    python backtesting/research_runner.py                 # write artifacts
    python backtesting/research_runner.py --check         # inputs only, no write
    python backtesting/research_runner.py --verify        # full byte-for-byte replay
    python backtesting/research_runner.py --verify-code   # code+env only (no candles,
                                                          # no git history needed)

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
    "config_id": "2026-08-warmup-semantics.v1",
    "trial_ids": ["V2-continuous", "V2-registry-windows", "V3-ER30-integrated"],
    "supersedes": {
        "config_id": "2026-08-evidence-hardening.v1",
        "reason": "incorrect warm-up semantics — declared gates failed OPEN "
                  "while their indicators were still warming up, so part of "
                  "every affected window was evaluated by a weaker mechanism "
                  "than the config declares. Results are NOT comparable.",
        "artifacts": "docs/research/artifacts/superseded/2026-08-evidence-hardening.v1/",
    },
    "primary_asset": "ZEC-USD",
    "assets": ["ZEC-USD", "BTC-USD", "ETH-USD", "SOL-USD"],
    "continuous_window": {"start": "2021-03-01", "end": "2026-07-12"},
    # First CACHED candle per asset, across both intervals — the earliest date
    # the data can support, not necessarily the exchange listing date (the cache
    # is capped at _DAILY_HISTORY_START for BTC/ETH). INFORMATIONAL ONLY: a
    # window may not start here, because the mechanism is not yet computable
    # then. The previous config used exactly these dates as evaluation starts.
    "asset_listing_start": {
        "ZEC-USD": "2020-12-08",
        "BTC-USD": "2020-01-02",   # 1d frame starts a day after the 1h frame
        "ETH-USD": "2020-01-02",
        "SOL-USD": "2021-06-17",
    },
    # REGISTERED evaluation boundaries: the first bar on the merged 1h grid at
    # which EVERY declared gate of the frozen mechanism is evaluable. Frozen
    # here and drift-checked at run time against
    # signal_scanner.asset_gate_availability(); a mismatch fails the run rather
    # than silently re-deriving the window from whatever the cache now holds.
    #
    # These are ~1 day later than the daily frame's own first-valid bar because
    # _attach_daily_context shifts daily stamps +1d to avoid look-ahead.
    "asset_effective_start": {
        "ZEC-USD": "2021-06-26T00:00:00+00:00",   # daily EMA200; listed 2020-12-08
        "BTC-USD": "2020-07-20T00:00:00+00:00",
        "ETH-USD": "2020-07-20T00:00:00+00:00",
        "SOL-USD": "2022-01-03T00:00:00+00:00",   # daily EMA200; listed 2021-06-17
    },
    "oos_freeze": "2026-07-12",
    # Referenced BY NAME from signal_scanner.PERIODS so the dates cannot drift
    # from the registry. Hardcoding them here previously produced three wrong
    # windows (bull_2021 ended 11-30 not 11-10; mid_year_holdout and recent_year
    # were both wrong), which silently changed the reported trade counts.
    "registry_window_names": [
        "bull_2021", "bear_2022", "mid_year_holdout", "recent_year",
    ],
    # Named CALENDAR windows. These are period cells, not regime cells — the
    # earlier artifact labelled them "regime", which conflated "when" with
    # "market state".
    "period_names": ["bull_2021", "bear_2022", "mid_year_holdout", "recent_year"],
    # True regime cells are cut on the regime METRIC measured at signal time.
    # Only the metrics the scanner actually computes are used: er_30 (Kaufman
    # efficiency) and vm_30 (vol-adjusted momentum). Realised-vol and Bollinger
    # bandwidth cells are NOT produced because this scanner does not compute
    # them — see non_reproducible_probes rather than inventing a proxy.
    "regime_metrics": {
        "er_30": [0.20, 0.35],     # chop | mixed | trend
        "vm_30": [0.0, 1.0],       # negative | weak | strong momentum
    },
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
        # Part of the mechanism: BTC's own config uses 48h, so omitting this
        # made the "frozen" transfer test run BTC on a different hold window.
        "max_hold_hours": 36,
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
        # The scanner computes er_30 / vm_30 / ema50_slope only. Realised-vol
        # and Bollinger-bandwidth regime cuts have no implementation here, so
        # they are declared rather than approximated with a stand-in metric.
        "realised_vol_regime", "bollinger_bandwidth_regime",
    ],
    # Coverage gaps that are KNOWN and accepted. Any gap not listed here fails
    # the run: checking only the first and last timestamp let interior holes
    # through, and a 2-5h hole silently removes signals.
    # Measured 2026-08-09; exchange-side outages, not download errors. Daily
    # series are complete. Raising a budget must be a deliberate, reviewed edit.
    "accepted_gap_bars": {
        "BTC_USD_1h.parquet": 16,   # 6 gaps
        "ETH_USD_1h.parquet": 16,   # 6 gaps
        "SOL_USD_1h.parquet": 15,   # 3 gaps
        "ZEC_USD_1h.parquet": 16,   # 5 gaps
        "BTC_USD_1d.parquet": 0,
        "ETH_USD_1d.parquet": 0,
        "SOL_USD_1d.parquet": 0,
        "ZEC_USD_1d.parquet": 0,
    },
}

# Any hourly gap strictly larger than one bar is a real gap. The previous 6h
# threshold hid 2-5h holes entirely, which is exactly the size of gap that
# silently removes signals without looking like missing data.
_GAP_THRESHOLD_H = 1


class ProvenanceError(RuntimeError):
    """Input data is missing, short, or otherwise unfit for a registered run."""


def sha256_file(path: Path) -> str:
    """Byte-exact hash. For DATA files, where every byte is the artifact."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_source(path: Path) -> str:
    """
    Hash of a SOURCE file with line endings normalised to LF.

    Byte-exact hashing is wrong for source. This repository has
    `core.autocrlf=true` and no `.gitattributes`, so a fresh clone checks the
    same commit out with CRLF while the authoring tree holds LF — the identical
    file hashed to two different values depending on the client's git config,
    and `--verify-code` failed on a clean clone of its own commit.

    A Python file's identity for reproducibility is its text, not its newline
    convention: CRLF and LF forms compile to the same program and produce the
    same numbers. Data files keep byte-exact hashing, where every byte matters.
    """
    raw = path.read_bytes()
    normalised = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalised).hexdigest()


# Files whose content determines the results. The recorded commit is the last
# one that touched THESE, not HEAD.
_CODE_PATHS = [
    "backtesting/research_runner.py",
    "backtesting/signal_scanner.py",
    "backtesting/equity_report.py",
    "backtesting/oos_replay.py",
    # The scanner executes these too, so a change in either can move results
    # without moving the stamp. Indicator construction and the candle loader are
    # as much a part of the computation as the scan loop itself.
    "backtesting/backtest.py",
    "exchange/coinbase_candles.py",
]


# ── Computational environment ────────────────────────────────────────────────
#
# These libraries compute the indicators, so their versions are as
# result-determining as the scanner source. requirements.txt pinned nothing at
# all, which meant a `ta` or `pandas` release could move every headline number
# with the artifact still verifying green.
#
# Python is declared to MAJOR.MINOR only. A patch release does not change
# floating-point semantics here, and pinning 3.13.5 exactly would break
# verification on 3.13.6 for no reproducibility gain. A minor-version change
# does matter and must be a deliberate re-registration.
_CANONICAL_PYTHON = "3.13"
_RESULT_DETERMINING_PACKAGES = ("numpy", "pandas", "ta", "pyarrow")


class EnvironmentError_(ProvenanceError):
    """The interpreter or libraries differ from the registered environment."""


def environment_fingerprint() -> dict:
    """Versions that determine the numbers, recorded alongside them."""
    import importlib.metadata as md

    packages = {}
    for name in _RESULT_DETERMINING_PACKAGES:
        try:
            packages[name] = md.version(name)
        except md.PackageNotFoundError:
            raise ProvenanceError(
                f"{name} is not installed but is result-determining. Install "
                "the pinned requirements before running a registered scan."
            ) from None
    return {"python": _CANONICAL_PYTHON, "packages": packages}


def assert_canonical_python() -> None:
    """
    Refuse to WRITE artifacts from a non-canonical interpreter.

    Without this, regenerating under a different minor version would silently
    rewrite the committed numbers and present them as the same experiment.
    """
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    if running != _CANONICAL_PYTHON:
        raise EnvironmentError_(
            f"registered environment is Python {_CANONICAL_PYTHON}, this is "
            f"{running}. Results are not comparable across minor versions; "
            "re-register deliberately rather than regenerating here."
        )


# ── Content-addressed code identity ──────────────────────────────────────────

def code_fingerprint() -> dict:
    """
    SHA-256 of every result-determining source file, plus an aggregate.

    This is the PRIMARY identity of the code that produced the results, and it
    is independent of git entirely. `code_commit` below is a convenience label:
    it goes stale on squash/rebase (twice already in this project's history,
    each time needing a follow-up commit), whereas these hashes survive any
    history rewrite that does not change file contents.
    """
    files = []
    for rel in _CODE_PATHS:
        path = ROOT / rel
        if not path.exists():
            raise ProvenanceError(f"result-determining file missing: {rel}")
        files.append({"file": rel, "sha256": sha256_source(path)})
    files.sort(key=lambda d: d["file"])
    agg = hashlib.sha256()
    for entry in files:
        agg.update(f"{entry['file']}:{entry['sha256']}\n".encode())
    return {"files": files, "code_sha256": agg.hexdigest()}


def assert_code_is_committed() -> None:
    """
    Refuse to write artifacts from a dirty working tree.

    A manifest generated from uncommitted edits describes code that exists
    nowhere but one laptop. Absence of git is not an excuse to skip the check
    silently — it is reported — but it is not a failure either, because a
    source tarball is a legitimate way to run this.
    """
    try:
        out = subprocess.run(
            ["git", "status", "--porcelain", "--", *_CODE_PATHS],
            cwd=str(ROOT), capture_output=True, text=True, timeout=30, check=False,
        )
    except Exception:
        print("note: git unavailable; cannot check for uncommitted research code",
              file=sys.stderr)
        return
    if out.returncode != 0:
        print("note: not a git checkout; skipping the dirty-tree check",
              file=sys.stderr)
        return
    dirty = [ln for ln in out.stdout.splitlines() if ln.strip()]
    if dirty:
        raise ProvenanceError(
            "result-determining code has uncommitted changes:\n  "
            + "\n  ".join(dirty)
            + "\nCommit them first: an artifact must describe code that exists "
              "outside this working tree."
        )


def _git_commit() -> str:
    """
    Commit of the CODE that produced these results.

    Deliberately NOT `rev-parse HEAD`. Artifacts are committed after the code,
    so a HEAD-based stamp is stale the moment the artifact commit lands: the
    committed manifest said ccd4a1b while regenerating produced 089ca43, which
    made the artifact impossible to verify from the final branch state. Pinning
    to the last commit that touched the result-determining code makes the stamp
    stable across artifact-only commits, so regeneration from the tip is
    byte-identical.
    """
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%H", "--", *_CODE_PATHS],
            cwd=str(ROOT), capture_output=True, text=True, timeout=30, check=False,
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

    # Endpoints alone are not coverage: interior holes remove signals while both
    # boundaries still look correct. Any gap beyond the declared budget fails.
    allowed = RESEARCH_CONFIG["accepted_gap_bars"].get(fname)
    if allowed is None:
        raise ProvenanceError(
            f"{fname} has no declared gap budget — refusing to run with "
            "undeclared coverage"
        )
    missing = rec.get("missing_bars_est", 0)
    if missing > allowed:
        raise ProvenanceError(
            f"{fname} is missing ~{missing} bars, above the declared budget of "
            f"{allowed}. Interior coverage changed; re-declare it deliberately "
            "instead of letting the run absorb it."
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
        # PRIMARY code identity: content-addressed, git-independent.
        "code": code_fingerprint(),
        # Informational provenance label only. Kept because it is useful for
        # "where did this come from", not relied on for identity: it goes stale
        # on squash/rebase while the hashes above do not.
        "code_commit": _git_commit(),
        "environment": environment_fingerprint(),
        "config": RESEARCH_CONFIG,
        "inputs": sorted(inputs, key=lambda d: d["file"]),
    }


def verify_code_and_environment() -> bool:
    """
    Cheap identity check: code hashes + environment, nothing else.

    Runs in milliseconds with NO candle cache and NO git history, so CI can
    assert that the committed artifact describes the checked-out code even where
    the full replay is impractical. The full --verify remains the stronger
    guarantee; this is the one that can run anywhere.
    """
    m_path = ARTIFACT_DIR / "manifest.json"
    if not m_path.exists():
        raise ProvenanceError("manifest is not committed — nothing to verify")
    committed = json.loads(m_path.read_text(encoding="utf-8"))

    ok = True
    fresh_code = code_fingerprint()
    old_code = committed.get("code") or {}
    if old_code.get("code_sha256") != fresh_code["code_sha256"]:
        ok = False
        print("MISMATCH: result-determining code differs from the artifact",
              file=sys.stderr)
        old_by_file = {e["file"]: e["sha256"] for e in old_code.get("files", [])}
        for entry in fresh_code["files"]:
            was = old_by_file.get(entry["file"])
            if was != entry["sha256"]:
                print(f"  {entry['file']}: manifest {was} != working tree "
                      f"{entry['sha256']}", file=sys.stderr)

    fresh_env = environment_fingerprint()
    if committed.get("environment") != fresh_env:
        ok = False
        print(f"MISMATCH: environment differs\n  manifest: "
              f"{committed.get('environment')}\n  running : {fresh_env}",
              file=sys.stderr)
    return ok


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


def _as_utc(stamp: str) -> pd.Timestamp:
    """Parse a bare date or a full ISO stamp to a UTC instant."""
    ts = pd.Timestamp(stamp)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts


def effective_start(asset: str, requested: str) -> str:
    """
    Clip a requested window start to the REGISTERED boundary at which the
    mechanism becomes computable.

    A window that opens before this point does not measure the declared
    strategy: until every gate has its inputs, the scanner can only refuse
    signals (previously: silently admit them under a weaker mechanism).
    """
    declared = _as_utc(RESEARCH_CONFIG["asset_effective_start"][asset])
    return max(_as_utc(requested), declared).isoformat()


def _scan_window(asset: str, start: str, end: str, *, v3_enforcement: bool,
                 warmup: Optional[str] = None,
                 config_override: Optional[dict] = None) -> dict:
    from backtesting.equity_report import summary
    from backtesting.signal_scanner import scan_asset

    requested = start
    start = effective_start(asset, requested)
    warmup = warmup or (pd.Timestamp(start) - pd.Timedelta(days=120)).date().isoformat()
    period = {"label": f"{asset} {start}->{end}", "btc_move": "",
              "warmup": warmup, "start": start, "end": end}
    res = scan_asset(asset, period, v3_enforcement=v3_enforcement,
                     config_override=config_override)
    sigs = res.get("signals", [])
    # Invariant, not a diagnostic: the window opens at the registered boundary,
    # so nothing inside it can be judged on a missing input. If this ever trips,
    # the declared boundary and the data no longer agree.
    if res.get("blocked_gate_unavailable", 0):
        raise ProvenanceError(
            f"{asset} {start}->{end}: {res['blocked_gate_unavailable']} signals "
            f"had unavailable gate inputs INSIDE the effective window "
            f"({res.get('gate_unavailable_by_input')}). The registered "
            "effective_start disagrees with the data."
        )
    # Right-censored trades carry no realised outcome (Phase 2).
    closed = [s for s in sigs if s["trade"].get("resolved", True)]
    trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
               "hold_h": s["trade"]["hold_h"]} for s in closed]
    m = summary(trades, start=start, end=end)
    return {
        "asset": asset, "start": start, "end": end,
        # Both boundaries travel with the row: a reader must be able to see that
        # the evaluated window is not the one that was asked for, and why.
        "requested_start": requested,
        "effective_start": start,
        # Compare INSTANTS, not strings. effective_start() returns a full ISO
        # stamp while a registered start is a bare date, so "2022-01-01" vs
        # "2022-01-01T00:00:00+00:00" is unequal as text and identical in time —
        # which flagged three unclipped registry windows as clipped.
        "start_clipped_to_gate_availability": bool(
            pd.Timestamp(start) > _as_utc(requested)),
        "n_gate_unavailable": int(res.get("blocked_gate_unavailable", 0)),
        "v3_enforcement": v3_enforcement,
        "mechanism": res.get("mechanism"),
        # The parameters ACTUALLY used. Dropping these hid the max_hold leak:
        # a row could claim mechanism="override" while still running the asset's
        # own hold window.
        "mechanism_params": res.get("mechanism_params"),
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


def _bucket(value: Optional[float], edges: list[float]) -> str:
    """Label a regime metric by fixed, frozen bin edges."""
    if value is None:
        return "unknown"
    for i, e in enumerate(edges):
        if value < e:
            return f"b{i}_lt_{e}"
    return f"b{len(edges)}_ge_{edges[-1]}"


def _regime_cells(asset: str, start: str, end: str) -> list[dict]:
    """
    TRUE regime cells: signals cut by the regime METRIC measured at signal time,
    not by calendar window. Uses the frozen ZEC mechanism so cells are
    comparable across assets.
    """
    from backtesting.equity_report import summary
    from backtesting.signal_scanner import scan_asset

    cfg = RESEARCH_CONFIG
    start = effective_start(asset, start)
    warmup = (pd.Timestamp(start) - pd.Timedelta(days=120)).date().isoformat()
    period = {"label": f"{asset} regimes", "btc_move": "",
              "warmup": warmup, "start": start, "end": end}
    res = scan_asset(asset, period, v3_enforcement=False,
                     config_override=cfg["frozen_mechanism"])
    closed = [s for s in res.get("signals", [])
              if s["trade"].get("resolved", True)]

    out: list[dict] = []
    for metric, edges in sorted(cfg["regime_metrics"].items()):
        buckets: dict[str, list] = {}
        for s in closed:
            label = _bucket(s.get("regime", {}).get(metric), edges)
            buckets.setdefault(label, []).append(s)
        for label in sorted(buckets):
            group = buckets[label]
            trades = [{"ts": s["timestamp"], "pnl_pct": s["trade"]["pnl_pct"],
                       "hold_h": s["trade"]["hold_h"]} for s in group]
            m = summary(trades, start=start, end=end)
            out.append({
                "trial": f"regime:{asset}:{metric}:{label}",
                "asset": asset, "start": start, "end": end,
                "regime_metric": metric, "regime_bucket": label,
                "n_closed": len(group),
                "pf": _round(m.get("pf")),
                "expectancy_pct": _round(m.get("expectancy")),
                "win_rate": _round(m.get("win_rate")),
            })
    return out


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

    # Coverage must be proven BEFORE any scan runs, against each asset's LISTING
    # date — the data has to reach that far back even though evaluation starts
    # later, because the daily warm-up is computed from those earlier bars.
    for a in cfg["assets"]:
        a_start = cfg["asset_listing_start"][a]
        for interval in ("1h", "1d"):
            assert_covers(inputs, a, interval, a_start, cw["end"])

    prev_strict = scanner.STRICT_COINBASE_ONLY
    scanner.STRICT_COINBASE_ONLY = True
    try:
        # 0. Drift check: the REGISTERED effective boundary must still match what
        #    the data says. Declared, then verified — never silently re-derived,
        #    or the evaluation window would move with the cache.
        boundaries: list[dict] = []
        for a in cfg["assets"]:
            avail = scanner.asset_gate_availability(a, frozen, cw["end"])
            declared = cfg["asset_effective_start"][a]
            if avail["effective_start"] != declared:
                raise ProvenanceError(
                    f"{a}: registered effective_start {declared} but the data "
                    f"now yields {avail['effective_start']}. Gate availability "
                    "drifted; re-register the boundary deliberately."
                )
            boundaries.append({
                "asset": a,
                "listing_start": cfg["asset_listing_start"][a],
                "effective_start": declared,
                "per_gate_first_valid": avail["per_gate"],
            })

        # 0b. Warm-up exclusion accounting: what the previous config evaluated
        #     between the listing date and the effective boundary, and which the
        #     old fail-open rule ADMITTED under a weaker mechanism. Scanned
        #     explicitly so the size of the correction is in the artifact rather
        #     than only in a commit message.
        exclusions: list[dict] = []
        for a in cfg["assets"]:
            lo = cfg["asset_listing_start"][a]
            hi = cfg["asset_effective_start"][a]
            prev_requested = max(pd.Timestamp(lo, tz="UTC"),
                                 pd.Timestamp(cw["start"], tz="UTC"))
            if prev_requested >= pd.Timestamp(hi):
                exclusions.append({"asset": a, "excluded_candles": 0,
                                   "refused_signals_with_missing_input": 0,
                                   "by_missing_input": {}, "window": None})
                continue
            w_start = prev_requested.isoformat()
            # HALF-OPEN: [requested_start, effective_start). The candle AT the
            # effective start is the first EVALUATED bar, so counting it here
            # would put one bar in both the excluded warm-up and the corrected
            # window. The loader treats `end` as inclusive, hence the -1h.
            w_end = (pd.Timestamp(hi) - pd.Timedelta(hours=1)).isoformat()
            period = {"label": f"{a} warmup-exclusion", "btc_move": "",
                      "warmup": (prev_requested - pd.Timedelta(days=120)).date().isoformat(),
                      "start": w_start, "end": w_end}
            res = scanner.scan_asset(a, period, v3_enforcement=False,
                                     config_override=frozen)
            last_excl = res.get("last_gate_unavailable_ts")
            if last_excl is not None and pd.Timestamp(last_excl) >= pd.Timestamp(hi):
                raise ProvenanceError(
                    f"{a}: warm-up exclusion reaches {last_excl}, at or past the "
                    f"effective start {hi}. The interval is no longer half-open."
                )
            exclusions.append({
                "asset": a,
                "window": {"start": w_start, "end": w_end,
                           "interval": "[requested_start, effective_start)"},
                "excluded_candles": int(res.get("candles", 0)),
                "last_excluded_signal_ts": last_excl,
                # Signals that FORMED in this span but could not be judged by the
                # declared mechanism. Under the previous fail-open rule each of
                # these was evaluated by the remaining gates instead, and those
                # that cleared them were traded. How many actually traded is a
                # property of the OLD code and is therefore recorded in
                # docs/trial_registry.md against the superseded artifact, not
                # invented here.
                "refused_signals_with_missing_input": int(
                    res.get("blocked_gate_unavailable", 0)),
                "by_missing_input": res.get("gate_unavailable_by_input", {}),
            })

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
            _ = name

        # 3. Integrated V3 comparison on the continuous window.
        rows.append({"trial": "V3-ER30-integrated", **_scan_window(
            asset, cw["start"], cw["end"], v3_enforcement=True)})

        # 4. Zero-tuning transfer: the FROZEN ZEC mechanism applied unchanged to
        #    every asset. Using each asset's own tuned config would answer a
        #    different question.
        #    All four assets REQUEST the same calendar start and are then clipped
        #    to their own gate-availability boundary. Requesting each asset's
        #    listing date instead would widen BTC/ETH by ~8 months relative to
        #    the superseded artifact, mixing a window change into what is meant
        #    to isolate the warm-up correction.
        for a in cfg["assets"]:
            rows.append({"trial": f"transfer-frozen-zec:{a}", **_scan_window(
                a, cw["start"], cw["end"], v3_enforcement=False,
                config_override=frozen)})

        # 5. Asset x CALENDAR PERIOD cells (named "period", not "regime" — these
        #    are windows in time, not market states).
        for a in cfg["assets"]:
            for name in cfg["period_names"]:
                p = PERIODS[name]
                # Instants, not strings: effective_start() returns a full ISO
                # stamp and p["end"] is a bare date, so text comparison is the
                # same defect class that mis-set the clip flag. Correct on
                # today's dates, wrong the moment a boundary carries a time.
                if _as_utc(effective_start(a, p["start"])) >= _as_utc(p["end"]):
                    # The mechanism is not evaluable anywhere inside this window
                    # for this asset — emit nothing rather than a cell whose
                    # label promises a window it never scanned. The clip is only
                    # consulted here; the REGISTERED date is what gets passed on.
                    continue
                # Pass the registered start, not the clipped one. Clipping here
                # made _scan_window see an already-clipped date, so the row
                # reported requested_start == effective_start and
                # start_clipped_to_gate_availability == false — erasing the very
                # fact the field exists to record. bull_2021 claimed it had been
                # asked for 2021-06-26 when it was asked for 2021-03-01.
                rows.append({"trial": f"period:{a}:{name}", **_scan_window(
                    a, p["start"], p["end"], v3_enforcement=False,
                    warmup=p["warmup"], config_override=frozen)})

        # 6. Asset x REGIME cells — cut on the regime metric at signal time.
        for a in cfg["assets"]:
            rows.extend(_regime_cells(a, cw["start"], cw["end"]))
    finally:
        scanner.STRICT_COINBASE_ONLY = prev_strict

    return {
        "config_id": cfg["config_id"],
        # The registered evaluation boundaries and the warm-up they exclude. A
        # reader must be able to see WHICH window produced these rows without
        # re-running anything.
        "gate_boundaries": boundaries,
        "warmup_exclusions": exclusions,
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
    # Writing is the moment provenance is claimed, so both guards live here and
    # not in verify: verifying a tarball with no git history is legitimate,
    # publishing numbers from uncommitted code on an unregistered interpreter is
    # not.
    assert_canonical_python()
    assert_code_is_committed()
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


def verify_artifacts() -> bool:
    """
    Self-verification: regenerate in memory and compare byte-for-byte with the
    committed artifacts. Exercised from the FINAL branch state, so it proves the
    committed files can actually be reproduced from the committed code.
    """
    m_path = ARTIFACT_DIR / "manifest.json"
    r_path = ARTIFACT_DIR / "results.json"
    if not m_path.exists() or not r_path.exists():
        raise ProvenanceError("artifacts are not committed — nothing to verify")

    manifest = build_manifest()
    results = run_results(manifest)
    fresh_m = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    fresh_r = json.dumps(results, indent=2, sort_keys=True) + "\n"

    ok = True
    if fresh_m != m_path.read_text(encoding="utf-8"):
        print("MISMATCH: manifest.json differs from a fresh run", file=sys.stderr)
        ok = False
    if fresh_r != r_path.read_text(encoding="utf-8"):
        print("MISMATCH: results.json differs from a fresh run", file=sys.stderr)
        ok = False
    return ok


def _cli() -> None:
    try:
        if "--check" in sys.argv:
            manifest = build_manifest()
            print(json.dumps(manifest, indent=2, sort_keys=True))
            print(f"\nOK: {len(manifest['inputs'])} input datasets hashed.")
            return
        if "--verify-code" in sys.argv:
            # Cheap identity check — no candle cache, no git history needed.
            if verify_code_and_environment():
                print("OK: code hashes and environment match the artifact.")
                return
            sys.exit(1)
        if "--verify" in sys.argv:
            # Code identity first: when both are wrong, "the code changed" is
            # the useful message and "the numbers changed" is its consequence.
            code_ok = verify_code_and_environment()
            if verify_artifacts() and code_ok:
                print("OK: committed artifacts reproduce byte-for-byte.")
                return
            sys.exit(1)
        m_path, r_path = write_artifacts()
        print(f"wrote {m_path.relative_to(ROOT)}")
        print(f"wrote {r_path.relative_to(ROOT)}")
    except ProvenanceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    _cli()
