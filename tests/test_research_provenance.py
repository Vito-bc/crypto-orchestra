"""
Provenance tests — the registry's headline claims must match the artifact.

Phase 5 of docs/tasks/2026-08-research-evidence-hardening.md requires that the
prose review and trial registry are either generated from the machine-readable
artifact or checked against it. These tests do the checking, so a documented
number cannot drift away from the code that produced it.

The heavy runner is NOT executed here (it replays years of candles). These tests
read the committed artifact and assert the documented claims against it.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "docs" / "research" / "artifacts"
MANIFEST = ARTIFACTS / "manifest.json"
RESULTS = ARTIFACTS / "results.json"


def _load(path: Path) -> dict:
    if not path.exists():
        pytest.skip(f"{path.name} not generated in this checkout")
    return json.loads(path.read_text(encoding="utf-8"))


def _row(results: dict, trial: str) -> dict:
    for r in results["rows"]:
        if r["trial"] == trial:
            return r
    raise AssertionError(f"trial {trial!r} missing from results.json")


# ── Manifest completeness ─────────────────────────────────────────────────────

def test_manifest_records_code_commit_and_config() -> None:
    m = _load(MANIFEST)
    assert m["code_commit"], "artifact must record the code commit"
    assert m["config_id"], "artifact must record a config/trial id"
    assert m["config"]["costs"], "fee/slippage assumptions must travel with results"


def test_manifest_hashes_every_input() -> None:
    m = _load(MANIFEST)
    assert m["inputs"], "no input datasets recorded"
    for inp in m["inputs"]:
        assert len(inp["sha256"]) == 64, f"{inp['file']}: not a SHA-256"
        assert inp["rows"] > 0
        assert inp["min_ts"] < inp["max_ts"]


def test_manifest_records_evaluation_boundaries() -> None:
    m = _load(MANIFEST)
    cw = m["config"]["continuous_window"]
    assert cw["start"] and cw["end"]
    assert m["config"]["oos_freeze"] == "2026-07-12"


# ── The retirement decision is backed by the artifact ─────────────────────────

def test_v3_integrated_is_worse_than_unfiltered() -> None:
    """
    The stated reason for retiring V3 is PF 0.69 integrated vs 0.86 without.
    If this ever stops holding, the retirement rationale in trial_registry.md is
    stale and must be revisited — not silently left in place.
    """
    r = _load(RESULTS)
    base = _row(r, "V2-continuous")
    integ = _row(r, "V3-ER30-integrated")
    assert base["pf"] == pytest.approx(0.86, abs=0.02), (
        f"registry documents PF 0.86 for the continuous window, artifact says {base['pf']}"
    )
    assert integ["pf"] == pytest.approx(0.69, abs=0.02), (
        f"registry documents PF 0.69 for integrated V3, artifact says {integ['pf']}"
    )
    assert integ["pf"] < base["pf"], "V3 must be worse — that is why it is retired"


def test_continuous_window_is_unprofitable() -> None:
    """CLAUDE.md states PF 0.86, -0.37%/trade, n=133. Keep prose honest."""
    r = _load(RESULTS)
    base = _row(r, "V2-continuous")
    assert base["n_closed"] == 133, f"documented n=133, artifact says {base['n_closed']}"
    assert base["pf"] < 1.0, "documented as not profitable"
    # expectancy is a fraction in the artifact (-0.003662 == -0.37%)
    assert base["expectancy_pct"] * 100 == pytest.approx(-0.37, abs=0.02)


def test_zec_drawdown_matches_documented_audit() -> None:
    """docs task records max DD ~-71.04% for the continuous ZEC window."""
    r = _load(RESULTS)
    base = _row(r, "V2-continuous")
    assert base["max_dd"] == pytest.approx(-0.7104, abs=0.001)


def test_integrated_path_blocks_more_than_zero() -> None:
    """The integrated row must come from a real enforced scan."""
    r = _load(RESULTS)
    integ = _row(r, "V3-ER30-integrated")
    assert integ["v3_enforcement"] is True
    assert integ["blocked_v3"] > 0
    base = _row(r, "V2-continuous")
    assert base["v3_enforcement"] is False
    assert base["n_closed"] != integ["n_closed"], (
        "enforcement must change the cohort; identical counts suggest a post-filter"
    )


def test_no_asset_is_profitable_on_the_continuous_window() -> None:
    """
    Guards the central verdict. If any asset turns PF > 1 here, the LIVE NO-GO
    reasoning in CLAUDE.md needs re-examination rather than quiet contradiction.
    """
    r = _load(RESULTS)
    asset_rows = [x for x in r["rows"] if x["trial"].startswith("V2-asset:")]
    assert asset_rows, "no per-asset rows in the artifact"
    for row in asset_rows:
        assert row["pf"] < 1.0, (
            f"{row['trial']} now shows PF {row['pf']} >= 1.0 — the documented "
            "'no positive edge' verdict must be revisited"
        )


# ── Raw market data must never be committed ───────────────────────────────────

def test_artifacts_contain_no_raw_market_data() -> None:
    """Only manifest/results belong in the repo — parquet stays out."""
    if not ARTIFACTS.exists():
        pytest.skip("artifacts not generated")
    offenders = [p.name for p in ARTIFACTS.rglob("*")
                 if p.is_file() and p.suffix.lower() in {".parquet", ".csv", ".feather"}]
    assert not offenders, f"raw data must not be committed: {offenders}"


def test_results_are_json_serialisable_and_stable() -> None:
    """Re-serialising must be byte-stable (sorted keys, no wall-clock field)."""
    r = _load(RESULTS)
    again = json.dumps(r, indent=2, sort_keys=True) + "\n"
    assert again == RESULTS.read_text(encoding="utf-8"), (
        "results.json is not in canonical sorted form — reruns would differ"
    )
    flat = json.dumps(r)
    for banned in ("generated_at", "run_at", "timestamp_utc"):
        assert banned not in flat, (
            f"{banned!r} in the artifact would break byte-identical reruns"
        )
