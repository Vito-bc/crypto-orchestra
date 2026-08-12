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
    asset_rows = [x for x in r["rows"] if x["trial"].startswith("transfer-frozen-zec:")]
    assert asset_rows, "no transfer rows in the artifact"
    for row in asset_rows:
        assert row["pf"] < 1.0, (
            f"{row['trial']} now shows PF {row['pf']} >= 1.0 — the documented "
            "'no positive edge' verdict must be revisited"
        )


def test_transfer_test_uses_the_frozen_mechanism() -> None:
    """
    The zero-tuning transfer claim requires the SAME mechanism on every asset.
    Using each asset's own tuned ASSET_CONFIG would measure per-asset tuned
    strategies and could not support "the ZEC mechanism does not transfer".
    """
    r = _load(RESULTS)
    rows = [x for x in r["rows"] if x["trial"].startswith("transfer-frozen-zec:")]
    assert len(rows) >= 4
    for row in rows:
        assert row["mechanism"] == "override", (
            f"{row['trial']} ran with its own config, not the frozen mechanism"
        )


def test_registry_windows_match_registered_dates() -> None:
    """
    Window dates come from signal_scanner.PERIODS, not hardcoded copies. Three
    of four were previously wrong, which silently changed the trade counts.
    """
    from backtesting.signal_scanner import PERIODS

    r = _load(RESULTS)
    seen = 0
    for row in r["rows"]:
        if not row["trial"].startswith("V2-registry:"):
            continue
        name = row["trial"].split(":", 1)[1]
        assert name in PERIODS, f"unknown period {name}"
        assert row["start"] == PERIODS[name]["start"], f"{name} start drifted"
        assert row["end"] == PERIODS[name]["end"], f"{name} end drifted"
        seen += 1
    assert seen == 4, f"expected 4 registry windows, found {seen}"


def test_registry_window_trade_counts_are_the_registered_ones() -> None:
    """Documented per-window closed-trade counts: 25 / 12 / 27 / 37."""
    r = _load(RESULTS)
    expected = {
        "bull_2021": 25, "bear_2022": 12,
        "mid_year_holdout": 27, "recent_year": 37,
    }
    got = {row["trial"].split(":", 1)[1]: row["n_closed"]
           for row in r["rows"] if row["trial"].startswith("V2-registry:")}
    assert got == expected, f"registry window counts changed: {got}"


def test_regime_matrix_has_real_cells() -> None:
    """An asset aggregate is not a regime matrix — there must be per-regime cells."""
    r = _load(RESULTS)
    cells = [x for x in r["rows"] if x["trial"].startswith("regime:")]
    assets = {x["asset"] for x in cells}
    regimes = {x["trial"].split(":")[2] for x in cells}
    assert len(assets) >= 4 and len(regimes) >= 4, (
        f"expected an asset x regime grid, got {len(assets)} assets x {len(regimes)} regimes"
    )
    assert len(cells) == len(assets) * len(regimes), "grid is incomplete"


def test_period_selection_artifact_is_visible() -> None:
    """
    The documented story: individual registry windows can look fine while the
    continuous window does not. If that ever stops holding, the 'period-selected
    windows' explanation in trial_registry.md is stale.
    """
    r = _load(RESULTS)
    zec_cells = [x for x in r["rows"]
                 if x["trial"].startswith("regime:ZEC-USD:") and x["pf"] is not None]
    assert any(c["pf"] > 1.0 for c in zec_cells), (
        "no ZEC regime window shows PF > 1 — the period-selection explanation "
        "for the earlier positive case no longer holds"
    )
    assert _row(r, "V2-continuous")["pf"] < 1.0


def test_non_reproducible_probes_are_declared_not_invented() -> None:
    """
    Four probes named in the review have no implementation in this repo. They
    must be recorded as non-reproducible; regenerating their numbers from memory
    is exactly what this provenance work exists to prevent.
    """
    r = _load(RESULTS)
    declared = {p["probe"] for p in r.get("non_reproducible", [])}
    assert {"mean_reversion", "slow_trend", "agent_vote_ic",
            "drawdown_buying"} <= declared
    # And they must NOT appear as if they had been reproduced.
    trials = {row["trial"] for row in r["rows"]}
    for probe in declared:
        assert not any(probe in t for t in trials), (
            f"{probe} is declared non-reproducible but a result row claims it"
        )


def test_manifest_records_per_asset_evaluation_starts() -> None:
    """
    SOL has no Coinbase candles before 2021-06-17. A row labelled 2021-03-01
    for SOL would misstate its own window, so the boundary is declared.
    """
    m = _load(MANIFEST)
    starts = m["config"]["asset_eval_start"]
    assert starts["SOL-USD"] == "2021-06-17"
    r = _load(RESULTS)
    sol = next(x for x in r["rows"] if x["trial"] == "transfer-frozen-zec:SOL-USD")
    assert sol["start"] == "2021-06-17", "SOL row must state its real window"


def test_gap_detection_is_not_blind_to_short_holes() -> None:
    """A 6h threshold hid 2-5h holes — the size that silently drops signals."""
    m = _load(MANIFEST)
    for inp in m["inputs"]:
        assert inp["gap_threshold_hours"] == 1, (
            "hourly gap detection must flag anything longer than one bar"
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
