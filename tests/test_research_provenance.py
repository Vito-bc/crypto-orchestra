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
RESEARCH_END = "2026-07-12"


def _load(path: Path) -> dict:
    if not path.exists():
        pytest.skip(f"{path.name} not generated in this checkout")
    return json.loads(path.read_text(encoding="utf-8"))


def _utc(stamp: str):
    """
    Parse a bare date or a full ISO stamp to a UTC instant.

    Window boundaries are compared as INSTANTS everywhere in this file.
    Comparing them as text accepts a real bug: "2022-01-01" and
    "2022-01-01T00:00:00+00:00" are unequal as strings and identical in time, so
    a window that was never clipped reads as clipped.
    """
    import pandas as pd

    ts = pd.Timestamp(stamp)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts


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


# ── Content-addressed code identity (Phase 6.9) ──────────────────────────────

def test_code_identity_is_content_addressed() -> None:
    """
    The PRIMARY identity of the research code is its content, not a commit.
    code_commit went stale twice in this project's history — once after a squash
    merge and once mid-PR when a refactor was left unstaged — each time needing
    a follow-up commit purely to repoint a label.
    """
    from backtesting.research_runner import _CODE_PATHS

    m = _load(MANIFEST)
    code = m["code"]
    assert len(code["code_sha256"]) == 64
    recorded = {e["file"] for e in code["files"]}
    assert recorded == set(_CODE_PATHS), (
        f"manifest hashes {recorded}, _CODE_PATHS declares {set(_CODE_PATHS)}"
    )
    for entry in code["files"]:
        assert len(entry["sha256"]) == 64, entry["file"]


def test_code_hashes_match_the_working_tree() -> None:
    """The committed artifact must describe the code that is checked out."""
    from backtesting.research_runner import code_fingerprint

    assert _load(MANIFEST)["code"] == code_fingerprint()


def test_code_hash_is_line_ending_independent(tmp_path) -> None:
    """
    The same source with CRLF and with LF must hash identically.

    core.autocrlf=true and no .gitattributes means a fresh clone checks the same
    commit out with CRLF while the authoring tree holds LF. Byte-exact hashing
    made --verify-code fail on a clean clone of its own commit — the hash
    identified the checkout, not the code.
    """
    from backtesting.research_runner import sha256_file, sha256_source

    body = "def f():\n    return 1\n"
    lf = tmp_path / "lf.py"
    crlf = tmp_path / "crlf.py"
    lf.write_bytes(body.encode())
    crlf.write_bytes(body.replace("\n", "\r\n").encode())

    assert sha256_source(lf) == sha256_source(crlf)
    # And the byte-exact hash used for DATA still distinguishes them, so the two
    # helpers have not been collapsed into one.
    assert sha256_file(lf) != sha256_file(crlf)


def test_code_verification_needs_neither_git_nor_candles(monkeypatch, tmp_path) -> None:
    """
    The cheap check has to run where the full replay cannot: shallow checkouts,
    source tarballs, and any environment without the candle cache. That is the
    whole reason it exists separately from --verify.
    """
    import backtesting.research_runner as rr

    monkeypatch.setattr(rr, "CANDLE_DIR", tmp_path / "no-candles-here")
    assert rr.verify_code_and_environment() is True


def test_changing_declared_research_code_invalidates_verification(monkeypatch) -> None:
    """A source edit must be detected by content, with no git involved."""
    import backtesting.research_runner as rr

    real = rr.code_fingerprint()
    tampered = {
        "code_sha256": "0" * 64,
        "files": [{**real["files"][0], "sha256": "1" * 64}, *real["files"][1:]],
    }
    monkeypatch.setattr(rr, "code_fingerprint", lambda: tampered)
    assert rr.verify_code_and_environment() is False


# ── The computational environment travels with the results ───────────────────

def test_manifest_records_the_result_determining_environment() -> None:
    """
    numpy/pandas/ta/pyarrow compute every indicator, so their versions are as
    result-determining as the source. requirements.txt pinned nothing at all, so
    an upgrade could move every headline number with the artifact still green.
    """
    from backtesting.research_runner import (
        _CANONICAL_PYTHON,
        _RESULT_DETERMINING_PACKAGES,
    )

    env = _load(MANIFEST)["environment"]
    assert env["python"] == _CANONICAL_PYTHON
    assert set(env["packages"]) == set(_RESULT_DETERMINING_PACKAGES)
    for name, version in env["packages"].items():
        assert version and version[0].isdigit(), f"{name}: {version!r}"


def test_environment_reports_the_running_interpreter_not_the_constant() -> None:
    """
    environment_fingerprint stamped _CANONICAL_PYTHON instead of the version it
    was running on, which made the field self-fulfilling: a run under 3.12 still
    recorded "3.13", so the one thing the field existed to detect was the one
    thing it could not.
    """
    import platform

    from backtesting.research_runner import environment_fingerprint

    assert environment_fingerprint()["python"] == platform.python_version()


def test_a_different_interpreter_fails_verification(monkeypatch) -> None:
    """Verifying under another interpreter proves nothing, so it must fail."""
    import backtesting.research_runner as rr

    monkeypatch.setattr(rr.platform, "python_version", lambda: "3.12.9")
    assert rr.verify_code_and_environment() is False


def test_a_different_interpreter_refuses_to_write(monkeypatch) -> None:
    """
    Regenerating on the wrong interpreter would silently rewrite the committed
    numbers and present them as the same experiment.
    """
    import backtesting.research_runner as rr

    monkeypatch.setattr(rr.platform, "python_version", lambda: "3.12.9")
    with pytest.raises(rr.ProvenanceError, match="3.12.9"):
        rr.assert_canonical_python()


def test_canonical_python_is_a_full_version() -> None:
    """
    Major.minor was too loose: patch releases of CPython have changed
    floating-point-adjacent behaviour before, and the artifact records an exact
    version, so the declared one must be exact too.
    """
    from backtesting.research_runner import _CANONICAL_PYTHON

    assert len(_CANONICAL_PYTHON.split(".")) == 3, _CANONICAL_PYTHON
    assert _load(MANIFEST)["environment"]["python"] == _CANONICAL_PYTHON


def test_pinned_requirements_match_the_recorded_environment() -> None:
    """
    The pins and the artifact must agree. If they drift, a clean install
    reproduces neither the recorded environment nor, potentially, the numbers.
    """
    import re

    env = _load(MANIFEST)["environment"]["packages"]
    text = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    pinned = dict(re.findall(r"^([A-Za-z0-9_.\-]+)==([^\s#]+)", text, re.M))
    for name, version in env.items():
        assert pinned.get(name) == version, (
            f"{name}: manifest says {version}, requirements.txt pins "
            f"{pinned.get(name)}"
        )


def test_every_direct_requirement_is_pinned_exactly() -> None:
    """A range or a bare name reintroduces exactly the drift this phase closed."""
    for fname in ("requirements.txt", "requirements-dev.txt"):
        for line in (ROOT / fname).read_text(encoding="utf-8").splitlines():
            line = line.split("#")[0].strip()
            if not line or line.startswith("-r "):
                continue
            assert "==" in line, f"{fname}: {line!r} is not pinned exactly"


def test_manifest_hashes_every_input() -> None:
    m = _load(MANIFEST)
    assert m["inputs"], "no input datasets recorded"
    for inp in m["inputs"]:
        assert len(inp["logical_sha256"]) == 64, f"{inp['file']}: not a SHA-256"
        assert inp["rows"] > 0
        assert _utc(inp["min_ts"]) < _utc(inp["max_ts"])


def test_manifest_records_evaluation_boundaries() -> None:
    m = _load(MANIFEST)
    cw = m["config"]["continuous_window"]
    assert cw["start"] and cw["end"]
    assert m["config"]["oos_freeze"] == "2026-07-12"


# ── The retirement decision is backed by the artifact ─────────────────────────

def test_v3_integrated_is_worse_than_unfiltered() -> None:
    """
    The stated reason for retiring V3 is PF 0.706 integrated vs 0.761 without
    (trial 2026-08-warmup-semantics.v1). If this ever stops holding, the
    retirement rationale in trial_registry.md is stale and must be revisited —
    not silently left in place.
    """
    r = _load(RESULTS)
    base = _row(r, "V2-continuous")
    integ = _row(r, "V3-ER30-integrated")
    assert base["pf"] == pytest.approx(0.76, abs=0.02), (
        f"registry documents PF 0.76 for the continuous window, artifact says {base['pf']}"
    )
    assert integ["pf"] == pytest.approx(0.71, abs=0.02), (
        f"registry documents PF 0.71 for integrated V3, artifact says {integ['pf']}"
    )
    assert integ["pf"] < base["pf"], "V3 must be worse — that is why it is retired"


def test_continuous_window_is_unprofitable() -> None:
    """CLAUDE.md states PF 0.76, -0.62%/trade, n=114. Keep prose honest."""
    r = _load(RESULTS)
    base = _row(r, "V2-continuous")
    assert base["n_closed"] == 114, f"documented n=114, artifact says {base['n_closed']}"
    assert base["pf"] < 1.0, "documented as not profitable"
    # expectancy is a fraction in the artifact (-0.006227 == -0.62%)
    assert base["expectancy_pct"] * 100 == pytest.approx(-0.62, abs=0.02)


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

    `requested_start` is the registered date; `start` may be later when the
    mechanism is not yet computable there (bull_2021 opens 2021-03-01 but ZEC's
    daily EMA200 does not exist until 2021-06-26). Both must be present, and the
    clip must be flagged rather than folded silently into `start`.
    """
    from backtesting.signal_scanner import PERIODS

    r = _load(RESULTS)
    seen = 0
    for row in r["rows"]:
        if not row["trial"].startswith("V2-registry:"):
            continue
        name = row["trial"].split(":", 1)[1]
        assert name in PERIODS, f"unknown period {name}"
        assert row["requested_start"] == PERIODS[name]["start"], f"{name} start drifted"
        assert row["end"] == PERIODS[name]["end"], f"{name} end drifted"
        assert _utc(row["start"]) >= _utc(row["requested_start"]), (
            "a window may only be clipped forward"
        )
        assert row["start_clipped_to_gate_availability"] == (
            _utc(row["start"]) > _utc(row["requested_start"])
        ), f"{name} clip flag disagrees with dates"
        seen += 1
    assert seen == 4, f"expected 4 registry windows, found {seen}"


def test_period_cells_record_the_registered_request_not_the_clip() -> None:
    """
    A period cell must state the window it was ASKED for. Clipping the date
    before handing it to the scanner made the row claim requested_start ==
    effective_start with the clip flag false, erasing the fact the field exists
    to record: period:ZEC-USD:bull_2021 was asked for 2021-03-01, not
    2021-06-26.
    """
    from backtesting.signal_scanner import PERIODS

    r = _load(RESULTS)
    cells = [x for x in r["rows"] if x["trial"].startswith("period:")]
    assert cells, "no period cells in the artifact"
    for c in cells:
        name = c["trial"].rsplit(":", 1)[1]
        assert c["requested_start"] == PERIODS[name]["start"], (
            f"{c['trial']} reports requested_start {c['requested_start']}, "
            f"but the registered window opens at {PERIODS[name]['start']}"
        )
        # Compare INSTANTS. Comparing the strings would accept the same bug the
        # runner had: "2022-01-01" != "2022-01-01T00:00:00+00:00" as text, so a
        # window that was never clipped reported clip=True.
        assert c["start_clipped_to_gate_availability"] == (
            _utc(c["start"]) > _utc(c["requested_start"])), (
            f"{c['trial']} clip flag disagrees with its own dates"
        )

    zec = {c["trial"].rsplit(":", 1)[1]: c
           for c in cells if c["asset"] == "ZEC-USD"}
    assert zec["bull_2021"]["requested_start"] == "2021-03-01"
    assert zec["bull_2021"]["start"].startswith("2021-06-26")
    assert zec["bull_2021"]["start_clipped_to_gate_availability"] is True
    # The other three ZEC windows open after the boundary and must NOT be
    # reported as clipped — otherwise the flag carries no information.
    for name in ("bear_2022", "mid_year_holdout", "recent_year"):
        assert zec[name]["start_clipped_to_gate_availability"] is False, (
            f"{name} was not clipped but claims it was"
        )


def test_warmup_exclusion_window_is_half_open() -> None:
    """
    [requested_start, effective_start). The bar AT the effective start is the
    first EVALUATED bar; counting it as excluded too would put one candle in
    both the warm-up and the corrected window.
    """
    r = _load(RESULTS)
    m = _load(MANIFEST)
    declared = m["config"]["asset_effective_start"]
    for e in r["warmup_exclusions"]:
        if e["window"] is None:
            assert e["excluded_candles"] == 0
            continue
        eff = declared[e["asset"]]
        assert _utc(e["window"]["end"]) < _utc(eff), (
            f"{e['asset']} warm-up ends at {e['window']['end']}, not strictly "
            f"before the effective start {eff}"
        )
        assert e["window"]["interval"] == "[requested_start, effective_start)"
        last = e.get("last_excluded_signal_ts")
        if last is not None:
            assert _utc(last) < _utc(eff), (
                f"{e['asset']} excluded a signal at {last}, at or past {eff}"
            )


def test_no_registered_row_contains_an_unevaluable_signal() -> None:
    """
    THE invariant of trial 2026-08-warmup-semantics: inside a registered window
    no signal may be accepted while a declared gate's input is missing. Before
    the fix, 327 such ZEC bars and 534 SOL bars were evaluated by a weaker
    mechanism, and the ones that cleared the remaining gates were traded.
    """
    r = _load(RESULTS)
    offenders = [x["trial"] for x in r["rows"] if x.get("n_gate_unavailable")]
    assert not offenders, f"rows evaluated on missing gate inputs: {offenders}"


def test_effective_boundaries_are_registered_and_explain_the_clipping() -> None:
    """
    The boundary must be declared in the artifact, not implied by the rows —
    otherwise a reader cannot tell a deliberate window from a silent truncation.
    """
    r = _load(RESULTS)
    m = _load(MANIFEST)
    declared = m["config"]["asset_effective_start"]
    assert set(declared) == {"ZEC-USD", "BTC-USD", "ETH-USD", "SOL-USD"}

    by_asset = {b["asset"]: b for b in r["gate_boundaries"]}
    for asset, start in declared.items():
        assert by_asset[asset]["effective_start"] == start
        assert by_asset[asset]["per_gate_first_valid"]["daily_trend"]

    # ZEC and SOL are the two assets whose daily EMA200 postdates their listing.
    listing = m["config"]["asset_listing_start"]
    assert _utc(declared["ZEC-USD"]) > _utc(listing["ZEC-USD"])
    assert _utc(declared["SOL-USD"]) > _utc(listing["SOL-USD"])

    for row in r["rows"]:
        if "effective_start" not in row:
            continue
        assert _utc(row["start"]) >= _utc(declared[row["asset"]]), (
            f"{row['trial']} starts before its registered boundary"
        )


def test_warmup_exclusions_are_quantified() -> None:
    """
    The correction's size belongs in the artifact. BTC/ETH are unaffected —
    their EMA200 predates the window — so a non-zero exclusion there would mean
    the boundary logic had started firing where it should not.
    """
    r = _load(RESULTS)
    excl = {e["asset"]: e for e in r["warmup_exclusions"]}
    assert excl["ZEC-USD"]["refused_signals_with_missing_input"] > 0
    assert excl["SOL-USD"]["refused_signals_with_missing_input"] > 0
    assert excl["BTC-USD"]["refused_signals_with_missing_input"] == 0
    assert excl["ETH-USD"]["refused_signals_with_missing_input"] == 0
    assert "ema200_1d" in excl["ZEC-USD"]["by_missing_input"]


def test_artifact_records_what_it_supersedes() -> None:
    """
    The previous artifact measured a different mechanism. Saying so in the
    artifact is what stops the two sets of numbers being compared as if they
    were the same experiment.
    """
    m = _load(MANIFEST)
    sup = m["config"]["supersedes"]
    assert sup["config_id"] == "2026-08-evidence-hardening.v1"
    assert m["config_id"] != sup["config_id"]
    old = ROOT / sup["artifacts"]
    assert (old / "results.json").exists(), "superseded artifact must be kept, not deleted"
    assert (old / "SUPERSEDED.md").exists(), "superseded artifact must say why"


def test_registry_window_trade_counts_are_the_registered_ones() -> None:
    """
    Documented per-window closed-trade counts: 6 / 12 / 27 / 37.

    bull_2021 was 25 under the superseded artifact. 19 of those trades fired
    before ZEC's daily EMA200 existed and contributed +22.28% between them; the
    6 that the declared mechanism actually judges are net negative. That single
    number is the clearest statement of what the warm-up defect was worth.
    """
    r = _load(RESULTS)
    expected = {
        "bull_2021": 6, "bear_2022": 12,
        "mid_year_holdout": 27, "recent_year": 37,
    }
    got = {row["trial"].split(":", 1)[1]: row["n_closed"]
           for row in r["rows"] if row["trial"].startswith("V2-registry:")}
    assert got == expected, f"registry window counts changed: {got}"


def test_calendar_cells_are_named_period_not_regime() -> None:
    """
    Calendar windows are PERIOD cells. Labelling them "regime" conflated when a
    trade happened with what the market was doing.
    """
    r = _load(RESULTS)
    period_cells = [x for x in r["rows"] if x["trial"].startswith("period:")]
    assert period_cells, "no period cells in the artifact"
    assets = {x["asset"] for x in period_cells}
    assert len(assets) >= 4


def test_regime_cells_are_cut_on_a_regime_metric() -> None:
    """
    A true regime cell is cut on the regime METRIC at signal time, not on a date
    range. Each must name the metric and bucket it belongs to.
    """
    r = _load(RESULTS)
    cells = [x for x in r["rows"] if x["trial"].startswith("regime:")]
    assert cells, "no regime cells in the artifact"
    for c in cells:
        assert c.get("regime_metric") in {"er_30", "vm_30"}, (
            f"{c['trial']} is not cut on a regime metric"
        )
        assert c.get("regime_bucket"), "regime cell must record its bucket"
        # A regime cell spans the whole window; it is not a calendar slice.
        assert c["end"] == RESEARCH_END, f"{c['trial']} is date-sliced"


def test_unimplemented_regime_cuts_are_declared() -> None:
    """RV and Bollinger-bandwidth cuts have no implementation — declare, not fake."""
    r = _load(RESULTS)
    declared = {p["probe"] for p in r.get("non_reproducible", [])}
    assert {"realised_vol_regime", "bollinger_bandwidth_regime"} <= declared
    metrics = {x.get("regime_metric") for x in r["rows"]
               if x["trial"].startswith("regime:")}
    assert "realised_vol" not in metrics and "bbw" not in metrics


def test_frozen_mechanism_includes_max_hold() -> None:
    """
    max_hold is part of the mechanism. BTC's own config uses 48h, so omitting it
    meant the "frozen ZEC mechanism" transfer test still ran BTC on 48h.
    """
    m = _load(MANIFEST)
    assert m["config"]["frozen_mechanism"]["max_hold_hours"] == 36


def test_period_selection_artifact_is_visible() -> None:
    """
    The documented story: individual calendar windows can look fine while the
    continuous window does not. If that ever stops holding, the 'period-selected
    windows' explanation in trial_registry.md is stale.
    """
    r = _load(RESULTS)
    zec_cells = [x for x in r["rows"]
                 if x["trial"].startswith("period:ZEC-USD:") and x["pf"] is not None]
    assert zec_cells, "no ZEC period cells"
    assert any(c["pf"] > 1.0 for c in zec_cells), (
        "no ZEC calendar window shows PF > 1 — the period-selection explanation "
        "for the earlier positive case no longer holds"
    )
    assert _row(r, "V2-continuous")["pf"] < 1.0


def test_code_commit_is_informational_not_identity() -> None:
    """
    Replaces the old assertion that code_commit must equal the last commit
    touching _CODE_PATHS.

    That test could only pass on a full checkout whose history had never been
    rewritten, so it skipped itself on every CI run (shallow) and failed on
    every squash. Worse, it asserted the wrong property: after content-addressing
    landed, a stale label with matching hashes is CORRECT, not a failure. The
    label is still recorded — it answers "where did this come from" — but
    identity is the hashes, and that is what is asserted here.
    """
    m = _load(MANIFEST)
    assert m["code_commit"], "the label must still be recorded"
    assert m["code"]["code_sha256"], "identity must be the content hash"


def test_verification_survives_a_rewritten_code_commit(monkeypatch) -> None:
    """
    Simulates squash/rebase: history is rewritten so the commit label no longer
    resolves, while every file's content is untouched.

    verify_artifacts() rebuilt code_commit from git and compared the whole
    manifest, so it failed after any rewrite even though all content hashes
    matched — reintroducing exactly the fragility content-addressing removed.

    Deliberately does NOT call build_manifest(): that reads the parquet cache,
    which is git-ignored, so this test failed in any clean checkout — the very
    environment whose behaviour it claims to describe. The inputs are taken from
    the committed manifest and only the git label is simulated.
    """
    import backtesting.research_runner as rr

    committed = _load(MANIFEST)

    # A rewritten history: the label resolves to something new, file contents
    # are untouched.
    monkeypatch.setattr(rr, "_git_commit", lambda: "0" * 40)
    rewritten = json.loads(json.dumps(committed))
    rewritten["code_commit"] = rr._git_commit()

    assert rewritten["code_commit"] != committed["code_commit"]
    # Identity is unaffected, and is computed from the working tree — no candles
    # and no git needed for either of these.
    assert rr.code_fingerprint() == committed["code"]
    assert rr.environment_fingerprint() == committed["environment"]
    # Carrying informational fields over restores full equality.
    assert rr.carry_over_informational(rewritten, committed) == committed
    # And the cheap check, which never consults git at all, still passes.
    assert rr.verify_code_and_environment() is True


def test_informational_fields_do_not_participate_in_identity() -> None:
    """
    physical_sha256 moves whenever the exchange completes a candle past the
    freeze or a different parquet writer is used, and code_commit moves on any
    squash. Both are recorded for humans; neither may fail verification, or the
    logical hash buys nothing.
    """
    import backtesting.research_runner as rr

    committed = _load(MANIFEST)
    drifted = json.loads(json.dumps(committed))
    drifted["code_commit"] = "f" * 40
    for entry in drifted["inputs"]:
        entry["physical_sha256"] = "0" * 64
        entry["physical_rows"] = entry["physical_rows"] + 500

    assert rr.carry_over_informational(drifted, committed) == committed


def test_a_logical_hash_change_is_NOT_carried_over() -> None:
    """The carry-over must not become a way to launder a real input change."""
    import backtesting.research_runner as rr

    committed = _load(MANIFEST)
    tampered = json.loads(json.dumps(committed))
    tampered["inputs"][0]["logical_sha256"] = "9" * 64

    assert rr.carry_over_informational(tampered, committed) != committed


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
    SOL has no Coinbase candles before 2021-06-17 — but its evaluation cannot
    start there either, because the frozen mechanism's 200-day daily EMA does
    not exist until 2022-01-03. The listing date is recorded as data coverage;
    the effective date is what the row is actually evaluated over.
    """
    m = _load(MANIFEST)
    assert m["config"]["asset_listing_start"]["SOL-USD"] == "2021-06-17"
    assert m["config"]["asset_effective_start"]["SOL-USD"].startswith("2022-01-03")
    r = _load(RESULTS)
    sol = next(x for x in r["rows"] if x["trial"] == "transfer-frozen-zec:SOL-USD")
    assert sol["start"].startswith("2022-01-03"), "SOL row must state its real window"
    assert sol["requested_start"] == "2021-03-01"


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
