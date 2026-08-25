"""
Phase 7R guards — the blind, and the shape of the feasibility audit.

The audit exists to size a future trial without anyone seeing what the rule
would have earned. If it ever reports P&L on pre-cutoff data, the decision to
proceed — or to nudge 55/20 — becomes contaminated by exactly the selection bias
the trial registry exists to prevent.

These tests assert the blind STRUCTURALLY, not by inspection: prices reach one
function, and that function returns timestamps.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

import backtesting.stf_feasibility as fz


def _closes(values, start="2024-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
    return pd.Series([float(v) for v in values], index=idx)


# ── The blind ────────────────────────────────────────────────────────────────

def test_events_carry_no_price_information() -> None:
    """
    The one function that sees prices returns timestamps and labels. If a price
    or a return ever appears in an event, P&L becomes reconstructible.
    """
    closes = _closes(list(range(100, 200)))
    events = fz.entry_exit_events(closes)
    assert events, "fixture produced no events"
    for ev in events:
        assert set(ev) == {"ts", "event"}, f"event leaked fields: {sorted(ev)}"
        assert ev["event"] in {"ENTRY", "EXIT"}


# Sections of the audit that carry DATA. The prose sections ("purpose",
# "blinding", "not_evidence") NAME what is excluded, so scanning them would
# flag the disclaimer. These keys are asserted to exist: the previous version
# selected "per_asset"/"portfolio", which the audit has never emitted, so eight
# parametrised cases passed while checking an empty dict.
_DATA_SECTIONS = ("per_asset_full_history", "common_universe", "rule")

_BANNED_IN_DATA = (
    "pnl", "profit_factor", "expectancy", "win_rate", "return", "equity",
    "sharpe", "drawdown",
)


def _performance_leak(audit: dict, banned: str) -> bool:
    """True if `banned` appears anywhere in the audit's data sections."""
    import json

    missing = [k for k in _DATA_SECTIONS if k not in audit]
    assert not missing, (
        f"audit is missing data section(s) {missing} — the blind check would "
        "silently scan nothing. Update _DATA_SECTIONS deliberately.")
    return banned in json.dumps({k: audit[k] for k in _DATA_SECTIONS}).lower()


@pytest.mark.parametrize("banned", _BANNED_IN_DATA)
def test_the_blind_check_actually_catches_a_planted_field(banned: str) -> None:
    """
    Negative control for the check below.

    The regression protection was broken for a release precisely because a
    passing parametrised test proves nothing on its own. Every banned token
    must be demonstrated to FAIL when planted in a data section.
    """
    clean = {"per_asset_full_history": {"BTC-USD": {"closed_trades": 23}},
             "common_universe": {"clusters": []},
             "rule": {"entry_lookback": 55}}
    assert not _performance_leak(clean, banned)

    planted = {**clean, "common_universe": {"clusters": [], banned: 1.42}}
    assert _performance_leak(planted, banned), (
        f"a planted {banned!r} was not detected — the blind check is inert")


def test_the_blind_check_refuses_to_scan_a_renamed_audit() -> None:
    """If a section is renamed the check must fail loudly, not scan nothing."""
    with pytest.raises(AssertionError, match="missing data section"):
        _performance_leak({"rule": {}}, "pnl")


@pytest.mark.integration
@pytest.mark.parametrize("banned", _BANNED_IN_DATA)
def test_the_audit_reports_no_performance_field(banned: str) -> None:
    """
    A blanket check on the real artifact. Adding a P&L field later would have
    to defeat this deliberately rather than by accident.
    """
    assert not _performance_leak(fz.build_audit(), banned), (
        f"the audit reports {banned!r}")


def test_the_module_never_computes_a_return() -> None:
    """
    Executable code must not touch performance concepts.

    Comments and string literals are stripped first: the module's own prose
    NAMES what it refuses to compute, so a raw text search would flag the
    disclaimer itself.
    """
    import io
    import tokenize

    src = inspect.getsource(fz)
    code = " ".join(
        tok.string
        for tok in tokenize.generate_tokens(io.StringIO(src).readline)
        if tok.type not in (tokenize.COMMENT, tokenize.STRING)
    )
    for pattern in ("pnl", "profit_factor", "expectancy", "win_rate",
                    "pct_change", "cumprod"):
        assert pattern not in code, (
            f"{pattern!r} appears in executable code - the blind leaks"
        )


# ── The rule is the declared one ─────────────────────────────────────────────

def test_entry_fires_on_a_new_high_of_closes() -> None:
    """Entry level is built from CLOSES, and the current bar is excluded."""
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0]
    events = fz.entry_exit_events(_closes(values))
    assert events and events[0]["event"] == "ENTRY"
    assert events[0]["ts"].startswith("2024-02-25")     # the 56th bar


def test_no_entry_before_the_lookback_is_complete() -> None:
    """A rising series shorter than the window cannot produce a signal."""
    values = list(range(100, 100 + fz.ENTRY_LOOKBACK - 1))
    assert fz.entry_exit_events(_closes(values)) == []


def test_the_current_bar_is_excluded_from_its_own_window() -> None:
    """
    Including the current close would make the comparison `close > max(...,
    close)` — never true — or, with `>=`, always true. Either way it is
    look-ahead-shaped and must not happen.
    """
    flat = [100.0] * (fz.ENTRY_LOOKBACK + 5)
    assert fz.entry_exit_events(_closes(flat)) == []


def test_exit_fires_on_a_new_low_and_only_while_in_position() -> None:
    values = ([100.0] * fz.ENTRY_LOOKBACK + [120.0]
              + [119.0] * fz.EXIT_LOOKBACK + [90.0])
    events = fz.entry_exit_events(_closes(values))
    labels = [e["event"] for e in events]
    assert labels == ["ENTRY", "EXIT"], labels


def test_positions_do_not_stack() -> None:
    """One position at a time: consecutive new highs must not re-enter."""
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0, 102.0, 103.0, 104.0]
    labels = [e["event"] for e in fz.entry_exit_events(_closes(values))]
    assert labels.count("ENTRY") == 1


def test_an_open_position_at_the_end_is_not_a_closed_trade() -> None:
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0, 102.0]
    spans, still_open = fz._spans(fz.entry_exit_events(_closes(values)))
    assert spans == []
    assert still_open == 1


# ── Portfolio structure ──────────────────────────────────────────────────────

def test_clusters_split_only_on_a_long_flat_gap() -> None:
    idx = pd.date_range("2024-01-01", periods=400, freq="D", tz="UTC")
    open_days = pd.Series(False, index=idx)
    open_days.iloc[0:20] = True
    second = 20 + fz.CLUSTER_GAP_DAYS + 5
    open_days.iloc[second:second + 20] = True     # single-step assignment
    clusters = fz._clusters(open_days)
    assert len(clusters) == 2

    adjacent = pd.Series(False, index=idx)
    adjacent.iloc[0:20] = True
    adjacent.iloc[30:50] = True          # only a 10-day gap
    assert len(fz._clusters(adjacent)) == 1


def test_the_cluster_gap_is_not_called_independence() -> None:
    """
    Sixty flat days makes two spans non-contiguous, not statistically
    independent. The vocabulary matters because the whole trial rests on how
    many independent observations it can claim.
    """
    import backtesting.stf_protocol as proto

    src = (inspect.getsource(fz) + inspect.getsource(proto)).lower()
    assert "cluster" in src
    assert "not statistically" in src or "independent episode" in src


def test_the_universe_is_fixed_and_the_rule_is_singular() -> None:
    """No grid, no per-asset selection — this is a feasibility audit."""
    assert fz.UNIVERSE == ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
    assert isinstance(fz.ENTRY_LOOKBACK, int)
    assert isinstance(fz.EXIT_LOOKBACK, int)
    src = inspect.getsource(fz)
    assert "for lookback in" not in src, "a parameter sweep appeared"


@pytest.mark.integration
def test_audit_reports_the_naming_correction() -> None:
    """
    The rule uses closes, not highs/lows, so it is not the classical Donchian
    channel. The artifact has to say so before anyone implements it.
    """
    rule = fz.build_audit()["rule"]
    assert rule["name"] == "STF-CLOSE-55-20"
    assert "not" in rule["note"].lower() and "donchian" in rule["note"].lower()


# ── Power study is calibrated, not guessed ───────────────────────────────────

@pytest.mark.integration
def test_power_study_is_calibrated_from_the_blinded_audit() -> None:
    import backtesting.stf_power as pw

    s = pw._structure()
    # The first version modelled one trade per asset per cluster and produced
    # ~2.8 trades/year against a measured 11.94, so no simulated trial reached
    # the minimums — an artefact of the model, not a property of the gates.
    assert s["implied_pooled_trades_per_year"] == pytest.approx(
        s["measured_pooled_trades_per_year"], abs=0.2)


def test_power_study_uses_no_historical_strategy_returns() -> None:
    """
    It may read the audit's STRUCTURE; it must not read prices.

    Executable tokens only: the module's prose describes the strategy ("the
    20-day close breakout"), and a raw text search flags that description.
    """
    import io
    import tokenize

    import backtesting.stf_power as pw

    code = " ".join(
        tok.string
        for tok in tokenize.generate_tokens(
            io.StringIO(inspect.getsource(pw)).readline)
        if tok.type not in (tokenize.COMMENT, tokenize.STRING)
    )
    for banned in ("read_parquet", "close", "CANDLE_DIR", "parquet"):
        assert banned not in code, f"{banned!r} in executable code"


def test_the_simulated_null_really_has_zero_expectancy() -> None:
    """If the null world had drift, every false-pass rate would be wrong."""
    import backtesting.stf_power as pw

    for win_rate in pw.WIN_RATES:
        for loss_pct in pw.LOSS_PCTS:
            w = pw._win_size(win_rate, loss_pct, 0.0)
            expectancy = (win_rate * w
                          - (1 - win_rate) * loss_pct
                          - pw.COST_PCT)
            assert expectancy == pytest.approx(0.0, abs=1e-9)


def test_erfinv_matches_the_normal_quantile() -> None:
    """The copula threshold depends on it; a wrong tail would skew win rates."""
    import backtesting.stf_power as pw

    for p, expected in [(0.5, 0.0), (0.975, 1.959964), (0.75, 0.674490)]:
        got = np.sqrt(2.0) * pw._erfinv(2.0 * p - 1.0)
        assert got == pytest.approx(expected, abs=1e-4)


# ── Hermetic equivalents of the artifact-dependent checks ───────────────────
#
# The three tests above call build_audit(), which reads the git-ignored parquet
# cache. In CI the tests job runs before any hydration and does not share a
# filesystem with the research-verify job, so a clean checkout has no candles.
# They are marked integration; these cover the same properties without data.

def test_the_rule_constants_are_the_registered_ones() -> None:
    assert fz.ENTRY_LOOKBACK == 55
    assert fz.EXIT_LOOKBACK == 20
    assert fz.UNIVERSE == ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]


def test_the_audit_window_is_frozen_and_matches_the_research_scope() -> None:
    """
    An unbounded end would change the artifact every time the cache grows, and
    would describe data the committed research manifest does not cover — which
    is also what let the tests depend on ungated data.
    """
    from backtesting.research_runner import _SCOPE_START, _scope_end

    assert fz.AUDIT_START == _SCOPE_START
    assert fz.AUDIT_END == _scope_end()


def test_portfolio_structure_counts_unique_assets_not_concurrency() -> None:
    """
    A cluster can average 2.3 open assets while touching all four over its life.
    Calibrating a simulation with the concurrent figure invents too few assets
    and too many repeat trades per asset.
    """
    idx = pd.date_range("2024-01-01", periods=200, freq="D", tz="UTC")
    # Two assets, never open at the same time, both inside one cluster.
    frames = {
        "BTC-USD": ([(idx[0], idx[10])], idx),
        "ETH-USD": ([(idx[20], idx[30])], idx),
        "SOL-USD": ([], idx),
        "ZEC-USD": ([], idx),
    }
    out = fz.portfolio_structure(frames, idx[0])
    assert out["exposure_clusters"] == 1
    assert out["mean_unique_assets_per_cluster"] == 2.0
    assert out["mean_concurrent_when_exposed"] == 1.0, (
        "the two metrics must not be conflated"
    )


def test_the_state_machine_restarts_flat_at_the_evaluation_start() -> None:
    """
    A forward trial begins with no position. Replaying the whole history and
    discarding early trades afterwards left the machine "in position" across the
    boundary, which SUPPRESSED the first post-start entry — the trial would have
    taken it. Worth 2 of 68 trades in the real audit.
    """
    # Rally into the boundary (opens a position), then a fresh breakout after it.
    values = ([100.0] * fz.ENTRY_LOOKBACK + [120.0] * 10
              + [150.0] * 10)
    closes = _closes(values)
    boundary = closes.index[fz.ENTRY_LOOKBACK + 5]

    carried = fz.entry_exit_events(closes)
    restarted = fz.entry_exit_events(closes, evaluation_start=boundary)

    # Carrying state: the position opened before the boundary, so no entry fires
    # after it. Restarting flat: the post-boundary breakout is taken.
    assert not [e for e in carried if e["event"] == "ENTRY"
                and pd.Timestamp(e["ts"]) >= boundary]
    assert [e for e in restarted if e["event"] == "ENTRY"
            and pd.Timestamp(e["ts"]) >= boundary]


def test_warm_up_history_still_feeds_the_rolling_levels() -> None:
    """
    Restarting the STATE must not restart the WINDOWS: a 55-day level needs 55
    days of history, which is what pre-cutoff data is for.
    """
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0, 102.0]
    closes = _closes(values)
    boundary = closes.index[fz.ENTRY_LOOKBACK]
    # The bar at the boundary is a new 55-day high only because the preceding
    # 55 bars are visible; without them the window would be incomplete.
    assert fz.entry_exit_events(closes, evaluation_start=boundary)


def test_the_artifact_carries_provenance() -> None:
    """
    Phase 6.9 made code identity, environment and input hashes mandatory for
    research artifacts. A new artifact must not reintroduce the old guarantees.
    """
    src = inspect.getsource(fz)
    for needed in ("code_sha256", "environment_fingerprint", "logical_sha256",
                   "assert_canonical_python", "assert_code_is_committed"):
        assert needed in src, f"{needed} missing from the audit"


def test_loss_size_is_a_sensitivity_axis_not_a_constant() -> None:
    """
    The rule has no stop: its only exit is the 20-day close breakout, so what a
    losing trade gives back is a property of price, not of a risk rule. Fixing
    it at one number would smuggle in a mechanism the strategy does not have.
    """
    import backtesting.stf_power as pw

    assert len(pw.LOSS_PCTS) >= 3
    assert not hasattr(pw, "LOSS_PCT"), "loss size was re-frozen to a constant"


def test_drawdown_uses_the_sleeve_denominator() -> None:
    """
    Sizing is a risk decision, frozen once for every component: a 2%-of-capital
    position is 10% of the 20% sleeve, and at most four are held. The sleeve and
    the drawdown limit are not relaxed so a strategy fits — no edge has been
    demonstrated, so the correct response to a binding limit is a smaller
    position.
    """
    import backtesting.stf_power as pw

    assert pw.POSITION_FRACTION_OF_CAPITAL == pytest.approx(0.02)
    assert pw.SLEEVE_FRACTION_OF_CAPITAL == pytest.approx(0.20)
    assert pw.POSITION_FRACTION_OF_SLEEVE == pytest.approx(0.10)


def test_drawdown_is_declared_not_assessed() -> None:
    """
    A realized-only trade sequence cannot produce the protocol's calendar-time,
    unrealized-inclusive drawdown. Saying so is required; silently evaluating a
    different quantity is not acceptable.
    """
    import backtesting.stf_power as pw

    assert "max_drawdown" in pw.DIAGNOSTIC_ONLY
    src = inspect.getsource(pw)
    assert "NOT ASSESSED" in src


def test_the_power_study_evaluates_the_final_gates() -> None:
    """
    The first study measured the superseded gates (18 months / 20 trades / 3
    clusters, with LOEO and drawdown mandatory) while the report proposed
    different ones, so its numbers described a trial nobody intended to run.
    """
    import backtesting.stf_power as pw

    assert pw.GATE_MIN_YEARS == 3.0
    assert pw.GATE_MIN_TRADES == 30
    assert pw.GATE_MIN_CLUSTERS == 5


def test_every_stf_component_shares_one_frozen_size() -> None:
    """
    The power study modelled 5% of capital while the cost probe measured the
    live 2%: cost, drawdown and the future shadow journal described three
    different mechanisms, and nothing flagged it because each owned its number.
    """
    import backtesting.stf_cost_probe as cp
    import backtesting.stf_power as pw
    import backtesting.stf_protocol as proto

    assert pw.POSITION_FRACTION_OF_CAPITAL is proto.POSITION_FRACTION_OF_CAPITAL
    with patch("pipeline.sizing.live_balance_usd", return_value=100.0):
        assert cp.trial_notional() == proto.position_notional(100.0)


def test_the_power_study_computes_no_drawdown_proxy() -> None:
    """
    A "sequential realized stress" counted against the protocol's 25% limit
    read as a drawdown result and was not one: trades are applied in an
    artificial cluster/asset order, while the gate is measured on a calendar
    sleeve curve including unrealized P&L. Removed, not relabelled — a
    statistic that bounds nothing has no reader-proof caption.
    """
    import io
    import textwrap
    import tokenize

    import backtesting.stf_power as pw

    for fn in (pw._simulate_one, pw._cell, pw._gates):
        code = " ".join(
            tok.string
            for tok in tokenize.generate_tokens(io.StringIO(
                textwrap.dedent(inspect.getsource(fn))).readline)
            if tok.type not in (tokenize.COMMENT, tokenize.STRING)
        )
        for banned in ("stress", "drawdown", "equity", "peak", "trough"):
            assert banned not in code, f"{fn.__name__} still computes {banned!r}"

    assert "NOT a bound" in inspect.getsource(pw)


def test_the_drawdown_limit_lives_in_the_shared_protocol() -> None:
    """
    25.0 was hardcoded in the power study. The real limit is a risk decision
    the future forward trial is judged against, so it belongs beside the sizing
    it constrains — and nowhere near a simulation that cannot evaluate it.
    """
    import backtesting.stf_protocol as proto

    assert proto.SLEEVE_MAX_DRAWDOWN_PCT == 25.0
    committed = (Path(fz.__file__).resolve().parents[1] / "docs" / "research"
                 / "artifacts" / "stf_feasibility" / "power.json")
    if committed.exists():
        text = committed.read_text(encoding="utf-8").lower()
        assert "stress" not in text, "the artifact still carries a drawdown proxy"
        assert "above_25" not in text


def test_the_trade_count_poisson_preserves_its_mean() -> None:
    """
    max(1, Poisson(m)) has a mean ABOVE m — clamping moves probability mass up.
    1 + Poisson(m-1) has mean exactly m.
    """
    import numpy as np

    import backtesting.stf_power as pw

    rng = np.random.default_rng(7)
    mean = 2.429
    clamped = np.maximum(1, rng.poisson(mean, 400_000)).mean()
    correct = (1 + rng.poisson(mean - 1.0, 400_000)).mean()

    assert clamped > mean + 0.01, "the clamped form should overshoot"
    assert correct == pytest.approx(mean, abs=0.01)
    assert "1 + int(rng.poisson(" in inspect.getsource(pw._simulate_one)
