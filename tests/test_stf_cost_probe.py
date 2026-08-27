"""
Phase 7R-2 cost probe — safety and arithmetic.

The probe was shipped without tests, and a probe is a measuring instrument: an
unnoticed defect here becomes a number in a registered protocol. Everything runs
against synthetic books and a mocked client — no network, no credentials.
"""
from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import backtesting.stf_cost_probe as cp


def _book_side(levels):
    return [{"price": str(p), "size": str(s)} for p, s in levels]


def _public_client_stub():
    """Unauthenticated client: public book and public candles only."""
    return SimpleNamespace(
        get_public_product_book=lambda product_id, limit: {
            "bids": _book_side([(100.0, 50.0)]),
            "asks": _book_side([(100.5, 50.0)]),
        },
        get_public_candles=lambda product_id, start, end, granularity: (
            SimpleNamespace(candles=[SimpleNamespace(start=start)])),
    )


_VIEW_ONLY = SimpleNamespace(can_view=True, can_trade=False, can_transfer=False)


def _fee_client_stub(permissions=_VIEW_ONLY, taker="0.006", maker="0.004",
                     pricing_tier="Advanced 1"):
    """View-only client: the permission call and the private fee endpoint."""
    return SimpleNamespace(
        get_api_key_permissions=lambda: permissions,
        get_transaction_summary=lambda: SimpleNamespace(
            fee_tier=SimpleNamespace(taker_fee_rate=taker, maker_fee_rate=maker,
                                     pricing_tier=pricing_tier)))


# ── It cannot reach an account ───────────────────────────────────────────────

def test_the_probe_cannot_place_or_cancel_an_order() -> None:
    """
    A read-only instrument must not be one refactor away from trading. Checked
    on executable tokens so the module's own prose about NOT ordering does not
    satisfy the test.
    """
    import io
    import tokenize

    code = " ".join(
        tok.string
        for tok in tokenize.generate_tokens(
            io.StringIO(inspect.getsource(cp)).readline)
        if tok.type not in (tokenize.COMMENT, tokenize.STRING)
    )
    for banned in ("create_order", "market_order", "limit_order", "cancel_orders",
                   "place_limit_order", "place_order_outbox", "cdp_api_key"):
        assert banned not in code, f"{banned!r} reachable from the cost probe"

    # `key_file` is deliberately NOT banned any more: the fee tier needs an
    # opt-in view-only credential, and banning the token only pushed the
    # capability out of sight. What matters is guarded directly below — the
    # single path it may load, and its refusal of the live trading key.
    assert code.count("key_file") <= 2, "key material is loaded in more than one place"


def test_only_the_designated_env_var_can_supply_key_material() -> None:
    src = inspect.getsource(cp._fee_client)
    assert "FEE_KEY_FILE_ENV" in src
    assert cp.FEE_KEY_FILE_ENV == "STF_FEE_VIEW_ONLY_KEY_FILE"
    # No other function may build an authenticated client.
    for name, fn in vars(cp).items():
        if name != "_fee_client" and callable(fn) and getattr(fn, "__module__", "") == cp.__name__:
            assert "key_file" not in inspect.getsource(fn), f"{name} loads key material"


def test_no_credential_means_no_client_and_no_assumed_fee(monkeypatch) -> None:
    monkeypatch.delenv(cp.FEE_KEY_FILE_ENV, raising=False)
    assert cp._fee_client() == (None, None)

    tier = cp._fee_tier(None)
    assert tier["available"] is False
    assert tier["measured"] is False
    assert cp.FEE_KEY_FILE_ENV in tier["reason"]
    assert "taker_fee_rate" not in tier


def test_the_live_trading_credential_is_refused_by_path(monkeypatch) -> None:
    """A measuring instrument must not hold a key that can trade."""
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(cp._LIVE_TRADING_KEY))
    with pytest.raises(cp.CostProbeError, match="live trading credential"):
        cp._fee_client()


def test_a_copy_of_the_live_credential_is_refused_by_name(tmp_path, monkeypatch) -> None:
    """Refusing only the exact path would be defeated by `cp` to another dir."""
    copy = tmp_path / cp._LIVE_TRADING_KEY.name
    copy.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(copy))
    with pytest.raises(cp.CostProbeError, match="live trading credential"):
        cp._fee_client()


def test_a_missing_key_file_is_an_error_not_a_silent_downgrade(tmp_path, monkeypatch) -> None:
    """
    The operator asked for a measured fee. Falling back to the public client
    would hand them an unmeasured one under the same field name.
    """
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(tmp_path / "absent.json"))
    with pytest.raises(cp.CostProbeError, match="not a file"):
        cp._fee_client()


def test_the_client_is_built_without_key_material() -> None:
    with patch("coinbase.rest.RESTClient") as rc:
        cp._public_client()
    rc.assert_called_once_with()


def test_only_public_endpoints_are_swept() -> None:
    src = inspect.getsource(cp)
    assert "get_public_product_book" in src
    assert "get_public_candles" in src
    assert "client.get_product_book" not in src


def test_an_unreadable_fee_tier_is_unavailable_not_assumed() -> None:
    """
    The probe must record that it could not read the fee, never substitute a
    guess — an unmeasured fee is what it exists to remove.
    """
    client = SimpleNamespace(
        get_transaction_summary=lambda: (_ for _ in ()).throw(
            RuntimeError("Unauthenticated request to private endpoint")))
    tier = cp._fee_tier(client)
    assert tier["available"] is False
    assert "taker_fee_rate" not in tier


def test_a_view_only_credential_can_supply_the_tier() -> None:
    """The path must work if a view-only key is deliberately provided."""
    perms = {"can_view": True, "can_trade": False, "can_transfer": False}
    tier = cp._fee_tier(_fee_client_stub(), perms)
    assert tier == {"available": True, "measured": True, "taker_fee_rate": 0.006,
                    "maker_fee_rate": 0.004, "pricing_tier": "Advanced 1",
                    "key_permissions": perms}


@pytest.mark.parametrize("rate", ["nan", "inf", "-inf", "-0.006", "abc", None,
                                  "0.5", ""])
def test_an_unusable_fee_rate_degrades_to_unavailable(rate) -> None:
    """
    float("nan") passed straight through as a measured rate, and a non-numeric
    value raised out of the whole reading. NaN is an assumed fee that
    arithmetic will never warn about; 50% is a parsing accident.
    """
    client = SimpleNamespace(get_transaction_summary=lambda: SimpleNamespace(
        fee_tier=SimpleNamespace(taker_fee_rate=rate, maker_fee_rate="0.004",
                                 pricing_tier="Advanced 1")))
    tier = cp._fee_tier(client)
    assert tier["available"] is False
    assert tier["measured"] is False
    assert "taker_fee_rate" not in tier


def test_a_fee_tier_without_rates_is_unavailable() -> None:
    client = SimpleNamespace(get_transaction_summary=lambda: SimpleNamespace(
        fee_tier=SimpleNamespace(pricing_tier="Advanced 1")))
    assert cp._fee_tier(client)["available"] is False


def test_observe_reads_the_tier_through_the_view_only_client(tmp_path, monkeypatch) -> None:
    """
    End to end through observe(), not just the parser: the previous version
    passed the UNAUTHENTICATED client to the fee call, so the tier could never
    be available no matter what the parser did.
    """
    key = tmp_path / "view_only_fee_key.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(key))
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "probe.jsonl")

    public, fee = _public_client_stub(), _fee_client_stub()
    built = []

    def _restclient(**kwargs):
        built.append(kwargs)
        return fee if kwargs else public

    with patch("coinbase.rest.RESTClient", side_effect=_restclient):
        obs = cp.observe(force=True)

    assert built == [{"key_file": str(key)}, {}], (
        "the fee client must be the only one built with key material")
    assert obs["fee_tier"]["measured"] is True
    assert obs["fee_tier"]["taker_fee_rate"] == 0.006
    assert obs["fee_tier"]["key_permissions"] == {
        "can_view": True, "can_trade": False, "can_transfer": False}
    assert obs["assets"][0]["daily_candle"]["available"] is True


def test_observe_without_a_credential_records_the_fee_as_unavailable(
        tmp_path, monkeypatch) -> None:
    monkeypatch.delenv(cp.FEE_KEY_FILE_ENV, raising=False)
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "probe.jsonl")

    with patch("coinbase.rest.RESTClient", side_effect=lambda **kw: _public_client_stub()):
        obs = cp.observe(force=True)

    assert obs["fee_tier"]["available"] is False
    assert obs["fee_tier"]["measured"] is False


def test_a_misconfigured_credential_fails_before_any_sampling(
        tmp_path, monkeypatch) -> None:
    """Fail loudly first, rather than after a reading has been taken."""
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(tmp_path / "absent.json"))
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "probe.jsonl")

    with patch("coinbase.rest.RESTClient",
               side_effect=AssertionError("no client may be built")):
        with pytest.raises(cp.CostProbeError, match="not a file"):
            cp.observe(force=True)
    assert not (tmp_path / "probe.jsonl").exists()


def test_a_view_only_key_is_accepted_and_its_permissions_recorded() -> None:
    assert cp._assert_view_only(_fee_client_stub()) == {
        "can_view": True, "can_trade": False, "can_transfer": False}


@pytest.mark.parametrize("perms", [
    SimpleNamespace(can_view=True, can_trade=True, can_transfer=False),
    SimpleNamespace(can_view=True, can_trade=False, can_transfer=True),
    SimpleNamespace(can_view=False, can_trade=False, can_transfer=False),
])
def test_a_key_that_can_trade_or_transfer_is_refused(perms) -> None:
    """
    The path and filename checks only recognise a credential we already know
    about. A RENAMED trading key, or a view-only key later granted trade
    rights, passes both and is caught only here.
    """
    with pytest.raises(cp.CostProbeError, match="required"):
        cp._assert_view_only(_fee_client_stub(permissions=perms))


@pytest.mark.parametrize("perms", [
    SimpleNamespace(can_view=True, can_trade=False),                 # missing
    SimpleNamespace(can_view=True, can_trade=None, can_transfer=False),
    SimpleNamespace(can_view=True, can_trade="false", can_transfer=False),
    SimpleNamespace(can_view=1, can_trade=0, can_transfer=0),        # not bools
])
def test_an_unreadable_permission_is_refused_not_assumed_benign(perms) -> None:
    with pytest.raises(cp.CostProbeError, match="not a boolean"):
        cp._assert_view_only(_fee_client_stub(permissions=perms))


def test_a_failing_permission_call_is_treated_as_a_trading_key() -> None:
    client = SimpleNamespace(
        get_api_key_permissions=lambda: (_ for _ in ()).throw(
            RuntimeError("Unauthorized")))
    with pytest.raises(cp.CostProbeError, match="treated as a trading key"):
        cp._assert_view_only(client)


def test_permissions_are_verified_before_any_market_request(
        tmp_path, monkeypatch) -> None:
    """
    A key that should not be here must be refused BEFORE it is used, not after
    a reading has been taken.
    """
    key = tmp_path / "not_actually_view_only.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(cp.FEE_KEY_FILE_ENV, str(key))
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "probe.jsonl")

    trading_key = _fee_client_stub(
        permissions=SimpleNamespace(can_view=True, can_trade=True,
                                    can_transfer=False))
    touched = []
    public = SimpleNamespace(
        get_public_product_book=lambda **kw: touched.append("book"),
        get_public_candles=lambda **kw: touched.append("candles"))

    with patch("coinbase.rest.RESTClient",
               side_effect=lambda **kw: trading_key if kw else public):
        with pytest.raises(cp.CostProbeError, match="required"):
            cp.observe(force=True)

    assert touched == [], "the book was swept before the key was verified"
    assert not (tmp_path / "probe.jsonl").exists()


# ── Book parsing ─────────────────────────────────────────────────────────────

def test_non_finite_levels_are_dropped() -> None:
    """
    `inf` passes a bare `x == x` NaN check and would swallow the whole order at
    one level, reporting a fantasy price.
    """
    raw = _book_side([("inf", "1"), ("nan", "1"), ("100", "2")])
    assert cp._validated_levels(raw, "ask") == [(100.0, 2.0)]


@pytest.mark.parametrize("bad", [("0", "1"), ("100", "0"), ("-5", "1")])
def test_non_positive_levels_are_dropped(bad) -> None:
    assert cp._validated_levels(_book_side([bad, ("100", "2")]), "ask") == [(100.0, 2.0)]


def test_levels_are_sorted_by_side() -> None:
    """
    Nothing in the response type guarantees ordering, and an out-of-order level
    silently corrupts a sweep.
    """
    scrambled = _book_side([("102", "1"), ("100", "1"), ("101", "1")])
    assert cp._validated_levels(scrambled, "ask") == [(100.0, 1.0), (101.0, 1.0),
                                                     (102.0, 1.0)]
    assert cp._validated_levels(scrambled, "bid") == [(102.0, 1.0), (101.0, 1.0),
                                                     (100.0, 1.0)]


def test_a_sweep_reports_incompleteness_rather_than_extrapolating() -> None:
    out = cp._sweep([(100.0, 0.001)], notional=1000.0)
    assert out["complete"] is False
    assert out["filled_quote"] < 1000.0


def test_a_sweep_is_size_weighted() -> None:
    out = cp._sweep([(100.0, 1.0), (200.0, 1.0)], notional=200.0)
    # $100 at 100 buys 1 unit; $100 at 200 buys 0.5. VWAP = 200 / 1.5.
    assert out["vwap"] == pytest.approx(133.33333333, abs=1e-6)
    assert out["complete"] is True


# ── Sizing is the frozen protocol value ──────────────────────────────────────

def test_the_notional_comes_from_the_frozen_protocol() -> None:
    """
    The power study modelled 5% of capital while this measured the live 2%, so
    cost and drawdown described different mechanisms.
    """
    from backtesting.stf_protocol import POSITION_FRACTION_OF_CAPITAL

    with patch("pipeline.sizing.live_balance_usd", return_value=1000.0):
        assert cp.trial_notional() == 1000.0 * POSITION_FRACTION_OF_CAPITAL


def test_the_probe_does_not_track_the_live_default() -> None:
    """A change to the live per-order default must not redefine a registered trial."""
    src = inspect.getsource(cp.trial_notional)
    assert "trade_size_pct" not in src
    assert "position_notional" in src


# ── The unattended runner ────────────────────────────────────────────────────

def _runner_script() -> str:
    root = Path(cp.__file__).resolve().parents[1]
    return (root / "scripts" / "run_stf_cost_probe.bat").read_text(encoding="utf-8")


def _runner_commands() -> list[str]:
    """Executable lines only — the script's own prose explains what it omits."""
    return [line.strip() for line in _runner_script().splitlines()
            if line.strip() and not line.strip().upper().startswith("REM")]


def test_the_scheduled_runner_never_forces_an_out_of_window_sample() -> None:
    """
    A missed day stays missed. --force in an unattended script would silently
    turn every outage into a sample of the wrong hour, and the cohort would
    exclude it anyway — after it had already been recorded as a reading.
    """
    assert "--force" not in " ".join(_runner_commands())


def test_the_scheduled_runner_cannot_block() -> None:
    """`pause` waits for a keypress nobody is there to press."""
    commands = " ".join(_runner_commands()).lower()
    assert "pause" not in commands


def test_the_scheduled_runner_carries_no_key_material() -> None:
    """
    The fee credential is opt-in through the environment. A path in a committed
    script is a path in the repository.

    Checked on COMMANDS, not the whole file: the script's own comments explain
    which secrets it deliberately omits, and a raw text search would flag that
    explanation.
    """
    commands = " ".join(_runner_commands()).lower()
    for banned in ("cdp_api_key", ".pem", "api_key", "secret", "token",
                   "key_file"):
        assert banned not in commands, f"{banned!r} appears in the runner"

    # The variable may be NAMED in prose; it must never be assigned here, or
    # the opt-in credential stops being opt-in.
    assert "stf_fee_view_only_key_file=" not in _runner_script().lower()


def test_the_scheduled_runner_uses_the_pinned_interpreter() -> None:
    """Not `python`: the ambient one on PATH is not the project environment."""
    commands = " ".join(_runner_commands())
    assert "venv\\Scripts\\python.exe" in commands
    assert "stf_cost_probe.py" in commands


def test_the_scheduled_runner_returns_the_probe_exit_code() -> None:
    """The scheduler decides whether to retry from this code."""
    assert "exit /b %ERRORLEVEL%" in _runner_script()


def test_the_operational_log_is_separate_from_the_observations() -> None:
    """
    Mixing run diagnostics into the JSONL would corrupt the file report()
    parses. They are different artifacts with different readers.
    """
    script = _runner_script()
    assert "stf_cost_probe_runs.log" in script
    assert "stf_cost_probe.jsonl" not in _runner_commands().__str__()
    assert cp.OBSERVATIONS.name == "stf_cost_probe.jsonl"


# ── The schedule ─────────────────────────────────────────────────────────────

def _register_script() -> str:
    root = Path(cp.__file__).resolve().parents[1]
    return (root / "scripts"
            / "register_stf_cost_probe_task.ps1").read_text(encoding="utf-8")


def _register_commands() -> str:
    """
    Executable lines only.

    Both comment forms are stripped: the leading <# ... #> block explains the
    defect this script exists to fix and quotes the offending code verbatim, so
    scanning the raw text would flag the explanation.
    """
    script = _register_script()
    script = script[script.index("#>") + 2:] if "#>" in script else script
    return " ".join(line.strip() for line in script.splitlines()
                    if line.strip() and not line.strip().startswith("#"))


def test_the_schedule_is_never_pinned_to_a_local_wall_clock_time() -> None:
    """
    `-At "20:05"` looks like a local-time rule and is not one: Task Scheduler
    stamps the boundary with the offset in force AT REGISTRATION, and an
    offset-bearing StartBoundary is an absolute instant. Registering the same
    script in winter would therefore have pinned 01:05 UTC instead of 00:05 —
    the sampling time would depend on the season someone happened to run it in.
    """
    assert not re.search(r'-At\s+["\']\d{1,2}:\d{2}', _register_commands()), (
        "the trigger is pinned to a local wall-clock string")


def test_the_schedule_is_computed_from_utc() -> None:
    commands = _register_commands()
    assert "[DateTime]::UtcNow" in commands
    assert "DateTimeKind]::Utc" in commands
    # Get-Date returns local time with no kind; that is how the defect entered.
    assert "Get-Date" not in commands


def test_the_registration_verifies_the_instant_it_stored() -> None:
    """
    The spelling is not ours to control — the service restates the boundary
    with the machine's own offset — so the script parses it back to UTC and
    compares. That check is what would have caught the seasonal defect.
    """
    commands = _register_commands()
    assert "ToUniversalTime()" in commands
    assert "throw" in commands


def test_the_scheduled_instant_lands_inside_the_execution_window() -> None:
    """
    The one number that must agree with the probe. If the window were ever
    narrowed below the scheduled minute, every future sample would be refused
    and the log would fill with exit code 2.
    """
    script = _register_script()
    hour = int(re.search(r"\$TargetUtcHour\s*=\s*(\d+)", script).group(1))
    minute = int(re.search(r"\$TargetUtcMinute\s*=\s*(\d+)", script).group(1))

    minute_of_day = hour * 60 + minute
    assert 0 <= minute_of_day <= cp.EXECUTION_WINDOW_MINUTES, (
        f"scheduled at minute {minute_of_day} of the UTC day, outside the "
        f"{cp.EXECUTION_WINDOW_MINUTES}-minute window")
    # Not at the very edge either: retries are ten minutes apart, and all three
    # attempts have to land inside the window.
    assert minute_of_day + 20 <= cp.EXECUTION_WINDOW_MINUTES, (
        "no room for two retries before the window closes")


def test_a_missed_day_is_not_made_up_later() -> None:
    """StartWhenAvailable would run the task late — at the wrong hour."""
    assert "$settings.StartWhenAvailable = $false" in _register_script()


def test_a_per_asset_failure_does_not_fail_the_run(tmp_path, monkeypatch) -> None:
    """
    The retry policy depends on this. One unreachable asset must still exit 0,
    because retrying would re-sample the assets that already succeeded and give
    those duplicates extra weight in the percentiles. The day is recorded as
    partial, and the per-asset coverage contract refuses to call it complete.
    """
    import datetime as dt

    class InWindow(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2026, 8, 21, 0, 10, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(cp, "datetime", InWindow)
    monkeypatch.delenv(cp.FEE_KEY_FILE_ENV, raising=False)
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "probe.jsonl")

    def _book(product_id, limit):
        if product_id == "ZEC-USD":
            raise RuntimeError("upstream unavailable")
        return {"bids": _book_side([(100.0, 50.0)]),
                "asks": _book_side([(100.5, 50.0)])}

    public = SimpleNamespace(
        get_public_product_book=_book,
        get_public_candles=lambda product_id, start, end, granularity: (
            SimpleNamespace(candles=[SimpleNamespace(start=start)])))

    with patch("coinbase.rest.RESTClient", side_effect=lambda **kw: public):
        obs = cp.observe()

    failed = [r for r in obs["assets"] if "error" in r]
    assert [r["asset"] for r in failed] == ["ZEC-USD"]
    assert len(obs["assets"]) == len(cp.UNIVERSE), "the other assets still sampled"

    # And the partial day counts for the assets that did succeed, for nobody else.
    days = cp.report()["readiness"]["usable_days_per_asset"]
    assert days["ZEC-USD"] == 0
    assert days["BTC-USD"] == 1


# ── Sampling window ──────────────────────────────────────────────────────────

def test_sampling_outside_the_execution_window_is_refused(monkeypatch) -> None:
    """Cost measured at another hour describes a different market."""
    import datetime as dt

    class Noon(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2026, 8, 21, 12, 0, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(cp, "datetime", Noon)
    with pytest.raises(cp.CostProbeError, match="execution window"):
        cp.observe()


# ── Report arithmetic ────────────────────────────────────────────────────────

_ABSENT = object()


def _fee(taker=0.006, maker=0.004, pricing_tier="Advanced 1"):
    return {"available": True, "measured": True, "taker_fee_rate": taker,
            "maker_fee_rate": maker, "pricing_tier": pricing_tier,
            "key_permissions": {"can_view": True, "can_trade": False,
                                "can_transfer": False}}


def _write_obs(path, rows, in_window=True, candle=True, fee=_ABSENT,
               observed_at="2026-08-21T00:10:00+00:00", assets=("BTC-USD",),
               complete=True):
    with path.open("a", encoding="utf-8") as fh:
        for spread in rows:
            impact = spread / 2 if isinstance(spread, (int, float)) else None
            rows_out = []
            for name in assets:
                asset = {"asset": name, "spread_bps": spread,
                         "buy": {"quoted_impact_bps": impact, "complete": complete},
                         "sell": {"quoted_impact_bps": impact, "complete": complete}}
                if candle is not _ABSENT:
                    asset["daily_candle"] = {"available": candle}
                rows_out.append(asset)
            obs = {"assets": rows_out}
            if observed_at is not _ABSENT:
                obs["observed_at"] = observed_at
            if fee is not _ABSENT:
                obs["fee_tier"] = fee
            if in_window is not _ABSENT:
                obs["in_execution_window"] = in_window
            fh.write(json.dumps(obs) + "\n")


def test_out_of_window_observations_are_excluded(tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], in_window=True)
    # Out of window AND stamped out of window: the record agrees with itself.
    _write_obs(obs, [900.0], in_window=False,
               observed_at="2026-08-21T12:00:00+00:00")

    r = cp.report()
    assert r["observations_used"] == 2
    assert r["observations_skipped_out_of_window"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 2.0


def test_readings_without_a_published_daily_bar_are_excluded(tmp_path, monkeypatch) -> None:
    """
    If the bar the signal needs was not published yet, the reading describes a
    moment the strategy could not have traded in.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], candle=True)
    _write_obs(obs, [500.0], candle=False)

    r = cp.report()
    assert r["asset_readings_skipped_no_daily_bar"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["n"] == 2
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 2.0


@pytest.mark.parametrize("flag", [_ABSENT, None, "true", 1, "yes"])
def test_an_unconfirmed_execution_window_is_excluded(tmp_path, monkeypatch, flag) -> None:
    """
    FAIL CLOSED. `obs.get("in_execution_window", True)` admitted a truncated or
    hand-edited line into the sample it exists to filter. Only an explicit True
    confirms the window; everything else is excluded and counted separately —
    "sampled at the wrong hour" and "cannot tell when we sampled" are different
    problems with different remedies.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], in_window=True)
    _write_obs(obs, [900.0], in_window=flag)

    r = cp.report()
    assert r["observations_used"] == 2
    assert r["observations_skipped_window_unconfirmed"] == 1
    assert r["observations_skipped_out_of_window"] == 0
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 2.0


@pytest.mark.parametrize("flag", [_ABSENT, None, "true", 1])
def test_an_unconfirmed_daily_bar_is_excluded(tmp_path, monkeypatch, flag) -> None:
    """Same contract for the bar the signal depends on."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], candle=True)
    _write_obs(obs, [500.0], candle=flag)

    r = cp.report()
    assert r["asset_readings_skipped_candle_unconfirmed"] == 1
    assert r["asset_readings_skipped_no_daily_bar"] == 0
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["n"] == 2


@pytest.mark.parametrize("spread", ["12.5", None, float("inf"), float("nan"), True])
def test_a_non_numeric_or_non_finite_reading_is_excluded(tmp_path, monkeypatch, spread) -> None:
    """A corrupted number must not become a percentile."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0])
    _write_obs(obs, [spread])

    r = cp.report()
    assert r["asset_readings_skipped_malformed"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["n"] == 2


def test_an_unknown_asset_is_not_silently_dropped_into_the_universe(
        tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    with obs.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "in_execution_window": True,
            "observed_at": "2026-08-21T00:10:00+00:00",
            "assets": [{"asset": "DOGE-USD", "spread_bps": 5.0,
                        "buy": {"quoted_impact_bps": 1.0},
                        "sell": {"quoted_impact_bps": 1.0},
                        "daily_candle": {"available": True}}],
        }) + "\n")

    r = cp.report()
    assert r["asset_readings_skipped_malformed"] == 1
    assert all(v["spread_bps"]["n"] == 0 for v in r["per_asset"].values())


def test_percentiles_use_nearest_rank(tmp_path, monkeypatch) -> None:
    """
    `int(p * n)` truncates: at n=10 the p90 picked the 10th observation — the
    maximum — instead of the 9th.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [float(i) for i in range(1, 11)])      # 1..10

    stats = cp.report()["per_asset"]["BTC-USD"]["spread_bps"]
    assert stats["n"] == 10
    assert stats["p90"] == 9.0, "p90 must be the 9th of ten, not the maximum"
    assert stats["p50"] == 5.0
    assert stats["max"] == 10.0


def test_p99_is_not_reported() -> None:
    """At 14-30 observations p99 is the single worst reading, not a tail estimate."""
    assert 0.99 not in cp._PERCENTILES
    src = inspect.getsource(cp)
    assert "why_no_p99" in src


def test_a_measured_fee_reaches_the_report(tmp_path, monkeypatch) -> None:
    """
    observe() recorded the tier and report() dropped it, so thirty days of
    sampling could not replace the protocol's assumed 1.4% with anything.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0, 3.0], fee=_fee())

    fee = cp.report()["fee_tier"]
    assert fee["observations_measured"] == 3
    assert fee["observed_tiers"] == [{
        "pricing_tier": "Advanced 1", "taker_fee_rate": 0.006,
        "maker_fee_rate": 0.004, "observations": 3,
        "first_seen": "2026-08-21T00:10:00+00:00",
        "last_seen": "2026-08-21T00:10:00+00:00"}]
    assert fee["tier_changed_during_the_probe"] is False
    assert fee["round_trip_taker_fee_pct"] == 1.2
    assert fee["draft_protocol_assumed_round_trip_pct"] == 1.4


def test_a_tier_change_is_surfaced_and_never_averaged(tmp_path, monkeypatch) -> None:
    """
    Two schedules must not collapse into one hidden mean: the account never
    paid the average, and the useful signal is that the tier CHANGED.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee(taker=0.006, pricing_tier="Advanced 1"))
    _write_obs(obs, [1.0], fee=_fee(taker=0.004, pricing_tier="Advanced 2"))

    fee = cp.report()["fee_tier"]
    assert fee["tier_changed_during_the_probe"] is True
    assert len(fee["observed_tiers"]) == 2
    assert fee["round_trip_taker_fee_pct"] is None, "two tiers must not blend"
    assert 0.005 not in [t["taker_fee_rate"] for t in fee["observed_tiers"]]


def test_a_relabelled_tier_with_moved_rates_is_a_separate_schedule(
        tmp_path, monkeypatch) -> None:
    """Same label, different rates: still a different fee schedule."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee(taker=0.006))
    _write_obs(obs, [1.0], fee=_fee(taker=0.0075))

    fee = cp.report()["fee_tier"]
    assert len(fee["observed_tiers"]) == 2
    assert fee["tier_changed_during_the_probe"] is True


@pytest.mark.parametrize("bad,bucket", [
    ({"available": False, "measured": False, "reason": "no credential"},
     "observations_unavailable"),
    (_ABSENT, "observations_unconfirmed"),
    ("Advanced 1", "observations_unconfirmed"),
    ({**_fee(), "taker_fee_rate": "nan"}, "observations_unconfirmed"),
    ({k: v for k, v in _fee().items() if k != "maker_fee_rate"},
     "observations_unconfirmed"),
])
def test_an_unusable_fee_reading_is_counted_not_dropped(
        tmp_path, monkeypatch, bad, bucket) -> None:
    """Same fail-closed contract as the rest of the cohort."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=bad)

    fee = cp.report()["fee_tier"]
    assert fee[bucket] == 1
    assert fee["observations_measured"] == 0
    assert fee["observed_tiers"] == []
    assert fee["round_trip_taker_fee_pct"] is None
    assert cp.report()["readiness"]["fee_component_measured"] is False


# ── Finding 2: the recorded permissions are re-checked ──────────────────────

@pytest.mark.parametrize("perms", [
    None,                                                        # absent
    {"can_view": True, "can_trade": True, "can_transfer": False},
    {"can_view": True, "can_trade": False, "can_transfer": True},
    {"can_view": False, "can_trade": False, "can_transfer": False},
    {"can_view": True, "can_trade": False},                      # missing field
    {"can_view": True, "can_trade": "false", "can_transfer": False},
    {"can_view": 1, "can_trade": 0, "can_transfer": 0},          # not bools
    "view-only",                                                 # not a dict
])
def test_a_rate_from_an_unverified_key_never_counts_as_measured(
        tmp_path, monkeypatch, perms) -> None:
    """
    observe() checks the key before reading a rate, but report() reads a FILE.
    A line from an older build, a hand-edited entry, or a run whose credential
    has since gained trade rights all arrive with measured=true. The recorded
    proof is re-checked rather than trusted.
    """
    row = {**_fee()}
    if perms is None:
        del row["key_permissions"]
    else:
        row["key_permissions"] = perms

    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=row)

    r = cp.report()
    assert r["fee_tier"]["observations_measured"] == 0
    assert r["fee_tier"]["observations_rejected_on_key_permissions"] == 1
    assert r["fee_tier"]["observed_tiers"] == []
    assert r["readiness"]["fee_component_measured"] is False


def test_a_verified_key_is_reported_apart_from_ordinary_failures(
        tmp_path, monkeypatch) -> None:
    """The credential and the plumbing need different responses."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee())
    _write_obs(obs, [1.0], fee={**_fee(), "key_permissions":
                                {"can_view": True, "can_trade": True,
                                 "can_transfer": False}})
    _write_obs(obs, [1.0], fee={"available": False, "measured": False,
                                "reason": "no credential"})

    fee = cp.report()["fee_tier"]
    assert fee["observations_measured"] == 1
    assert fee["observations_rejected_on_key_permissions"] == 1
    assert fee["observations_unavailable"] == 1
    assert fee["observations_unconfirmed"] == 0


# ── Timestamps ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("stamp", [_ABSENT, None, "yesterday",
                                   "2026-08-21T00:10:00"])
def test_a_reading_that_cannot_be_placed_in_time_is_excluded(
        tmp_path, monkeypatch, stamp) -> None:
    """
    A naive stamp is refused rather than assumed UTC: the whole point is WHICH
    90 minutes the reading came from, and "any timezone" answers a different
    question.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee())
    _write_obs(obs, [900.0], fee=_fee(), observed_at=stamp)

    r = cp.report()
    assert r["observations_used"] == 1
    assert r["observations_skipped_unusable_timestamp"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 1.0


def test_first_and_last_seen_are_min_and_max_not_file_order(
        tmp_path, monkeypatch) -> None:
    """Separate runs append to this log; nothing keeps it chronological."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee(), observed_at="2026-08-20T00:10:00+00:00")
    _write_obs(obs, [1.0], fee=_fee(), observed_at="2026-08-18T00:10:00+00:00")
    _write_obs(obs, [1.0], fee=_fee(), observed_at="2026-08-19T00:10:00+00:00")

    tier = cp.report()["fee_tier"]["observed_tiers"][0]
    assert tier["first_seen"] == "2026-08-18T00:10:00+00:00"
    assert tier["last_seen"] == "2026-08-20T00:10:00+00:00"
    assert tier["observations"] == 3


def test_a_non_utc_stamp_is_normalised_before_the_day_is_counted(
        tmp_path, monkeypatch) -> None:
    """22:30 in New York on the 20th is the 21st in UTC — one day, not two."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], observed_at="2026-08-21T00:30:00+00:00")
    _write_obs(obs, [1.0], observed_at="2026-08-20T20:30:00-04:00")

    assert cp.report()["readiness"]["unique_utc_days_sampled"] == 1


# ── Readiness ───────────────────────────────────────────────────────────────

def test_one_measured_reading_is_not_a_cost_decision(tmp_path, monkeypatch) -> None:
    """
    The report used to announce that the fee "replaces the assumed round trip"
    after a SINGLE reading — one minute of one day, no asset coverage at all.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee(), assets=cp.UNIVERSE)

    ready = cp.report()["readiness"]
    assert ready["fee_component_measured"] is True
    assert ready["execution_sample_ready"] is False
    assert ready["ready_for_protocol_cost_decision"] is False
    assert ready["unique_utc_days_sampled"] == 1
    assert ready["usable_days_per_asset"] == {a: 1 for a in cp.UNIVERSE}
    assert ready["assets_below_minimum"] == sorted(cp.UNIVERSE)


def test_the_declared_minimum_is_counted_in_unique_days(tmp_path, monkeypatch) -> None:
    """
    Sampling twice on one day is one day's worth of market. Counting rows
    instead would let a single afternoon satisfy a fourteen-day contract.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    for minute in range(cp.MIN_USABLE_DAYS_PER_ASSET + 2):
        _write_obs(obs, [1.0], fee=_fee(), assets=cp.UNIVERSE,
                   observed_at=f"2026-08-21T00:{minute:02d}:00+00:00")

    ready = cp.report()["readiness"]
    assert ready["unique_utc_days_sampled"] == 1
    assert ready["usable_days_per_asset"] == {a: 1 for a in cp.UNIVERSE}
    assert ready["execution_sample_ready"] is False


def test_full_coverage_reports_ready_but_still_not_a_verdict(
        tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    for day in range(1, cp.MIN_USABLE_DAYS_PER_ASSET + 1):
        _write_obs(obs, [1.0], fee=_fee(), assets=cp.UNIVERSE,
                   observed_at=f"2026-08-{day:02d}T00:10:00+00:00")

    ready = cp.report()["readiness"]
    assert ready["usable_days_per_asset"] == {
        a: cp.MIN_USABLE_DAYS_PER_ASSET for a in cp.UNIVERSE}
    assert ready["assets_below_minimum"] == []
    assert ready["execution_sample_ready"] is True
    assert ready["ready_for_protocol_cost_decision"] is True
    # Ready is an input, not a conclusion.
    assert "decision for the protocol" in ready["this_is_not_a_verdict"]


def test_one_uncovered_asset_blocks_readiness(tmp_path, monkeypatch) -> None:
    """Three assets measured and one not is not a portfolio cost measurement."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    covered = [a for a in cp.UNIVERSE if a != "ZEC-USD"]
    for day in range(1, cp.MIN_USABLE_DAYS_PER_ASSET + 1):
        _write_obs(obs, [1.0], fee=_fee(), assets=covered,
                   observed_at=f"2026-08-{day:02d}T00:10:00+00:00")

    ready = cp.report()["readiness"]
    assert ready["assets_below_minimum"] == ["ZEC-USD"]
    assert ready["execution_sample_ready"] is False
    assert ready["ready_for_protocol_cost_decision"] is False


def _full_days(obs, days, assets=None, complete=True, first=1):
    """`days` distinct UTC days of complete readings across the universe."""
    for day in range(first, first + days):
        _write_obs(obs, [1.0], fee=_fee(), complete=complete,
                   assets=assets or cp.UNIVERSE,
                   observed_at=f"2026-08-{day:02d}T00:10:00+00:00")


def test_an_incomplete_sweep_is_not_a_usable_reading(tmp_path, monkeypatch) -> None:
    """
    _sweep() reports `complete`, and the report ignored it. If the visible book
    could not cover the intended notional, the VWAP prices a SMALLER order than
    the protocol would send — understating cost exactly where it matters.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _full_days(obs, cp.MIN_USABLE_DAYS_PER_ASSET, complete=False)

    r = cp.report()
    assert r["asset_readings_with_incomplete_book"] == (
        cp.MIN_USABLE_DAYS_PER_ASSET * len(cp.UNIVERSE))
    assert r["readiness"]["usable_days_per_asset"] == {a: 0 for a in cp.UNIVERSE}
    assert r["readiness"]["execution_sample_ready"] is False
    # The spread still stands on its own; only impact needed a filled sweep.
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["n"] == cp.MIN_USABLE_DAYS_PER_ASSET
    assert r["per_asset"]["BTC-USD"]["quoted_impact_round_trip_bps"]["n"] == 0


def test_one_side_failing_to_fill_is_enough_to_reject(tmp_path, monkeypatch) -> None:
    """A round trip needs both legs; a filled buy and a partial sell is not one."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    with obs.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "observed_at": "2026-08-21T00:10:00+00:00",
            "in_execution_window": True,
            "assets": [{"asset": "BTC-USD", "spread_bps": 2.0,
                        "buy": {"quoted_impact_bps": 1.0, "complete": True},
                        "sell": {"quoted_impact_bps": 1.0, "complete": False},
                        "daily_candle": {"available": True}}],
        }) + "\n")

    r = cp.report()
    assert r["asset_readings_with_incomplete_book"] == 1
    assert r["readiness"]["usable_days_per_asset"]["BTC-USD"] == 0


@pytest.mark.parametrize("missing", ["complete", "quoted_impact_bps"])
def test_a_sweep_that_does_not_say_it_filled_is_not_assumed_to_have(
        tmp_path, monkeypatch, missing) -> None:
    """Fail closed: an absent field is not a completed fill."""
    side = {"quoted_impact_bps": 1.0, "complete": True}
    del side[missing]
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    with obs.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "observed_at": "2026-08-21T00:10:00+00:00",
            "in_execution_window": True,
            "assets": [{"asset": "BTC-USD", "spread_bps": 2.0,
                        "buy": dict(side), "sell": dict(side),
                        "daily_candle": {"available": True}}],
        }) + "\n")

    assert cp.report()["readiness"]["usable_days_per_asset"]["BTC-USD"] == 0


def test_repeat_readings_of_one_day_padded_with_empty_days_are_not_coverage(
        tmp_path, monkeypatch) -> None:
    """
    The exact hole in the old contract: fourteen readings of ONE asset on ONE
    afternoon, plus thirteen days on which nothing usable was captured, cleared
    both "14 unique days" and "14 readings per asset" while three assets went
    unmeasured.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    for minute in range(cp.MIN_USABLE_DAYS_PER_ASSET):
        _write_obs(obs, [1.0], fee=_fee(), assets=["BTC-USD"],
                   observed_at=f"2026-08-01T00:{minute:02d}:00+00:00")
    # Thirteen further days that yield nothing usable.
    for day in range(2, cp.MIN_USABLE_DAYS_PER_ASSET + 1):
        _write_obs(obs, [1.0], fee=_fee(), assets=cp.UNIVERSE, complete=False,
                   observed_at=f"2026-08-{day:02d}T00:10:00+00:00")

    ready = cp.report()["readiness"]
    assert ready["unique_utc_days_sampled"] == cp.MIN_USABLE_DAYS_PER_ASSET
    assert ready["usable_days_per_asset"]["BTC-USD"] == 1
    assert ready["assets_below_minimum"] == sorted(cp.UNIVERSE)
    assert ready["execution_sample_ready"] is False


def test_a_window_flag_contradicted_by_its_own_timestamp_is_excluded(
        tmp_path, monkeypatch) -> None:
    """
    in_execution_window was believed on sight. A line stamped noon UTC with the
    flag set to True is internally inconsistent — one of the two is wrong and
    nothing says which — so it is excluded rather than half-believed.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], observed_at="2026-08-21T00:10:00+00:00")
    _write_obs(obs, [900.0], observed_at="2026-08-21T12:00:00+00:00")

    r = cp.report()
    assert r["observations_used"] == 1
    assert r["observations_skipped_window_flag_disputed_by_timestamp"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 1.0


def test_a_false_flag_on_an_in_window_timestamp_is_also_excluded(
        tmp_path, monkeypatch) -> None:
    """The disagreement is what disqualifies it, in either direction."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], in_window=False,
               observed_at="2026-08-21T00:10:00+00:00")

    r = cp.report()
    assert r["observations_used"] == 0
    assert r["observations_skipped_window_flag_disputed_by_timestamp"] == 1
    assert r["observations_skipped_out_of_window"] == 0


def test_an_honestly_out_of_window_reading_is_still_just_out_of_window(
        tmp_path, monkeypatch) -> None:
    """A --force sample agrees with its own clock; it is excluded, not disputed."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], in_window=False,
               observed_at="2026-08-21T12:00:00+00:00")

    r = cp.report()
    assert r["observations_skipped_out_of_window"] == 1
    assert r["observations_skipped_window_flag_disputed_by_timestamp"] == 0


def test_the_window_edge_matches_the_declared_width(tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    edge = cp.EXECUTION_WINDOW_MINUTES
    _write_obs(obs, [1.0], observed_at=f"2026-08-21T{edge // 60:02d}:{edge % 60:02d}:00+00:00")
    _write_obs(obs, [2.0], observed_at=f"2026-08-22T{(edge + 1) // 60:02d}:{(edge + 1) % 60:02d}:00+00:00")

    r = cp.report()
    assert r["observations_used"] == 1
    assert r["observations_skipped_window_flag_disputed_by_timestamp"] == 1


def test_a_tier_change_blocks_the_fee_component(tmp_path, monkeypatch) -> None:
    """Two schedules is not one measured fee, however many days were sampled."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    for day in range(1, cp.MIN_USABLE_DAYS_PER_ASSET + 1):
        _write_obs(obs, [1.0], assets=cp.UNIVERSE,
                   fee=_fee(taker=0.006 if day % 2 else 0.004),
                   observed_at=f"2026-08-{day:02d}T00:10:00+00:00")

    ready = cp.report()["readiness"]
    assert ready["execution_sample_ready"] is True
    assert ready["fee_component_measured"] is False
    assert ready["ready_for_protocol_cost_decision"] is False


def test_an_out_of_window_observation_contributes_no_fee(tmp_path, monkeypatch) -> None:
    """The fee cohort is the same cohort as the cost one."""
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], in_window=False, fee=_fee())

    fee = cp.report()["fee_tier"]
    assert fee["observations_measured"] == 0
    assert fee["observations_unconfirmed"] == 0


def test_fee_and_impact_are_reported_separately_not_summed(tmp_path, monkeypatch) -> None:
    """
    Combining a fee with quoted impact against a static book is a modelling
    decision for the protocol, not something the instrument should decide.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0], fee=_fee())

    r = cp.report()
    assert "never summed here" in r["readiness"]["this_is_not_a_verdict"]
    assert "total" not in json.dumps(r).lower()
    # No field claims the assumption has been replaced.
    assert "replace" not in json.dumps(r["fee_tier"]).lower()


def test_the_report_says_it_measures_quoted_impact(tmp_path, monkeypatch) -> None:
    """
    A static book gives impact against displayed liquidity, not a fill. Calling
    it slippage would overstate what was measured.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0])
    r = cp.report()
    assert "QUOTED IMPACT" in r["measures"]
    assert "quoted_impact_round_trip_bps" in r["per_asset"]["BTC-USD"]


def test_report_without_observations_fails_loudly(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "absent.jsonl")
    with pytest.raises(cp.CostProbeError, match="no observations"):
        cp.report()
