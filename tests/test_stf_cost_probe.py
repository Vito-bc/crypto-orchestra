"""
Phase 7R-2 cost probe — safety and arithmetic.

The probe was shipped without tests, and a probe is a measuring instrument: an
unnoticed defect here becomes a number in a registered protocol. Everything runs
against synthetic books and a mocked client — no network, no credentials.
"""
from __future__ import annotations

import inspect
import json
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


def _write_obs(path, rows, in_window=True, candle=True, fee=_ABSENT):
    with path.open("a", encoding="utf-8") as fh:
        for spread in rows:
            asset = {
                "asset": "BTC-USD", "spread_bps": spread,
                "buy": {"quoted_impact_bps": spread / 2 if isinstance(spread, (int, float)) else None},
                "sell": {"quoted_impact_bps": spread / 2 if isinstance(spread, (int, float)) else None},
            }
            if candle is not _ABSENT:
                asset["daily_candle"] = {"available": candle}
            obs = {"observed_at": "2026-08-21T00:10:00+00:00",
                   "assets": [asset]}
            if fee is not _ABSENT:
                obs["fee_tier"] = fee
            if in_window is not _ABSENT:
                obs["in_execution_window"] = in_window
            fh.write(json.dumps(obs) + "\n")


def test_out_of_window_observations_are_excluded(tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], in_window=True)
    _write_obs(obs, [900.0], in_window=False)

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
    assert "measurable" in fee["assumption_status"]


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
    assert "changed" in fee["assumption_status"]
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
    ({"available": True, "measured": True, "taker_fee_rate": "nan",
      "maker_fee_rate": 0.004}, "observations_unconfirmed"),
    ({"available": True, "measured": True, "taker_fee_rate": 0.006},
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
    assert "still an assumption" in fee["assumption_status"]


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
    assert "NOT summed" in r["fee_tier"]["assumption_status"]
    assert "total" not in json.dumps(r).lower()


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
