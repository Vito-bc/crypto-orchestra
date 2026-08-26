"""
Phase 7R-2 — prospective execution-cost probe. READ ONLY, NO ORDERS.

The draft protocol asserted a 1.4% round-trip cost. That was an assumption, and
a trial whose acceptance depends on an unmeasured assumption can be falsified by
its own plumbing. This probe measures what it can, forward, before any capital
is committed.

WHAT IT MEASURES — AND WHAT IT DOES NOT
---------------------------------------
It sweeps the PUBLIC order book for the intended notional and reports QUOTED
IMPACT: the size-weighted price a marketable order would touch against the book
as displayed. That is not execution slippage. Real slippage also contains queue
movement between the quote and the fill, partial fills, and the exchange's
own routing — none of which is observable without trading. The field names say
`quoted_impact_bps` for that reason.

Coinbase serves the public book from a short-lived cache, so two reads seconds
apart can be identical. Treat each run as one sample of the quote, not as a tick
stream.

The taker FEE is tier-dependent and lives behind a private endpoint, so the
unauthenticated client cannot read it and must not be able to. Reading it is
therefore OPT-IN and separate: point STF_FEE_VIEW_ONLY_KEY_FILE at a Coinbase
key created with VIEW-ONLY permission and nothing else.

    There is no fallback. With the variable unset the fee is recorded as
    unavailable and never assumed.

    The key's PERMISSIONS are verified against the exchange before any market
    request: can_view true, can_trade false, can_transfer false, each an actual
    boolean. A key that fails, errors, or answers with a missing field is
    refused. Path and filename checks also refuse the repository's own trading
    credential, but those only recognise a key we already know about — a
    renamed trading key, or a view-only key that was later granted trade
    rights, is caught by the permission call and nothing else.

Run once per day inside the execution window the protocol specifies, for
14-30 days:
    python backtesting/stf_cost_probe.py           # append one observation
    python backtesting/stf_cost_probe.py --force   # sample outside the window
    python backtesting/stf_cost_probe.py --report  # summarise what was collected
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from math import isfinite
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.stf_protocol import UNIVERSE, position_notional  # noqa: E402

OBSERVATIONS = ROOT / "logs" / "stf_cost_probe.jsonl"

# Opt-in, view-only credential, used for the fee tier and nothing else.
FEE_KEY_FILE_ENV = "STF_FEE_VIEW_ONLY_KEY_FILE"
_LIVE_TRADING_KEY = ROOT / "cdp_api_key.json"

# A rate outside this range is a parsing accident, not a fee schedule: the
# highest Coinbase Advanced taker tier is far below 10%.
_MAX_PLAUSIBLE_FEE_RATE = 0.10

# What the exchange must say about the key before this probe will use it.
# Checking the file name proves nothing about what the key can do.
_REQUIRED_PERMISSIONS = {"can_view": True, "can_trade": False, "can_transfer": False}

# The protocol executes on the first hourly bar after the daily close, so the
# cost that matters is the cost in that window. Sampling at an arbitrary hour
# would measure a different market.
EXECUTION_WINDOW_MINUTES = 90

# Percentiles worth quoting at this sample size. p99 from 14-30 observations is
# an order statistic of the single worst reading, so it is not reported.
_PERCENTILES = (0.5, 0.9)

# What the draft protocol assumed, carried in the report so the measurement is
# always shown next to the number it exists to replace.
_DRAFT_ASSUMED_ROUND_TRIP_PCT = 1.4


class CostProbeError(RuntimeError):
    """The probe could not take a reading."""


def _public_client():
    """
    Unauthenticated client — same discipline as the research hydration path.

    No key material. This probe must remain incapable of touching an account
    balance or placing an order.
    """
    from coinbase.rest import RESTClient

    return RESTClient()


def _fee_client():
    """
    Optional client for the fee tier alone, with its verified permissions.

    Returns (client, permissions), or (None, None) when not configured.

    The fee used to be requested with the unauthenticated client, which can
    never succeed — so the tier was permanently "unavailable" while the code
    implied it could be measured, and the protocol's 1.4% stayed an assumption.

    Deliberately narrow:
      - only the path named by FEE_KEY_FILE_ENV is ever loaded;
      - the repository's live trading key is refused by resolved path AND by
        filename, so a copy of it cannot be slipped in;
      - a missing path is an error, not a silent downgrade to the public
        client: the operator asked for a measured fee and must not receive an
        unmeasured one;
      - the exchange must confirm the key cannot trade or transfer BEFORE any
        market request is made.
    """
    import os

    raw = os.environ.get(FEE_KEY_FILE_ENV, "").strip()
    if not raw:
        return None, None

    path = Path(raw).expanduser()
    if (path.name == _LIVE_TRADING_KEY.name
            or path.resolve() == _LIVE_TRADING_KEY.resolve()):
        raise CostProbeError(
            f"{FEE_KEY_FILE_ENV} points at the live trading credential "
            f"({path}). This probe reads a fee tier; it must never hold a key "
            "that can place an order. Create a view-only key instead.")
    if not path.is_file():
        raise CostProbeError(
            f"{FEE_KEY_FILE_ENV} points at {path}, which is not a file")

    from coinbase.rest import RESTClient

    client = RESTClient(key_file=str(path))
    return client, _assert_view_only(client)


def _assert_view_only(client) -> dict:
    """
    Verify with the EXCHANGE that this key cannot trade. Raises otherwise.

    Path and filename checks recognise a credential we already know about. They
    say nothing about a renamed trading key, or about a view-only key that was
    later granted trade rights — only the exchange knows that. This runs before
    any market request, so a key that should not be here never gets used.

    Fails closed on everything: a request error, a missing field, or a value
    that is not an actual boolean is treated as "this key can trade".
    """
    try:
        perms = client.get_api_key_permissions()
    except Exception as exc:
        raise CostProbeError(
            f"{type(exc).__name__}: could not read the key's permissions. A key "
            "whose permissions cannot be verified is treated as a trading key "
            "and refused.") from exc

    granted = {}
    for field in _REQUIRED_PERMISSIONS:
        value = getattr(perms, field, None)
        if not isinstance(value, bool):
            raise CostProbeError(
                f"key_permissions.{field} is {value!r}, not a boolean. An "
                "unreadable permission is refused rather than assumed benign.")
        granted[field] = value

    wrong = {f: granted[f] for f, want in _REQUIRED_PERMISSIONS.items()
             if granted[f] is not want}
    if wrong:
        raise CostProbeError(
            f"{FEE_KEY_FILE_ENV} names a key with {wrong} — required "
            f"{_REQUIRED_PERMISSIONS}. This probe reads a fee tier; it must not "
            "hold a key that can trade or transfer. Create a view-only key.")
    return granted


def trial_notional() -> float:
    """
    The notional one STF position would take.

    Size comes from the FROZEN trial protocol, not from pipeline.sizing: the
    power study and this probe once used different fractions (5% and 2%), so
    cost and drawdown described different mechanisms. The live default may move
    again; a registered trial must not move with it.
    """
    from pipeline.sizing import live_balance_usd

    return position_notional(live_balance_usd())


def _validated_levels(raw, side: str) -> list[tuple[float, float]]:
    """
    Parse and sort one side of the book, dropping unusable levels.

    Coinbase returns levels in order, but nothing in the response type
    guarantees it, and an out-of-order or zero-size level silently corrupts a
    sweep. Bids descend, asks ascend.
    """
    levels = []
    for lvl in raw:
        try:
            price = float(lvl["price"] if isinstance(lvl, dict) else lvl.price)
            size = float(lvl["size"] if isinstance(lvl, dict) else lvl.size)
        except (KeyError, AttributeError, TypeError, ValueError):
            continue
        # isfinite, not a bare NaN check: inf passes `x == x` and would sweep
        # the whole book into one level.
        if price > 0 and size > 0 and isfinite(price) and isfinite(size):
            levels.append((price, size))
    return sorted(levels, key=lambda p: p[0], reverse=(side == "bid"))


def _book(client, product_id: str, depth: int = 50):
    # PUBLIC book endpoint. get_product_book is private in this SDK and rejects
    # an unauthenticated client, which is the correct behaviour here.
    resp = client.get_public_product_book(product_id=product_id, limit=depth)
    book = getattr(resp, "pricebook", None) or resp
    bids_raw = getattr(book, "bids", None) or book["bids"]
    asks_raw = getattr(book, "asks", None) or book["asks"]
    return _validated_levels(bids_raw, "bid"), _validated_levels(asks_raw, "ask")


def _sweep(levels: list[tuple[float, float]], notional: float) -> dict:
    """
    Size-weighted price a marketable order of `notional` would touch.

    QUOTED IMPACT against the displayed book — not a fill. If the visible book
    cannot cover the order, that is reported rather than extrapolated.
    """
    spent = filled_base = 0.0
    for price, size in levels:
        if spent >= notional:
            break
        take_quote = min(price * size, notional - spent)
        spent += take_quote
        filled_base += take_quote / price
    if filled_base == 0:
        return {"vwap": None, "filled_quote": 0.0, "complete": False}
    return {
        "vwap": round(spent / filled_base, 8),
        "filled_quote": round(spent, 2),
        "complete": spent >= notional - 1e-9,
    }


def _validated_rate(value) -> float | None:
    """
    A fee rate, or None if it is not one.

    `float(getattr(..., "nan"))` accepted NaN and infinity as measured rates
    and raised on a non-numeric value, taking the whole reading with it. An
    unusable rate must degrade to "unavailable": this probe exists to remove
    assumed fees, and NaN is an assumed fee that arithmetic will not warn about.
    """
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(rate) or rate < 0.0 or rate > _MAX_PLAUSIBLE_FEE_RATE:
        return None
    return rate


def _fee_tier(client, permissions: dict | None = None) -> dict:
    """
    The account fee tier, if a view-only credential was deliberately supplied.

    get_transaction_summary is a private endpoint; `client` is None unless
    FEE_KEY_FILE_ENV named a key file whose permissions checked out. Recorded
    as unavailable, never assumed.
    """
    if client is None:
        return {"available": False, "measured": False,
                "reason": (f"no view-only credential: set {FEE_KEY_FILE_ENV} to "
                           "read the account fee tier")}
    try:
        summary = client.get_transaction_summary()
    except Exception as exc:
        return {"available": False, "measured": False,
                "reason": f"{type(exc).__name__}: fee tier unreadable"}
    tier = getattr(summary, "fee_tier", None)
    if tier is None:
        return {"available": False, "measured": False,
                "reason": "no fee_tier in response"}

    taker = _validated_rate(getattr(tier, "taker_fee_rate", None))
    maker = _validated_rate(getattr(tier, "maker_fee_rate", None))
    if taker is None or maker is None:
        return {"available": False, "measured": False,
                "reason": ("fee_tier present but a rate is missing, non-numeric, "
                           "non-finite, or outside "
                           f"[0, {_MAX_PLAUSIBLE_FEE_RATE}]")}
    return {
        "available": True,
        "measured": True,
        "taker_fee_rate": taker,
        "maker_fee_rate": maker,
        "pricing_tier": getattr(tier, "pricing_tier", None),
        # Provenance: which key produced this rate, and what it was allowed to
        # do at the moment of reading.
        "key_permissions": permissions,
    }


def _daily_candle_latency(client, product_id: str, now: datetime) -> dict:
    """
    Is the completed daily bar for YESTERDAY actually published yet?

    The previous version recorded only the clock, which says nothing about data
    availability — the quantity the protocol depends on is when the bar the
    signal needs becomes readable.
    """
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = int((midnight - timedelta(days=1)).timestamp())
    try:
        resp = client.get_public_candles(
            product_id=product_id, start=str(start), end=str(int(midnight.timestamp())),
            granularity="ONE_DAY")
        candles = getattr(resp, "candles", None) or []
        available = any(int(getattr(c, "start", 0)) == start for c in candles)
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
    return {
        "available": bool(available),
        "bar_start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
        "minutes_after_close": round((now - midnight).total_seconds() / 60.0, 1),
    }


def observe(force: bool = False) -> dict:
    """One reading across the universe. Appends to the observation log."""
    now = datetime.now(timezone.utc)
    minutes_after_midnight = (
        now - now.replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds() / 60.0
    in_window = minutes_after_midnight <= EXECUTION_WINDOW_MINUTES
    if not in_window and not force:
        raise CostProbeError(
            f"{minutes_after_midnight:.0f} minutes after 00:00 UTC is outside the "
            f"{EXECUTION_WINDOW_MINUTES}-minute execution window. Cost measured "
            "at another hour describes a different market. Use --force to record "
            "an out-of-window sample; it is flagged and excluded from the report.")

    # Built and permission-checked before any sampling: a key that should not
    # be here must be refused before it is used, not after.
    fee_client, fee_permissions = _fee_client()
    client = _public_client()
    notional = trial_notional()

    rows = []
    for asset in UNIVERSE:
        try:
            bids, asks = _book(client, asset)
        except Exception as exc:
            rows.append({"asset": asset, "error": f"{type(exc).__name__}: {exc}"})
            continue
        if not bids or not asks:
            rows.append({"asset": asset, "error": "empty or unusable book"})
            continue
        if bids[0][0] >= asks[0][0]:
            rows.append({"asset": asset, "error": "crossed book"})
            continue

        best_bid, best_ask = bids[0][0], asks[0][0]
        mid = (best_bid + best_ask) / 2.0
        buy, sell = _sweep(asks, notional), _sweep(bids, notional)
        rows.append({
            "asset": asset,
            "best_bid": best_bid, "best_ask": best_ask, "mid": round(mid, 8),
            "spread_bps": round((best_ask - best_bid) / mid * 10_000, 2),
            "levels_seen": {"bids": len(bids), "asks": len(asks)},
            "buy": {**buy, "quoted_impact_bps": (
                round((buy["vwap"] - mid) / mid * 10_000, 2) if buy["vwap"] else None)},
            "sell": {**sell, "quoted_impact_bps": (
                round((mid - sell["vwap"]) / mid * 10_000, 2) if sell["vwap"] else None)},
            "daily_candle": _daily_candle_latency(client, asset, now),
        })

    observation = {
        "observed_at": now.isoformat(),
        "minutes_after_utc_midnight": round(minutes_after_midnight, 1),
        "in_execution_window": in_window,
        "execution_window_minutes": EXECUTION_WINDOW_MINUTES,
        "notional_usd": notional,
        "measures": "quoted impact against the displayed book, not realised fills",
        "fee_tier": _fee_tier(fee_client, fee_permissions),
        "assets": rows,
    }
    OBSERVATIONS.parent.mkdir(parents=True, exist_ok=True)
    with OBSERVATIONS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(observation, sort_keys=True) + "\n")
    return observation


def _collect_fee(obs: dict, tiers: dict) -> str:
    """
    Fold one observation's fee reading into the tier tally.

    Returns "measured", "unavailable" or "unconfirmed". Same fail-closed
    contract as the rest of the cohort: only an explicitly measured tier with
    two usable rates counts as measured.
    """
    row = obs.get("fee_tier")
    if not isinstance(row, dict):
        return "unconfirmed"
    if row.get("measured") is not True or row.get("available") is not True:
        return "unavailable" if row.get("available") is False else "unconfirmed"

    taker = _validated_rate(row.get("taker_fee_rate"))
    maker = _validated_rate(row.get("maker_fee_rate"))
    if taker is None or maker is None:
        return "unconfirmed"

    at = obs.get("observed_at")
    # Keyed on the RATES as well as the tier name: a tier that keeps its label
    # while its rates move is still a different fee schedule.
    key = (str(row.get("pricing_tier")), taker, maker)
    seen = tiers.setdefault(key, {
        "pricing_tier": row.get("pricing_tier"),
        "taker_fee_rate": taker,
        "maker_fee_rate": maker,
        "observations": 0,
        "first_seen": at,
        "last_seen": at,
    })
    seen["observations"] += 1
    seen["last_seen"] = at
    return "measured"


def _fee_summary(measured: int, unavailable: int, unconfirmed: int,
                 tiers: dict) -> dict:
    """
    What the probe learned about the FEE half of the cost.

    The report used to omit this entirely: observe() recorded the tier and
    report() dropped it, so thirty days of sampling could not replace the
    protocol's assumed 1.4% round trip with anything.

    Rates from different tiers are never averaged. A mean of two schedules is a
    rate the account never paid, and the useful signal in two tiers is that the
    tier CHANGED.
    """
    observed = sorted(tiers.values(),
                      key=lambda t: (t["first_seen"] or "", t["taker_fee_rate"]))
    single = observed[0] if len(observed) == 1 else None
    return {
        "observations_measured": measured,
        "observations_unavailable": unavailable,
        "observations_unconfirmed": unconfirmed,
        "observed_tiers": observed,
        "tier_changed_during_the_probe": len(observed) > 1,
        "rates_are_never_averaged_because": (
            "distinct tiers are distinct fee schedules; their mean is a rate "
            "the account never paid"),
        # Taker on both legs: the strategy exits on a signal, so it cannot
        # count on a maker fill. Reported only when ONE tier was observed.
        "round_trip_taker_fee_pct": (
            round(single["taker_fee_rate"] * 2 * 100, 4) if single else None),
        "draft_protocol_assumed_round_trip_pct": _DRAFT_ASSUMED_ROUND_TRIP_PCT,
        "assumption_status": (
            "still an assumption: no fee tier was measured" if not measured else
            "measured, but the tier changed during the probe — pick a tier "
            "deliberately rather than blending them" if single is None else
            "measurable: this fee plus the quoted impact above replaces the "
            "assumed round trip. They are reported separately and NOT summed "
            "here; combining them is a modelling decision for the protocol"),
    }


def _measurement(value) -> float | None:
    """A finite numeric reading, or None. bool is not a measurement."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if isfinite(value) else None


def report() -> dict:
    """Percentiles of the measured quoted impact. No trading decisions."""
    if not OBSERVATIONS.exists():
        raise CostProbeError(f"no observations at {OBSERVATIONS}")

    import statistics

    per_asset: dict = {a: {"spread_bps": [], "round_trip_bps": []} for a in UNIVERSE}
    used = out_of_window = unconfirmed_window = 0
    no_candle = unconfirmed_candle = malformed = 0
    fee_measured = fee_unavailable = fee_unconfirmed = 0
    fee_tiers: dict = {}
    for line in OBSERVATIONS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obs = json.loads(line)
        # FAIL CLOSED on the cohort. `obs.get(..., True)` admitted an
        # observation whose window flag was missing, None, or a string — a
        # truncated or hand-edited line silently joined the sample it is
        # supposed to be excluded from. Only an explicit True is a confirmation.
        window = obs.get("in_execution_window")
        if window is False:
            out_of_window += 1    # out-of-window samples describe another market
            continue
        if window is not True:
            unconfirmed_window += 1
            continue
        used += 1
        status = _collect_fee(obs, fee_tiers)
        fee_measured += status == "measured"
        fee_unavailable += status == "unavailable"
        fee_unconfirmed += status == "unconfirmed"
        for row in obs.get("assets", []):
            if "error" in row:
                continue
            # Likewise: the bar the signal needs must be CONFIRMED published,
            # otherwise the reading describes a moment the strategy could not
            # have traded in.
            available = row.get("daily_candle", {}).get("available")
            if available is False:
                no_candle += 1
                continue
            if available is not True:
                unconfirmed_candle += 1
                continue

            spread = _measurement(row.get("spread_bps"))
            buy = _measurement(row.get("buy", {}).get("quoted_impact_bps"))
            sell = _measurement(row.get("sell", {}).get("quoted_impact_bps"))
            if spread is None or row.get("asset") not in per_asset:
                malformed += 1
                continue
            per_asset[row["asset"]]["spread_bps"].append(spread)
            if buy is not None and sell is not None:
                per_asset[row["asset"]]["round_trip_bps"].append(buy + sell)

    def _pcts(values: list[float]) -> dict:
        if not values:
            return {"n": 0}
        s = sorted(values)
        out = {"n": len(s), "mean": round(statistics.fmean(s), 2),
               "max": round(s[-1], 2)}
        for p in _PERCENTILES:
            # Nearest-rank: ceil(p*n) - 1. `int(p*n)` truncates, so at n=10 the
            # p90 picked the 10th observation (the maximum) instead of the 9th.
            rank = max(1, -((-int(p * len(s) * 1000)) // 1000))
            out[f"p{int(p * 100)}"] = round(s[min(rank, len(s)) - 1], 2)
        return out

    return {
        "observations_used": used,
        "fee_tier": _fee_summary(fee_measured, fee_unavailable, fee_unconfirmed,
                                 fee_tiers),
        "observations_skipped_out_of_window": out_of_window,
        # Separate counters, not one bucket: "we sampled at the wrong hour" and
        # "we cannot tell when we sampled" call for different remedies.
        "observations_skipped_window_unconfirmed": unconfirmed_window,
        "asset_readings_skipped_no_daily_bar": no_candle,
        "asset_readings_skipped_candle_unconfirmed": unconfirmed_candle,
        "asset_readings_skipped_malformed": malformed,
        "cohort_rule": ("an observation joins the sample only if its execution "
                        "window and its daily bar are both explicitly "
                        "confirmed; anything missing or unrecognised is "
                        "excluded and counted"),
        "measures": ("QUOTED IMPACT against the displayed book plus the spread; "
                     "this is not realised execution slippage"),
        "percentiles_reported": [f"p{int(p * 100)}" for p in _PERCENTILES],
        "why_no_p99": ("at 14-30 observations p99 is the single worst reading, "
                       "not an estimate of the tail"),
        "per_asset": {a: {"spread_bps": _pcts(v["spread_bps"]),
                          "quoted_impact_round_trip_bps": _pcts(v["round_trip_bps"])}
                      for a, v in per_asset.items()},
    }


def main() -> None:
    try:
        if "--report" in sys.argv:
            print(json.dumps(report(), indent=2))
            return
        obs = observe(force="--force" in sys.argv)
        flag = "" if obs["in_execution_window"] else "  [OUT OF WINDOW]"
        print(f"recorded {obs['observed_at']}  notional ${obs['notional_usd']:.2f}{flag}")
        for row in obs["assets"]:
            if "error" in row:
                print(f"  {row['asset']:9s} ERROR {row['error']}")
                continue
            print(f"  {row['asset']:9s} spread {row['spread_bps']:6.2f} bps   "
                  f"quoted impact buy {row['buy']['quoted_impact_bps']:6.2f} / "
                  f"sell {row['sell']['quoted_impact_bps']:6.2f}   "
                  f"daily bar published: {row['daily_candle']['available']}")
        tier = obs["fee_tier"]
        print(f"  fee tier: {tier if tier['available'] else tier['reason']}")
    except CostProbeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
