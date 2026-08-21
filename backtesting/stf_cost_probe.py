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

The taker FEE is tier-dependent and lives behind a private endpoint. An
unauthenticated client cannot read it, by design — this probe must remain
incapable of reaching an account. It is recorded as unavailable unless a
view-only credential is deliberately supplied.

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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OBSERVATIONS = ROOT / "logs" / "stf_cost_probe.jsonl"
UNIVERSE = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# The protocol executes on the first hourly bar after the daily close, so the
# cost that matters is the cost in that window. Sampling at an arbitrary hour
# would measure a different market.
EXECUTION_WINDOW_MINUTES = 90

# Percentiles worth quoting at this sample size. p99 from 14-30 observations is
# an order statistic of the single worst reading, so it is not reported.
_PERCENTILES = (0.5, 0.9)


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


def trial_notional() -> float:
    """
    The notional one STF position would take, from the single sizing source.

    Read through pipeline.sizing rather than os.getenv so the probe cannot drift
    from the contract the rest of the system enforces — the per-order default
    moved from 5% to 2% and a local copy would have kept measuring the old size.
    """
    from pipeline.sizing import live_balance_usd, trade_size_pct

    return live_balance_usd() * trade_size_pct()


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
        if price > 0 and size > 0 and price == price and size == size:
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


def _taker_tier(client) -> dict:
    """
    The account taker rate, if a view-only credential was supplied.

    get_transaction_summary is a private endpoint. An unauthenticated client
    cannot reach it and must not be able to: recorded as unavailable rather
    than assumed, because an unmeasured fee is what this probe exists to remove.
    """
    try:
        summary = client.get_transaction_summary()
    except Exception as exc:
        return {"available": False,
                "reason": f"{type(exc).__name__}: private endpoint, no credential"}
    tier = getattr(summary, "fee_tier", None)
    if tier is None:
        return {"available": False, "reason": "no fee_tier in response"}
    return {
        "available": True,
        "taker_fee_rate": float(getattr(tier, "taker_fee_rate", "nan")),
        "maker_fee_rate": float(getattr(tier, "maker_fee_rate", "nan")),
        "pricing_tier": getattr(tier, "pricing_tier", None),
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
        "fee_tier": _taker_tier(client),
        "assets": rows,
    }
    OBSERVATIONS.parent.mkdir(parents=True, exist_ok=True)
    with OBSERVATIONS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(observation, sort_keys=True) + "\n")
    return observation


def report() -> dict:
    """Percentiles of the measured quoted impact. No trading decisions."""
    if not OBSERVATIONS.exists():
        raise CostProbeError(f"no observations at {OBSERVATIONS}")

    import statistics

    per_asset: dict = {a: {"spread_bps": [], "round_trip_bps": []} for a in UNIVERSE}
    used = skipped = 0
    for line in OBSERVATIONS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obs = json.loads(line)
        if not obs.get("in_execution_window", True):
            skipped += 1          # out-of-window samples describe another market
            continue
        used += 1
        for row in obs["assets"]:
            if "error" in row:
                continue
            buy = row["buy"].get("quoted_impact_bps")
            sell = row["sell"].get("quoted_impact_bps")
            per_asset[row["asset"]]["spread_bps"].append(row["spread_bps"])
            if buy is not None and sell is not None:
                per_asset[row["asset"]]["round_trip_bps"].append(buy + sell)

    def _pcts(values: list[float]) -> dict:
        if not values:
            return {"n": 0}
        s = sorted(values)
        out = {"n": len(s), "mean": round(statistics.fmean(s), 2),
               "max": round(s[-1], 2)}
        for p in _PERCENTILES:
            out[f"p{int(p * 100)}"] = round(s[min(int(p * len(s)), len(s) - 1)], 2)
        return out

    return {
        "observations_used": used,
        "observations_skipped_out_of_window": skipped,
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
