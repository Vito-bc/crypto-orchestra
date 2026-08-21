"""
Phase 7R-2 — prospective execution-cost probe. READ ONLY, NO ORDERS.

The draft protocol asserted a 1.4% round-trip cost. That was an assumption, and
a trial whose acceptance depends on an unmeasured assumption is a trial that can
be falsified by its own plumbing. This probe measures the cost instead, forward,
before any capital is committed.

It never places, cancels or simulates an order. It reads the public order book
and the account's fee tier, and appends one observation per asset per run.

WHAT IT RECORDS, per asset per run
  * best bid / best ask / mid, and the spread in basis points
  * the size-weighted average price a market order of the intended notional
    would sweep to, on both sides, and the resulting slippage vs mid
  * the account's current taker tier, when readable without trading permissions
  * how long after 00:00 UTC the completed daily candle became available

WHY IT MATTERS
  Coinbase taker fees are tier-dependent and change with 30-day volume, so a
  hardcoded 0.6% is a guess about the future. The protocol should therefore say
  `fee = max(0.60%, actual taker tier at signal time)` and carry a measured
  slippage distribution rather than a single number.

Run it daily shortly after 00:00 UTC for 14-30 days:
    python backtesting/stf_cost_probe.py           # append one observation set
    python backtesting/stf_cost_probe.py --report  # summarise what was collected
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OBSERVATIONS = ROOT / "logs" / "stf_cost_probe.jsonl"
UNIVERSE = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# The notional a single STF position would take: 5% of the allocated balance.
# Read from the environment so the probe measures the size actually intended.
def _notional() -> float:
    import os

    balance = float(os.getenv("LIVE_BALANCE_USD", "100"))
    return balance * float(os.getenv("TRADE_SIZE_PCT", "0.05"))


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


def _sweep(levels: list[tuple[float, float]], notional: float) -> dict:
    """
    Size-weighted average price a market order of `notional` would achieve.

    Returns the VWAP and how deep into the book it had to reach. If the book
    cannot fill the order, that is reported rather than extrapolated.
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


def _book(client, product_id: str, depth: int = 50) -> tuple[list, list]:
    # PUBLIC book endpoint. get_product_book is a private endpoint in this
    # SDK and rejects an unauthenticated client, which is the correct
    # behaviour for a probe that must not be able to reach an account.
    resp = client.get_public_product_book(product_id=product_id, limit=depth)
    book = getattr(resp, "pricebook", None) or resp
    bids = [(float(b["price"]), float(b["size"]))
            for b in (getattr(book, "bids", None) or book["bids"])]
    asks = [(float(a["price"]), float(a["size"]))
            for a in (getattr(book, "asks", None) or book["asks"])]
    return bids, asks


def _taker_tier(client) -> dict:
    """
    The account's current taker rate, if it can be read without trade scope.

    Recorded as unavailable rather than assumed: an unmeasured fee is exactly
    what this probe exists to remove.
    """
    try:
        summary = client.get_transaction_summary()
        tier = getattr(summary, "fee_tier", None)
        if tier is None:
            return {"available": False, "reason": "no fee_tier in response"}
        return {
            "available": True,
            "taker_fee_rate": float(getattr(tier, "taker_fee_rate", "nan")),
            "maker_fee_rate": float(getattr(tier, "maker_fee_rate", "nan")),
            "pricing_tier": getattr(tier, "pricing_tier", None),
        }
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}


def observe() -> dict:
    """One reading across the universe. Appends to the observation log."""
    client = _public_client()
    notional = _notional()
    now = datetime.now(timezone.utc)
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0,
                                                microsecond=0)).total_seconds()

    rows = []
    for asset in UNIVERSE:
        try:
            bids, asks = _book(client, asset)
        except Exception as exc:
            rows.append({"asset": asset, "error": f"{type(exc).__name__}: {exc}"})
            continue
        if not bids or not asks:
            rows.append({"asset": asset, "error": "empty book"})
            continue

        best_bid, best_ask = bids[0][0], asks[0][0]
        mid = (best_bid + best_ask) / 2.0
        buy = _sweep(asks, notional)
        sell = _sweep(bids, notional)
        rows.append({
            "asset": asset,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": round(mid, 8),
            "spread_bps": round((best_ask - best_bid) / mid * 10_000, 2),
            "buy": {**buy, "slippage_bps": (
                round((buy["vwap"] - mid) / mid * 10_000, 2)
                if buy["vwap"] else None)},
            "sell": {**sell, "slippage_bps": (
                round((mid - sell["vwap"]) / mid * 10_000, 2)
                if sell["vwap"] else None)},
        })

    observation = {
        "observed_at": now.isoformat(),
        "seconds_after_utc_midnight": round(seconds_since_midnight, 1),
        "notional_usd": notional,
        "fee_tier": _taker_tier(client),
        "assets": rows,
    }
    OBSERVATIONS.parent.mkdir(parents=True, exist_ok=True)
    with OBSERVATIONS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(observation, sort_keys=True) + "\n")
    return observation


def report() -> dict:
    """Percentiles of the measured cost, per asset. No trading decisions."""
    if not OBSERVATIONS.exists():
        raise CostProbeError(f"no observations at {OBSERVATIONS}")

    import statistics

    per_asset: dict = {a: {"spread_bps": [], "round_trip_bps": []} for a in UNIVERSE}
    n = 0
    for line in OBSERVATIONS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obs = json.loads(line)
        n += 1
        for row in obs["assets"]:
            if "error" in row:
                continue
            buy, sell = row["buy"]["slippage_bps"], row["sell"]["slippage_bps"]
            per_asset[row["asset"]]["spread_bps"].append(row["spread_bps"])
            if buy is not None and sell is not None:
                per_asset[row["asset"]]["round_trip_bps"].append(buy + sell)

    def _pcts(values: list[float]) -> dict:
        if not values:
            return {"n": 0}
        s = sorted(values)
        def q(p): return round(s[min(int(p * len(s)), len(s) - 1)], 2)
        return {"n": len(s), "p50": q(0.5), "p90": q(0.9), "p99": q(0.99),
                "mean": round(statistics.fmean(s), 2), "max": round(s[-1], 2)}

    return {
        "observations": n,
        "note": ("slippage is measured against mid for the intended notional; "
                 "exchange fees are additional and tier-dependent"),
        "per_asset": {a: {"spread_bps": _pcts(v["spread_bps"]),
                          "slippage_round_trip_bps": _pcts(v["round_trip_bps"])}
                      for a, v in per_asset.items()},
    }


def main() -> None:
    try:
        if "--report" in sys.argv:
            print(json.dumps(report(), indent=2))
            return
        obs = observe()
        print(f"recorded {obs['observed_at']}  notional ${obs['notional_usd']:.2f}")
        for row in obs["assets"]:
            if "error" in row:
                print(f"  {row['asset']:9s} ERROR {row['error']}")
                continue
            print(f"  {row['asset']:9s} spread {row['spread_bps']:6.2f} bps   "
                  f"buy slip {row['buy']['slippage_bps']:6.2f}   "
                  f"sell slip {row['sell']['slippage_bps']:6.2f}   "
                  f"fillable {row['buy']['complete'] and row['sell']['complete']}")
        tier = obs["fee_tier"]
        print(f"  fee tier: {tier if tier['available'] else tier['reason']}")
    except CostProbeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
