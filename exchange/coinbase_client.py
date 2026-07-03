"""
Coinbase Advanced Trade API client.

Wraps coinbase-advanced-py (RESTClient) and exposes only the four operations
the system needs:
  - place_limit_buy   — maker limit order at support level
  - cancel_order      — cancel a pending limit order
  - check_order_filled — poll whether a limit order filled
  - place_market_sell — taker market sell for stop/target/max-hold exits

DRY_RUN mode (default: true in .env):
  All methods log what they WOULD do and return synthetic IDs.
  Set DRY_RUN=false in .env to go live.

Coinbase API key setup:
  1. Go to coinbase.com/settings/api
  2. Create key with "Trade" + "View" permissions, Ed25519 algorithm
  3. Download the JSON file and place it at project root as cdp_api_key.json
  4. Set DRY_RUN=false in .env when ready to trade live
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

_DRY_RUN  = os.getenv("DRY_RUN", "true").lower() not in ("false", "0", "no")
_KEY_FILE = ROOT / "cdp_api_key.json"


def _get_client():
    """Return a live RESTClient using the CDP JSON key file (ECDSA format)."""
    if not _KEY_FILE.exists():
        raise RuntimeError(
            f"Coinbase key file not found: {_KEY_FILE}\n"
            "Download cdp_api_key.json from coinbase.com/settings/api (ECDSA algorithm) "
            "and place it at the project root."
        )
    from coinbase.rest import RESTClient
    return RESTClient(key_file=str(_KEY_FILE))


def _parse_balance(account) -> tuple[str, float]:
    """Parse account balance from coinbase-advanced-py v1.x response (dict or object)."""
    ab = account.get("available_balance", {}) if isinstance(account, dict) else getattr(account, "available_balance", {})
    if isinstance(ab, dict):
        return ab.get("currency", ""), float(ab.get("value", 0))
    return getattr(ab, "currency", ""), float(getattr(ab, "value", 0))


def is_dry_run() -> bool:
    return _DRY_RUN


# ── Public API ────────────────────────────────────────────────────────────────

def _make_order_id(client_order_id: str | None = None) -> str:
    """
    Return a unique order id.  Uses the full UUID (without hyphens) so there
    are 128 bits of randomness — the 8-char truncation had only 32 bits and a
    birthday collision becomes likely at ~65k orders.
    """
    return client_order_id or uuid.uuid4().hex   # 32 hex chars, no hyphens


def place_limit_buy(
    product_id: str,
    quote_size_usd: float,
    limit_price: float,
    client_order_id: str | None = None,
) -> str:
    """
    Place a limit buy (post-only maker order).
    Returns the exchange order ID, or "DRY-<uuid>" in dry-run mode.

    Args:
        product_id:      e.g. "ETH-USD"
        quote_size_usd:  USD amount to spend, e.g. 200.0
        limit_price:     limit price in USD
        client_order_id: idempotency key (our internal order id)
    """
    oid = _make_order_id(client_order_id)

    if _DRY_RUN:
        print(f"[Coinbase DRY] limit BUY  {product_id}  ${quote_size_usd:.2f} @ ${limit_price:,.2f}  id={oid}")
        return f"DRY-{oid}"

    base_size = str(round(quote_size_usd / limit_price, 8))

    client = _get_client()
    resp = client.create_order(
        client_order_id=oid,
        product_id=product_id,
        side="BUY",
        order_configuration={
            "limit_limit_gtc": {
                "base_size":   base_size,
                "limit_price": str(round(limit_price, 2)),
                "post_only":   True,
            }
        },
    )
    order_id = resp.get("order_id") or resp.get("success_response", {}).get("order_id", "")
    if not order_id:
        raise RuntimeError(
            f"Coinbase returned no order_id for {product_id} BUY — "
            f"order may not have been placed. Raw response keys: {list(resp.keys())}"
        )
    print(f"[Coinbase LIVE] limit BUY placed  {product_id}  {base_size} @ ${limit_price:,.2f}  order_id={order_id}")
    return order_id


def cancel_order(exchange_order_id: str) -> bool:
    """
    Cancel a pending limit order by its exchange order ID.
    Returns True if cancelled, False if already filled/unknown.
    """
    if _DRY_RUN or exchange_order_id.startswith("DRY-"):
        print(f"[Coinbase DRY] cancel order {exchange_order_id}")
        return True

    try:
        client = _get_client()
        resp = client.cancel_orders(order_ids=[exchange_order_id])
        results = resp.get("results", [])
        if results and results[0].get("success"):
            print(f"[Coinbase LIVE] order {exchange_order_id} cancelled")
            return True
        print(f"[Coinbase LIVE] cancel failed for {exchange_order_id}: {results}")
        return False
    except Exception as exc:
        print(f"[Coinbase LIVE] cancel error for {exchange_order_id}: {exc}")
        return False


def check_order_filled(exchange_order_id: str) -> tuple[bool, float | None]:
    """
    Query order status from Coinbase.
    Returns (filled: bool, average_filled_price: float | None).

    In dry-run mode, returns (False, None) — fill simulation is done locally
    by comparing current price to limit_price in limit_orders.py.
    """
    if _DRY_RUN or exchange_order_id.startswith("DRY-"):
        return False, None

    try:
        client = _get_client()
        resp   = client.get_order(order_id=exchange_order_id)
        order  = resp.get("order", resp)
        status = order.get("status", "")
        if status == "FILLED":
            avg_price = float(order.get("average_filled_price") or 0)
            return True, (avg_price or None)
        return False, None
    except Exception as exc:
        print(f"[Coinbase LIVE] check_order error {exchange_order_id}: {exc}")
        return False, None


def place_market_sell(
    product_id: str,
    base_size_coins: float,
    client_order_id: str | None = None,
) -> str:
    """
    Place a market sell (taker) for stop-loss / take-profit / max-hold exits.
    Returns the exchange order ID, or "DRY-<uuid>" in dry-run mode.

    Args:
        product_id:      e.g. "ETH-USD"
        base_size_coins: amount in base currency (e.g. 0.05 ETH)
        client_order_id: idempotency key
    """
    oid = _make_order_id(client_order_id)

    if _DRY_RUN:
        print(f"[Coinbase DRY] market SELL {product_id}  {base_size_coins:.6f} coins  id={oid}")
        return f"DRY-{oid}"

    client = _get_client()
    resp = client.create_order(
        client_order_id=oid,
        product_id=product_id,
        side="SELL",
        order_configuration={
            "market_market_ioc": {
                "base_size": str(round(base_size_coins, 8)),
            }
        },
    )
    order_id = resp.get("order_id") or resp.get("success_response", {}).get("order_id", "")
    if not order_id:
        raise RuntimeError(
            f"Coinbase returned no order_id for {product_id} SELL — "
            f"exit may not have been placed. Raw response keys: {list(resp.keys())}"
        )
    print(f"[Coinbase LIVE] market SELL placed  {product_id}  {base_size_coins:.6f} coins  order_id={order_id}")
    return order_id
