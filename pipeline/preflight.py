"""
Read-only Coinbase preflight.

Checks (in order):
  1. API key permissions — can_view=True required; can_transfer must be False
     (bot has no need for withdrawal rights).
  2. Portfolio UUID — reported; cross-checked against COINBASE_PORTFOLIO_UUID
     in .env when that variable is set.
  3. USD account — available_balance, hold, active/ready status (paginated).
  4. Product rules + trading flags for each asset (get_tradability_status=True).

Returns PreflightResult.
  - entry_allowed()  → True only when overall_status == "OK"
  - exit_allowed()   → always True (preflight failure must never block risk
                        reduction; EXIT is gated by LKG product rules instead)

Secrets: full portfolio UUID is masked to 8 chars in all string representations.
API responses are never logged verbatim.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

_KEY_FILE          = ROOT / "cdp_api_key.json"
_EXPECTED_UUID_ENV = "COINBASE_PORTFOLIO_UUID"    # optional; set to cross-check

_DRY_RUN = os.getenv("DRY_RUN", "true").lower() not in ("false", "0", "no")


# ── Structured result types ───────────────────────────────────────────────────

@dataclass
class KeyPermissions:
    can_view: bool
    can_trade: bool
    can_transfer: bool
    portfolio_uuid: str     # full value — masked in __repr__

    def __repr__(self) -> str:
        masked = self.portfolio_uuid[:8] + "…" if self.portfolio_uuid else "(none)"
        return (
            f"KeyPermissions(can_view={self.can_view}, can_trade={self.can_trade}, "
            f"can_transfer={self.can_transfer}, portfolio_uuid={masked!r})"
        )


@dataclass
class AccountSummary:
    currency: str
    available_balance: Decimal
    hold: Decimal
    active: bool
    ready: bool
    errors: list[str] = field(default_factory=list)


@dataclass
class ProductState:
    product_id: str
    # Numeric rules
    base_increment: str
    base_min_size: str
    base_max_size: str
    quote_increment: str
    quote_min_size: str
    quote_max_size: str
    # Trading flags
    is_disabled: bool
    trading_disabled: bool
    cancel_only: bool
    limit_only: bool
    post_only: bool
    auction_mode: bool
    view_only: bool
    # Derived
    tradeable: bool          # True only when all blocking flags are False
    errors: list[str] = field(default_factory=list)


@dataclass
class PreflightResult:
    timestamp: str
    portfolio_uuid: str      # full — callers must mask if logging
    key_permissions: Optional[KeyPermissions]
    accounts_summary: list[AccountSummary]
    product_states: list[ProductState]
    latency_ms: float
    errors: list[str]
    overall_status: str      # "OK" | "ENTRY_BLOCKED" | "CRITICAL"

    def entry_allowed(self) -> bool:
        return self.overall_status == "OK"

    def exit_allowed(self) -> bool:
        """EXIT is never blocked by preflight; LKG rules gate it separately."""
        return True


# ── Read-only client facade ───────────────────────────────────────────────────

class _ReadOnlyClient:
    """
    Thin wrapper around the coinbase-advanced-py RESTClient that exposes only
    the four read operations required by preflight.  create_order, cancel_order,
    and any transfer/withdrawal methods are deliberately absent.
    """

    def __init__(self, sdk_client) -> None:
        self._c = sdk_client

    def get_api_key_permissions(self):
        return self._c.get_api_key_permissions()

    def get_portfolios(self):
        return self._c.get_portfolios()

    def get_accounts(self, limit: int = 250, cursor: str = "") -> dict:
        kwargs: dict = {"limit": limit}
        if cursor:
            kwargs["cursor"] = cursor
        resp = self._c.get_accounts(**kwargs)
        return resp.to_dict() if hasattr(resp, "to_dict") else resp

    def get_product(self, product_id: str) -> dict:
        resp = self._c.get_product(
            product_id=product_id,
            get_tradability_status=True,
        )
        return resp.to_dict() if hasattr(resp, "to_dict") else resp


def _build_read_only_client() -> _ReadOnlyClient:
    if not _KEY_FILE.exists():
        raise RuntimeError(
            f"Coinbase key file not found: {_KEY_FILE}\n"
            "Download cdp_api_key.json from coinbase.com/settings/api."
        )
    from coinbase.rest import RESTClient
    return _ReadOnlyClient(RESTClient(key_file=str(_KEY_FILE)))


# ── Helper parsers ────────────────────────────────────────────────────────────

def _safe_decimal(value, field_name: str, errors: list[str]) -> Optional[Decimal]:
    if value is None:
        errors.append(f"{field_name} is missing")
        return None
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0:
            errors.append(f"{field_name}={value!r} must be finite and non-negative")
            return None
        return d
    except InvalidOperation:
        errors.append(f"{field_name}={value!r} is not a valid Decimal")
        return None


def _mask_uuid(uuid_str: str) -> str:
    if not uuid_str:
        return "(none)"
    return uuid_str[:8] + "…"


# ── Check functions ───────────────────────────────────────────────────────────

def _check_key_permissions(
    client: _ReadOnlyClient,
    errors: list[str],
) -> Optional[KeyPermissions]:
    try:
        resp = client.get_api_key_permissions()
        data = resp.to_dict() if hasattr(resp, "to_dict") else resp
        can_view     = bool(data.get("can_view", False))
        can_trade    = bool(data.get("can_trade", False))
        can_transfer = bool(data.get("can_transfer", False))
        portfolio_uuid = data.get("portfolio_uuid") or ""

        if not can_view:
            errors.append("API key lacks can_view — all reads will fail")
        if can_transfer:
            errors.append(
                "API key has can_transfer=True — bot should not have withdrawal rights; "
                "revoke transfer permission to reduce blast radius"
            )
        return KeyPermissions(
            can_view=can_view,
            can_trade=can_trade,
            can_transfer=can_transfer,
            portfolio_uuid=portfolio_uuid,
        )
    except Exception as exc:
        errors.append(f"get_api_key_permissions failed: {exc}")
        return None


def _check_portfolio_uuid(
    kp: Optional[KeyPermissions],
    client: _ReadOnlyClient,
    errors: list[str],
) -> str:
    """Return the portfolio UUID from key permissions or portfolios list."""
    uuid_from_key = kp.portfolio_uuid if kp else ""

    expected = os.getenv(_EXPECTED_UUID_ENV, "").strip()
    if expected and uuid_from_key and uuid_from_key != expected:
        errors.append(
            f"portfolio_uuid mismatch: key reports {_mask_uuid(uuid_from_key)!r}, "
            f"COINBASE_PORTFOLIO_UUID env is {_mask_uuid(expected)!r}"
        )

    if not uuid_from_key:
        # Fall back to portfolios list
        try:
            resp = client.get_portfolios()
            data = resp.to_dict() if hasattr(resp, "to_dict") else resp
            portfolios = data.get("portfolios", [])
            if portfolios:
                uuid_from_key = (portfolios[0] or {}).get("uuid", "")
        except Exception as exc:
            errors.append(f"get_portfolios failed: {exc}")

    return uuid_from_key


def _check_accounts(
    client: _ReadOnlyClient,
    errors: list[str],
) -> list[AccountSummary]:
    """Paginate List Accounts and return a summary for USD."""
    summaries: list[AccountSummary] = []
    cursor = ""
    page = 0
    max_pages = 20

    all_accounts: list[dict] = []
    try:
        while page < max_pages:
            page += 1
            data = client.get_accounts(cursor=cursor)
            accounts = data.get("accounts", [])
            all_accounts.extend(accounts)
            has_next = data.get("has_next", False)
            cursor = data.get("cursor", "")
            if not has_next or not cursor:
                break
        if page >= max_pages and data.get("has_next"):
            errors.append(
                f"Account list truncated after {max_pages} pages — pagination incomplete"
            )
    except Exception as exc:
        errors.append(f"get_accounts failed: {exc}")
        return summaries

    for acct in all_accounts:
        currency = (acct.get("currency") or "").upper()
        if currency != "USD":
            continue

        acct_errors: list[str] = []
        ab = acct.get("available_balance") or {}
        hold = acct.get("hold") or {}
        avail = _safe_decimal(
            ab.get("value") if isinstance(ab, dict) else getattr(ab, "value", None),
            "available_balance",
            acct_errors,
        )
        hold_val = _safe_decimal(
            hold.get("value") if isinstance(hold, dict) else getattr(hold, "value", None),
            "hold",
            acct_errors,
        )
        active = bool(acct.get("active", False))
        ready  = bool(acct.get("ready", False))

        if not active:
            acct_errors.append("USD account is not active")
        if not ready:
            acct_errors.append("USD account is not ready")

        summaries.append(AccountSummary(
            currency=currency,
            available_balance=avail or Decimal("0"),
            hold=hold_val or Decimal("0"),
            active=active,
            ready=ready,
            errors=acct_errors,
        ))
        if acct_errors:
            errors.extend(f"USD account: {e}" for e in acct_errors)

    if not summaries:
        errors.append("No USD account found in account list")

    return summaries


def _check_product(
    client: _ReadOnlyClient,
    product_id: str,
    errors: list[str],
) -> Optional[ProductState]:
    """Fetch product rules and trading flags for one product."""
    prod_errors: list[str] = []
    try:
        data = client.get_product(product_id)
    except Exception as exc:
        errors.append(f"get_product({product_id}) failed: {exc}")
        return None

    def _get_str(key: str, default: str = "") -> str:
        return str(data.get(key) or default)

    def _get_bool(key: str) -> bool:
        return bool(data.get(key, False))

    # Validate required numeric fields
    for key in ("base_increment", "base_min_size"):
        raw = data.get(key)
        d = _safe_decimal(raw, f"{product_id}.{key}", prod_errors)
        if d is None or d <= 0:
            prod_errors.append(f"{product_id}.{key} must be positive; got {raw!r}")

    is_disabled      = _get_bool("is_disabled")
    trading_disabled = _get_bool("trading_disabled")
    cancel_only      = _get_bool("cancel_only")
    limit_only       = _get_bool("limit_only")
    post_only        = _get_bool("post_only")
    auction_mode     = _get_bool("auction_mode")
    view_only        = _get_bool("view_only")

    # Any of these means a market SELL order will be rejected
    hard_blocks = {
        "is_disabled":      is_disabled,
        "trading_disabled": trading_disabled,
        "cancel_only":      cancel_only,
        "view_only":        view_only,
    }
    for flag, val in hard_blocks.items():
        if val:
            prod_errors.append(
                f"{product_id}: {flag}=True — market orders will be rejected"
            )

    if limit_only:
        prod_errors.append(
            f"{product_id}: limit_only=True — market SELL blocked; "
            "fallback to aggressive limit IOC requires separate testing"
        )

    tradeable = not any(hard_blocks.values()) and not limit_only and not prod_errors

    state = ProductState(
        product_id=product_id,
        base_increment=_get_str("base_increment"),
        base_min_size=_get_str("base_min_size"),
        base_max_size=_get_str("base_max_size"),
        quote_increment=_get_str("quote_increment"),
        quote_min_size=_get_str("quote_min_size"),
        quote_max_size=_get_str("quote_max_size"),
        is_disabled=is_disabled,
        trading_disabled=trading_disabled,
        cancel_only=cancel_only,
        limit_only=limit_only,
        post_only=post_only,
        auction_mode=auction_mode,
        view_only=view_only,
        tradeable=tradeable,
        errors=prod_errors,
    )
    if prod_errors:
        errors.extend(prod_errors)
    return state


# ── DRY_RUN synthetic result ──────────────────────────────────────────────────

def _dry_run_result(product_ids: list[str]) -> PreflightResult:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()
    fake_products = [
        ProductState(
            product_id=pid,
            base_increment="0.00000001", base_min_size="0.00000001",
            base_max_size="9999", quote_increment="0.01",
            quote_min_size="1", quote_max_size="999999",
            is_disabled=False, trading_disabled=False,
            cancel_only=False, limit_only=False, post_only=False,
            auction_mode=False, view_only=False,
            tradeable=True,
        )
        for pid in product_ids
    ]
    return PreflightResult(
        timestamp=ts,
        portfolio_uuid="DRY-RUN",
        key_permissions=KeyPermissions(
            can_view=True, can_trade=True, can_transfer=False,
            portfolio_uuid="DRY-RUN",
        ),
        accounts_summary=[
            AccountSummary(
                currency="USD",
                available_balance=Decimal("100"),
                hold=Decimal("0"),
                active=True,
                ready=True,
            )
        ],
        product_states=fake_products,
        latency_ms=0.0,
        errors=[],
        overall_status="OK",
    )


# ── Public entry point ────────────────────────────────────────────────────────

def run_preflight(product_ids: list[str]) -> PreflightResult:
    """
    Execute all read-only checks against Coinbase and return a PreflightResult.

    In DRY_RUN mode returns a synthetic safe result without making any API calls.

    overall_status:
      "OK"            — all checks passed; ENTRY is allowed
      "ENTRY_BLOCKED" — non-critical issues (e.g. can_transfer=True); ENTRY blocked
      "CRITICAL"      — key/account/product errors; ENTRY blocked, page oncall
    """
    if _DRY_RUN:
        return _dry_run_result(product_ids)

    from datetime import datetime, timezone
    t0 = time.monotonic()
    ts = datetime.now(timezone.utc).isoformat()
    errors: list[str] = []

    try:
        client = _build_read_only_client()
    except RuntimeError as exc:
        return PreflightResult(
            timestamp=ts,
            portfolio_uuid="",
            key_permissions=None,
            accounts_summary=[],
            product_states=[],
            latency_ms=0.0,
            errors=[str(exc)],
            overall_status="CRITICAL",
        )

    kp            = _check_key_permissions(client, errors)
    portfolio_uuid = _check_portfolio_uuid(kp, client, errors)
    accounts      = _check_accounts(client, errors)
    products      = [
        s for pid in product_ids
        if (s := _check_product(client, pid, errors)) is not None
    ]

    latency_ms = (time.monotonic() - t0) * 1000

    # Classify severity
    critical_keywords = ("failed", "missing", "not active", "not ready", "mismatch")
    entry_blocked_keywords = ("can_transfer=True", "limit_only", "auction_mode")

    if any(k in e for e in errors for k in critical_keywords):
        status = "CRITICAL"
    elif any(k in e for e in errors for k in entry_blocked_keywords):
        status = "ENTRY_BLOCKED"
    elif errors:
        status = "ENTRY_BLOCKED"
    else:
        status = "OK"

    return PreflightResult(
        timestamp=ts,
        portfolio_uuid=portfolio_uuid,
        key_permissions=kp,
        accounts_summary=accounts,
        product_states=products,
        latency_ms=latency_ms,
        errors=errors,
        overall_status=status,
    )
