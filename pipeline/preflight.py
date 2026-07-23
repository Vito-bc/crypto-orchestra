"""
Read-only Coinbase preflight.

live_reads=True executes real Coinbase GET calls (view permission only) even
when DRY_RUN=true.  Create / Cancel / Transfer remain blocked at all times —
the _ReadOnlyClient facade physically does not expose those methods.

Checks (in order):
  1. API key permissions  — can_view required; can_trade required for LIVE
     readiness; can_transfer must be False.
  2. Portfolio UUID       — COINBASE_PORTFOLIO_UUID env var is mandatory;
     UUID from key must match; multiple portfolios without a pinned UUID → CRITICAL.
  3. USD account          — fully paginated with cursor-cycle detection and UUID
     dedup; has_next=True + empty cursor → CRITICAL.
  4. Product state        — all 7 flags via strict bool parsing; all 6 numeric
     fields via strict Decimal; min≤max relations; product_id echo check.

Error classification (no keyword matching — explicit prefixes):
  Errors starting with "CRITICAL:" → overall_status = "CRITICAL"
  Other errors                     → overall_status = "ENTRY_BLOCKED"
  No errors                        → overall_status = "OK"

exit_supervision_allowed() is always True — preflight must never block risk
reduction.  exit_allowed() is a deprecated alias kept for backward compatibility.
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
_EXPECTED_UUID_ENV = "COINBASE_PORTFOLIO_UUID"

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
    # Trading flags (raw)
    is_disabled: bool
    trading_disabled: bool
    cancel_only: bool
    limit_only: bool
    post_only: bool
    auction_mode: bool
    view_only: bool
    # Granular capability flags
    entry_supported: bool       # limit BUY can be placed
    market_exit_supported: bool # market SELL can be placed
    cancel_supported: bool      # open orders can be cancelled
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

    def exit_supervision_allowed(self) -> bool:
        """
        Preflight failure must never block risk reduction.
        Returns True unconditionally — EXIT eligibility is governed by LKG
        product rules in product_state.py, not by preflight status.
        """
        return True

    def exit_allowed(self) -> bool:
        """Deprecated alias for exit_supervision_allowed()."""
        return self.exit_supervision_allowed()


# ── Read-only client facade ───────────────────────────────────────────────────

class _ReadOnlyClient:
    """
    Wraps coinbase-advanced-py RESTClient, exposing only GET operations.
    create_order, cancel_order, and all transfer/withdrawal methods are
    deliberately absent — the facade cannot place or cancel orders.
    """

    def __init__(self, sdk_client) -> None:
        self._c = sdk_client

    def get_api_key_permissions(self) -> dict:
        resp = self._c.get_api_key_permissions()
        return resp.to_dict() if hasattr(resp, "to_dict") else resp

    def get_portfolios(self) -> dict:
        resp = self._c.get_portfolios()
        return resp.to_dict() if hasattr(resp, "to_dict") else resp

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


# ── Strict parsers ────────────────────────────────────────────────────────────

def _strict_bool(value, field_name: str, errors: list[str]) -> Optional[bool]:
    """
    Accept only a native Python bool.

    Coinbase returns JSON booleans which the SDK parses to Python bools.
    A string "false" or integer 0 signals a malformed/unexpected response
    and must not silently pass as False — bool("false") == True is the bug
    this parser closes.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        errors.append(f"CRITICAL: {field_name} is missing in API response")
    else:
        errors.append(
            f"CRITICAL: {field_name}={value!r} is not a boolean "
            f"(got {type(value).__name__!r}) — malformed API response"
        )
    return None


def _strict_positive_decimal(
    value, field_name: str, errors: list[str]
) -> Optional[Decimal]:
    if value is None:
        errors.append(f"CRITICAL: {field_name} is missing")
        return None
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"CRITICAL: {field_name}={value!r} is not a valid Decimal")
        return None
    if not d.is_finite():
        errors.append(f"CRITICAL: {field_name}={value!r} is NaN or Infinity")
        return None
    if d <= 0:
        errors.append(f"CRITICAL: {field_name}={value!r} must be positive")
        return None
    return d


def _safe_decimal(value, field_name: str, errors: list[str]) -> Optional[Decimal]:
    """Non-negative Decimal — for balance fields (may be zero)."""
    if value is None:
        errors.append(f"CRITICAL: {field_name} is missing")
        return None
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"CRITICAL: {field_name}={value!r} is not a valid Decimal")
        return None
    if not d.is_finite() or d < 0:
        errors.append(f"CRITICAL: {field_name}={value!r} must be finite and non-negative")
        return None
    return d


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
        data = client.get_api_key_permissions()
    except Exception as exc:
        errors.append(f"CRITICAL: get_api_key_permissions failed: {exc}")
        return None

    perm_errors: list[str] = []

    can_view     = _strict_bool(data.get("can_view"),     "can_view",     perm_errors)
    can_trade    = _strict_bool(data.get("can_trade"),    "can_trade",    perm_errors)
    can_transfer = _strict_bool(data.get("can_transfer"), "can_transfer", perm_errors)
    portfolio_uuid = data.get("portfolio_uuid") or ""

    if perm_errors:
        errors.extend(perm_errors)
        return None

    if not can_view:
        errors.append("CRITICAL: API key lacks can_view — all reads will fail")
    if not can_trade:
        errors.append(
            "can_trade=False — API key cannot place orders; "
            "LIVE trading requires can_trade=True"
        )
    if can_transfer:
        errors.append(
            "can_transfer=True — bot must not have withdrawal rights; "
            "revoke transfer permission before going LIVE"
        )

    return KeyPermissions(
        can_view=bool(can_view),
        can_trade=bool(can_trade),
        can_transfer=bool(can_transfer),
        portfolio_uuid=portfolio_uuid,
    )


def _check_portfolio_uuid(
    kp: Optional[KeyPermissions],
    client: _ReadOnlyClient,
    errors: list[str],
) -> str:
    """
    Resolve and validate the portfolio UUID.

    COINBASE_PORTFOLIO_UUID must be set in .env — unconfirmed identity blocks
    ENTRY.  If the key returns a UUID it must match exactly.  Multiple portfolios
    without a pinned UUID is CRITICAL (cannot determine which one to trade).
    """
    uuid_from_key = kp.portfolio_uuid if kp else ""
    expected      = os.getenv(_EXPECTED_UUID_ENV, "").strip()

    # Fetch portfolios list for cross-check and fallback
    portfolios: list[dict] = []
    try:
        data       = client.get_portfolios()
        portfolios = data.get("portfolios") or []
    except Exception as exc:
        errors.append(f"CRITICAL: get_portfolios failed: {exc}")

    if not uuid_from_key and portfolios:
        uuid_from_key = (portfolios[0] or {}).get("uuid", "")

    if not expected:
        if len(portfolios) > 1:
            errors.append(
                "CRITICAL: COINBASE_PORTFOLIO_UUID not set and key has multiple portfolios — "
                "cannot determine which to trade; set the env var before LIVE"
            )
        else:
            errors.append(
                "COINBASE_PORTFOLIO_UUID not set — portfolio identity unconfirmed; "
                "set this env var before LIVE"
            )
    elif uuid_from_key and uuid_from_key != expected:
        errors.append(
            f"CRITICAL: portfolio_uuid mismatch — key reports {_mask_uuid(uuid_from_key)!r}, "
            f"COINBASE_PORTFOLIO_UUID env is {_mask_uuid(expected)!r}"
        )

    return uuid_from_key


def _check_accounts(
    client: _ReadOnlyClient,
    errors: list[str],
) -> list[AccountSummary]:
    """Paginate List Accounts and return a summary for each USD account."""
    summaries: list[AccountSummary] = []
    cursor            = ""
    page              = 0
    max_pages         = 20
    seen_cursors: set[str]       = set()
    seen_uuids:   set[str]       = set()
    all_accounts: list[dict]     = []
    last_data:    dict           = {}

    try:
        while page < max_pages:
            page     += 1
            last_data = client.get_accounts(cursor=cursor)
            accounts  = last_data.get("accounts") or []
            has_next  = last_data.get("has_next", False)
            next_cur  = last_data.get("cursor", "")

            # Deduplicate by account UUID before appending
            for acct in accounts:
                uid = acct.get("uuid") or acct.get("id") or ""
                if uid and uid in seen_uuids:
                    continue
                if uid:
                    seen_uuids.add(uid)
                all_accounts.append(acct)

            if has_next and not next_cur:
                errors.append(
                    "CRITICAL: Coinbase returned has_next=True with empty cursor — "
                    "account list is incomplete"
                )
                break

            if next_cur and next_cur in seen_cursors:
                errors.append(
                    "CRITICAL: pagination cursor cycle detected — account list is incomplete"
                )
                break

            if next_cur:
                seen_cursors.add(next_cur)

            cursor = next_cur
            if not has_next:
                break
        else:
            # Exited via max_pages
            if last_data.get("has_next"):
                errors.append(
                    f"CRITICAL: account list truncated after {max_pages} pages — "
                    "pagination incomplete"
                )
    except Exception as exc:
        errors.append(f"CRITICAL: get_accounts failed: {exc}")
        return summaries

    for acct in all_accounts:
        currency = (acct.get("currency") or "").upper()
        if currency != "USD":
            continue

        acct_errors: list[str] = []
        ab   = acct.get("available_balance") or {}
        hold = acct.get("hold") or {}

        avail_raw = ab.get("value") if isinstance(ab, dict) else getattr(ab, "value", None)
        hold_raw  = hold.get("value") if isinstance(hold, dict) else getattr(hold, "value", None)

        avail    = _safe_decimal(avail_raw, "available_balance", acct_errors)
        hold_val = _safe_decimal(hold_raw,  "hold",              acct_errors)

        active_raw = acct.get("active")
        ready_raw  = acct.get("ready")
        active = _strict_bool(active_raw, "account.active", acct_errors)
        ready  = _strict_bool(ready_raw,  "account.ready",  acct_errors)

        if active is False:
            acct_errors.append("CRITICAL: USD account is not active")
        if ready is False:
            acct_errors.append("CRITICAL: USD account is not ready")

        summaries.append(AccountSummary(
            currency=currency,
            available_balance=avail    or Decimal("0"),
            hold=hold_val              or Decimal("0"),
            active=bool(active),
            ready=bool(ready),
            errors=acct_errors,
        ))
        errors.extend(acct_errors)

    if not summaries:
        errors.append("CRITICAL: No USD account found in account list")

    return summaries


def _check_product(
    client: _ReadOnlyClient,
    product_id: str,
    errors: list[str],
) -> Optional[ProductState]:
    """Fetch and strictly validate product rules and trading flags."""
    prod_errors: list[str] = []

    try:
        data = client.get_product(product_id)
    except Exception as exc:
        errors.append(f"CRITICAL: get_product({product_id}) failed: {exc}")
        return None

    # product_id echo check
    resp_pid = data.get("product_id", "")
    if resp_pid and resp_pid != product_id:
        prod_errors.append(
            f"CRITICAL: product_id mismatch — requested {product_id!r}, "
            f"response contains {resp_pid!r}"
        )

    # ── Numeric rules (all required) ─────────────────────────────────────────
    b_inc  = _strict_positive_decimal(data.get("base_increment"),  f"{product_id}.base_increment",  prod_errors)
    b_min  = _strict_positive_decimal(data.get("base_min_size"),   f"{product_id}.base_min_size",   prod_errors)
    b_max  = _strict_positive_decimal(data.get("base_max_size"),   f"{product_id}.base_max_size",   prod_errors)
    q_inc  = _strict_positive_decimal(data.get("quote_increment"),  f"{product_id}.quote_increment", prod_errors)
    q_min  = _strict_positive_decimal(data.get("quote_min_size"),  f"{product_id}.quote_min_size",  prod_errors)
    q_max  = _strict_positive_decimal(data.get("quote_max_size"),  f"{product_id}.quote_max_size",  prod_errors)

    if b_min and b_max and b_min > b_max:
        prod_errors.append(
            f"CRITICAL: {product_id} base_min_size {b_min} > base_max_size {b_max}"
        )
    if q_min and q_max and q_min > q_max:
        prod_errors.append(
            f"CRITICAL: {product_id} quote_min_size {q_min} > quote_max_size {q_max}"
        )

    def _str(key: str) -> str:
        return str(data.get(key) or "")

    # ── Trading flags (strict bool — missing/string → CRITICAL) ─────────────
    flag_errors: list[str] = []
    is_disabled      = _strict_bool(data.get("is_disabled"),      "is_disabled",      flag_errors)
    trading_disabled = _strict_bool(data.get("trading_disabled"), "trading_disabled", flag_errors)
    cancel_only      = _strict_bool(data.get("cancel_only"),      "cancel_only",      flag_errors)
    limit_only       = _strict_bool(data.get("limit_only"),       "limit_only",       flag_errors)
    post_only        = _strict_bool(data.get("post_only"),        "post_only",        flag_errors)
    auction_mode     = _strict_bool(data.get("auction_mode"),     "auction_mode",     flag_errors)
    view_only        = _strict_bool(data.get("view_only"),        "view_only",        flag_errors)

    prod_errors.extend(f"{product_id}: {e}" for e in flag_errors)

    # Default to the safe (blocking) value when parsing failed
    _dis   = bool(is_disabled)
    _tdis  = bool(trading_disabled)
    _cnly  = bool(cancel_only)
    _lonly = bool(limit_only)
    _ponly = bool(post_only)
    _auct  = bool(auction_mode)
    _vonly = bool(view_only)

    # ── Granular capability flags ─────────────────────────────────────────────
    # entry_supported: can we place a limit BUY?
    #   blocked by: is_disabled, trading_disabled, cancel_only, view_only, auction_mode
    #   limit_only=True is OK for limit entry; post_only=True is OK for maker entry
    entry_supported = not (_dis or _tdis or _cnly or _vonly or _auct)

    # market_exit_supported: can we place a market SELL?
    #   blocked additionally by: limit_only (market orders rejected under limit_only)
    market_exit_supported = not (_dis or _tdis or _cnly or _vonly or _lonly)

    # cancel_supported: can we cancel open orders?
    #   cancel_only=True actually ENABLES cancels; does not block cancel_supported
    cancel_supported = not (_dis or _tdis or _vonly)

    # Annotate operational impact for flags that affect current trading
    if _dis:
        prod_errors.append(f"{product_id}: is_disabled=True — all order types rejected")
    if _tdis:
        prod_errors.append(f"{product_id}: trading_disabled=True — all orders rejected")
    if _cnly:
        prod_errors.append(f"{product_id}: cancel_only=True — new orders rejected; only cancels work")
    if _vonly:
        prod_errors.append(f"{product_id}: view_only=True — read-only; no order operations")
    if _lonly:
        prod_errors.append(
            f"{product_id}: limit_only=True — market SELL rejected; "
            "limit IOC fallback requires separate testing before use"
        )
    if _auct:
        prod_errors.append(f"{product_id}: auction_mode=True — limit BUY blocked during auction")

    state = ProductState(
        product_id=product_id,
        base_increment=_str("base_increment"),
        base_min_size=_str("base_min_size"),
        base_max_size=_str("base_max_size"),
        quote_increment=_str("quote_increment"),
        quote_min_size=_str("quote_min_size"),
        quote_max_size=_str("quote_max_size"),
        is_disabled=_dis,
        trading_disabled=_tdis,
        cancel_only=_cnly,
        limit_only=_lonly,
        post_only=_ponly,
        auction_mode=_auct,
        view_only=_vonly,
        entry_supported=entry_supported,
        market_exit_supported=market_exit_supported,
        cancel_supported=cancel_supported,
        errors=prod_errors,
    )
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
            entry_supported=True, market_exit_supported=True, cancel_supported=True,
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

def run_preflight(
    product_ids: list[str],
    *,
    live_reads: bool = False,
) -> PreflightResult:
    """
    Execute read-only Coinbase preflight checks and return a PreflightResult.

    live_reads=True  — perform real Coinbase GET calls even when DRY_RUN=true.
                       Requires a valid cdp_api_key.json with can_view=True.
                       Order placement (DRY_RUN) is unaffected.
    live_reads=False — in DRY_RUN mode returns synthetic safe result (default).
                       In LIVE mode always performs real checks (live_reads is
                       implicitly True when DRY_RUN=false).

    overall_status:
      "OK"            — all checks passed; ENTRY is allowed
      "ENTRY_BLOCKED" — non-critical issues (can_trade missing, UUID unset, …)
      "CRITICAL"      — key/account/product errors; alert oncall
    """
    if _DRY_RUN and not live_reads:
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
            errors=[f"CRITICAL: {exc}"],
            overall_status="CRITICAL",
        )

    kp             = _check_key_permissions(client, errors)
    portfolio_uuid = _check_portfolio_uuid(kp, client, errors)
    accounts       = _check_accounts(client, errors)
    products       = [
        s for pid in product_ids
        if (s := _check_product(client, pid, errors)) is not None
    ]

    latency_ms = (time.monotonic() - t0) * 1000

    has_critical = any(e.upper().startswith("CRITICAL") for e in errors)
    status = "CRITICAL" if has_critical else "ENTRY_BLOCKED" if errors else "OK"

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
