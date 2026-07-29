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
from decimal import Decimal
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from pipeline.product_parse import (
    ALL_NUMERIC,
    parse_product_payload,
    safe_decimal,
    strict_bool,
    strict_positive_decimal,
)

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
    errors: list[str]        # account/key/portfolio scope — these block everything
    overall_status: str      # "OK" | "ENTRY_BLOCKED" | "CRITICAL"
    # Product-scope problems, keyed by product_id.  These block ENTRY for that
    # asset only — one delisted or halted product must not stop every other
    # asset from trading.
    product_errors: dict[str, list[str]] = field(default_factory=dict)
    blocked_products: dict[str, str] = field(default_factory=dict)

    def entry_allowed(self) -> bool:
        """Account-level verdict: are the global preconditions for ENTRY met?"""
        return self.overall_status == "OK"

    def entry_allowed_for(self, product_id: str) -> tuple[bool, str]:
        """
        Per-asset ENTRY verdict: global checks AND this product's own state.

        Returns (True, "") or (False, reason).  Fails closed for a product that
        was never successfully fetched — an asset with no known state is not an
        asset that is fine.
        """
        if not self.entry_allowed():
            return False, f"preflight {self.overall_status}: " + "; ".join(self.errors[:3])
        if product_id in self.blocked_products:
            return False, self.blocked_products[product_id]
        if not any(p.product_id == product_id for p in self.product_states):
            return False, f"{product_id}: no product state returned by preflight"
        return True, ""

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

# Re-exported from the shared parser so preflight and product_state can never
# drift apart on what a valid Coinbase payload looks like.  The module-level
# names are kept for backward compatibility with existing callers and tests.
_strict_bool             = strict_bool
_strict_positive_decimal = strict_positive_decimal
_safe_decimal            = safe_decimal


def _mask_uuid(uuid_str: str) -> str:
    if not uuid_str:
        return "(none)"
    return uuid_str[:8] + "…"


# ── Check functions ───────────────────────────────────────────────────────────

def _as_dict(value, label: str, errors: list[str]) -> Optional[dict]:
    """
    Fail closed on a malformed global payload.

    A non-dict Get response (None, a list, an SDK object that lost its parse)
    must produce a structured CRITICAL error, never an AttributeError that
    escapes run_preflight and aborts run_all_assets with no status and no alert.
    """
    if isinstance(value, dict):
        return value
    errors.append(
        f"CRITICAL: {label} returned {type(value).__name__}, expected a dict"
    )
    return None


def _check_key_permissions(
    client: _ReadOnlyClient,
    errors: list[str],
) -> Optional[KeyPermissions]:
    try:
        data = client.get_api_key_permissions()
    except Exception as exc:
        errors.append(f"CRITICAL: get_api_key_permissions failed: {exc}")
        return None

    data = _as_dict(data, "get_api_key_permissions", errors)
    if data is None:
        return None

    perm_errors: list[str] = []

    can_view     = _strict_bool(data.get("can_view"),     "can_view",     perm_errors)
    can_trade    = _strict_bool(data.get("can_trade"),    "can_trade",    perm_errors)
    can_transfer = _strict_bool(data.get("can_transfer"), "can_transfer", perm_errors)

    # portfolio_uuid must be a string (Coinbase returns the portfolio's string
    # ID).  A non-string (int, dict) is malformed and would blow up masking/
    # comparison downstream — reject it rather than coerce.
    raw_uuid = data.get("portfolio_uuid")
    if raw_uuid is None:
        portfolio_uuid = ""
    elif isinstance(raw_uuid, str):
        portfolio_uuid = raw_uuid
    else:
        perm_errors.append(
            f"CRITICAL: portfolio_uuid is {type(raw_uuid).__name__}, expected a string"
        )
        portfolio_uuid = ""

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
    Resolve and validate the trading key's portfolio identity.

    The ONLY proof that orders will land in the intended portfolio is the API
    key's own portfolio binding: for API-key connections create_order() derives
    the portfolio from the key itself (no portfolio id is passed — see
    coinbase_client.place_limit_buy / place_market_sell), and Key Permissions
    reports that binding as portfolio_uuid.

    Therefore LIVE readiness requires a non-empty key_permissions.portfolio_uuid
    that EXACTLY equals COINBASE_PORTFOLIO_UUID.  List Portfolios is only a
    diagnostic cross-check: it proves the account CONTAINS a portfolio, never that
    THIS key trades it, so it can never substitute for the key-scope match.  A
    missing key uuid is CRITICAL even when the expected uuid appears in the list.

    Because it is diagnostic, a List Portfolios failure/malformed payload does NOT
    block ENTRY once the key match is confirmed — it is logged, not gated.  Its
    problems are gate-blocking only when no UUID is pinned (the list is then load-
    bearing for multiplicity detection) or when identity already fails for another
    reason.
    """
    uuid_from_key = kp.portfolio_uuid if kp else ""
    expected      = os.getenv(_EXPECTED_UUID_ENV, "").strip()

    # List Portfolios problems go here, NOT straight into `errors`.  Once the key
    # itself proves scope (uuid_from_key == expected), a transient failure of this
    # diagnostic endpoint must not block ENTRY — that would let a List Portfolios
    # blip halt trading for a fully identity-proven key.  These are promoted to
    # gate-blocking errors ONLY where the list is genuinely load-bearing (no
    # pinned UUID → we need it to detect multiplicity) or where we are already
    # blocking for an identity reason and the context is useful.
    portfolio_diag: list[str] = []
    portfolios: list[dict] = []
    data = None
    try:
        raw = client.get_portfolios()
    except Exception as exc:
        portfolio_diag.append(f"CRITICAL: get_portfolios failed: {exc}")
    else:
        # A None/list/other return (no exception) is still a malformed payload.
        data = _as_dict(raw, "get_portfolios", portfolio_diag)
    raw_portfolios = data.get("portfolios") if data else None
    if isinstance(raw_portfolios, list):
        portfolios = raw_portfolios
    elif raw_portfolios is not None:
        portfolio_diag.append(
            f"CRITICAL: get_portfolios 'portfolios' is {type(raw_portfolios).__name__}, "
            "expected a list"
        )

    portfolio_uuids = {
        p["uuid"] for p in portfolios
        if isinstance(p, dict) and isinstance(p.get("uuid"), str) and p["uuid"]
    }

    if not expected:
        # No pinned identity: the portfolios list IS load-bearing (multiplicity),
        # so its problems block.
        errors.extend(portfolio_diag)
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
        return uuid_from_key

    # expected is set — prove the KEY is bound to it.
    if not uuid_from_key:
        # List Portfolios cannot stand in for key scope: the key could enumerate
        # portfolios it does not trade.  Fail closed even if `expected` is visible.
        errors.extend(portfolio_diag)
        seen = " (present in List Portfolios, which does not prove key scope)" \
            if expected in portfolio_uuids else ""
        errors.append(
            "CRITICAL: key permissions report no portfolio_uuid — cannot prove the "
            f"trading key is bound to COINBASE_PORTFOLIO_UUID {_mask_uuid(expected)!r}; "
            f"orders would target the key's own portfolio{seen}"
        )
        return expected
    if uuid_from_key != expected:
        errors.extend(portfolio_diag)
        errors.append(
            f"CRITICAL: portfolio_uuid mismatch — key reports {_mask_uuid(uuid_from_key)!r}, "
            f"COINBASE_PORTFOLIO_UUID env is {_mask_uuid(expected)!r}"
        )
        return uuid_from_key

    # uuid_from_key == expected → key scope CONFIRMED.  List Portfolios is now
    # purely diagnostic: log any problem or cross-check discrepancy, but do NOT
    # block ENTRY on it.  Strip the "CRITICAL:" prefix so external monitoring that
    # greps for CRITICAL does not false-alert on a non-blocking diagnostic — the
    # prefix is preserved only where these are promoted to gate-driving errors.
    for d in portfolio_diag:
        detail = d[len("CRITICAL: "):] if d.startswith("CRITICAL: ") else d
        print(f"[Preflight] WARNING List Portfolios diagnostic (non-blocking): {detail}")
    if not portfolio_diag and portfolio_uuids and expected not in portfolio_uuids:
        print(
            f"[Preflight] note: COINBASE_PORTFOLIO_UUID {_mask_uuid(expected)!r} matches the "
            "key but is not in List Portfolios output (diagnostic only)."
        )
    return expected


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
            if not isinstance(last_data, dict):
                errors.append(
                    f"CRITICAL: get_accounts returned {type(last_data).__name__}, "
                    "expected a dict"
                )
                return summaries
            accounts  = last_data.get("accounts") or []
            has_next  = last_data.get("has_next", False)
            next_cur  = last_data.get("cursor", "")

            if not isinstance(accounts, list):
                errors.append(
                    f"CRITICAL: get_accounts 'accounts' is {type(accounts).__name__}, "
                    "expected a list"
                )
                return summaries

            # Deduplicate by account UUID before appending
            for acct in accounts:
                if not isinstance(acct, dict):
                    errors.append(
                        f"CRITICAL: account entry is {type(acct).__name__}, expected a dict"
                    )
                    continue
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
        # currency must be a string before .upper(); a malformed scalar (int,
        # dict) must not raise an AttributeError that aborts the whole preflight.
        currency_raw = acct.get("currency")
        if currency_raw is not None and not isinstance(currency_raw, str):
            errors.append(
                f"CRITICAL: account.currency is {type(currency_raw).__name__}, expected a string"
            )
            continue
        currency = (currency_raw or "").upper()
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

    # A non-dict payload (list, None, SDK object that lost its parse) must stay
    # contained to THIS product.  Returning None here routes it to the caller's
    # blocked_products for `product_id` alone; falling through to `data.get(...)`
    # below would raise AttributeError and take down the whole preflight run,
    # breaking per-asset isolation.  The parser also flags it, but we cannot
    # build a ProductState from a non-dict, so return closed.
    if not isinstance(data, dict):
        errors.append(
            f"CRITICAL: get_product({product_id}) returned "
            f"{type(data).__name__}, expected a dict"
        )
        return None

    # Shared strict parser — identical rules to product_state.  Numeric fields
    # are all required here; product_state requires only the four it uses.
    # (This supersedes the inline per-field validation that used to live here;
    # the min/max relation and product_id echo checks moved into the parser.)
    parsed = parse_product_payload(data, product_id, required_numeric=ALL_NUMERIC)
    prod_errors.extend(parsed.errors)

    def _str(key: str) -> str:
        return str(data.get(key) or "")

    # Flags fail CLOSED: a flag that could not be parsed comes back from the
    # shared parser as its blocking value (True), never as a permissive False.
    _dis   = parsed.flag("is_disabled")
    _tdis  = parsed.flag("trading_disabled")
    _cnly  = parsed.flag("cancel_only")
    _lonly = parsed.flag("limit_only")
    _ponly = parsed.flag("post_only")
    _auct  = parsed.flag("auction_mode")
    _vonly = parsed.flag("view_only")

    # ── Granular capability flags ─────────────────────────────────────────────
    # entry_supported: can we place a limit BUY?
    #   blocked by: is_disabled, trading_disabled, cancel_only, view_only, auction_mode
    #   limit_only=True is OK for limit entry; post_only=True is OK for maker entry
    entry_supported = not (_dis or _tdis or _cnly or _vonly or _auct)

    # market_exit_supported: can we place a market SELL?
    #   blocked additionally by: limit_only (market orders rejected under
    #   limit_only) and post_only (a market IOC is inherently taker, so a
    #   maker-only product rejects it).  EXIT uses market_market_ioc.
    market_exit_supported = not (_dis or _tdis or _cnly or _vonly or _lonly or _ponly)

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

    # ── Global scope: a failure here blocks ENTRY for every asset ────────────
    kp             = _check_key_permissions(client, errors)
    portfolio_uuid = _check_portfolio_uuid(kp, client, errors)
    accounts       = _check_accounts(client, errors)

    # ── Product scope: a failure here blocks ENTRY for that asset only ───────
    products: list[ProductState] = []
    product_errors: dict[str, list[str]] = {}
    blocked_products: dict[str, str] = {}

    for pid in product_ids:
        pid_errors: list[str] = []
        state = _check_product(client, pid, pid_errors)
        if pid_errors:
            product_errors[pid] = pid_errors

        if state is None:
            # get_product itself failed — no state at all. Fail closed.
            blocked_products[pid] = (
                f"{pid}: product state unavailable — "
                + (pid_errors[0] if pid_errors else "Get Product failed")
            )
            continue

        products.append(state)

        if any(e.upper().startswith("CRITICAL") for e in pid_errors):
            blocked_products[pid] = f"{pid}: malformed or invalid product payload"
        elif not state.entry_supported:
            blocked_products[pid] = f"{pid}: exchange flags block order placement"
        elif not state.market_exit_supported:
            # Placement would succeed, but the market SELL we exit with would be
            # rejected. Never open a position we cannot exit with the order type
            # we actually use. This is why limit_only blocks ENTRY for the asset
            # while leaving entry_supported (a pure placement-capability flag) True.
            blocked_products[pid] = (
                f"{pid}: market EXIT unsupported (limit_only/post_only) — "
                "refusing to open a position that cannot be market-exited"
            )

    latency_ms = (time.monotonic() - t0) * 1000

    # overall_status reflects GLOBAL scope only. Product problems surface through
    # blocked_products / entry_allowed_for() so one bad asset cannot halt the rest.
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
        product_errors=product_errors,
        blocked_products=blocked_products,
    )
