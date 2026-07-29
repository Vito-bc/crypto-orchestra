"""
Single strict parser for the Coinbase Get Product payload.

Both `preflight._check_product` and `product_state._fetch_from_coinbase` consume
this module so that one payload can never be interpreted two different ways.
Before this existed, preflight validated 6 numeric fields with strict Decimal and
7 flags with strict bool, while product_state used `bool(d.get(name, False))` —
which silently turned a *missing* flag into `False` and let ENTRY proceed on a
product whose tradability state had never actually been read.

Validation rules (identical for every caller):
  • Trading flags accept ONLY a native Python bool.  A string, an int, or a
    missing key is an error — never a silent False.
  • On flag parse failure the flag defaults to its BLOCKING value, so a
    malformed response fails closed rather than open.
  • Numeric fields must be finite, positive Decimals.
  • min ≤ max relations are checked when both sides parsed.
  • The response `product_id` must echo the requested one.

Callers differ only in WHICH numeric fields they require, via `required_numeric`.
preflight needs all six; product_state needs the four it actually uses.  The
validation applied to a field that is required is the same in both cases.

Error strings prefixed "CRITICAL:" are classified as critical by preflight.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

# All seven exchange trading flags returned by Get Product with
# get_tradability_status=True.
TRADING_FLAGS: tuple[str, ...] = (
    "is_disabled",
    "trading_disabled",
    "cancel_only",
    "limit_only",
    "post_only",
    "auction_mode",
    "view_only",
)

# The blocking value for each flag — used as the fail-closed default when the
# field is missing or malformed.  Every one of these flags restricts trading
# when True, so True is always the conservative choice.
_BLOCKING_DEFAULT = True

ALL_NUMERIC: tuple[str, ...] = (
    "base_increment",
    "base_min_size",
    "base_max_size",
    "quote_increment",
    "quote_min_size",
    "quote_max_size",
)

# The subset product_state actually uses for order rounding and sizing.
CORE_NUMERIC: tuple[str, ...] = (
    "base_increment",
    "base_min_size",
    "base_max_size",
    "quote_increment",
)


# ── Strict scalar parsers ─────────────────────────────────────────────────────

def strict_bool(value, field_name: str, errors: list[str]) -> Optional[bool]:
    """
    Accept only a native Python bool.

    Coinbase returns JSON booleans which the SDK parses to Python bools.
    A string "false" or integer 0 signals a malformed/unexpected response and
    must not silently pass as False — bool("false") == True and
    d.get(name, False) == False are the two bugs this parser closes.
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


def strict_positive_decimal(
    value, field_name: str, errors: list[str]
) -> Optional[Decimal]:
    """Finite, strictly positive Decimal.  Anything else is an error."""
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


def safe_decimal(value, field_name: str, errors: list[str]) -> Optional[Decimal]:
    """Finite, non-negative Decimal — for balance fields, which may be zero."""
    if value is None:
        errors.append(f"CRITICAL: {field_name} is missing")
        return None
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"CRITICAL: {field_name}={value!r} is not a valid Decimal")
        return None
    if not d.is_finite() or d < 0:
        errors.append(
            f"CRITICAL: {field_name}={value!r} must be finite and non-negative"
        )
        return None
    return d


# ── Parsed payload ────────────────────────────────────────────────────────────

@dataclass
class ParsedProduct:
    """
    Result of validating one Get Product payload.

    `flags` always contains all seven keys.  When a flag failed to parse its
    value is the blocking default (True), so callers that read `flags` without
    checking `errors` still fail closed.
    """
    product_id: str
    raw: dict[str, str]                 # requested numeric fields, as strings
    decimals: dict[str, Decimal]        # the same fields, parsed
    flags: dict[str, bool]
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def flag(self, name: str) -> bool:
        return self.flags[name]


def parse_product_payload(
    data: dict,
    product_id: str,
    *,
    required_numeric: tuple[str, ...] = ALL_NUMERIC,
) -> ParsedProduct:
    """
    Validate a Get Product response.  Never raises — all problems land in
    `.errors`, and `.flags` fails closed.

    required_numeric: which numeric fields must be present and positive.
      Fields outside this tuple are still parsed when present but their absence
      is not an error.
    """
    errors: list[str] = []

    if not isinstance(data, dict):
        errors.append(
            f"CRITICAL: {product_id} Get Product returned "
            f"{type(data).__name__!r}, expected a dict"
        )
        return ParsedProduct(
            product_id=product_id,
            raw={},
            decimals={},
            flags={name: _BLOCKING_DEFAULT for name in TRADING_FLAGS},
            errors=errors,
        )

    # ── product_id echo check ────────────────────────────────────────────────
    # A response with no product_id is not "no mismatch, therefore fine" — we
    # cannot confirm the exchange answered about the product we asked for, so an
    # absent echo is as much a red flag as a wrong one.  Both fail closed.
    resp_pid = data.get("product_id", "")
    if not resp_pid:
        errors.append(
            f"CRITICAL: {product_id} response has no product_id field — "
            "cannot confirm the exchange answered about the requested product"
        )
    elif resp_pid != product_id:
        errors.append(
            f"CRITICAL: product_id mismatch — requested {product_id!r}, "
            f"response contains {resp_pid!r}"
        )

    # ── Numeric fields ───────────────────────────────────────────────────────
    raw: dict[str, str] = {}
    decimals: dict[str, Decimal] = {}
    for name in ALL_NUMERIC:
        value = data.get(name)
        if name in required_numeric:
            parsed = strict_positive_decimal(value, f"{product_id}.{name}", errors)
            if parsed is not None:
                decimals[name] = parsed
                raw[name] = str(value)
        elif value is not None:
            # Present but not required — validate into a throwaway list so a
            # malformed optional field cannot block, but still record the value.
            _ignored: list[str] = []
            parsed = strict_positive_decimal(value, f"{product_id}.{name}", _ignored)
            if parsed is not None:
                decimals[name] = parsed
                raw[name] = str(value)

    # ── min ≤ max relations ──────────────────────────────────────────────────
    for lo, hi in (("base_min_size", "base_max_size"),
                   ("quote_min_size", "quote_max_size")):
        d_lo, d_hi = decimals.get(lo), decimals.get(hi)
        if d_lo is not None and d_hi is not None and d_lo > d_hi:
            errors.append(f"CRITICAL: {product_id} {lo} {d_lo} > {hi} {d_hi}")

    # ── Trading flags — strict, fail closed ──────────────────────────────────
    flags: dict[str, bool] = {}
    for name in TRADING_FLAGS:
        parsed_flag = strict_bool(data.get(name), name, errors)
        # A failed parse becomes the blocking value, never a permissive False.
        flags[name] = _BLOCKING_DEFAULT if parsed_flag is None else parsed_flag

    return ParsedProduct(
        product_id=product_id,
        raw=raw,
        decimals=decimals,
        flags=flags,
        errors=errors,
    )
