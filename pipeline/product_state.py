"""
ProductRules and ProductState — in-process cache with TTL and durable LKG.

ProductRules  — numeric exchange constraints (base_increment, min/max sizes).
  TTL 4 h: product listings change only on listing/delisting events.
  LKG: persisted to data/product_lkg.json; survives scheduler restarts.

ProductState  — live trading flags (is_disabled, cancel_only, limit_only, …).
  TTL 5 min: flags can change during market halts or regional restrictions.
  LKG: stored alongside rules; used for EXIT numeric rules, never for flag checks.

Entry policy  (fail-closed):
  • Stale or missing state                   → block.
  • Any hard-block flag                       → block + Telegram.
    Hard-block: is_disabled, trading_disabled, cancel_only, view_only.
  • limit_only=True                           → block (market SELL would reject).

Exit policy   (fail-open — risk reduction must never be blocked by metadata):
  • State fetch failure or stale              → use LKG numeric rules for rounding.
  • LKG also missing                          → fall back to 8-dp / tiny-min defaults.
  • Trading flags are NEVER checked for EXIT.

Call order:
  1. prewarm(product_ids)   — at startup, after reconciliation.
  2. is_entry_allowed(pid)  — before placing any ENTRY order.
  3. get_rules_for_exit(pid)— inside exit_executor to round sell qty.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

ROOT    = Path(__file__).resolve().parents[1]
LKG_PATH = ROOT / "data" / "product_lkg.json"

_RULES_TTL_S = 4 * 3600   # 4 hours — listings rarely change
_STATE_TTL_S = 5 * 60     # 5 minutes — flags can change during halts

# Dry-run defaults: 8 dp, vanishingly small min so no order is ever DUST in sim.
_DEFAULTS: dict = {
    "base_increment": "0.00000001",
    "base_min_size":  "0.00000001",
}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ProductRules:
    product_id: str
    base_increment: str
    base_min_size: str
    base_max_size: str
    quote_increment: str
    fetched_wall: float    # time.time() when fetched — for LKG age reporting

    def as_exit_dict(self) -> dict:
        return {
            "base_increment": self.base_increment,
            "base_min_size":  self.base_min_size,
        }


@dataclass
class ProductState:
    product_id: str
    is_disabled: bool
    trading_disabled: bool
    cancel_only: bool
    limit_only: bool
    post_only: bool
    auction_mode: bool
    view_only: bool
    fetched_wall: float    # time.time()
    fetched_mono: float    # time.monotonic() — used for TTL checks

    @property
    def hard_blocked(self) -> bool:
        return (
            self.is_disabled or self.trading_disabled
            or self.cancel_only or self.view_only
        )

    @property
    def entry_allowed(self) -> bool:
        return not self.hard_blocked and not self.limit_only

    def blocking_flags(self) -> list[str]:
        flags = []
        for name in ("is_disabled", "trading_disabled", "cancel_only", "view_only", "limit_only"):
            if getattr(self, name):
                flags.append(name)
        return flags


# ── In-process cache ──────────────────────────────────────────────────────────

_rules_cache: dict[str, ProductRules] = {}
_state_cache: dict[str, ProductState] = {}


def _rules_fresh(pid: str) -> bool:
    r = _rules_cache.get(pid)
    if r is None:
        return False
    return (time.monotonic() - r.fetched_mono) < _RULES_TTL_S if hasattr(r, "fetched_mono") else False


def _state_fresh(pid: str) -> bool:
    s = _state_cache.get(pid)
    if s is None:
        return False
    return (time.monotonic() - s.fetched_mono) < _STATE_TTL_S


# ── LKG persistence ───────────────────────────────────────────────────────────

def _load_lkg() -> dict:
    if LKG_PATH.exists():
        try:
            with open(LKG_PATH, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}


def _save_lkg(rules: ProductRules, state: ProductState) -> None:
    LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = _load_lkg()
    data[rules.product_id] = {
        "rules": {
            "product_id":     rules.product_id,
            "base_increment": rules.base_increment,
            "base_min_size":  rules.base_min_size,
            "base_max_size":  rules.base_max_size,
            "quote_increment": rules.quote_increment,
            "fetched_wall":   rules.fetched_wall,
        },
        "state": {
            "product_id":       state.product_id,
            "is_disabled":      state.is_disabled,
            "trading_disabled": state.trading_disabled,
            "cancel_only":      state.cancel_only,
            "limit_only":       state.limit_only,
            "post_only":        state.post_only,
            "auction_mode":     state.auction_mode,
            "view_only":        state.view_only,
            "fetched_wall":     state.fetched_wall,
        },
    }
    with open(LKG_PATH, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def _rules_from_lkg(pid: str) -> Optional[ProductRules]:
    data = _load_lkg()
    entry = data.get(pid)
    if not entry or "rules" not in entry:
        return None
    r = entry["rules"]
    try:
        return ProductRules(
            product_id=r["product_id"],
            base_increment=r["base_increment"],
            base_min_size=r["base_min_size"],
            base_max_size=r.get("base_max_size", ""),
            quote_increment=r.get("quote_increment", "0.01"),
            fetched_wall=r.get("fetched_wall", 0.0),
        )
    except (KeyError, TypeError):
        return None


# ── Coinbase fetch ────────────────────────────────────────────────────────────

def _fetch_from_coinbase(product_id: str) -> tuple[ProductRules, ProductState]:
    """
    One API call: Get Product with get_tradability_status=True.
    Returns (ProductRules, ProductState) or raises on failure.
    """
    from exchange.coinbase_client import _get_client  # type: ignore[attr-defined]

    client = _get_client()
    resp = client.get_product(
        product_id=product_id,
        get_tradability_status=True,
    )
    d = resp.to_dict() if hasattr(resp, "to_dict") else resp

    def _req(key: str) -> str:
        val = d.get(key)
        if not val:
            raise RuntimeError(
                f"Get Product {product_id}: missing required field {key!r}"
            )
        return str(val)

    now_wall = time.time()
    now_mono = time.monotonic()

    rules = ProductRules(
        product_id=product_id,
        base_increment=_req("base_increment"),
        base_min_size=_req("base_min_size"),
        base_max_size=str(d.get("base_max_size") or ""),
        quote_increment=str(d.get("quote_increment") or "0.01"),
        fetched_wall=now_wall,
    )
    # Attach monotonic for TTL — not serialised to LKG
    rules.fetched_mono = now_mono  # type: ignore[attr-defined]

    state = ProductState(
        product_id=product_id,
        is_disabled=bool(d.get("is_disabled", False)),
        trading_disabled=bool(d.get("trading_disabled", False)),
        cancel_only=bool(d.get("cancel_only", False)),
        limit_only=bool(d.get("limit_only", False)),
        post_only=bool(d.get("post_only", False)),
        auction_mode=bool(d.get("auction_mode", False)),
        view_only=bool(d.get("view_only", False)),
        fetched_wall=now_wall,
        fetched_mono=now_mono,
    )
    return rules, state


# ── Public API ────────────────────────────────────────────────────────────────

def prewarm(product_ids: list[str]) -> dict[str, bool]:
    """
    Fetch fresh ProductRules + ProductState for each product_id.

    Ignores DRY_RUN — the only context where you'd want real data at startup.
    In unit tests, patch _fetch_from_coinbase or call _inject_cache directly.

    Returns {product_id: True} for each that succeeded, False on failure.
    """
    results: dict[str, bool] = {}
    for pid in product_ids:
        try:
            rules, state = _fetch_from_coinbase(pid)
            _rules_cache[pid] = rules
            _state_cache[pid] = state
            _save_lkg(rules, state)
            results[pid] = True
            print(f"[ProductState] prewarm OK  {pid}  "
                  f"tradeable={state.entry_allowed}  "
                  f"inc={rules.base_increment}  min={rules.base_min_size}")
        except Exception as exc:
            results[pid] = False
            print(f"[ProductState] prewarm FAILED  {pid}: {exc}")
    return results


def get_rules(product_id: str) -> Optional[ProductRules]:
    """
    Return cached ProductRules (fresh or stale-but-valid) or LKG.
    Rules TTL is long (4 h); stale cache is still better than nothing.
    Returns None only when completely unavailable.
    """
    cached = _rules_cache.get(product_id)
    if cached is not None:
        return cached   # use even if stale — numeric rules don't drift
    return _rules_from_lkg(product_id)


def get_state(product_id: str) -> Optional[ProductState]:
    """
    Return cached ProductState if within TTL, else None.
    Stale state is intentionally NOT returned — flags must be fresh for ENTRY.
    """
    s = _state_cache.get(product_id)
    if s is not None and _state_fresh(product_id):
        return s
    return None


def is_entry_allowed(product_id: str) -> tuple[bool, str]:
    """
    Fail-closed ENTRY check.

    Returns (True, "") when all checks pass.
    Returns (False, reason) when ENTRY must be blocked:
      - state unavailable or stale
      - any hard-block flag
      - limit_only (market orders would reject)
    """
    state = get_state(product_id)
    if state is None:
        return False, f"{product_id}: product state unavailable or stale — ENTRY blocked"
    if state.hard_blocked or state.limit_only:
        flags = state.blocking_flags()
        return False, (
            f"{product_id}: trading blocked by exchange flags: "
            + ", ".join(f"{f}=True" for f in flags)
        )
    return True, ""


def get_rules_for_exit(product_id: str) -> dict:
    """
    Return numeric rules dict for EXIT order rounding.  Never raises.

    Uses (in order): fresh cache → stale cache → LKG → 8-dp defaults.
    Trading flags are NOT checked — EXIT must not be blocked by metadata.
    """
    try:
        rules = get_rules(product_id)
        if rules:
            return rules.as_exit_dict()
    except Exception:
        pass
    return dict(_DEFAULTS)


def _inject_cache(
    product_id: str,
    rules: Optional[ProductRules] = None,
    state: Optional[ProductState] = None,
) -> None:
    """Test helper — directly populate cache without a Coinbase call."""
    if rules is not None:
        _rules_cache[product_id] = rules
    if state is not None:
        _state_cache[product_id] = state


def _clear_cache() -> None:
    """Test helper — reset all in-process state."""
    _rules_cache.clear()
    _state_cache.clear()
