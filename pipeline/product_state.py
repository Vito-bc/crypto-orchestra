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
  • get_rules_for_exit() reports which source it used so the caller can alert
    when it is running on defaults or on an over-age LKG entry.

Payload parsing is delegated to pipeline/product_parse.py, shared with preflight,
so one Get Product response can never be interpreted two different ways.

Call order:
  1. prewarm(product_ids)   — at startup, after reconciliation.
  2. is_entry_allowed(pid)  — before placing any ENTRY order.
  3. get_rules_for_exit(pid)— inside exit_executor to round sell qty.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from pipeline.product_parse import (
    CORE_NUMERIC,
    TRADING_FLAGS,
    parse_product_payload,
    strict_positive_decimal,
)

ROOT    = Path(__file__).resolve().parents[1]
LKG_PATH = ROOT / "data" / "product_lkg.json"

_RULES_TTL_S = 4 * 3600   # 4 hours — listings rarely change
_STATE_TTL_S = 5 * 60     # 5 minutes — flags can change during halts

# Beyond this age an LKG entry is still usable for EXIT rounding — never
# blocking risk reduction — but is reported as stale so the caller can alert.
_LKG_MAX_AGE_S = 7 * 24 * 3600   # 7 days

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
    # Set only when this instance was reconstructed from the LKG file; None for
    # values that came straight from Coinbase.  Seconds since it was fetched.
    lkg_age_s: Optional[float] = None

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

# Bounded spin for the cross-process write lock.  The critical section is a
# small read-modify-write, so contention is measured in milliseconds; the
# timeout only matters if another process died holding the lock.
_LKG_LOCK_TIMEOUT_S = 10.0
_LKG_LOCK_POLL_S    = 0.05


def _lock_path() -> Path:
    return LKG_PATH.with_suffix(LKG_PATH.suffix + ".lock")


@contextlib.contextmanager
def _lkg_write_lock() -> Iterator[bool]:
    """
    Cross-process exclusive lock for the LKG read-modify-write cycle.

    Yields True when the lock was acquired, False on timeout.  The caller MUST
    check the yielded value and skip the write when it is False — proceeding
    unlocked reopens the lost-update race this lock exists to close, precisely
    in the failure branch where a concurrent writer is most likely.

    Only writers take this lock.  Readers (get_rules_for_exit → _rules_from_lkg)
    stay lock-free: os.replace makes every write atomic, so a reader always sees
    a complete old-or-new file and EXIT is never blocked waiting on a writer.

    Without this, two processes — e.g. the scheduler and a concurrent
    `runner.py ZEC-USD` CLI run, the pattern documented in CLAUDE.md — each read
    the file, add their own asset, and write back, so whoever writes last
    silently drops the other's entry.

    Uses OS advisory locks (msvcrt on Windows, fcntl elsewhere) with a bounded
    non-blocking spin.  On timeout we do NOT block a trading cycle indefinitely;
    we hand back False so the caller can skip persistence (the in-process cache
    still updates, and the next prewarm retries) while leaving the old LKG file
    byte-for-byte intact.
    """
    LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_file = _lock_path()
    fh = open(lock_file, "a+")
    acquired = False
    deadline = time.monotonic() + _LKG_LOCK_TIMEOUT_S
    try:
        while True:
            if _try_lock(fh):
                acquired = True
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(_LKG_LOCK_POLL_S)
        yield acquired
    finally:
        if acquired:
            _unlock(fh)
        fh.close()


try:
    import msvcrt  # type: ignore

    def _try_lock(fh) -> bool:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def _unlock(fh) -> None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

except ImportError:  # POSIX
    import fcntl  # type: ignore

    def _try_lock(fh) -> bool:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(fh) -> None:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


class LKGCorruptError(Exception):
    """The LKG file exists but could not be parsed as a JSON object."""


def _read_lkg_file() -> dict:
    """
    Read and structurally validate the LKG file.

    Returns {} when the file does not exist — that is a normal first run.
    Raises LKGCorruptError when the file exists but is unreadable or is not a
    JSON object.  Absent and corrupt must stay distinguishable: treating a
    corrupt file as {} is what allowed one bad write to erase every product's
    entry on the next save.
    """
    if not LKG_PATH.exists():
        return {}
    try:
        with open(LKG_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        raise LKGCorruptError(f"{LKG_PATH}: {exc}") from exc
    if not isinstance(data, dict):
        raise LKGCorruptError(
            f"{LKG_PATH}: top-level JSON value is {type(data).__name__}, expected object"
        )
    return data


def _quarantine_lkg(reason: str) -> Optional[Path]:
    """
    Move a corrupt LKG file aside instead of overwriting it.

    The bad file is preserved for forensics — an LKG that cannot be parsed is
    evidence of a crashed write or disk problem, and silently discarding it
    hides that.  Returns the quarantine path, or None if the move failed.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = LKG_PATH.with_suffix(f".corrupt-{stamp}.json")
    try:
        os.replace(LKG_PATH, dest)
    except OSError as exc:
        print(f"[ProductState] LKG quarantine FAILED ({reason}): {exc}")
        return None
    print(
        f"[ProductState] LKG corrupt ({reason}) — quarantined to {dest.name}. "
        "Product rules will be re-fetched from Coinbase on the next prewarm."
    )
    return dest


def _load_lkg() -> dict:
    """
    Best-effort read of the LKG file.  Never raises.

    A corrupt file is quarantined (not silently dropped) and {} is returned.
    """
    try:
        return _read_lkg_file()
    except LKGCorruptError as exc:
        _quarantine_lkg(str(exc))
        return {}


def _atomic_write_json(path: Path, payload: dict) -> None:
    """
    Write JSON via temp file + os.replace so a crash never leaves a partial
    file behind.  Same pattern as limit_orders.py and position_tracker.py.

    The temp name is unique per WRITE (tempfile.mkstemp), not merely per process:
    a PID-scoped name still collides between two writers inside the same process
    (two threads, or one thread persisting several assets), letting one
    os.replace pick up the other's half-written bytes.  mkstemp hands out a fresh
    name every call, in the destination directory so the rename stays on one
    filesystem and therefore atomic.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=path.name + ".", suffix=".tmp"
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        # If json.dump raised, the temp is orphaned — remove it so a failed
        # write never litters the data dir with stale scratch files.
        if tmp.exists():
            with contextlib.suppress(OSError):
                os.remove(tmp)


def _save_lkg(rules: ProductRules, state: ProductState) -> bool:
    """
    Merge one product's rules+state into the LKG file.

    Returns True when the entry was persisted, False when the write was skipped
    because the cross-process lock could not be acquired.

    The whole read-modify-write runs under the lock so a concurrent writer
    cannot drop this product's entry (or have its own dropped).  On a lock
    timeout we SKIP the write entirely rather than proceed unlocked: an unlocked
    read-modify-write is exactly the lost-update race the lock closes, and it is
    most likely precisely when the lock is already contended.  The in-process
    cache is updated by the caller regardless, and the next prewarm retries, so
    skipping durable persistence for one cycle is safe; corrupting or truncating
    another writer's entry is not.
    """
    LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _lkg_write_lock() as acquired:
        if not acquired:
            print(
                f"[ProductState] CRITICAL: LKG write lock timeout after "
                f"{_LKG_LOCK_TIMEOUT_S}s for {rules.product_id} — persistence "
                "SKIPPED; existing LKG left unchanged; in-process cache still "
                "updated; next prewarm will retry."
            )
            return False
        _save_lkg_locked(rules, state)
        return True


def _save_lkg_locked(rules: ProductRules, state: ProductState) -> None:
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
    _atomic_write_json(LKG_PATH, data)


def _rules_from_lkg(pid: str) -> Optional[ProductRules]:
    """
    Reconstruct ProductRules from the LKG file, with strict validation.

    Returns None when the entry is absent, structurally wrong, or carries a
    non-positive increment/min-size — a bad persisted value must not silently
    become an order quantity.  An over-age entry is still returned; its age is
    reported via `lkg_age_s` so the caller can decide (EXIT uses it and alerts;
    it must never block risk reduction).
    """
    data = _load_lkg()
    entry = data.get(pid)
    if not isinstance(entry, dict) or not isinstance(entry.get("rules"), dict):
        return None
    r = entry["rules"]
    try:
        base_increment = r["base_increment"]
        base_min_size  = r["base_min_size"]
    except (KeyError, TypeError):
        return None

    # Reject an entry whose stored product_id disagrees with the key it was
    # filed under — a mismatched or swapped record must not supply rounding
    # rules for the wrong product.  Returning None keeps EXIT fail-open (it falls
    # through to defaults) rather than rounding a SELL with another product's rules.
    stored_pid = r.get("product_id", pid)
    if stored_pid != pid:
        print(
            f"[ProductState] LKG entry keyed {pid!r} has product_id {stored_pid!r} "
            "— mismatch rejected"
        )
        return None

    validation_errors: list[str] = []
    strict_positive_decimal(base_increment, f"{pid}.base_increment", validation_errors)
    strict_positive_decimal(base_min_size,  f"{pid}.base_min_size",  validation_errors)
    if validation_errors:
        print(f"[ProductState] LKG entry for {pid} is invalid: {validation_errors[0]}")
        return None

    # A non-finite or unparseable timestamp must not read as "fresh".  Infinity
    # would make (now - inf) negative and clamp age to 0.0; treat any invalid
    # timestamp as maximally old so get_rules_for_exit reports it lkg_stale.
    fetched_wall = r.get("fetched_wall", 0.0)
    try:
        fw = float(fetched_wall)
    except (TypeError, ValueError):
        fw = None
    if fw is None or not math.isfinite(fw):
        age_s = float("inf")
        fw = 0.0
    else:
        age_s = max(0.0, time.time() - fw)

    return ProductRules(
        product_id=pid,
        base_increment=str(base_increment),
        base_min_size=str(base_min_size),
        base_max_size=str(r.get("base_max_size", "")),
        quote_increment=str(r.get("quote_increment", "0.01")),
        fetched_wall=fw,
        lkg_age_s=age_s,
    )


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

    # Shared strict parser: flags accept only native bools, numeric fields must
    # be finite and positive.  A missing flag is an error, never a silent False —
    # ENTRY must not proceed on a product whose tradability was never read.
    parsed = parse_product_payload(d, product_id, required_numeric=CORE_NUMERIC)
    if not parsed.ok:
        raise RuntimeError(
            f"Get Product {product_id}: malformed response — "
            + "; ".join(parsed.errors[:5])
        )

    now_wall = time.time()
    now_mono = time.monotonic()

    rules = ProductRules(
        product_id=product_id,
        base_increment=parsed.raw["base_increment"],
        base_min_size=parsed.raw["base_min_size"],
        base_max_size=parsed.raw.get("base_max_size", ""),
        quote_increment=parsed.raw.get("quote_increment", "0.01"),
        fetched_wall=now_wall,
    )
    # Attach monotonic for TTL — not serialised to LKG
    rules.fetched_mono = now_mono  # type: ignore[attr-defined]

    state = ProductState(
        product_id=product_id,
        fetched_wall=now_wall,
        fetched_mono=now_mono,
        **{name: parsed.flags[name] for name in TRADING_FLAGS},
    )
    return rules, state


# ── Public API ────────────────────────────────────────────────────────────────

# Products whose LKG durable write is NOT known to be current.  A product enters
# this set when its _save_lkg fails/skips, and it leaves ONLY on a confirmed
# _save_lkg == True.  This is deliberately STICKY across prewarm calls: if a
# product persist-fails and then, on a later pass, its Coinbase FETCH fails (so we
# never even attempt a write), the product must remain flagged — otherwise the
# runner would send a false RECOVERED while the on-disk LKG is still stale or
# missing.  Fetch succeeded means the in-process cache is fresh (ENTRY is safe);
# durability is tracked separately and never blocks ENTRY.
_last_persistence_failures: set[str] = set()


def last_persistence_failures() -> list[str]:
    """Product ids whose LKG write is not confirmed current (sorted, stable)."""
    return sorted(_last_persistence_failures)


def prewarm(product_ids: list[str]) -> dict[str, bool]:
    """
    Fetch fresh ProductRules + ProductState for each product_id.

    Ignores DRY_RUN — the only context where you'd want real data at startup.
    In unit tests, patch _fetch_from_coinbase or call _inject_cache directly.

    Returns {product_id: True/False} reflecting the FETCH outcome only.  A True
    means the live state was read and cached, so ENTRY may proceed.  Durable LKG
    persistence is best-effort and deliberately does NOT affect this result: a
    persistence failure leaves the session's cache intact and must not block
    ENTRY.  Persistence status is tracked separately (sticky, delivery-confirmed)
    via last_persistence_failures().
    """
    results: dict[str, bool] = {}
    for pid in product_ids:
        try:
            rules, state = _fetch_from_coinbase(pid)
        except Exception as exc:
            results[pid] = False
            # FETCH failed — we could not confirm a durable write this pass, so
            # DO NOT clear any existing persistence-failure flag for this product.
            print(f"[ProductState] prewarm FAILED  {pid}: {exc}")
            continue

        # Fetch succeeded — cache is authoritative for this session.
        _rules_cache[pid] = rules
        _state_cache[pid] = state
        results[pid] = True

        # Persistence is best-effort; a failure here never demotes the fetch.
        try:
            persisted = _save_lkg(rules, state)
        except Exception as exc:
            persisted = False
            print(f"[ProductState] prewarm LKG persist error  {pid}: {exc}")

        if persisted:
            _last_persistence_failures.discard(pid)   # confirmed durable write
        else:
            _last_persistence_failures.add(pid)

        print(f"[ProductState] prewarm OK  {pid}  "
              f"tradeable={state.entry_allowed}  "
              f"inc={rules.base_increment}  min={rules.base_min_size}"
              + ("  [LKG NOT PERSISTED]" if not persisted else ""))

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
    Return numeric rules for EXIT order rounding.  Never raises, never blocks.

    Resolution order: in-process cache → LKG → 8-dp defaults.

    The returned dict always carries provenance so the caller can alert when
    rounding on something weaker than live data:

        base_increment / base_min_size — the values to round with
        source  — "cache" | "lkg" | "lkg_stale" | "defaults"
        stale   — True when the source is over _LKG_MAX_AGE_S or is defaults
        age_s   — age of the LKG entry in seconds (absent for cache/defaults)

    "defaults" is deliberately permissive (8 dp, 1e-8 minimum): a sell that the
    exchange rejects is recoverable, a sell that never gets placed is not.
    Trading flags are NOT consulted — EXIT must not be blocked by metadata.
    """
    try:
        cached = _rules_cache.get(product_id)
        if cached is not None:
            # Numeric rules don't drift mid-session; stale cache still beats LKG.
            return {**cached.as_exit_dict(), "source": "cache", "stale": False}

        lkg = _rules_from_lkg(product_id)
        if lkg is not None:
            age_s = lkg.lkg_age_s if lkg.lkg_age_s is not None else float("inf")
            stale = age_s > _LKG_MAX_AGE_S
            return {
                **lkg.as_exit_dict(),
                "source": "lkg_stale" if stale else "lkg",
                "stale": stale,
                "age_s": age_s,
            }
    except Exception as exc:
        print(f"[ProductState] get_rules_for_exit fell through to defaults: {exc}")

    return {**_DEFAULTS, "source": "defaults", "stale": True}


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
