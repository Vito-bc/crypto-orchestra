"""
SQLite order/position/epoch ledger — single source of truth for all order state.
Schema V2.

Design:
  WAL mode + foreign_keys=ON on every connection.
  PRAGMA user_version tracks schema version; run_migrations() applies sequential patches.
  Immutability triggers on fills and position_events (BEFORE UPDATE/DELETE → RAISE).
  Partial UNIQUE INDEX ensures at most one active epoch (ended_at IS NULL).
  apply_fill() is the only public path to record fills — atomic fill+transition+VWAP.
  Fail-closed: on unresolvable discrepancy with Coinbase, halt before placing new orders.

Tables: risk_epochs, orders, fills, positions, position_events,
        account_snapshots, reconciliation_runs

Order state machine:
  SUBMITTING → OPEN          (Coinbase accepted the order)
  SUBMITTING → CANCELLED     (Coinbase rejected before exchange state)
  OPEN       → PARTIAL       (first partial fill received)
  OPEN       → FILLED        (complete fill in one shot)
  OPEN       → CANCELLED
  OPEN       → EXPIRED
  PARTIAL    → FILLED        (remaining qty filled)
  PARTIAL    → CANCELLED
  (FILLED, CANCELLED, EXPIRED are terminal)

Position state machine:
  OPEN → CLOSING (exit order placed) → CLOSED

Stop/target live on positions, not on individual exchange orders.
"""

from __future__ import annotations

import json
import sqlite3
import uuid as _uuid_mod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

from pathlib import Path

ROOT    = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logs" / "ledger.db"

SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# Allowed order state transitions
# ---------------------------------------------------------------------------

_TRANSITIONS: dict[str, set[str]] = {
    "SUBMITTING": {"OPEN", "CANCELLED"},
    "OPEN":       {"PARTIAL", "FILLED", "CANCELLED", "EXPIRED"},
    "PARTIAL":    {"FILLED", "CANCELLED"},
    "FILLED":     set(),
    "CANCELLED":  set(),
    "EXPIRED":    set(),
}

_TERMINAL_STATES = {"FILLED", "CANCELLED", "EXPIRED"}


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

@contextmanager
def get_db(path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    """
    Yield a WAL-mode SQLite connection inside an explicit transaction.
    Commits on clean exit; rolls back on any exception.
    foreign_keys=ON is enforced per-connection (SQLite default is OFF).
    """
    db_path = path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema V2
# ---------------------------------------------------------------------------

_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS risk_epochs (
    epoch_id      TEXT    PRIMARY KEY,
    paper_capital REAL    NOT NULL CHECK(paper_capital > 0),
    reason        TEXT    NOT NULL,
    started_at    TEXT    NOT NULL,
    ended_at      TEXT                -- NULL = currently active
);

-- At most one epoch may be active at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_epoch
    ON risk_epochs(1) WHERE ended_at IS NULL;

-- Universal exchange-order model: entry limit orders, exit market/stop orders, etc.
-- stop_price and target_price live on the position, not here.
CREATE TABLE IF NOT EXISTS orders (
    id                 TEXT  PRIMARY KEY,             -- full UUID (client_order_id to Coinbase)
    epoch_id           TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset              TEXT  NOT NULL,
    side               TEXT  NOT NULL CHECK(side IN ('BUY','SELL')),
    order_type         TEXT  NOT NULL CHECK(order_type IN ('LIMIT','MARKET','STOP_LIMIT')),
    purpose            TEXT  NOT NULL CHECK(purpose IN ('ENTRY','EXIT')),
    position_id        TEXT  REFERENCES positions(id),  -- NULL for ENTRY; set for EXIT
    qty_base_requested REAL,                            -- base asset qty (coins); NULL if sized in USD
    qty_usd_requested  REAL,                            -- notional USD; NULL if sized in base
    limit_price        REAL,                            -- NULL for MARKET orders
    placed_at          TEXT  NOT NULL,
    expires_at         TEXT,                            -- NULL = GTC
    reasoning          TEXT  NOT NULL DEFAULT '',
    status             TEXT  NOT NULL DEFAULT 'SUBMITTING'
                             CHECK(status IN ('SUBMITTING','OPEN','PARTIAL','FILLED','CANCELLED','EXPIRED')),
    exchange_order_id  TEXT  UNIQUE,                    -- NULL until Coinbase accepts
    cancelled_at       TEXT,
    expired_at         TEXT
);

-- Immutable fill records. exchange_fill_id UNIQUE prevents double-counting on reconciliation.
CREATE TABLE IF NOT EXISTS fills (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id         TEXT    NOT NULL REFERENCES orders(id),
    exchange_fill_id TEXT    UNIQUE,
    fill_price       REAL    NOT NULL CHECK(fill_price > 0),
    fill_qty_base    REAL    NOT NULL CHECK(fill_qty_base > 0),
    fill_qty_usd     REAL    NOT NULL CHECK(fill_qty_usd > 0),
    fee_usd          REAL    NOT NULL DEFAULT 0.0 CHECK(fee_usd >= 0),
    is_taker         INTEGER NOT NULL DEFAULT 1 CHECK(is_taker IN (0,1)),
    filled_at        TEXT    NOT NULL
);

-- Position: stop/target live here, not on individual exchange orders.
-- entry_order_id is TEXT (no FK) to avoid circular dependency with orders.position_id.
CREATE TABLE IF NOT EXISTS positions (
    id                      TEXT  PRIMARY KEY,
    entry_order_id          TEXT  NOT NULL UNIQUE,      -- references orders(id); TEXT to break circular FK
    epoch_id                TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset                   TEXT  NOT NULL,
    entry_price             REAL,                       -- VWAP of entry fills; NULL until first fill
    qty_base                REAL,                       -- filled base qty (coins)
    qty_usd                 REAL,                       -- filled notional USD
    entry_fee_usd           REAL  NOT NULL DEFAULT 0.0,
    opened_at               TEXT,
    stop_price              REAL,
    target_price            REAL,
    high_water_mark         REAL,
    extensions_used         INTEGER NOT NULL DEFAULT 0,
    extension_trailing_stop REAL,
    status                  TEXT  NOT NULL DEFAULT 'OPEN'
                                  CHECK(status IN ('OPEN','CLOSING','CLOSED')),
    exit_price              REAL,
    exit_time               TEXT,
    exit_reason             TEXT,
    exit_fee_usd            REAL,
    pnl_usd                 REAL,
    pnl_pct                 REAL,
    closed_at               TEXT
);

-- Immutable event log for positions.
CREATE TABLE IF NOT EXISTS position_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id TEXT    NOT NULL REFERENCES positions(id),
    event_type  TEXT    NOT NULL,
    payload     TEXT    NOT NULL DEFAULT '{}',
    occurred_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS account_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshotted_at TEXT    NOT NULL,
    usd_balance    REAL    NOT NULL,
    total_nav      REAL    NOT NULL,
    unrealized_pnl REAL    NOT NULL,
    open_positions INTEGER NOT NULL
);

-- Reconciliation runs: track what was found vs resolved vs still unresolved.
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT    NOT NULL,
    completed_at TEXT,
    status       TEXT    NOT NULL DEFAULT 'RUNNING'
                         CHECK(status IN ('RUNNING','COMPLETE','COMPLETE_WITH_ACTIONS','FAILED')),
    discovered   TEXT    NOT NULL DEFAULT '[]',   -- all discrepancies found
    resolved     TEXT    NOT NULL DEFAULT '[]',   -- auto-fixed by reconciler
    unresolved   TEXT    NOT NULL DEFAULT '[]'    -- require human intervention → FAILED
);

CREATE INDEX IF NOT EXISTS idx_orders_asset_status    ON orders(asset, status);
CREATE INDEX IF NOT EXISTS idx_orders_epoch           ON orders(epoch_id);
CREATE INDEX IF NOT EXISTS idx_orders_position        ON orders(position_id) WHERE position_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fills_order            ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_positions_epoch_status ON positions(epoch_id, status);
CREATE INDEX IF NOT EXISTS idx_position_events_pos    ON position_events(position_id);

-- Immutability: fills are append-only.
CREATE TRIGGER IF NOT EXISTS trg_fills_no_update
    BEFORE UPDATE ON fills
BEGIN
    SELECT RAISE(ABORT, 'fills are immutable: UPDATE not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_fills_no_delete
    BEFORE DELETE ON fills
BEGIN
    SELECT RAISE(ABORT, 'fills are immutable: DELETE not allowed');
END;

-- Immutability: position_events are append-only.
CREATE TRIGGER IF NOT EXISTS trg_position_events_no_update
    BEFORE UPDATE ON position_events
BEGIN
    SELECT RAISE(ABORT, 'position_events are immutable: UPDATE not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_position_events_no_delete
    BEFORE DELETE ON position_events
BEGIN
    SELECT RAISE(ABORT, 'position_events are immutable: DELETE not allowed');
END;
"""


# ---------------------------------------------------------------------------
# Schema versioning and migrations
# ---------------------------------------------------------------------------

def _get_schema_version(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA user_version").fetchone()[0]


def run_migrations(path: Optional[Path] = None) -> None:
    """
    Apply all pending schema migrations in order.
    Called instead of init_db() — it's the main entry point for schema setup.
    Uses a raw connection because executescript() auto-commits any active BEGIN.
    """
    db_path = path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        version = _get_schema_version(conn)
        if version < 2:
            conn.executescript(_SCHEMA_V2)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        # Future: elif version < 3: conn.executescript(_MIGRATION_V3) ...
    finally:
        conn.close()


# Keep init_db() as an alias so existing callers and tests work.
def init_db(path: Optional[Path] = None) -> None:
    run_migrations(path)


# ---------------------------------------------------------------------------
# Epoch operations
# ---------------------------------------------------------------------------

def insert_epoch(
    epoch_id: str,
    paper_capital: float,
    reason: str,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Raw epoch insert — for migration and testing only.
    For live use, call start_epoch() which atomically closes the previous active epoch.
    Raises sqlite3.IntegrityError on duplicate epoch_id or active-epoch constraint violation.
    """
    ts = started_at or datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at, ended_at)
        VALUES (?,?,?,?,?)
    """
    if conn:
        conn.execute(sql, (epoch_id, paper_capital, reason, ts, ended_at))
    else:
        with get_db() as c:
            c.execute(sql, (epoch_id, paper_capital, reason, ts, ended_at))


def start_epoch(
    epoch_id: str,
    paper_capital: float,
    reason: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Live epoch transition: atomically closes the current active epoch and starts a new one.
    The partial UNIQUE INDEX enforces at most one active epoch at all times.
    Raises ValueError on duplicate epoch_id.
    """
    ts = datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        c.execute("UPDATE risk_epochs SET ended_at=? WHERE ended_at IS NULL", (ts,))
        try:
            c.execute(
                "INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at) VALUES (?,?,?,?)",
                (epoch_id, paper_capital, reason, ts),
            )
        except sqlite3.IntegrityError as e:
            if "risk_epochs.epoch_id" in str(e):
                raise ValueError(f"Epoch '{epoch_id}' already exists.") from e
            raise

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def get_active_epoch(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Return the single active epoch (ended_at IS NULL), or None."""
    return conn.execute(
        "SELECT * FROM risk_epochs WHERE ended_at IS NULL"
    ).fetchone()


# ---------------------------------------------------------------------------
# Order operations
# ---------------------------------------------------------------------------

def insert_order(
    order_id: str,
    epoch_id: str,
    asset: str,
    side: str,
    order_type: str,
    purpose: str,
    placed_at: str,
    qty_base_requested: Optional[float] = None,
    qty_usd_requested: Optional[float] = None,
    limit_price: Optional[float] = None,
    expires_at: Optional[str] = None,
    position_id: Optional[str] = None,
    reasoning: str = "",
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Insert a new order in SUBMITTING state.
    At least one of qty_base_requested or qty_usd_requested must be provided.
    Raises ValueError on validation failure; IntegrityError on duplicate id.
    """
    if qty_base_requested is None and qty_usd_requested is None:
        raise ValueError(
            f"Order '{order_id}': must set qty_base_requested or qty_usd_requested."
        )
    sql = """
        INSERT INTO orders(
            id, epoch_id, asset, side, order_type, purpose, position_id,
            qty_base_requested, qty_usd_requested, limit_price,
            placed_at, expires_at, reasoning, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'SUBMITTING')
    """
    args = (
        order_id, epoch_id, asset, side, order_type, purpose, position_id,
        qty_base_requested, qty_usd_requested, limit_price,
        placed_at, expires_at, reasoning,
    )
    if conn:
        conn.execute(sql, args)
    else:
        with get_db() as c:
            c.execute(sql, args)


def transition_order(
    order_id: str,
    new_status: str,
    exchange_order_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Atomically advance an order to new_status.
    Raises ValueError on forbidden transition; RuntimeError if order not found.
    """
    def _run(c: sqlite3.Connection) -> None:
        row = c.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
        if row is None:
            raise RuntimeError(f"Order '{order_id}' not found in ledger.")
        current = row["status"]
        if new_status not in _TRANSITIONS.get(current, set()):
            raise ValueError(
                f"Order '{order_id}': invalid transition {current!r} → {new_status!r}. "
                f"Allowed from {current!r}: {_TRANSITIONS.get(current, set())}"
            )
        now = datetime.now(timezone.utc).isoformat()
        extra: dict[str, object] = {"status": new_status}
        if exchange_order_id:
            extra["exchange_order_id"] = exchange_order_id
        if new_status == "CANCELLED":
            extra["cancelled_at"] = now
        elif new_status == "EXPIRED":
            extra["expired_at"] = now
        set_clause = ", ".join(f"{k}=?" for k in extra)
        c.execute(f"UPDATE orders SET {set_clause} WHERE id=?", (*extra.values(), order_id))

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def get_open_orders_for_asset(asset: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM orders WHERE asset=? AND status IN ('OPEN','PARTIAL')",
        (asset,),
    ).fetchall()


def get_open_orders_for_position(
    position_id: str, conn: sqlite3.Connection
) -> list[sqlite3.Row]:
    """Return all non-terminal orders associated with a position (e.g. exit orders)."""
    return conn.execute(
        "SELECT * FROM orders WHERE position_id=? AND status NOT IN ('FILLED','CANCELLED','EXPIRED')",
        (position_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Fill operations — apply_fill() is the only public write path
# ---------------------------------------------------------------------------

def apply_fill(
    order_id: str,
    fill_price: float,
    fill_qty_base: float,
    fee_usd: float = 0.0,
    is_taker: bool = True,
    filled_at: Optional[str] = None,
    exchange_fill_id: Optional[str] = None,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """
    Atomically apply a fill to an order:
      1. INSERT fill (idempotent on exchange_fill_id — re-runs safely on reconciliation).
      2. Aggregate VWAP + totals from all fills for this order.
      3. Transition order to PARTIAL or FILLED.
      4. For ENTRY orders: create position on first fill, or update VWAP on subsequent fills.

    stop_price / target_price are only used when creating the position (ENTRY, first fill).

    Returns {"status": new_order_status, "position_id": str or None}.

    Raises RuntimeError if order not found, is SUBMITTING, or is in a terminal state.
    Must be called within get_db() — caller provides the connection.
    """
    ts = filled_at or datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> dict:
        # Early idempotency check: if this exchange_fill_id was already recorded
        # (e.g. reconciliation re-run), skip the whole operation and return current state.
        if exchange_fill_id:
            already = c.execute(
                "SELECT order_id FROM fills WHERE exchange_fill_id=?", (exchange_fill_id,)
            ).fetchone()
            if already:
                order = c.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
                pos = c.execute(
                    "SELECT id FROM positions WHERE entry_order_id=?", (order_id,)
                ).fetchone()
                return {
                    "status": order["status"] if order else "UNKNOWN",
                    "position_id": pos["id"] if pos else None,
                }

        order = c.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if order is None:
            raise RuntimeError(f"Order '{order_id}' not found in ledger.")
        if order["status"] in _TERMINAL_STATES:
            raise RuntimeError(
                f"Cannot apply fill to {order['status']!r} order '{order_id}'."
            )
        if order["status"] == "SUBMITTING":
            raise RuntimeError(
                f"Order '{order_id}' is still SUBMITTING — "
                "call transition_order(..., 'OPEN') before applying fills."
            )

        # INSERT fill (non-idempotent path: exchange_fill_id is new or None)
        c.execute("""
            INSERT INTO fills(
                order_id, exchange_fill_id, fill_price, fill_qty_base,
                fill_qty_usd, fee_usd, is_taker, filled_at
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            order_id, exchange_fill_id, fill_price, fill_qty_base,
            fill_price * fill_qty_base, fee_usd, 1 if is_taker else 0, ts,
        ))

        # VWAP + totals across all fills for this order
        agg = c.execute("""
            SELECT
                SUM(fill_price * fill_qty_base) / SUM(fill_qty_base) AS vwap,
                SUM(fill_qty_base) AS total_base,
                SUM(fill_qty_usd)  AS total_usd,
                SUM(fee_usd)       AS total_fee
            FROM fills WHERE order_id=?
        """, (order_id,)).fetchone()

        # Determine completeness
        req_base = order["qty_base_requested"]
        req_usd  = order["qty_usd_requested"]
        is_complete = False
        if req_base is not None:
            is_complete = agg["total_base"] >= req_base * 0.999
        elif req_usd is not None:
            is_complete = agg["total_usd"] >= req_usd * 0.999
        new_status = "FILLED" if is_complete else "PARTIAL"
        c.execute("UPDATE orders SET status=? WHERE id=?", (new_status, order_id))

        # Create or update position for ENTRY orders
        position_id = None
        if order["purpose"] == "ENTRY":
            existing = c.execute(
                "SELECT id FROM positions WHERE entry_order_id=?", (order_id,)
            ).fetchone()
            if existing is None:
                position_id = str(_uuid_mod.uuid4())
                c.execute("""
                    INSERT INTO positions(
                        id, entry_order_id, epoch_id, asset,
                        entry_price, qty_base, qty_usd, entry_fee_usd,
                        opened_at, stop_price, target_price, high_water_mark, status
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')
                """, (
                    position_id, order_id, order["epoch_id"], order["asset"],
                    agg["vwap"], agg["total_base"], agg["total_usd"], agg["total_fee"],
                    ts, stop_price, target_price, agg["vwap"],
                ))
                c.execute(
                    "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
                    " VALUES (?,?,?,?)",
                    (position_id, "OPENED",
                     json.dumps({"entry_price": agg["vwap"], "qty_base": agg["total_base"]}),
                     ts),
                )
            else:
                position_id = existing["id"]
                c.execute("""
                    UPDATE positions SET
                        entry_price=?, qty_base=?, qty_usd=?, entry_fee_usd=?
                    WHERE id=?
                """, (agg["vwap"], agg["total_base"], agg["total_usd"], agg["total_fee"],
                      position_id))

        return {"status": new_status, "position_id": position_id}

    if conn:
        return _run(conn)
    else:
        with get_db() as c:
            return _run(c)


def get_fills_for_order(order_id: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fills WHERE order_id=? ORDER BY filled_at", (order_id,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Position operations
# ---------------------------------------------------------------------------

def insert_position(
    position_id: str,
    entry_order_id: str,
    epoch_id: str,
    asset: str,
    entry_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    qty_base: Optional[float] = None,
    qty_usd: Optional[float] = None,
    entry_fee_usd: float = 0.0,
    opened_at: Optional[str] = None,
    status: str = "OPEN",
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Direct position insert — for migration and reconciliation only.
    Normal fills must go through apply_fill() which sets VWAP correctly.
    """
    ts = opened_at or datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO positions(
            id, entry_order_id, epoch_id, asset,
            entry_price, qty_base, qty_usd, entry_fee_usd,
            opened_at, stop_price, target_price, high_water_mark, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    args = (
        position_id, entry_order_id, epoch_id, asset,
        entry_price, qty_base, qty_usd, entry_fee_usd,
        ts, stop_price, target_price, entry_price, status,
    )
    if conn:
        conn.execute(sql, args)
    else:
        with get_db() as c:
            c.execute(sql, args)


def close_position(
    position_id: str,
    exit_price: float,
    exit_reason: str,
    pnl_usd: float,
    pnl_pct: float,
    exit_fee_usd: float,
    exit_time: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Mark a position as CLOSED. Raises RuntimeError if not found or not OPEN/CLOSING.
    Records a CLOSED position_event.
    """
    ts = exit_time or datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        row = c.execute("SELECT status FROM positions WHERE id=?", (position_id,)).fetchone()
        if row is None:
            raise RuntimeError(f"Position '{position_id}' not found.")
        if row["status"] not in ("OPEN", "CLOSING"):
            raise RuntimeError(
                f"Position '{position_id}' is {row['status']!r} — only OPEN or CLOSING can be closed."
            )
        c.execute("""
            UPDATE positions SET
                status='CLOSED', exit_price=?, exit_time=?, exit_reason=?,
                exit_fee_usd=?, pnl_usd=?, pnl_pct=?, closed_at=?
            WHERE id=?
        """, (exit_price, ts, exit_reason, exit_fee_usd, pnl_usd, pnl_pct, ts, position_id))
        payload = json.dumps({
            "exit_price": exit_price, "exit_reason": exit_reason,
            "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
        })
        c.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at) VALUES (?,?,?,?)",
            (position_id, "CLOSED", payload, ts),
        )

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def update_position_stop(
    position_id: str,
    new_stop: float,
    new_hwm: float,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Update trailing stop and high-water mark on an OPEN position.
    Raises RuntimeError if position not found or not OPEN.
    The STOP_UPDATED event is only inserted if the UPDATE succeeded.
    """
    ts = datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        c.execute(
            "UPDATE positions SET stop_price=?, high_water_mark=? WHERE id=? AND status='OPEN'",
            (new_stop, new_hwm, position_id),
        )
        rows_changed = c.execute("SELECT changes()").fetchone()[0]
        if rows_changed == 0:
            row = c.execute("SELECT status FROM positions WHERE id=?", (position_id,)).fetchone()
            if row is None:
                raise RuntimeError(f"Position '{position_id}' not found.")
            raise RuntimeError(
                f"Position '{position_id}' is {row['status']!r} — stop update only allowed on OPEN."
            )
        payload = json.dumps({"new_stop": new_stop, "new_hwm": new_hwm})
        c.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at) VALUES (?,?,?,?)",
            (position_id, "STOP_UPDATED", payload, ts),
        )

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def get_open_positions_for_asset(
    asset: Optional[str],
    conn: sqlite3.Connection,
) -> list[sqlite3.Row]:
    if asset:
        return conn.execute(
            "SELECT * FROM positions WHERE asset=? AND status IN ('OPEN','CLOSING')", (asset,)
        ).fetchall()
    return conn.execute(
        "SELECT * FROM positions WHERE status IN ('OPEN','CLOSING')"
    ).fetchall()


def get_epoch_closed_pnl(epoch_id: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM positions WHERE epoch_id=? AND status='CLOSED' ORDER BY closed_at",
        (epoch_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

_STALE_RUN_MINUTES = 30


def start_reconciliation(
    conn: sqlite3.Connection,
    stale_threshold_minutes: int = _STALE_RUN_MINUTES,
) -> int:
    """
    Begin a new reconciliation run.

    If a RUNNING run exists and is older than stale_threshold_minutes, marks it FAILED
    (hung-run recovery) and starts a new one.
    Raises RuntimeError if a recent RUNNING run already exists (< stale_threshold_minutes old).
    """
    now = datetime.now(timezone.utc)
    running = conn.execute(
        "SELECT id, started_at FROM reconciliation_runs WHERE status='RUNNING' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if running:
        started = datetime.fromisoformat(running["started_at"])
        # Ensure both are timezone-aware for comparison
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        age_minutes = (now - started).total_seconds() / 60
        if age_minutes < stale_threshold_minutes:
            raise RuntimeError(
                f"Reconciliation run #{running['id']} is already RUNNING "
                f"(started {age_minutes:.0f}m ago, threshold={stale_threshold_minutes}m). "
                "Wait for it to complete or investigate."
            )
        conn.execute(
            "UPDATE reconciliation_runs SET status='FAILED', completed_at=? WHERE id=?",
            (now.isoformat(), running["id"]),
        )
    ts = now.isoformat()
    conn.execute(
        "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)", (ts, "RUNNING")
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def complete_reconciliation(
    run_id: int,
    discovered: list,
    resolved: list,
    unresolved: list,
    conn: sqlite3.Connection,
) -> None:
    """
    Close a reconciliation run.
    status = COMPLETE if no discrepancies were found.
    status = COMPLETE_WITH_ACTIONS if discrepancies were found but all resolved.
    status = FAILED if any remain unresolved (require human intervention).
    """
    ts = datetime.now(timezone.utc).isoformat()
    if unresolved:
        status = "FAILED"
    elif resolved:
        status = "COMPLETE_WITH_ACTIONS"
    else:
        status = "COMPLETE"
    conn.execute("""
        UPDATE reconciliation_runs
        SET completed_at=?, status=?, discovered=?, resolved=?, unresolved=?
        WHERE id=?
    """, (ts, status, json.dumps(discovered), json.dumps(resolved), json.dumps(unresolved), run_id))


# ---------------------------------------------------------------------------
# JSON migration
# ---------------------------------------------------------------------------

def migrate_from_json(
    epochs_jsonl:  Optional[Path] = None,
    orders_json:   Optional[Path] = None,
    history_jsonl: Optional[Path] = None,
    db_path:       Optional[Path] = None,
) -> dict[str, int]:
    """
    One-time import of existing JSON/JSONL data into the SQLite ledger.

    Idempotency: exact duplicate primary-key rows are silently skipped.
    Any other IntegrityError (e.g. conflicting exchange_order_id, orphan data)
    raises RuntimeError rather than being silently swallowed.

    Pre-epoch orders/positions (epoch_id is None) are skipped — they have no
    representation in the epoch-scoped ledger.

    Returns counts: {"epochs": N, "orders": N, "positions": N}
    """
    _epochs  = epochs_jsonl  or (ROOT / "logs" / "risk_epochs.jsonl")
    _orders  = orders_json   or (ROOT / "logs" / "pending_orders.json")
    _history = history_jsonl or (ROOT / "logs" / "trade_history.jsonl")

    counts = {"epochs": 0, "orders": 0, "positions": 0}

    with get_db(db_path) as conn:

        # -- Epochs -----------------------------------------------------------
        epoch_records: list[dict] = []
        if _epochs.exists():
            for line in _epochs.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("event") == "RISK_EPOCH_STARTED":
                    epoch_records.append(rec)

        epoch_records.sort(key=lambda r: r.get("timestamp", ""))
        for i, rec in enumerate(epoch_records):
            # Existence check before insert avoids ambiguous partial-index vs PK IntegrityError.
            already = conn.execute(
                "SELECT 1 FROM risk_epochs WHERE epoch_id=?", (rec["epoch_id"],)
            ).fetchone()
            if already:
                continue  # already migrated — idempotent skip
            ended_at = (
                epoch_records[i + 1].get("timestamp") if i < len(epoch_records) - 1 else None
            )
            try:
                conn.execute(
                    "INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at, ended_at)"
                    " VALUES (?,?,?,?,?)",
                    (rec["epoch_id"], rec["paper_capital"], rec.get("reason", ""),
                     rec.get("timestamp", datetime.now(timezone.utc).isoformat()), ended_at),
                )
                counts["epochs"] += 1
            except sqlite3.IntegrityError as e:
                raise RuntimeError(
                    f"Migration: unexpected IntegrityError for epoch {rec.get('epoch_id')!r}: {e}"
                ) from e

        # -- Pending orders ---------------------------------------------------
        if _orders.exists():
            orders_data = json.loads(_orders.read_text(encoding="utf-8"))
            for o in orders_data:
                epoch_id = o.get("epoch_id")
                if epoch_id is None:
                    continue  # pre-epoch order — no DB representation
                row = conn.execute(
                    "SELECT epoch_id FROM risk_epochs WHERE epoch_id=?", (epoch_id,)
                ).fetchone()
                if row is None:
                    continue  # orphan order (epoch not migrated) — skip
                status_map = {
                    "OPEN": "OPEN", "FILLED": "FILLED",
                    "CANCELLED": "CANCELLED", "EXPIRED": "EXPIRED",
                }
                status = status_map.get(o.get("status", ""), "OPEN")
                already = conn.execute(
                    "SELECT 1 FROM orders WHERE id=?", (o["id"],)
                ).fetchone()
                if already:
                    continue  # already migrated — idempotent skip
                try:
                    conn.execute("""
                        INSERT INTO orders(
                            id, epoch_id, asset, side, order_type, purpose,
                            qty_usd_requested, limit_price,
                            placed_at, expires_at, reasoning, status, exchange_order_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        o["id"], epoch_id, o["asset"],
                        "BUY", "LIMIT", "ENTRY",      # legacy orders are all BUY LIMIT ENTRY
                        o.get("qty_usd_requested"),   # NULL if not present — unknown is NULL
                        o.get("limit_price"),
                        o["placed_at"], o.get("expires_at"), o.get("reasoning", ""),
                        status, o.get("exchange_order_id"),
                    ))
                    counts["orders"] += 1
                except sqlite3.IntegrityError as e:
                    raise RuntimeError(
                        f"Migration: unexpected IntegrityError for order {o.get('id')!r}: {e}"
                    ) from e

        # -- Trade history → positions ----------------------------------------
        if _history.exists():
            for line in _history.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                t = json.loads(line)
                epoch_id = t.get("epoch_id")
                if epoch_id is None:
                    continue  # pre-epoch — no DB representation
                order_row = conn.execute(
                    "SELECT id FROM orders WHERE id=?", (t["id"],)
                ).fetchone()
                if order_row is None:
                    continue  # order not migrated — skip position
                try:
                    entry_price = t.get("entry_price")
                    qty_usd = t.get("qty_usd")
                    qty_base = (qty_usd / entry_price) if (qty_usd and entry_price) else None
                    conn.execute("""
                        INSERT INTO positions(
                            id, entry_order_id, epoch_id, asset,
                            entry_price, qty_base, qty_usd, entry_fee_usd,
                            opened_at, status,
                            exit_price, exit_time, exit_reason,
                            exit_fee_usd, pnl_usd, pnl_pct, closed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        t["id"], t["id"], epoch_id, t["asset"],
                        entry_price, qty_base, qty_usd,
                        t.get("entry_fee_usd", 0),
                        t.get("entry_time", t.get("closed_at_utc", "")),
                        "CLOSED",
                        t.get("exit_price"), t.get("exit_time"),
                        t.get("reason"), t.get("exit_fee_usd", 0),
                        t.get("pnl_usd"), t.get("pnl_pct"),
                        t.get("closed_at_utc", t.get("exit_time")),
                    ))
                    counts["positions"] += 1
                except sqlite3.IntegrityError as e:
                    if "positions.id" in str(e):
                        pass  # duplicate — already migrated
                    else:
                        raise RuntimeError(
                            f"Migration: unexpected IntegrityError for position {t.get('id')!r}: {e}"
                        ) from e

    return counts
