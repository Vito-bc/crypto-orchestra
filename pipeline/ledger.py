"""
SQLite order/position/epoch ledger — single source of truth for all order state.

Design:
  WAL mode + foreign_keys=ON on every connection.
  Immutable fill records (only INSERT, never UPDATE on fills).
  Transactional state transitions with allowed-transition guard.
  UNIQUE constraints replace the TOCTOU-vulnerable JSON read-check-write pattern.
  On any unresolvable discrepancy with Coinbase: fail-closed before placing new orders.

Tables: risk_epochs, orders, fills, positions, position_events,
        account_snapshots, reconciliation_runs

State machine (orders):
  SUBMITTING → OPEN          (Coinbase accepted the order)
  SUBMITTING → CANCELLED     (Coinbase rejected; before any exchange state)
  OPEN       → PARTIAL       (first partial fill received)
  OPEN       → FILLED        (single complete fill)
  OPEN       → CANCELLED
  OPEN       → EXPIRED
  PARTIAL    → FILLED        (remaining qty filled)
  PARTIAL    → CANCELLED
  (FILLED, CANCELLED, EXPIRED are terminal)

Migration path (JSON → SQLite):
  Use migrate_from_json() on first startup; JSON files kept as read-only audit trail.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

ROOT    = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logs" / "ledger.db"

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
    Commits on clean exit; rolls back on exception.
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
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS risk_epochs (
    epoch_id      TEXT    PRIMARY KEY,
    paper_capital REAL    NOT NULL CHECK(paper_capital > 0),
    reason        TEXT    NOT NULL,
    started_at    TEXT    NOT NULL,
    ended_at      TEXT                -- NULL = currently active
);

CREATE TABLE IF NOT EXISTS orders (
    id                TEXT  PRIMARY KEY,
    epoch_id          TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset             TEXT  NOT NULL,
    limit_price       REAL  NOT NULL,
    stop_price        REAL  NOT NULL,
    target_price      REAL  NOT NULL,
    position_size_pct REAL,
    qty_usd_requested REAL  NOT NULL,
    placed_at         TEXT  NOT NULL,
    expires_at        TEXT  NOT NULL,
    reasoning         TEXT  NOT NULL DEFAULT '',
    status            TEXT  NOT NULL DEFAULT 'SUBMITTING'
                            CHECK(status IN ('SUBMITTING','OPEN','PARTIAL','FILLED','CANCELLED','EXPIRED')),
    exchange_order_id TEXT  UNIQUE,   -- NULL until Coinbase accepts
    cancelled_at      TEXT,
    expired_at        TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id         TEXT    NOT NULL REFERENCES orders(id),
    exchange_fill_id TEXT    UNIQUE,  -- NULL for dry-run; real fill id prevents duplicates
    fill_price       REAL    NOT NULL,
    fill_qty_coins   REAL    NOT NULL,
    fill_qty_usd     REAL    NOT NULL,
    fee_usd          REAL    NOT NULL DEFAULT 0.0,
    is_taker         INTEGER NOT NULL DEFAULT 1,
    filled_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    id                      TEXT  PRIMARY KEY,   -- same as order_id
    order_id                TEXT  NOT NULL UNIQUE REFERENCES orders(id),
    epoch_id                TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset                   TEXT  NOT NULL,
    entry_price             REAL  NOT NULL,
    stop_price              REAL  NOT NULL,
    target_price            REAL  NOT NULL,
    qty_coins               REAL  NOT NULL,
    qty_usd                 REAL  NOT NULL,
    entry_fee_usd           REAL  NOT NULL DEFAULT 0.0,
    opened_at               TEXT  NOT NULL,
    status                  TEXT  NOT NULL DEFAULT 'OPEN'
                                  CHECK(status IN ('OPEN','CLOSED')),
    high_water_mark         REAL,
    extensions_used         INTEGER NOT NULL DEFAULT 0,
    extension_trailing_stop REAL,
    exit_price              REAL,
    exit_time               TEXT,
    exit_reason             TEXT,
    exit_fee_usd            REAL,
    pnl_usd                 REAL,
    pnl_pct                 REAL,
    closed_at               TEXT
);

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

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     TEXT    NOT NULL,
    completed_at   TEXT,
    status         TEXT    NOT NULL DEFAULT 'RUNNING'
                           CHECK(status IN ('RUNNING','COMPLETE','FAILED')),
    discrepancies  TEXT    NOT NULL DEFAULT '[]',
    actions_taken  TEXT    NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_orders_asset_status
    ON orders(asset, status);
CREATE INDEX IF NOT EXISTS idx_orders_epoch
    ON orders(epoch_id);
CREATE INDEX IF NOT EXISTS idx_fills_order
    ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_positions_epoch_status
    ON positions(epoch_id, status);
CREATE INDEX IF NOT EXISTS idx_position_events_position
    ON position_events(position_id);
"""


def init_db(path: Optional[Path] = None) -> None:
    """
    Create all tables and indexes if they do not exist.
    Uses a raw connection — executescript() commits any active transaction
    internally, so it must not run inside get_db()'s explicit BEGIN.
    """
    db_path = path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Epoch operations
# ---------------------------------------------------------------------------

def insert_epoch(
    epoch_id: str,
    paper_capital: float,
    reason: str,
    started_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Insert a new epoch record.
    Raises sqlite3.IntegrityError on duplicate epoch_id (UNIQUE PRIMARY KEY).
    """
    ts = started_at or datetime.now(timezone.utc).isoformat()
    sql = "INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at) VALUES (?,?,?,?)"
    if conn:
        conn.execute(sql, (epoch_id, paper_capital, reason, ts))
    else:
        with get_db() as c:
            c.execute(sql, (epoch_id, paper_capital, reason, ts))


def get_active_epoch(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Return the epoch with ended_at IS NULL (most recently started if multiple), or None."""
    return conn.execute(
        "SELECT * FROM risk_epochs WHERE ended_at IS NULL ORDER BY started_at DESC LIMIT 1"
    ).fetchone()


# ---------------------------------------------------------------------------
# Order operations
# ---------------------------------------------------------------------------

def insert_order(
    order_id: str,
    epoch_id: str,
    asset: str,
    limit_price: float,
    stop_price: float,
    target_price: float,
    qty_usd_requested: float,
    placed_at: str,
    expires_at: str,
    position_size_pct: Optional[float] = None,
    reasoning: str = "",
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert a new order in SUBMITTING state. Raises IntegrityError on duplicate id."""
    sql = """
        INSERT INTO orders(
            id, epoch_id, asset, limit_price, stop_price, target_price,
            position_size_pct, qty_usd_requested, placed_at, expires_at, reasoning, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'SUBMITTING')
    """
    args = (
        order_id, epoch_id, asset, limit_price, stop_price, target_price,
        position_size_pct, qty_usd_requested, placed_at, expires_at, reasoning,
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
    Raises ValueError if the transition is not allowed from the current state.
    Raises RuntimeError if the order is not found or already in a terminal state.
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
    """Return all OPEN or PARTIAL orders for the given asset."""
    return conn.execute(
        "SELECT * FROM orders WHERE asset=? AND status IN ('OPEN','PARTIAL')",
        (asset,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Fill operations (immutable — INSERT only, never UPDATE)
# ---------------------------------------------------------------------------

def insert_fill(
    order_id: str,
    fill_price: float,
    fill_qty_coins: float,
    fill_qty_usd: float,
    fee_usd: float = 0.0,
    is_taker: bool = True,
    filled_at: Optional[str] = None,
    exchange_fill_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    """
    Record a fill. exchange_fill_id UNIQUE prevents duplicate fills on repeated reconciliation.
    Returns the rowid of the inserted fill.
    """
    ts = filled_at or datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO fills(order_id, exchange_fill_id, fill_price, fill_qty_coins,
                          fill_qty_usd, fee_usd, is_taker, filled_at)
        VALUES (?,?,?,?,?,?,?,?)
    """
    args = (order_id, exchange_fill_id, fill_price, fill_qty_coins,
            fill_qty_usd, fee_usd, 1 if is_taker else 0, ts)
    if conn:
        conn.execute(sql, args)
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        with get_db() as c:
            c.execute(sql, args)
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def get_fills_for_order(order_id: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fills WHERE order_id=? ORDER BY filled_at", (order_id,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Position operations
# ---------------------------------------------------------------------------

def insert_position(
    position_id: str,
    order_id: str,
    epoch_id: str,
    asset: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    qty_coins: float,
    qty_usd: float,
    entry_fee_usd: float,
    opened_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Open a position. Raises IntegrityError if position already exists (UNIQUE order_id)."""
    ts = opened_at or datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO positions(
            id, order_id, epoch_id, asset, entry_price, stop_price, target_price,
            qty_coins, qty_usd, entry_fee_usd, opened_at, status, high_water_mark
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)
    """
    args = (position_id, order_id, epoch_id, asset, entry_price, stop_price,
            target_price, qty_coins, qty_usd, entry_fee_usd, ts, entry_price)
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
    Mark a position as CLOSED. Raises RuntimeError if not found or not OPEN.
    Records a CLOSED event in position_events.
    """
    ts = exit_time or datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        row = c.execute("SELECT status FROM positions WHERE id=?", (position_id,)).fetchone()
        if row is None:
            raise RuntimeError(f"Position '{position_id}' not found.")
        if row["status"] != "OPEN":
            raise RuntimeError(f"Position '{position_id}' is {row['status']!r}, not OPEN.")
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
    """Update trailing stop and high-water mark; append a STOP_UPDATED event."""
    ts = datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        c.execute(
            "UPDATE positions SET stop_price=?, high_water_mark=? WHERE id=? AND status='OPEN'",
            (new_stop, new_hwm, position_id),
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
            "SELECT * FROM positions WHERE asset=? AND status='OPEN'", (asset,)
        ).fetchall()
    return conn.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()


def get_epoch_closed_pnl(epoch_id: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return all CLOSED positions for the epoch, ordered by closed_at."""
    return conn.execute(
        "SELECT * FROM positions WHERE epoch_id=? AND status='CLOSED' ORDER BY closed_at",
        (epoch_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def start_reconciliation(conn: sqlite3.Connection) -> int:
    """Insert a RUNNING reconciliation record. Returns its rowid."""
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)", (ts, "RUNNING")
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def complete_reconciliation(
    run_id: int,
    discrepancies: list,
    actions_taken: list,
    conn: sqlite3.Connection,
) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    status = "FAILED" if discrepancies else "COMPLETE"
    conn.execute("""
        UPDATE reconciliation_runs
        SET completed_at=?, status=?, discrepancies=?, actions_taken=?
        WHERE id=?
    """, (ts, status, json.dumps(discrepancies), json.dumps(actions_taken), run_id))


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
    Idempotent: records that already exist (by primary key) are skipped.

    Returns counts: {"epochs": N, "orders": N, "positions": N}
    """
    _epochs  = epochs_jsonl  or (ROOT / "logs" / "risk_epochs.jsonl")
    _orders  = orders_json   or (ROOT / "logs" / "pending_orders.json")
    _history = history_jsonl or (ROOT / "logs" / "trade_history.jsonl")

    counts = {"epochs": 0, "orders": 0, "positions": 0}

    with get_db(db_path) as conn:
        # Migrate epochs
        if _epochs.exists():
            for line in _epochs.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("event") != "RISK_EPOCH_STARTED":
                    continue
                try:
                    conn.execute(
                        "INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)"
                        " VALUES (?,?,?,?)",
                        (rec["epoch_id"], rec["paper_capital"], rec.get("reason", ""),
                         rec.get("timestamp", datetime.now(timezone.utc).isoformat())),
                    )
                    counts["epochs"] += 1
                except sqlite3.IntegrityError:
                    pass  # already migrated

        # Migrate pending orders
        if _orders.exists():
            orders_data = json.loads(_orders.read_text(encoding="utf-8"))
            for o in orders_data:
                # Look up epoch_id from the order record (may be None for pre-epoch orders)
                epoch_id = o.get("epoch_id")
                if epoch_id is None:
                    # Can't migrate without epoch reference — skip
                    continue
                # Check epoch exists
                row = conn.execute(
                    "SELECT epoch_id FROM risk_epochs WHERE epoch_id=?", (epoch_id,)
                ).fetchone()
                if row is None:
                    continue  # orphan order — skip
                status_map = {"OPEN": "OPEN", "FILLED": "FILLED",
                              "CANCELLED": "CANCELLED", "EXPIRED": "EXPIRED"}
                status = status_map.get(o.get("status", ""), "OPEN")
                try:
                    conn.execute("""
                        INSERT INTO orders(
                            id, epoch_id, asset, limit_price, stop_price, target_price,
                            position_size_pct, qty_usd_requested, placed_at, expires_at,
                            reasoning, status, exchange_order_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        o["id"], epoch_id, o["asset"], o["limit_price"], o["stop_price"],
                        o["target_price"], o.get("position_size_pct"),
                        o.get("qty_usd_requested", o.get("limit_price", 0) * 0.02),
                        o["placed_at"], o["expires_at"], o.get("reasoning", ""),
                        status, o.get("exchange_order_id"),
                    ))
                    counts["orders"] += 1
                except sqlite3.IntegrityError:
                    pass

        # Migrate closed trade history into positions (approximate, no fills breakdown)
        if _history.exists():
            for line in _history.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                t = json.loads(line)
                epoch_id = t.get("epoch_id")
                if epoch_id is None:
                    continue  # pre-epoch trade — no DB representation needed
                pos_id = t["id"]
                # Position references an order — ensure order exists
                order_row = conn.execute(
                    "SELECT id FROM orders WHERE id=?", (pos_id,)
                ).fetchone()
                if order_row is None:
                    continue  # order not migrated — skip position
                try:
                    conn.execute("""
                        INSERT INTO positions(
                            id, order_id, epoch_id, asset, entry_price, stop_price,
                            target_price, qty_coins, qty_usd, entry_fee_usd,
                            opened_at, status, exit_price, exit_time, exit_reason,
                            exit_fee_usd, pnl_usd, pnl_pct, closed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        pos_id, pos_id, epoch_id, t["asset"],
                        t["entry_price"], t.get("stop_price", 0), t.get("target_price", 0),
                        t["qty_usd"] / t["entry_price"],   # approximate qty_coins
                        t["qty_usd"], t.get("entry_fee_usd", 0),
                        t.get("entry_time", t.get("closed_at_utc", "")),
                        "CLOSED",
                        t.get("exit_price"), t.get("exit_time"),
                        t.get("reason"), t.get("exit_fee_usd", 0),
                        t.get("pnl_usd"), t.get("pnl_pct"),
                        t.get("closed_at_utc", t.get("exit_time")),
                    ))
                    counts["positions"] += 1
                except sqlite3.IntegrityError:
                    pass

    return counts
