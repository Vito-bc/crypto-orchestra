"""
SQLite order/position/epoch ledger — single source of truth for all order state.
Schema V2.1 (PRAGMA user_version = 3).

Design:
  WAL mode + foreign_keys=ON + busy_timeout=10s on every connection.
  PRAGMA user_version tracks schema. run_migrations() applies sequential patches.
  Pre-wiring prototypes (v<3) are backed up and reset on first migration.
  Immutability triggers on fills and position_events (BEFORE UPDATE/DELETE → RAISE).
  Partial UNIQUE INDEXes enforce one active epoch and one running reconciliation.
  apply_fill() is the ONLY public path to record fills:
    ENTRY fill → VWAP aggregate → PARTIAL/FILLED transition → create/update position.
    EXIT fill  → exit VWAP → CLOSING/CLOSED transition → P&L calculation.
  trade_intents: durable stop/target written before Coinbase call; survives crash.
  start_epoch() checks for open exposure before transitioning epochs.
  start_reconciliation() is atomic via UNIQUE INDEX (no TOCTOU race).

Tables: risk_epochs, orders, fills, positions, trade_intents, position_events,
        account_snapshots, reconciliation_runs

Order state machine:
  SUBMITTING → OPEN → PARTIAL → FILLED  (normal fill path)
  SUBMITTING → OPEN → CANCELLED
  SUBMITTING → CANCELLED
  OPEN/PARTIAL → EXPIRED
  (FILLED, CANCELLED, EXPIRED are terminal)

Position state machine:
  OPEN → CLOSING (first exit fill, qty remaining > 0)
  OPEN/CLOSING → CLOSED (fully exited)
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import uuid as _uuid_mod
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

ROOT    = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logs" / "ledger.db"

SCHEMA_VERSION = 8

_TRANSITIONS: dict[str, set[str]] = {
    "SUBMITTING": {"OPEN", "CANCELLED", "REJECTED"},
    "OPEN":       {"PARTIAL", "FILLED", "CANCELLED", "EXPIRED"},
    "PARTIAL":    {"FILLED", "CANCELLED", "EXPIRED"},
    "FILLED":     set(),
    "CANCELLED":  set(),
    "EXPIRED":    set(),
    "REJECTED":   set(),
}

_TERMINAL_STATES = {"FILLED", "CANCELLED", "EXPIRED", "REJECTED"}


class LedgerConsistencyError(RuntimeError):
    """
    Raised when a fill would create an inconsistent ledger state that requires
    human/reconciliation intervention before trading can resume.
    The reconciliation run must flag the affected position/epoch as UNRESOLVED
    and halt all new orders until the discrepancy is resolved.
    """


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

@contextmanager
def get_db(
    path: Optional[Path] = None,
    begin_immediate: bool = False,
) -> Iterator[sqlite3.Connection]:
    """
    Yield a WAL-mode SQLite connection inside an explicit transaction.
    busy_timeout=10s handles transient database-locked errors.
    foreign_keys=ON enforced per-connection. Commits on clean exit; rolls back on error.

    begin_immediate=True: use BEGIN IMMEDIATE to acquire the write lock up-front.
    Required for operations that do a gate-check followed by an insert, to prevent
    another writer from slipping in between the check and the write.
    """
    db_path = path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("BEGIN IMMEDIATE" if begin_immediate else "BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema V3 (externally "V2.1")
# ---------------------------------------------------------------------------

# V6 full schema (used for fresh installs).  Renamed from _SCHEMA_V5 when V6 was
# introduced; kept as _SCHEMA_CURRENT going forward so the name tracks the version.
_SCHEMA_CURRENT = """
CREATE TABLE IF NOT EXISTS risk_epochs (
    epoch_id      TEXT    PRIMARY KEY,
    paper_capital REAL    NOT NULL CHECK(paper_capital > 0),
    reason        TEXT    NOT NULL,
    started_at    TEXT    NOT NULL,
    ended_at      TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id                      TEXT  PRIMARY KEY,
    epoch_id                TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset                   TEXT  NOT NULL,
    side                    TEXT  NOT NULL CHECK(side IN ('BUY','SELL')),
    order_type              TEXT  NOT NULL CHECK(order_type IN ('LIMIT','MARKET','STOP_LIMIT')),
    purpose                 TEXT  NOT NULL CHECK(purpose IN ('ENTRY','EXIT')),
    position_id             TEXT  REFERENCES positions(id),
    qty_base_requested      REAL  CHECK(qty_base_requested IS NULL OR qty_base_requested > 0),
    qty_usd_requested       REAL  CHECK(qty_usd_requested IS NULL OR qty_usd_requested > 0),
    limit_price             REAL,
    placed_at               TEXT  NOT NULL,
    expires_at              TEXT,
    reasoning               TEXT  NOT NULL DEFAULT '',
    status                  TEXT  NOT NULL DEFAULT 'SUBMITTING'
                                   CHECK(status IN ('SUBMITTING','OPEN','PARTIAL','FILLED','CANCELLED','EXPIRED','REJECTED')),
    exchange_order_id       TEXT  UNIQUE,
    cancelled_at            TEXT,
    expired_at              TEXT,
    rejected_at             TEXT,
    rejection_reason        TEXT,
    fills_finalized_at      TEXT,
    base_increment_applied  TEXT,
    CHECK((purpose = 'ENTRY') OR (purpose = 'EXIT' AND position_id IS NOT NULL))
);

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

CREATE TABLE IF NOT EXISTS positions (
    id                      TEXT  PRIMARY KEY,
    entry_order_id          TEXT  NOT NULL UNIQUE REFERENCES orders(id),
    epoch_id                TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
    asset                   TEXT  NOT NULL,
    entry_price             REAL,
    qty_base                REAL,
    qty_base_remaining      REAL,
    qty_usd                 REAL,
    entry_fee_usd           REAL  NOT NULL DEFAULT 0.0,
    opened_at               TEXT,
    stop_price              REAL,
    target_price            REAL,
    high_water_mark         REAL,
    extensions_used         INTEGER NOT NULL DEFAULT 0,
    extension_trailing_stop REAL,
    status                  TEXT  NOT NULL DEFAULT 'OPEN'
                                  CHECK(status IN ('OPEN','CLOSING','CLOSED','DUST')),
    exit_price              REAL,
    exit_time               TEXT,
    exit_reason             TEXT,
    exit_fee_usd            REAL,
    pnl_usd                 REAL,
    pnl_pct                 REAL,
    closed_at               TEXT
);

CREATE TABLE IF NOT EXISTS trade_intents (
    order_id     TEXT PRIMARY KEY REFERENCES orders(id),
    stop_price   REAL NOT NULL CHECK(stop_price > 0),
    target_price REAL NOT NULL CHECK(target_price > 0),
    recorded_at  TEXT NOT NULL
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
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT    NOT NULL,
    completed_at TEXT,
    status       TEXT    NOT NULL DEFAULT 'RUNNING'
                         CHECK(status IN ('RUNNING','COMPLETE','COMPLETE_WITH_ACTIONS','FAILED')),
    discovered   TEXT    NOT NULL DEFAULT '[]',
    resolved     TEXT    NOT NULL DEFAULT '[]',
    unresolved   TEXT    NOT NULL DEFAULT '[]'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_epoch
    ON risk_epochs(1) WHERE ended_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running_reconciliation
    ON reconciliation_runs(1) WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS idx_orders_asset_status    ON orders(asset, status);
CREATE INDEX IF NOT EXISTS idx_orders_epoch           ON orders(epoch_id);
CREATE INDEX IF NOT EXISTS idx_orders_position        ON orders(position_id) WHERE position_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_entry_per_asset
    ON orders(asset) WHERE purpose='ENTRY'
    AND status IN ('SUBMITTING','OPEN','PARTIAL');
CREATE INDEX IF NOT EXISTS idx_fills_order            ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_positions_epoch_status ON positions(epoch_id, status);
CREATE INDEX IF NOT EXISTS idx_unfinalized_terminal
    ON orders(id)
    WHERE status IN ('EXPIRED','CANCELLED')
    AND exchange_order_id IS NOT NULL
    AND fills_finalized_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_exit_per_position
    ON orders(position_id)
    WHERE purpose='EXIT'
    AND status IN ('SUBMITTING','OPEN','PARTIAL');
CREATE INDEX IF NOT EXISTS idx_position_events_pos    ON position_events(position_id);

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

# Backward-compat aliases — kept so existing tests can still import by version name.
_SCHEMA_V6 = _SCHEMA_CURRENT
_SCHEMA_V5 = _SCHEMA_CURRENT


# ---------------------------------------------------------------------------
# Schema versioning and migrations
# ---------------------------------------------------------------------------

def run_migrations(path: Optional[Path] = None) -> None:
    """
    Apply schema migrations. Called on every startup before any DB access.

    v0 (no DB or fresh file):       fresh install of V8 schema.
    v0 (file with user tables):     unversioned prototype — backup to .v0.bak, fresh V8.
    v1/v2/v3 (pre-outbox proto):    backup to .vN.bak, fresh V8.
    v4 (outbox, pre-stacking-guard): in-place — ADD COLUMN rejection_reason + index → V5.
    v5 (pre-terminal-finalization):  in-place — ADD COLUMN fills_finalized_at → V6.
    v6 (pre-exit-outbox):            in-place — ADD INDEX idx_one_active_exit_per_position → V7.
    v7 (pre-product-rules):          12-step positions table recreate (adds DUST status) +
                                     ADD COLUMN base_increment_applied on orders → V8.
    v8: already current — no-op.
    Migrations chain: V4→V5→V6→V7→V8 in a single run_migrations() call.

    Backup+reset is safe for v0–v3 (no live orders were ever in those DBs).
    In-place migrations v4→v5 and v5→v6 use ALTER TABLE ADD COLUMN (non-destructive).
    V7→V8: positions table recreated via 12-step rename (CHECK constraint change requires
    this), plus ALTER TABLE ADD COLUMN for orders.base_increment_applied.
    """
    db_path = path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version == SCHEMA_VERSION:
            return
        # Detect unversioned prototype: user_version=0 but user tables exist.
        has_user_tables = (
            version == 0
            and conn.execute(
                "SELECT COUNT(*) FROM sqlite_master"
                " WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchone()[0] > 0
        )
    finally:
        conn.close()

    # V4 → V5: non-destructive in-place migration.
    # Adds rejection_reason column and one-active-entry-per-asset UNIQUE index.
    # Wrapped in BEGIN IMMEDIATE so that all three DDL statements are atomic:
    # if the index cannot be created (e.g. existing duplicate active ENTRY rows),
    # the whole migration rolls back and user_version stays V4 — safe to retry.
    if version == 4:
        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Idempotent column add: skip if rejection_reason already exists
                # (handles the partial-failure / re-run scenario).
                existing_cols = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(orders)")
                }
                if "rejection_reason" not in existing_cols:
                    conn.execute(
                        "ALTER TABLE orders ADD COLUMN rejection_reason TEXT"
                    )

                # Detect conflicting rows before attempting to create the UNIQUE
                # index — a failed CREATE INDEX inside a transaction still rolls
                # back cleanly, but a clear error message is far more actionable.
                conflicts = conn.execute("""
                    SELECT asset, COUNT(*) AS cnt
                    FROM orders
                    WHERE purpose='ENTRY'
                      AND status IN ('SUBMITTING','OPEN','PARTIAL')
                    GROUP BY asset
                    HAVING COUNT(*) > 1
                """).fetchall()
                if conflicts:
                    details = ", ".join(
                        f"{r[0]} ({r[1]} orders)" for r in conflicts
                    )
                    raise RuntimeError(
                        f"V4→V5 migration blocked: duplicate active ENTRY orders "
                        f"exist for {details}. Cancel the duplicates manually "
                        "(leaving at most one SUBMITTING/OPEN/PARTIAL per asset) "
                        "then run again."
                    )

                # Idempotent index creation: skip if already present.
                idx_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master"
                    " WHERE type='index'"
                    "   AND name='idx_one_active_entry_per_asset'"
                ).fetchone()
                if not idx_exists:
                    conn.execute("""
                        CREATE UNIQUE INDEX idx_one_active_entry_per_asset
                            ON orders(asset)
                            WHERE purpose='ENTRY'
                              AND status IN ('SUBMITTING','OPEN','PARTIAL')
                    """)

                conn.execute("PRAGMA user_version = 5")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        version = 5  # fall through to V5→V6 migration

    # V5 → V6: adds fills_finalized_at column for terminal-order finalization.
    # Excludes finalized terminal orders from startup reconciliation, preventing
    # O(total-history) growth of Coinbase API calls on each bot restart.
    if version == 5:
        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing_cols = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(orders)")
                }
                if "fills_finalized_at" not in existing_cols:
                    conn.execute(
                        "ALTER TABLE orders ADD COLUMN fills_finalized_at TEXT"
                    )
                # Add partial index for unfinalized terminal orders if not present.
                idx_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master"
                    " WHERE type='index' AND name='idx_unfinalized_terminal'"
                ).fetchone()
                if not idx_exists:
                    conn.execute("""
                        CREATE INDEX idx_unfinalized_terminal ON orders(id)
                        WHERE status IN ('EXPIRED','CANCELLED')
                          AND exchange_order_id IS NOT NULL
                          AND fills_finalized_at IS NULL
                    """)
                conn.execute("PRAGMA user_version = 6")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        version = 6  # fall through to V6→V7 migration

    # V6 → V7: adds idx_one_active_exit_per_position partial unique index.
    # Enforces at most one active (SUBMITTING/OPEN/PARTIAL) EXIT order per position,
    # preventing double-sells after crashes or repeated scheduler ticks.
    if version == 6:
        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                idx_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master"
                    " WHERE type='index'"
                    "   AND name='idx_one_active_exit_per_position'"
                ).fetchone()
                if not idx_exists:
                    conn.execute("""
                        CREATE UNIQUE INDEX idx_one_active_exit_per_position
                            ON orders(position_id)
                            WHERE purpose='EXIT'
                              AND status IN ('SUBMITTING','OPEN','PARTIAL')
                    """)
                conn.execute("PRAGMA user_version = 7")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        version = 7  # fall through to V7→V8 migration

    # V7 → V8: product rules — two changes in one transaction:
    #   a) orders.base_increment_applied TEXT column (ALTER TABLE ADD COLUMN).
    #   b) positions.status CHECK constraint gains 'DUST' — requires 12-step rename
    #      because SQLite cannot ALTER TABLE to change a CHECK constraint in place.
    #      foreign_keys=OFF required during DROP TABLE to suppress FK referential
    #      checks from orders and position_events.
    if version == 7:
        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("BEGIN IMMEDIATE")
            try:
                # a) ADD COLUMN to orders (idempotent).
                existing_cols = {
                    row[1] for row in conn.execute("PRAGMA table_info(orders)")
                }
                if "base_increment_applied" not in existing_cols:
                    conn.execute(
                        "ALTER TABLE orders ADD COLUMN base_increment_applied TEXT"
                    )

                # b) Recreate positions with DUST in CHECK constraint — only if the
                # table exists.  Older migration test fixtures (V4 chain) may reach
                # V7 without a positions table; skip recreation in that case.
                _positions_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='positions'"
                ).fetchone() is not None

                if _positions_exists:
                    conn.execute("""
                        CREATE TABLE positions_new (
                            id                      TEXT  PRIMARY KEY,
                            entry_order_id          TEXT  NOT NULL UNIQUE REFERENCES orders(id),
                            epoch_id                TEXT  NOT NULL REFERENCES risk_epochs(epoch_id),
                            asset                   TEXT  NOT NULL,
                            entry_price             REAL,
                            qty_base                REAL,
                            qty_base_remaining      REAL,
                            qty_usd                 REAL,
                            entry_fee_usd           REAL  NOT NULL DEFAULT 0.0,
                            opened_at               TEXT,
                            stop_price              REAL,
                            target_price            REAL,
                            high_water_mark         REAL,
                            extensions_used         INTEGER NOT NULL DEFAULT 0,
                            extension_trailing_stop REAL,
                            status                  TEXT  NOT NULL DEFAULT 'OPEN'
                                                          CHECK(status IN ('OPEN','CLOSING','CLOSED','DUST')),
                            exit_price              REAL,
                            exit_time               TEXT,
                            exit_reason             TEXT,
                            exit_fee_usd            REAL,
                            pnl_usd                 REAL,
                            pnl_pct                 REAL,
                            closed_at               TEXT
                        )
                    """)
                    # Copy column-by-column: older schemas may have fewer columns.
                    # NOT NULL columns absent from the old table get their defaults.
                    _old_pos_cols = {
                        row[1] for row in conn.execute("PRAGMA table_info(positions)")
                    }
                    _not_null_defaults = {
                        "entry_fee_usd": "0.0",
                        "extensions_used": "0",
                    }
                    _new_pos_cols = [
                        "id", "entry_order_id", "epoch_id", "asset", "entry_price",
                        "qty_base", "qty_base_remaining", "qty_usd", "entry_fee_usd",
                        "opened_at", "stop_price", "target_price", "high_water_mark",
                        "extensions_used", "extension_trailing_stop", "status",
                        "exit_price", "exit_time", "exit_reason", "exit_fee_usd",
                        "pnl_usd", "pnl_pct", "closed_at",
                    ]
                    _select_parts = [
                        c if c in _old_pos_cols
                        else _not_null_defaults.get(c, "NULL")
                        for c in _new_pos_cols
                    ]
                    conn.execute(
                        f"INSERT INTO positions_new ({', '.join(_new_pos_cols)}) "
                        f"SELECT {', '.join(_select_parts)} FROM positions"
                    )
                    conn.execute("DROP TABLE positions")
                    conn.execute("ALTER TABLE positions_new RENAME TO positions")

                    # Re-create the index that was on the old positions table.
                    _idx_exists = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='index'"
                        " AND name='idx_positions_epoch_status'"
                    ).fetchone()
                    if not _idx_exists:
                        conn.execute(
                            "CREATE INDEX idx_positions_epoch_status"
                            " ON positions(epoch_id, status)"
                        )

                    # Verify no FK violations were introduced.
                    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                    if violations:
                        raise RuntimeError(
                            f"V7→V8 migration: FK violations after positions recreate: {violations}"
                        )

                conn.execute("PRAGMA user_version = 8")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.close()
        return

    # V0/V1/V2/V3: backup + fresh install.
    is_legacy = (0 < version < 4) or (version == 0 and has_user_tables)
    if is_legacy and db_path.exists():
        backup = db_path.with_suffix(f".v{version}.bak")
        shutil.copy2(str(db_path), str(backup))
        db_path.unlink()

    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA_CURRENT)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    finally:
        conn.close()


def init_db(path: Optional[Path] = None) -> None:
    """Alias for run_migrations() — keeps existing callers working."""
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
    For live use, call start_epoch() which atomically guards and closes the previous.
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
    Live epoch transition. Atomically:
      1. Verifies no open orders, positions, or running reconciliation.
      2. Closes the current active epoch (UPDATE ended_at).
      3. Inserts the new active epoch.

    Raises ValueError on exposure or duplicate epoch_id.
    """
    def _run(c: sqlite3.Connection) -> None:
        open_positions = c.execute(
            "SELECT COUNT(*) FROM positions WHERE status IN ('OPEN','CLOSING','DUST')"
        ).fetchone()[0]
        if open_positions:
            raise ValueError(
                f"Cannot start new epoch: {open_positions} open/closing/dust position(s). "
                "Close all positions first (DUST positions require manual write-off)."
            )
        open_orders = c.execute(
            "SELECT COUNT(*) FROM orders WHERE status IN ('SUBMITTING','OPEN','PARTIAL')"
        ).fetchone()[0]
        if open_orders:
            raise ValueError(
                f"Cannot start new epoch: {open_orders} open order(s). "
                "Cancel all orders first."
            )
        running_recon = c.execute(
            "SELECT COUNT(*) FROM reconciliation_runs WHERE status='RUNNING'"
        ).fetchone()[0]
        if running_recon:
            raise ValueError(
                "Cannot start new epoch: a reconciliation run is in progress."
            )
        ts = datetime.now(timezone.utc).isoformat()
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
    return conn.execute("SELECT * FROM risk_epochs WHERE ended_at IS NULL").fetchone()


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
    base_increment_applied: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Insert a new order in SUBMITTING state.
    At least one of qty_base_requested / qty_usd_requested must be provided.
    EXIT orders must have a position_id (enforced here and by CHECK constraint).
    base_increment_applied: the exchange step string used to ROUND_DOWN qty_base_requested
    (e.g. "0.00000001"). Stored for audit; None for ENTRY orders or legacy paths.
    """
    if qty_base_requested is None and qty_usd_requested is None:
        raise ValueError(f"Order '{order_id}': must set qty_base_requested or qty_usd_requested.")
    if purpose == "EXIT" and position_id is None:
        raise ValueError(f"Order '{order_id}': EXIT orders require a position_id.")
    sql = """
        INSERT INTO orders(
            id, epoch_id, asset, side, order_type, purpose, position_id,
            qty_base_requested, qty_usd_requested, limit_price,
            placed_at, expires_at, reasoning, status, base_increment_applied
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'SUBMITTING',?)
    """
    args = (
        order_id, epoch_id, asset, side, order_type, purpose, position_id,
        qty_base_requested, qty_usd_requested, limit_price,
        placed_at, expires_at, reasoning, base_increment_applied,
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
    def _run(c: sqlite3.Connection) -> None:
        row = c.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
        if row is None:
            raise RuntimeError(f"Order '{order_id}' not found.")
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
        elif new_status == "REJECTED":
            extra["rejected_at"] = now
        set_clause = ", ".join(f"{k}=?" for k in extra)
        c.execute(f"UPDATE orders SET {set_clause} WHERE id=?", (*extra.values(), order_id))

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def get_open_orders_for_asset(asset: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM orders WHERE asset=? AND status IN ('OPEN','PARTIAL')", (asset,)
    ).fetchall()


def get_open_orders_for_position(
    position_id: str, conn: sqlite3.Connection
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM orders WHERE position_id=?"
        " AND status NOT IN ('FILLED','CANCELLED','EXPIRED')",
        (position_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Trade intents — durable stop/target before first fill
# ---------------------------------------------------------------------------

def insert_trade_intent(
    order_id: str,
    stop_price: float,
    target_price: float,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Record intended stop/target at order-placement time.
    Must be written in the same transaction as insert_order(), before the Coinbase call.
    apply_fill() reads this to populate position.stop_price / target_price on first fill.
    """
    ts = datetime.now(timezone.utc).isoformat()
    sql = "INSERT INTO trade_intents(order_id, stop_price, target_price, recorded_at) VALUES (?,?,?,?)"
    if conn:
        conn.execute(sql, (order_id, stop_price, target_price, ts))
    else:
        with get_db() as c:
            c.execute(sql, (order_id, stop_price, target_price, ts))


def get_trade_intent(order_id: str, conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM trade_intents WHERE order_id=?", (order_id,)
    ).fetchone()


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
    reconciliation_mode: bool = False,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """
    Atomically apply a fill to an order.

    ENTRY orders:
      INSERT fill → VWAP aggregate → PARTIAL/FILLED transition →
      create position (first fill, reads trade_intent for stop/target) or
      update VWAP/remaining (subsequent fills, accounting for exits).

    EXIT orders:
      INSERT fill → aggregate all exit fills for the position →
      update qty_base_remaining →
      OPEN/CLOSING → CLOSING (partial) or CLOSED (fully exited) →
      compute P&L on close.

    Idempotency: if exchange_fill_id already exists for THIS order, returns
    current state without error. If the same exchange_fill_id is submitted for
    a DIFFERENT order, raises RuntimeError (Coinbase/local mismatch).

    reconciliation_mode=True: allows fills on CANCELLED or EXPIRED orders —
    used when reconciliation discovers Coinbase executed a fill that was locally
    marked terminal (e.g. partial fill before cancel). Order status is NOT
    changed; the fill is recorded and position is created/updated. The
    reconciliation run is responsible for flagging the discrepancy.

    Returns {"status": order_status, "position_id": str or None,
             "reconciliation": bool, "replayed": bool (True only on idempotent replay)}.
    Raises RuntimeError on order not found, SUBMITTING state, or terminal state
    (unless reconciliation_mode=True).
    """
    ts = filled_at or datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> dict:
        # Early idempotency check
        if exchange_fill_id:
            existing_fill = c.execute(
                "SELECT order_id FROM fills WHERE exchange_fill_id=?", (exchange_fill_id,)
            ).fetchone()
            if existing_fill:
                if existing_fill["order_id"] != order_id:
                    raise RuntimeError(
                        f"exchange_fill_id '{exchange_fill_id}' was previously recorded "
                        f"for order '{existing_fill['order_id']}', not '{order_id}'. "
                        "Possible Coinbase/local order-ID mismatch — halt and investigate."
                    )
                # Same fill replayed for same order — return current state.
                # Position lookup differs: ENTRY orders are referenced by positions via
                # entry_order_id; EXIT orders carry position_id directly on the order.
                order = c.execute(
                    "SELECT status, purpose, position_id FROM orders WHERE id=?", (order_id,)
                ).fetchone()
                if order and order["purpose"] == "EXIT":
                    resolved_pos_id = order["position_id"]
                else:
                    row = c.execute(
                        "SELECT id FROM positions WHERE entry_order_id=?", (order_id,)
                    ).fetchone()
                    resolved_pos_id = row["id"] if row else None
                return {
                    "status": order["status"] if order else "UNKNOWN",
                    "position_id": resolved_pos_id,
                    "reconciliation": reconciliation_mode,
                    "replayed": True,
                }

        order = c.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if order is None:
            raise RuntimeError(f"Order '{order_id}' not found.")
        if order["status"] in _TERMINAL_STATES and not reconciliation_mode:
            raise RuntimeError(f"Cannot fill {order['status']!r} order '{order_id}'.")
        if order["status"] == "SUBMITTING":
            raise RuntimeError(
                f"Order '{order_id}' is SUBMITTING — call transition_order(..., 'OPEN') first."
            )

        # Insert fill
        c.execute("""
            INSERT INTO fills(order_id, exchange_fill_id, fill_price, fill_qty_base,
                              fill_qty_usd, fee_usd, is_taker, filled_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (order_id, exchange_fill_id, fill_price, fill_qty_base,
               fill_price * fill_qty_base, fee_usd, 1 if is_taker else 0, ts))

        # VWAP + totals for THIS order
        agg = c.execute("""
            SELECT SUM(fill_price * fill_qty_base) / SUM(fill_qty_base) AS vwap,
                   SUM(fill_qty_base) AS total_base,
                   SUM(fill_qty_usd)  AS total_usd,
                   SUM(fee_usd)       AS total_fee
            FROM fills WHERE order_id=?
        """, (order_id,)).fetchone()

        # In reconciliation_mode the order status is authoritative on Coinbase, not locally.
        # Keep the terminal status; only normal-path fills transition the order.
        if order["status"] in _TERMINAL_STATES:
            new_status = order["status"]  # reconciliation_mode: leave status unchanged
        else:
            req_base = order["qty_base_requested"]
            req_usd  = order["qty_usd_requested"]
            is_complete = False
            if req_base is not None:
                is_complete = agg["total_base"] >= req_base * 0.999
            elif req_usd is not None:
                is_complete = agg["total_usd"] >= req_usd * 0.999
            new_status = "FILLED" if is_complete else "PARTIAL"
            c.execute("UPDATE orders SET status=? WHERE id=?", (new_status, order_id))

        position_id = None

        # ----- ENTRY fill -----
        if order["purpose"] == "ENTRY":
            existing = c.execute(
                "SELECT id FROM positions WHERE entry_order_id=?", (order_id,)
            ).fetchone()
            if existing is None:
                # Resolve stop/target: explicit args > trade_intent > NULL
                intent = c.execute(
                    "SELECT stop_price, target_price FROM trade_intents WHERE order_id=?",
                    (order_id,),
                ).fetchone()
                _stop   = stop_price   if stop_price   is not None else (intent["stop_price"]   if intent else None)
                _target = target_price if target_price is not None else (intent["target_price"] if intent else None)
                position_id = str(_uuid_mod.uuid4())
                c.execute("""
                    INSERT INTO positions(
                        id, entry_order_id, epoch_id, asset,
                        entry_price, qty_base, qty_base_remaining, qty_usd,
                        entry_fee_usd, opened_at,
                        stop_price, target_price, high_water_mark, status
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')
                """, (
                    position_id, order_id, order["epoch_id"], order["asset"],
                    agg["vwap"], agg["total_base"], agg["total_base"], agg["total_usd"],
                    agg["total_fee"], ts, _stop, _target, agg["vwap"],
                ))
                c.execute(
                    "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
                    " VALUES (?,?,?,?)",
                    (position_id, "OPENED",
                     json.dumps({"entry_price": agg["vwap"], "qty_base": agg["total_base"]}), ts),
                )
            else:
                position_id = existing["id"]
                pos_status = c.execute(
                    "SELECT status FROM positions WHERE id=?", (position_id,)
                ).fetchone()["status"]
                # Guard: if the position is already CLOSED, a late fill would create
                # real open exposure invisible to position tracking. Silently updating
                # qty_base on a CLOSED position is the most dangerous silent corruption
                # possible. Raise and let the reconciler flag this as UNRESOLVED.
                if pos_status == "CLOSED":
                    raise LedgerConsistencyError(
                        f"Late ENTRY fill for order '{order_id}' arrived after position "
                        f"'{position_id}' was CLOSED. Ledger has real open exposure that "
                        "is not tracked. Flag this run as UNRESOLVED and halt new orders "
                        "on this epoch until the discrepancy is resolved manually."
                    )
                # Compute remaining = total_entry - already_exited.
                # Must NOT just use total_entry: position may be partially exited.
                total_exit = c.execute("""
                    SELECT COALESCE(SUM(f.fill_qty_base), 0)
                    FROM fills f
                    JOIN orders o ON f.order_id = o.id
                    WHERE o.position_id=? AND o.purpose='EXIT'
                """, (position_id,)).fetchone()[0]
                remaining = max(0.0, agg["total_base"] - total_exit)
                c.execute("""
                    UPDATE positions SET
                        entry_price=?, qty_base=?, qty_base_remaining=?,
                        qty_usd=?, entry_fee_usd=?
                    WHERE id=?
                """, (agg["vwap"], agg["total_base"], remaining,
                      agg["total_usd"], agg["total_fee"], position_id))
                if pos_status == "DUST":
                    # Late ENTRY fill increased qty — position is no longer
                    # effectively unsellable; transition to CLOSING so the exit
                    # executor re-evaluates it on the next tick.
                    c.execute(
                        "UPDATE positions SET status='CLOSING'"
                        " WHERE id=? AND status='DUST'",
                        (position_id,),
                    )
                    c.execute(
                        "INSERT INTO position_events"
                        "(position_id, event_type, payload, occurred_at)"
                        " VALUES (?,?,?,?)",
                        (position_id, "DUST_REVIVED",
                         json.dumps({"reason": "late_entry_fill_increased_qty",
                                     "new_qty_base": remaining}), ts),
                    )

        # ----- EXIT fill -----
        elif order["purpose"] == "EXIT":
            pos_id = order["position_id"]
            pos = c.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
            if pos is None:
                raise RuntimeError(
                    f"EXIT order '{order_id}' references position '{pos_id}' which does not exist."
                )
            if pos["status"] == "CLOSED":
                raise RuntimeError(
                    f"Cannot fill EXIT order '{order_id}': position '{pos_id}' is already CLOSED."
                )

            # Aggregate ALL exit fills for this position (across all exit orders)
            exit_agg = c.execute("""
                SELECT SUM(f.fill_price * f.fill_qty_base) / SUM(f.fill_qty_base) AS exit_vwap,
                       SUM(f.fill_qty_base) AS total_exit_base,
                       SUM(f.fee_usd)       AS total_exit_fee
                FROM fills f
                JOIN orders o ON f.order_id = o.id
                WHERE o.position_id=? AND o.purpose='EXIT'
            """, (pos_id,)).fetchone()

            entry_qty = pos["qty_base"] or 0.0
            exit_qty  = exit_agg["total_exit_base"] or 0.0

            # Validate: exit quantity must not exceed entry quantity
            if exit_qty > entry_qty * 1.001:
                raise RuntimeError(
                    f"EXIT fill would overfill position '{pos_id}': "
                    f"total_exit={exit_qty:.8f} > entry_qty={entry_qty:.8f}."
                )

            remaining = max(0.0, entry_qty - exit_qty)
            is_closed = remaining < 1e-9  # below 0.1 satoshi — float epsilon only
            new_pos_status = "CLOSED" if is_closed else "CLOSING"

            if is_closed:
                entry_cost = (pos["entry_price"] or 0.0) * entry_qty
                exit_vwap  = exit_agg["exit_vwap"] or fill_price
                pnl_usd = (
                    (exit_vwap - (pos["entry_price"] or 0.0)) * entry_qty
                    - (exit_agg["total_exit_fee"] or 0.0)
                    - pos["entry_fee_usd"]
                )
                pnl_pct = (pnl_usd / entry_cost * 100) if entry_cost else 0.0
                c.execute("""
                    UPDATE positions SET
                        status='CLOSED', qty_base_remaining=0,
                        exit_price=?, exit_time=?, exit_reason=?,
                        exit_fee_usd=?, pnl_usd=?, pnl_pct=?, closed_at=?
                    WHERE id=?
                """, (exit_vwap, ts, order["reasoning"] or "EXIT_FILL",
                      exit_agg["total_exit_fee"], pnl_usd, pnl_pct, ts, pos_id))
                c.execute(
                    "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
                    " VALUES (?,?,?,?)",
                    (pos_id, "CLOSED",
                     json.dumps({"exit_price": exit_vwap, "pnl_usd": pnl_usd,
                                 "exit_reason": order["reasoning"]}), ts),
                )
            else:
                c.execute(
                    "UPDATE positions SET status='CLOSING', qty_base_remaining=? WHERE id=?",
                    (remaining, pos_id),
                )
                c.execute(
                    "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
                    " VALUES (?,?,?,?)",
                    (pos_id, "PARTIAL_EXIT",
                     json.dumps({"exit_qty": exit_qty, "remaining": remaining}), ts),
                )
            position_id = pos_id

        return {
            "status": new_status,
            "position_id": position_id,
            "reconciliation": reconciliation_mode,
            "replayed": False,
        }

    if conn:
        # Use a SAVEPOINT so that any exception inside _run() rolls back only
        # the fill's writes, not the entire outer transaction.  This guarantees
        # atomicity even when the caller catches LedgerConsistencyError (or any
        # other error) inside their own `with get_db()` block.
        sp = f"af_{_uuid_mod.uuid4().hex[:8]}"
        conn.execute(f"SAVEPOINT {sp}")
        try:
            result = _run(conn)
            conn.execute(f"RELEASE SAVEPOINT {sp}")
            return result
        except Exception:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
            raise
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
    """Direct position insert for migration and reconciliation. Prefer apply_fill() for normal fills."""
    ts = opened_at or datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO positions(
            id, entry_order_id, epoch_id, asset,
            entry_price, qty_base, qty_base_remaining, qty_usd, entry_fee_usd,
            opened_at, stop_price, target_price, high_water_mark, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    if conn:
        conn.execute(sql, (
            position_id, entry_order_id, epoch_id, asset,
            entry_price, qty_base, qty_base, qty_usd, entry_fee_usd,
            ts, stop_price, target_price, entry_price, status,
        ))
    else:
        with get_db() as c:
            c.execute(sql, (
                position_id, entry_order_id, epoch_id, asset,
                entry_price, qty_base, qty_base, qty_usd, entry_fee_usd,
                ts, stop_price, target_price, entry_price, status,
            ))


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
    Direct position close — for manual and reconciliation use.
    Normal path: close via apply_fill() on the EXIT order.
    Raises RuntimeError if not found or not OPEN/CLOSING.
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
                status='CLOSED', qty_base_remaining=0,
                exit_price=?, exit_time=?, exit_reason=?,
                exit_fee_usd=?, pnl_usd=?, pnl_pct=?, closed_at=?
            WHERE id=?
        """, (exit_price, ts, exit_reason, exit_fee_usd, pnl_usd, pnl_pct, ts, position_id))
        c.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at) VALUES (?,?,?,?)",
            (position_id, "CLOSED",
             json.dumps({"exit_price": exit_price, "exit_reason": exit_reason, "pnl_usd": pnl_usd}), ts),
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
    Update trailing stop and HWM for an OPEN or CLOSING position.
    CLOSING positions may still have qty_base_remaining > 0 (partial exit),
    so trailing stop updates remain relevant.
    Raises RuntimeError if position not found or already CLOSED.
    STOP_UPDATED event only written after a successful rowcount check.
    """
    ts = datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        c.execute(
            "UPDATE positions SET stop_price=?, high_water_mark=?"
            " WHERE id=? AND status IN ('OPEN','CLOSING')",
            (new_stop, new_hwm, position_id),
        )
        if c.execute("SELECT changes()").fetchone()[0] == 0:
            row = c.execute("SELECT status FROM positions WHERE id=?", (position_id,)).fetchone()
            if row is None:
                raise RuntimeError(f"Position '{position_id}' not found.")
            raise RuntimeError(
                f"Position '{position_id}' is {row['status']!r} — "
                "stop update only on OPEN or CLOSING."
            )
        c.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at) VALUES (?,?,?,?)",
            (position_id, "STOP_UPDATED",
             json.dumps({"new_stop": new_stop, "new_hwm": new_hwm}), ts),
        )

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def update_position_extensions(
    position_id: str,
    extensions_used: int,
    extension_trailing_stop: Optional[float] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Persist hold-extension count and optional ATR trailing stop after an extension review.
    Called when the extension callback grants another hold window.
    Raises RuntimeError if position not found or not OPEN/CLOSING.
    """
    ts = datetime.now(timezone.utc).isoformat()

    def _run(c: sqlite3.Connection) -> None:
        c.execute(
            "UPDATE positions SET extensions_used=?, extension_trailing_stop=?"
            " WHERE id=? AND status IN ('OPEN','CLOSING')",
            (extensions_used, extension_trailing_stop, position_id),
        )
        if c.execute("SELECT changes()").fetchone()[0] == 0:
            row = c.execute("SELECT status FROM positions WHERE id=?", (position_id,)).fetchone()
            if row is None:
                raise RuntimeError(f"Position '{position_id}' not found.")
            raise RuntimeError(
                f"Position '{position_id}' is {row['status']!r} — "
                "extension update only on OPEN or CLOSING."
            )
        c.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at) VALUES (?,?,?,?)",
            (position_id, "HOLD_EXTENDED",
             json.dumps({"extensions_used": extensions_used,
                         "extension_trailing_stop": extension_trailing_stop}), ts),
        )

    if conn:
        _run(conn)
    else:
        with get_db() as c:
            _run(c)


def transition_position_to_dust(
    position_id: str,
    conn: sqlite3.Connection,
) -> None:
    """
    Mark an OPEN or CLOSING position as DUST.

    Called when qty_base_remaining rounds DOWN to zero (or below base_min_size)
    under the exchange's base_increment rules — the position can no longer be
    exited because no valid SELL qty exists.  DUST is real open exposure:
    it blocks new ENTRY orders for the asset and prevents epoch transitions.

    Records a DUST_SETTLED position_event for audit.
    Raises RuntimeError if position not found or not OPEN/CLOSING.
    """
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE positions SET status='DUST' WHERE id=? AND status IN ('OPEN','CLOSING')",
        (position_id,),
    )
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        row = conn.execute(
            "SELECT status FROM positions WHERE id=?", (position_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Position '{position_id}' not found.")
        raise RuntimeError(
            f"Position '{position_id}' is {row['status']!r} — "
            "DUST transition only allowed from OPEN or CLOSING."
        )
    conn.execute(
        "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
        " VALUES (?,?,?,?)",
        (position_id, "DUST_SETTLED",
         json.dumps({"reason": "qty_base_remaining below base_min_size"}), ts),
    )


def get_open_positions_for_asset(
    asset: Optional[str], conn: sqlite3.Connection
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

    First recovers any stale RUNNING run (older than threshold → FAILED).
    Then inserts a new RUNNING run. The partial UNIQUE INDEX on (1) WHERE status='RUNNING'
    prevents two concurrent RUNNING runs atomically — no TOCTOU race.

    Raises RuntimeError if a recent RUNNING run already exists.
    """
    now = datetime.now(timezone.utc)
    stale_cutoff = (now - timedelta(minutes=stale_threshold_minutes)).isoformat()
    conn.execute(
        "UPDATE reconciliation_runs SET status='FAILED', completed_at=?"
        " WHERE status='RUNNING' AND started_at < ?",
        (now.isoformat(), stale_cutoff),
    )
    try:
        conn.execute(
            "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)",
            (now.isoformat(), "RUNNING"),
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError:
        running = conn.execute(
            "SELECT id, started_at FROM reconciliation_runs WHERE status='RUNNING' LIMIT 1"
        ).fetchone()
        raise RuntimeError(
            f"Reconciliation run #{running['id'] if running else '?'} is already RUNNING "
            f"(started {running['started_at'] if running else 'unknown'}). "
            "Wait for it to complete or investigate."
        )


def complete_reconciliation(
    run_id: int,
    discovered: list,
    resolved: list,
    unresolved: list,
    conn: sqlite3.Connection,
) -> None:
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
        WHERE id=? AND status='RUNNING'
    """, (ts, status, json.dumps(discovered), json.dumps(resolved),
          json.dumps(unresolved), run_id))
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        raise RuntimeError(
            f"complete_reconciliation: run #{run_id} not found or not in RUNNING state."
        )


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

    Idempotency: existence check before every INSERT; only confirmed new rows are counted.
    Non-idempotent IntegrityErrors (e.g. conflicting exchange_order_id) raise RuntimeError.
    Pre-epoch records (epoch_id is None) are skipped — no representation in the ledger.
    Multiple epochs in JSONL: all but last get ended_at set to the next epoch's started_at.

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
            if conn.execute(
                "SELECT 1 FROM risk_epochs WHERE epoch_id=?", (rec["epoch_id"],)
            ).fetchone():
                continue
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
            for o in json.loads(_orders.read_text(encoding="utf-8")):
                epoch_id = o.get("epoch_id")
                if epoch_id is None:
                    continue
                if not conn.execute(
                    "SELECT 1 FROM risk_epochs WHERE epoch_id=?", (epoch_id,)
                ).fetchone():
                    continue
                if conn.execute("SELECT 1 FROM orders WHERE id=?", (o["id"],)).fetchone():
                    continue
                status_map = {"OPEN": "OPEN", "FILLED": "FILLED",
                              "CANCELLED": "CANCELLED", "EXPIRED": "EXPIRED"}
                try:
                    conn.execute("""
                        INSERT INTO orders(
                            id, epoch_id, asset, side, order_type, purpose,
                            qty_usd_requested, limit_price,
                            placed_at, expires_at, reasoning, status, exchange_order_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        o["id"], epoch_id, o["asset"], "BUY", "LIMIT", "ENTRY",
                        o.get("qty_usd_requested"),
                        o.get("limit_price"),
                        o["placed_at"], o.get("expires_at"), o.get("reasoning", ""),
                        status_map.get(o.get("status", ""), "OPEN"),
                        o.get("exchange_order_id"),
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
                    continue
                if not conn.execute("SELECT 1 FROM orders WHERE id=?", (t["id"],)).fetchone():
                    continue
                if conn.execute("SELECT 1 FROM positions WHERE id=?", (t["id"],)).fetchone():
                    continue
                entry_price = t.get("entry_price")
                qty_usd     = t.get("qty_usd")
                qty_base    = (qty_usd / entry_price) if (qty_usd and entry_price) else None
                try:
                    conn.execute("""
                        INSERT INTO positions(
                            id, entry_order_id, epoch_id, asset,
                            entry_price, qty_base, qty_base_remaining, qty_usd,
                            entry_fee_usd, opened_at, status,
                            exit_price, exit_time, exit_reason,
                            exit_fee_usd, pnl_usd, pnl_pct, closed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        t["id"], t["id"], epoch_id, t["asset"],
                        entry_price, qty_base, 0.0, qty_usd,
                        t.get("entry_fee_usd", 0),
                        t.get("entry_time", t.get("closed_at_utc", "")),
                        "CLOSED",
                        t.get("exit_price"), t.get("exit_time"), t.get("reason"),
                        t.get("exit_fee_usd", 0), t.get("pnl_usd"), t.get("pnl_pct"),
                        t.get("closed_at_utc", t.get("exit_time")),
                    ))
                    counts["positions"] += 1
                except sqlite3.IntegrityError as e:
                    raise RuntimeError(
                        f"Migration: unexpected IntegrityError for position {t.get('id')!r}: {e}"
                    ) from e

    return counts
