"""
Project-wide pytest fixtures.

autouse fixtures here apply to every test in this directory, providing
safety guards that prevent tests from touching production resources.
"""

from __future__ import annotations

import builtins
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

# The repository's real logs/ directory — never a tmp_path, never a fixture
# double. Computed once here rather than imported from pipeline.runner so the
# guard below does not depend on any module having patched its own copy.
REAL_LOG_DIR = (Path(__file__).resolve().parents[1] / "logs").resolve()

_WRITE_MODE_CHARS = frozenset("wax+")


def _is_write_mode(mode) -> bool:
    if not isinstance(mode, str):
        return True  # non-str mode (e.g. an int flag from os.open) — assume writable
    return any(c in _WRITE_MODE_CHARS for c in mode)


def _is_real_log_path(candidate) -> bool:
    try:
        resolved = Path(candidate).resolve()
    except (OSError, ValueError, TypeError):
        return False
    return resolved == REAL_LOG_DIR or REAL_LOG_DIR in resolved.parents


@pytest.fixture(autouse=True)
def _guard_real_logs_dir(monkeypatch):
    """
    Fail loudly if any test writes to the repository's REAL logs/ directory.

    2026-09-18: logs/agent_decisions.jsonl was found polluted with 36
    synthetic records written by this suite through the real
    pipeline.runner._log_order_event. Tests correctly patch POSITIONS_FILE /
    ORDERS_FILE / TRADE_HISTORY, but runner.DECISIONS_LOG (and
    runner._SIGNALS_DB) were never patched by the offending tests, so every
    run appended straight into the production log.

    Rather than trust every current and future test module to remember to
    patch its own module-level log-path constant, this fixture is a blanket
    backstop: it intercepts file/DB writes at the point they touch disk
    (Path.open, builtins.open, sqlite3.connect) and raises immediately if the
    target resolves inside the real logs/ directory. It fails LOUDLY — it does
    not silently redirect the write, because a silent redirect would hide the
    next unpatched constant the same way this one was hidden.

    A test that legitimately needs to read a committed fixture from outside
    tmp_path is unaffected — only write-mode opens are checked.
    """
    real_path_open = Path.open
    real_builtins_open = builtins.open
    real_sqlite_connect = sqlite3.connect

    def guarded_path_open(self, mode="r", *args, **kwargs):
        if _is_write_mode(mode) and _is_real_log_path(self):
            raise AssertionError(
                f"Test attempted to open a REAL logs/ path for writing: {self} "
                f"(mode={mode!r}). Patch the module-level constant to a "
                "tmp_path instead of letting it fall through to the real path."
            )
        return real_path_open(self, mode, *args, **kwargs)

    def guarded_builtins_open(file, mode="r", *args, **kwargs):
        if _is_write_mode(mode) and _is_real_log_path(file):
            raise AssertionError(
                f"Test attempted to open a REAL logs/ path for writing: {file} "
                f"(mode={mode!r}). Patch the module-level constant to a "
                "tmp_path instead of letting it fall through to the real path."
            )
        return real_builtins_open(file, mode, *args, **kwargs)

    def guarded_sqlite_connect(database, *args, **kwargs):
        if isinstance(database, (str, bytes, Path)) and _is_real_log_path(database):
            raise AssertionError(
                f"Test attempted to open a REAL logs/ sqlite database: "
                f"{database!r}. Patch the module-level constant to a "
                "tmp_path instead of letting it fall through to the real path."
            )
        return real_sqlite_connect(database, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_path_open, raising=True)
    monkeypatch.setattr(builtins, "open", guarded_builtins_open, raising=True)
    monkeypatch.setattr(sqlite3, "connect", guarded_sqlite_connect, raising=True)
    yield


@pytest.fixture(autouse=True)
def _reset_module_level_caches():
    """
    Clear process-wide caches that outlive a single test.

    product_state holds ProductRules/ProductState in module-level dicts, and
    exit_executor keeps an alert cooldown map.  Both are correct for a
    long-running scheduler process and both leak between tests: a cooldown set
    by one test silently suppresses the alert another test asserts on.
    """
    import pipeline.exit_executor as _exit_executor
    import pipeline.product_state as _product_state
    import pipeline.runner as _runner

    def _reset():
        _product_state._clear_cache()
        _product_state._last_persistence_failures.clear()
        _exit_executor._rules_alert_until.clear()
        _exit_executor._dust_delivered.clear()
        _exit_executor._dust_retry_until.clear()
        _runner._persist_reported = frozenset()

    _reset()
    yield
    _reset()


@pytest.fixture(autouse=True)
def _block_telegram_sends():
    """
    Prevent any test from sending real Telegram messages.

    Patches at the transport layer (notifications.telegram.request.urlopen)
    rather than individual module-level aliases.  Every call to
    send_telegram_message — from pipeline.runner, pipeline.daily_summary,
    pipeline.weekly_review, or any future module — ultimately calls
    request.urlopen inside notifications/telegram.py, so this single patch
    is a true global guard with no per-module enumeration needed.

    Tests that need to assert Telegram behaviour can apply their own inner
    patch; it will shadow this one while active, then yield back to the
    no-op on exit.
    """
    with patch("notifications.telegram.request.urlopen") as mocked_urlopen:
        yield
        assert not mocked_urlopen.called, (
            f"A test sent a real Telegram message ({mocked_urlopen.call_count} call(s)). "
            "If this was intentional, apply an inner patch inside the test body."
        )
