"""
Regression test for the real-logs-directory write guard in conftest.py.

Background: logs/agent_decisions.jsonl was found polluted with 36 synthetic
records because runner.DECISIONS_LOG / runner._SIGNALS_DB were never patched
by the tests that exercised _log_order_event. The _guard_real_logs_dir
autouse fixture in conftest.py is the backstop for the *next* unpatched
constant. This test is the demonstration that the backstop is load-bearing:
if it is ever removed or weakened, this test fails.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REAL_LOG_DIR = (Path(__file__).resolve().parent.parent / "logs").resolve()


def test_writing_to_the_real_logs_dir_via_path_open_is_refused():
    target = REAL_LOG_DIR / "test_log_guard_canary.jsonl"
    assert not target.exists(), (
        "canary path already exists in the real logs/ dir — remove it manually "
        "before re-running this test"
    )
    with pytest.raises(AssertionError, match="REAL logs/"):
        target.open("a", encoding="utf-8")
    assert not target.exists(), "guard raised but the real file was created anyway"


def test_writing_to_the_real_logs_dir_via_builtin_open_is_refused():
    target = REAL_LOG_DIR / "test_log_guard_canary.jsonl"
    assert not target.exists()
    with pytest.raises(AssertionError, match="REAL logs/"):
        open(target, "w", encoding="utf-8")
    assert not target.exists()


def test_writing_to_a_real_logs_sqlite_db_is_refused():
    target = REAL_LOG_DIR / "test_log_guard_canary.db"
    assert not target.exists(), (
        "canary db already exists in the real logs/ dir — remove it manually "
        "before re-running this test"
    )
    with pytest.raises(AssertionError, match="REAL logs/"):
        sqlite3.connect(str(target))
    assert not target.exists(), "guard raised but the real db file was created anyway"


def test_reading_a_real_pre_existing_log_file_is_still_allowed():
    # The guard targets writes only — a legitimate read of a real, committed
    # or pre-existing log file must not be blocked.
    real_file = REAL_LOG_DIR / "trade_history.jsonl"
    if not real_file.exists():
        pytest.skip("no trade_history.jsonl present to exercise a real read")
    with real_file.open("r", encoding="utf-8"):
        pass


def test_writing_under_tmp_path_is_unaffected(tmp_path):
    # Sanity check that the guard is scoped to the real logs/ dir and does
    # not break ordinary tmp_path-based test I/O.
    target = tmp_path / "agent_decisions.jsonl"
    with target.open("a", encoding="utf-8") as f:
        f.write("{}\n")
    assert target.read_text(encoding="utf-8") == "{}\n"
