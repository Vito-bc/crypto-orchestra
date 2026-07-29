"""
Project-wide pytest fixtures.

autouse fixtures here apply to every test in this directory, providing
safety guards that prevent tests from touching production resources.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


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
