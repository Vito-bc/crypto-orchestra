"""
Project-wide pytest fixtures.

autouse fixtures here apply to every test in this directory, providing
safety guards that prevent tests from touching production resources.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _block_telegram_sends():
    """
    Prevent any test from sending real Telegram messages.

    All tests run with pipeline.runner.send_telegram_message replaced by a
    no-op MagicMock. Tests that need to assert Telegram behaviour apply their
    own patch inside the test body (the inner patch shadows this one while
    active), then restore to the no-op on exit — still safe.

    Without this fixture, unit tests that call run_all_assets() without fully
    mocking _get_open_position_assets() read the real ledger, fail, and fire
    live CRITICAL alerts to the production Telegram channel.
    """
    with patch("pipeline.runner.send_telegram_message"):
        yield
