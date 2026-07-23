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
    with patch("notifications.telegram.request.urlopen"):
        yield
