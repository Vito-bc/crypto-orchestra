"""
Root conftest — the test bootstrap.

pytest loads the rootdir conftest before `tests/conftest.py` and before any test
module, so this file runs BEFORE the first project import. That ordering is the
whole point: safety flags are snapshotted at module import time
(`exchange.coinbase_client._DRY_RUN`, `pipeline.preflight._DRY_RUN`,
`pipeline.position_tracker.PAPER_BALANCE`), so an autouse fixture — which runs
after collection has already imported everything — is far too late to change
them.

Nothing here may import a project module at module scope. Doing so would import
it before the environment is pinned and defeat the file.

Two guarantees:

  1. The suite runs on pinned, safe configuration regardless of the developer's
     local .env. Verified by running with an external DRY_RUN=false: the
     bootstrap still forces DRY_RUN=true.
  2. No test can reach the network. Enforced at the socket layer, so it covers
     requests/urllib/httpx/anthropic/coinbase-advanced-py and any SDK that
     hides its transport, rather than enumerating call sites.
"""

from __future__ import annotations

import os
import socket
import traceback

import pytest

# ── 1. Pin safe configuration BEFORE any project import ──────────────────────
#
# python-dotenv's load_dotenv() does not override variables that already exist
# in the environment, so setting them here makes the local .env inert for the
# suite. Values are pinned rather than merely defaulted: a developer with
# DRY_RUN=false in .env must still get a safe suite, and CI (no .env at all)
# must exercise the same configuration as a workstation.
_TEST_ENV = {
    # Safety flags. DRY_RUN is the one that decides whether order placement is
    # simulated; it is read at import and cached in module constants.
    "DRY_RUN": "true",
    # Sizing. Without this the suite silently runs at the .env value locally
    # (100) and at the code default in CI (10000) — two different test runs.
    "LIVE_BALANCE_USD": "100",
    "TRADE_SIZE_PCT": "0.05",
    "MAX_POSITIONS": "3",
    "DAILY_LOSS_LIMIT": "2.0",
    "PIPELINE_INTERVAL_MINUTES": "60",
    # Credentials: deliberately EMPTY, not fake-but-plausible. Any code path
    # that needs a real credential must fail loudly in tests rather than reach
    # for one. BaseAgent raises on an empty ANTHROPIC_API_KEY, which is the
    # desired outcome: it proves no test constructs a live agent.
    "ANTHROPIC_API_KEY": "",
    "CRYPTOPANIC_API_KEY": "",
    "TELEGRAM_BOT_TOKEN": "",
    "TELEGRAM_CHAT_ID": "",
    "COINBASE_PORTFOLIO_UUID": "",
    # Pin models so a .env override cannot change what tests assert on.
    "SUBAGENT_MODEL": "claude-haiku-4-5-20251001",
    "ORCHESTRATOR_MODEL": "claude-sonnet-4-6",
}

for _key, _value in _TEST_ENV.items():
    os.environ[_key] = _value


# ── 2. Default-deny outbound network ─────────────────────────────────────────

_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost", ""})

_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex
_real_getaddrinfo = socket.getaddrinfo
_real_create_connection = socket.create_connection


class NetworkAccessBlocked(RuntimeError):
    """A test attempted a real outbound connection."""


def _describe(address) -> str:
    if isinstance(address, (tuple, list)) and address:
        return f"{address[0]}:{address[1] if len(address) > 1 else '?'}"
    return str(address)


def _host_of(address) -> str:
    if isinstance(address, (tuple, list)) and address:
        return str(address[0])
    return str(address)


def _refuse(address, what: str):
    site = "".join(traceback.format_stack()[-6:-1])
    return NetworkAccessBlocked(
        f"Blocked outbound {what} to {_describe(address)}.\n"
        "The suite is hermetic: mock the transport inside your test "
        "(patch the client/session/urlopen the code under test uses).\n"
        f"Attempted from:\n{site}"
    )


def _guard_connect(self, address, *args, **kwargs):
    if _host_of(address) in _LOOPBACK:
        return _real_connect(self, address, *args, **kwargs)
    raise _refuse(address, "connect")


def _guard_connect_ex(self, address, *args, **kwargs):
    if _host_of(address) in _LOOPBACK:
        return _real_connect_ex(self, address, *args, **kwargs)
    raise _refuse(address, "connect_ex")


def _guard_create_connection(address, *args, **kwargs):
    if _host_of(address) in _LOOPBACK:
        return _real_create_connection(address, *args, **kwargs)
    raise _refuse(address, "create_connection")


def _guard_getaddrinfo(host, *args, **kwargs):
    # DNS is blocked too. Allowing resolution while blocking connect would let a
    # test leak the fact that it is running to a DNS server, and would make the
    # eventual failure surface far from its cause.
    if host is None or str(host) in _LOOPBACK:
        return _real_getaddrinfo(host, *args, **kwargs)
    raise _refuse((host,), "DNS lookup")


def pytest_configure(config: pytest.Config) -> None:
    socket.socket.connect = _guard_connect
    socket.socket.connect_ex = _guard_connect_ex
    socket.create_connection = _guard_create_connection
    socket.getaddrinfo = _guard_getaddrinfo
    config.addinivalue_line(
        "markers",
        "allow_network: test is permitted real outbound network access "
        "(must be justified; none currently use it)",
    )


def pytest_unconfigure(config: pytest.Config) -> None:
    socket.socket.connect = _real_connect
    socket.socket.connect_ex = _real_connect_ex
    socket.create_connection = _real_create_connection
    socket.getaddrinfo = _real_getaddrinfo
