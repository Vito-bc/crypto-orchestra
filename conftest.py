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
  2. No test reaches the network IN THIS INTERPRETER, and no test starts a new
     one without saying so. Two guards, and they are not equally strong:

     The socket layer is closed by construction. TCP connect, DNS resolution
     and connectionless UDP sends are all patched, so it covers
     requests/urllib/httpx/anthropic/coinbase-advanced-py and any SDK that
     hides its transport, rather than enumerating call sites. Loopback is
     denied unless the individual test carries the explicit `allow_loopback`
     marker; one local-socket test must not open localhost to the whole suite.

     A CHILD PROCESS gets a fresh interpreter without these patches, so it is
     outside the socket guard entirely and process creation is denied by
     default too. That guard ENUMERATES CPython's process-entry points —
     subprocess, os.system/popen, the os.spawn* and os.exec* families,
     os.posix_spawn*, and multiprocessing.Process.start. Enumeration is weaker
     than the socket layer: it can go stale if CPython grows a new entry point,
     and `test_no_unguarded_process_entry_point_exists` in
     tests/test_test_bootstrap.py exists to fail when that happens.

     os.fork is deliberately NOT guarded. A fork keeps the patched interpreter
     image, so the child is still inside the socket guard — it is not an
     escape, and blocking it would break tooling for no gain. Only a FRESH
     interpreter escapes, which is what the list above covers.
"""

from __future__ import annotations

import contextlib
import multiprocessing.process
import os
import platform
import socket
import subprocess
import sys
import traceback
from pathlib import Path

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
    # The cost probe's opt-in view-only fee credential. Pinned empty so a
    # workstation that has one configured cannot let a test build an
    # authenticated client — the suite must exercise the unconfigured path.
    "STF_FEE_VIEW_ONLY_KEY_FILE": "",
    # Pin models so a .env override cannot change what tests assert on.
    "SUBAGENT_MODEL": "claude-haiku-4-5-20251001",
    "ORCHESTRATOR_MODEL": "claude-sonnet-4-6",
}

for _key, _value in _TEST_ENV.items():
    os.environ[_key] = _value


# ── 2. Default-deny outbound network ─────────────────────────────────────────

_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})
_loopback_allowed = False

_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex
_real_getaddrinfo = socket.getaddrinfo
_real_gethostbyname = socket.gethostbyname
_real_gethostbyname_ex = socket.gethostbyname_ex
_real_gethostbyaddr = socket.gethostbyaddr
_real_getnameinfo = socket.getnameinfo
_real_create_connection = socket.create_connection
# Connectionless sends bypass connect() entirely, so a UDP datagram was the one
# way out of this guard. Guarding only TCP+DNS and calling it "no network" was
# a stronger claim than the code made good on.
_real_sendto = socket.socket.sendto
_real_sendmsg = getattr(socket.socket, "sendmsg", None)
_real_popen = subprocess.Popen
_real_os_system = os.system
_real_os_popen = os.popen
_real_mp_start = multiprocessing.process.BaseProcess.start
_allowed_subprocesses: frozenset[str] = frozenset()

# Every os-level way to start a FRESH interpreter, with the position of the
# executable in the call's positional arguments. os.spawn* takes the mode
# first, so the program is argument 1; os.exec* and os.posix_spawn* take the
# program first. Absent names are skipped: the spawn*p/exec*p/posix_spawn
# variants are POSIX-only and simply do not exist on Windows.
#
# NOT in this list, on purpose: os.fork (keeps the patched image — see the
# module docstring) and _winapi/_posixsubprocess (private primitives reached
# only THROUGH the entry points above, which are already guarded).
_OS_PROCESS_ENTRY_POINTS = (
    ("spawnv", 1), ("spawnve", 1), ("spawnvp", 1), ("spawnvpe", 1),
    ("spawnl", 1), ("spawnle", 1), ("spawnlp", 1), ("spawnlpe", 1),
    ("posix_spawn", 0), ("posix_spawnp", 0),
    ("execv", 0), ("execve", 0), ("execvp", 0), ("execvpe", 0),
    ("execl", 0), ("execle", 0), ("execlp", 0), ("execlpe", 0),
)
_real_os_process_entries: dict[str, object] = {
    name: getattr(os, name) for name, _ in _OS_PROCESS_ENTRY_POINTS
    if hasattr(os, name)
}

# subprocess.Popen reaches os.posix_spawn internally on POSIX, and
# multiprocessing reaches the spawn family. Once the outer call has been
# authorised, the inner one must not be re-checked against a basename it never
# saw ("python3" vs the marker's "python") — that would fail the suite on Linux
# while passing on Windows.
_authorised_depth = 0

# On Windows, platform.machine() may shell out to the built-in `ver` command on
# its first call. Prime that standard-library cache before the process guard is
# installed; otherwise importing pandas during fixture setup looks like an
# unapproved test subprocess even though no test initiated it.
_BOOTSTRAP_MACHINE = platform.machine()


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


def _may_use(address) -> bool:
    host = _host_of(address)
    # None/empty is used for local bind/address discovery and cannot identify a
    # remote peer. Named/numeric loopback needs an explicit per-test marker.
    return host in {"None", ""} or (_loopback_allowed and host in _LOOPBACK)


def _refuse(address, what: str):
    site = "".join(traceback.format_stack()[-6:-1])
    return NetworkAccessBlocked(
        f"Blocked outbound {what} to {_describe(address)}.\n"
        "The suite is hermetic: mock the transport inside your test "
        "(patch the client/session/urlopen the code under test uses).\n"
        f"Attempted from:\n{site}"
    )


def _guard_connect(self, address, *args, **kwargs):
    if _may_use(address):
        return _real_connect(self, address, *args, **kwargs)
    raise _refuse(address, "connect")


def _guard_connect_ex(self, address, *args, **kwargs):
    if _may_use(address):
        return _real_connect_ex(self, address, *args, **kwargs)
    raise _refuse(address, "connect_ex")


def _guard_create_connection(address, *args, **kwargs):
    if _may_use(address):
        return _real_create_connection(address, *args, **kwargs)
    raise _refuse(address, "create_connection")


def _guard_sendto(self, data, *args, **kwargs):
    # Signatures: sendto(data, address) and sendto(data, flags, address).
    address = args[-1] if args else None
    if address is None or _may_use(address):
        return _real_sendto(self, data, *args, **kwargs)
    raise _refuse(address, "UDP sendto")


def _guard_sendmsg(self, buffers, ancdata=(), flags=0, address=None, *rest):
    if address is None or _may_use(address):
        return _real_sendmsg(self, buffers, ancdata, flags, address, *rest)
    raise _refuse(address, "UDP sendmsg")


def _guard_getaddrinfo(host, *args, **kwargs):
    # DNS is blocked too. Allowing resolution while blocking connect would let a
    # test leak the fact that it is running to a DNS server, and would make the
    # eventual failure surface far from its cause.
    if host is None or _may_use((host,)):
        return _real_getaddrinfo(host, *args, **kwargs)
    raise _refuse((host,), "DNS lookup")


def _guard_gethostbyname(host):
    if _may_use((host,)):
        return _real_gethostbyname(host)
    raise _refuse((host,), "DNS gethostbyname")


def _guard_gethostbyname_ex(host):
    if _may_use((host,)):
        return _real_gethostbyname_ex(host)
    raise _refuse((host,), "DNS gethostbyname_ex")


def _guard_gethostbyaddr(host):
    if _may_use((host,)):
        return _real_gethostbyaddr(host)
    raise _refuse((host,), "reverse DNS gethostbyaddr")


def _guard_getnameinfo(sockaddr, flags):
    if _may_use(sockaddr):
        return _real_getnameinfo(sockaddr, flags)
    raise _refuse(sockaddr, "reverse DNS getnameinfo")


def _basename_of(executable) -> str:
    """Lowercased file name of whatever a process API was handed, or ''."""
    if executable is None:
        return ""
    try:
        return Path(os.fsdecode(executable)).name.lower()
    except (TypeError, ValueError):
        return ""


def _require_process_permission(executable, how: str) -> None:
    """One rule for every process-entry point, so they cannot disagree."""
    if _authorised_depth:
        return  # inner call of an already-approved outer one
    name = _basename_of(executable)
    if name not in _allowed_subprocesses:
        site = "".join(traceback.format_stack()[-6:-1])
        raise NetworkAccessBlocked(
            f"Blocked {how} executable {name or executable!r}. A child process "
            "runs a fresh interpreter with none of this file's patches, so it "
            "is outside the socket guard; mark this test with allow_subprocess "
            "and list only the executable basenames it needs.\n"
            f"Attempted from:\n{site}"
        )


@contextlib.contextmanager
def _authorised():
    """Suppress re-checking while an approved call runs its own inner spawn."""
    global _authorised_depth
    _authorised_depth += 1
    try:
        yield
    finally:
        _authorised_depth -= 1


def _guard_popen(args, *popen_args, **kwargs):
    """Refuse process escape unless the individual test names the executable."""
    if kwargs.get("shell"):
        raise NetworkAccessBlocked(
            "Blocked shell subprocess. Tests may opt in only to an explicitly "
            "named executable, never to shell=True."
        )
    executable = kwargs.get("executable")
    if executable is None:
        if isinstance(args, (list, tuple)) and args:
            executable = args[0]
        elif isinstance(args, (str, bytes, os.PathLike)):
            executable = args
    _require_process_permission(executable, "subprocess")
    with _authorised():
        return _real_popen(args, *popen_args, **kwargs)


def _guard_shell_process(*args, **kwargs):
    """os.system/os.popen always invoke a shell and never receive an exception."""
    raise NetworkAccessBlocked(
        "Blocked shell process. Use subprocess with an explicit "
        "allow_subprocess marker; shell execution is never authorized in tests."
    )


def _make_os_process_guard(name: str, index: int, real):
    """Wrap one os.spawn*/exec*/posix_spawn* entry point with the shared rule."""
    def guard(*args, **kwargs):
        executable = args[index] if len(args) > index else None
        _require_process_permission(executable, f"os.{name}")
        with _authorised():
            return real(*args, **kwargs)
    guard.__name__ = f"_guard_os_{name}"
    return guard


def _guard_mp_start(self, *args, **kwargs):
    """
    multiprocessing never goes through subprocess.Popen.

    On Windows the default start method is "spawn", which reaches
    _winapi.CreateProcess directly and produces a fresh interpreter — a real
    escape. "fork" would keep the patched image and is not one, but a single
    rule here is simpler than a per-start-method exception, and no test uses
    multiprocessing at all.
    """
    _require_process_permission(sys.executable, "multiprocessing.Process.start")
    with _authorised():
        return _real_mp_start(self, *args, **kwargs)


def pytest_configure(config: pytest.Config) -> None:
    socket.socket.connect = _guard_connect
    socket.socket.connect_ex = _guard_connect_ex
    socket.create_connection = _guard_create_connection
    socket.getaddrinfo = _guard_getaddrinfo
    socket.gethostbyname = _guard_gethostbyname
    socket.gethostbyname_ex = _guard_gethostbyname_ex
    socket.gethostbyaddr = _guard_gethostbyaddr
    socket.getnameinfo = _guard_getnameinfo
    socket.socket.sendto = _guard_sendto
    if _real_sendmsg is not None:
        socket.socket.sendmsg = _guard_sendmsg
    subprocess.Popen = _guard_popen
    os.system = _guard_shell_process
    os.popen = _guard_shell_process
    for _name, _index in _OS_PROCESS_ENTRY_POINTS:
        _real = _real_os_process_entries.get(_name)
        if _real is not None:
            setattr(os, _name, _make_os_process_guard(_name, _index, _real))
    multiprocessing.process.BaseProcess.start = _guard_mp_start
    config.addinivalue_line(
        "markers",
        "allow_loopback: this test alone may use localhost/127.0.0.1/::1; "
        "public network remains denied",
    )
    config.addinivalue_line(
        "markers",
        "allow_subprocess(*executables): this test alone may start the listed "
        "executable basenames; shell=True remains denied",
    )


@pytest.fixture(autouse=True)
def _network_scope(request: pytest.FixtureRequest):
    """Narrow escape permissions to one marked test and its fixtures."""
    global _loopback_allowed, _allowed_subprocesses
    _loopback_allowed = request.node.get_closest_marker("allow_loopback") is not None
    marker = request.node.get_closest_marker("allow_subprocess")
    if marker is None:
        _allowed_subprocesses = frozenset()
    else:
        if marker.kwargs or not marker.args or any(
                not isinstance(item, str) or not item or Path(item).name != item
                for item in marker.args):
            raise pytest.UsageError(
                "allow_subprocess accepts one or more executable basenames only"
            )
        _allowed_subprocesses = frozenset(item.lower() for item in marker.args)
    try:
        yield
    finally:
        _loopback_allowed = False
        _allowed_subprocesses = frozenset()


def pytest_unconfigure(config: pytest.Config) -> None:
    socket.socket.connect = _real_connect
    socket.socket.connect_ex = _real_connect_ex
    socket.create_connection = _real_create_connection
    socket.getaddrinfo = _real_getaddrinfo
    socket.gethostbyname = _real_gethostbyname
    socket.gethostbyname_ex = _real_gethostbyname_ex
    socket.gethostbyaddr = _real_gethostbyaddr
    socket.getnameinfo = _real_getnameinfo
    socket.socket.sendto = _real_sendto
    if _real_sendmsg is not None:
        socket.socket.sendmsg = _real_sendmsg
    subprocess.Popen = _real_popen
    os.system = _real_os_system
    os.popen = _real_os_popen
    for _name, _real in _real_os_process_entries.items():
        setattr(os, _name, _real)
    multiprocessing.process.BaseProcess.start = _real_mp_start
