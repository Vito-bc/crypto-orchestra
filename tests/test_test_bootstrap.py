"""
Regression tests for the test bootstrap itself (root conftest.py).

The bootstrap is what makes every other test trustworthy: it pins safe
configuration before the first project import and denies outbound network. That
contract was verified by hand and by CI passing, neither of which fails loudly
if someone later reorders the file, moves it under tests/ (too late to matter),
or drops a key. These tests pin it.

They assert the OBSERVED state of already-imported modules, not just os.environ:
the flags that matter are snapshotted at import time, so `os.environ["DRY_RUN"]`
being right proves nothing on its own.
"""
from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest


ROOT_CONFTEST = Path(__file__).resolve().parents[1] / "conftest.py"


@pytest.fixture(scope="module")
def bootstrap(request):
    """
    The ROOT conftest module object, fetched from the plugin manager.

    It cannot be imported. `import conftest` resolves to tests/conftest.py —
    both files share a basename, and the root module is not registered in
    sys.modules under any name once tests/conftest.py claims "conftest".
    Re-importing the root file by path would build a SECOND module with its own
    NetworkAccessBlocked class; pytest.raises compares by identity, so every
    assertion below would silently stop matching. pytest registers each conftest
    as a plugin keyed by its absolute path, which is the one stable handle.
    """
    plugin = request.config.pluginmanager.get_plugin(str(ROOT_CONFTEST))
    assert plugin is not None, (
        f"root conftest at {ROOT_CONFTEST} is not loaded — the bootstrap did "
        "not run and nothing in this suite is pinned"
    )
    return plugin


def test_the_root_conftest_is_shadowed_by_name_but_still_loaded() -> None:
    """
    Documents the trap the fixture works around: `import conftest` gives the
    tests/ one, so anything reaching for the bootstrap by name gets the wrong
    module and fails confusingly.
    """
    import conftest as by_name

    assert Path(by_name.__file__).resolve() != ROOT_CONFTEST
    assert not hasattr(by_name, "NetworkAccessBlocked")


# ── Configuration is pinned, and it actually took effect ─────────────────────

def test_dry_run_is_forced_on_in_the_environment() -> None:
    assert os.environ["DRY_RUN"] == "true"


def test_dry_run_reached_the_modules_that_cache_it() -> None:
    """
    These are read once at import and never re-read. If the bootstrap ran too
    late, the environment would look right while the live flags were wrong —
    which is precisely the failure an autouse fixture could not prevent.
    """
    import exchange.coinbase_client as cb
    import pipeline.preflight as pf

    assert cb._DRY_RUN is True
    assert cb.is_dry_run() is True
    assert pf._DRY_RUN is True


def test_sizing_is_pinned_identically_here_and_in_ci() -> None:
    """
    Without pinning this was 100 locally (.env) and 10000 in CI (code default),
    so the two environments ran different tests.
    """
    import pipeline.position_tracker as pt

    assert os.environ["LIVE_BALANCE_USD"] == "100"
    assert os.environ["TRADE_SIZE_PCT"] == "0.05"
    assert pt.PAPER_BALANCE == 100


@pytest.mark.parametrize("name", [
    "ANTHROPIC_API_KEY", "CRYPTOPANIC_API_KEY",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "COINBASE_PORTFOLIO_UUID",
    # The cost probe's opt-in view-only fee key: a workstation that has one
    # configured must not let any test build an authenticated client.
    "STF_FEE_VIEW_ONLY_KEY_FILE",
])
def test_credentials_are_empty_not_merely_absent(name) -> None:
    """
    Empty, not fake-but-plausible: a code path needing a real credential must
    fail loudly rather than proceed with a stub that looks usable.
    """
    assert name in os.environ, f"{name} must be pinned, not left to the ambient env"
    assert os.environ[name] == "", f"{name} leaked a value into the suite"


def test_an_agent_cannot_be_constructed_in_the_suite() -> None:
    """
    The practical consequence of the empty key: no test may build a live client.
    If this ever starts passing, something is supplying real credentials.
    """
    from agents.technical_agent import TechnicalAgent

    with pytest.raises(EnvironmentError):
        TechnicalAgent()


def test_local_dotenv_cannot_override_the_bootstrap() -> None:
    """
    python-dotenv does not overwrite variables that already exist, which is what
    makes the pinning effective. Re-running the project's own loader must not
    move the flag.
    """
    from pathlib import Path

    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    assert os.environ["DRY_RUN"] == "true"
    assert os.environ["LIVE_BALANCE_USD"] == "100"


# ── Network is denied ────────────────────────────────────────────────────────

def test_tcp_connect_to_the_internet_is_blocked(bootstrap) -> None:
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
            ("api.okx.com", 443))


def test_create_connection_is_blocked(bootstrap) -> None:
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        socket.create_connection(("www.okx.com", 443), timeout=1)


def test_dns_resolution_is_blocked(bootstrap) -> None:
    """
    Blocked as well as connect: resolving alone still tells a DNS server the
    suite is running, and lets the eventual failure surface far from its cause.
    """
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        socket.getaddrinfo("api.anthropic.com", 443)


@pytest.mark.parametrize("resolver,args", [
    (socket.gethostbyname, ("api.anthropic.com",)),
    (socket.gethostbyname_ex, ("api.anthropic.com",)),
    (socket.gethostbyaddr, ("8.8.8.8",)),
    (socket.getnameinfo, (("8.8.8.8", 53), 0)),
])
def test_alternate_dns_resolution_paths_are_blocked(bootstrap, resolver, args) -> None:
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        resolver(*args)


def test_udp_sendto_is_blocked(bootstrap) -> None:
    """
    Connectionless sends bypass connect() entirely, so this was the one way out
    of the guard while it still claimed "no network".
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        s.sendto(b"ping", ("8.8.8.8", 53))


def _exception_chain_contains(exc: BaseException, needle: str) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if needle in str(current):
            return True
        current = current.__cause__ or current.__context__
    return False


@pytest.mark.parametrize("client", ["urllib", "requests", "httpx"])
def test_common_sync_http_clients_cannot_escape(bootstrap, client) -> None:
    """The socket boundary protects clients added later, not a hardcoded mock list."""
    with pytest.raises(Exception) as exc:
        if client == "urllib":
            import urllib.request

            # tests/conftest patches the module-level urlopen for Telegram.
            # A fresh opener exercises urllib's real transport without
            # shadowing or clearing that independent notification guard.
            urllib.request.build_opener().open(
                "https://example.invalid", timeout=1)
        elif client == "requests":
            import requests

            requests.get("https://example.invalid", timeout=1)
        else:
            import httpx

            httpx.get("https://example.invalid", timeout=1)
    assert _exception_chain_contains(exc.value, "Blocked outbound")


@pytest.mark.allow_loopback
def test_async_http_client_cannot_escape(bootstrap) -> None:
    """Asyncio may build a local self-pipe; public resolution stays denied."""
    import asyncio

    import httpx

    async def request():
        async with httpx.AsyncClient() as client:
            await client.get("https://example.invalid", timeout=1)

    with pytest.raises(Exception) as exc:
        asyncio.run(request())
    assert _exception_chain_contains(exc.value, "Blocked outbound")


def test_unmarked_loopback_is_blocked(bootstrap) -> None:
    """One local-socket test must not authorize localhost suite-wide."""
    with pytest.raises(bootstrap.NetworkAccessBlocked):
        socket.getaddrinfo("localhost", 0)


def test_unmarked_test_cannot_start_a_subprocess(bootstrap) -> None:
    import subprocess
    import sys

    with pytest.raises(bootstrap.NetworkAccessBlocked, match="Blocked subprocess"):
        subprocess.run([sys.executable, "-c", "pass"], check=True)


@pytest.mark.allow_subprocess("python", "python.exe")
def test_explicit_marker_allows_only_the_named_executable(bootstrap) -> None:
    import subprocess
    import sys

    completed = subprocess.run(
        [sys.executable, "-c", "print('isolated-child')"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "isolated-child"
    with pytest.raises(bootstrap.NetworkAccessBlocked, match="Blocked subprocess"):
        subprocess.run(["definitely-not-allowed", "--version"], check=True)


@pytest.mark.allow_subprocess("python", "python.exe")
def test_shell_subprocess_is_denied_even_when_an_executable_is_allowed(
        bootstrap) -> None:
    import subprocess

    with pytest.raises(bootstrap.NetworkAccessBlocked, match="shell subprocess"):
        subprocess.run("echo escape", shell=True, check=True)
    with pytest.raises(bootstrap.NetworkAccessBlocked, match="shell process"):
        os.system("echo escape")
    with pytest.raises(bootstrap.NetworkAccessBlocked, match="shell process"):
        os.popen("echo escape")


@pytest.mark.allow_loopback
def test_explicitly_marked_loopback_is_allowed() -> None:
    """A narrowly authorized local socket remains available."""
    assert socket.getaddrinfo("localhost", 0)
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    try:
        client = socket.socket()
        client.connect(srv.getsockname())      # must NOT raise
        client.close()
    finally:
        srv.close()


def test_the_block_names_where_it_came_from(bootstrap) -> None:
    """A bare refusal in a large suite is hard to trace back to its caller."""
    with pytest.raises(bootstrap.NetworkAccessBlocked) as exc:
        socket.getaddrinfo("example.invalid", 443)
    msg = str(exc.value)
    assert "example.invalid" in msg
    assert "mock the transport" in msg
    assert "Attempted from" in msg


def test_real_socket_functions_are_kept_for_restoration(bootstrap) -> None:
    """pytest_unconfigure must be able to put the interpreter back."""
    for name in ("_real_connect", "_real_connect_ex", "_real_getaddrinfo",
                 "_real_gethostbyname", "_real_gethostbyname_ex",
                 "_real_gethostbyaddr", "_real_getnameinfo",
                 "_real_create_connection", "_real_sendto", "_real_popen",
                 "_real_os_system", "_real_os_popen"):
        assert getattr(bootstrap, name) is not None
