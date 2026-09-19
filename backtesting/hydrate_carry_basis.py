"""
Hydrate the PREMIUM-INDEX series the carry scoping reads. CREDENTIAL-FREE.

WHAT THIS PULLS, AND WHAT IT DOES NOT
-------------------------------------
From `https://data.binance.vision` (public archive, no key, no account):
4h **premiumIndexKlines** for BTCUSDT and ETHUSDT USDT-margined perps,
2020-01 .. 2026-08 monthly plus the closed days of 2026-09. The premium index
is the perp's own published measure of how far the contract trades from its
underlying index, which is the basis quantity Deliverable 4 asks for.

**No new symbol is pulled.** These are the same two symbols the perps gate
already hydrated, in a second series.

It does **not** pull the 4h klines or the funding history. Those come from
`backtesting/hydrate_perps_proxy.py`, which arrives with the perps-gate branch
(PR #24). If that file is not on this tree yet, run it from that branch, or
copy an existing `data/perps_proxy/` across. `backtesting/carry_scoping.py`
fails closed with that instruction rather than computing on partial inputs.

Archives are verified against Binance's own published sha256 sidecar before
extraction; a mismatch is an error, not a warning. Files already on disk are
never re-downloaded.

USAGE
    python backtesting/hydrate_carry_basis.py             # fetch what is missing
    python backtesting/hydrate_carry_basis.py --digests   # list files + sha256
"""

from __future__ import annotations

import hashlib
import io
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "perps_proxy"

ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
INTERVAL = "4h"

HISTORY_START = (2020, 1)
LAST_FULL_MONTH = (2026, 8)
DAILY_END = date(2026, 9, 17)


class HydrationError(RuntimeError):
    pass


def _months(start: tuple[int, int], end: tuple[int, int]) -> list[str]:
    y, m = start
    out = []
    while (y, m) <= end:
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _days_of_partial_month(end: date) -> list[str]:
    d = end.replace(day=1)
    out = []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def planned_files() -> list[tuple[Path, str]]:
    """(local path, archive url) for every premium-index file, fixed order."""
    plan: list[tuple[Path, str]] = []
    for sym in SYMBOLS:
        for mon in _months(HISTORY_START, LAST_FULL_MONTH):
            plan.append((DATA_DIR / f"prem_{sym}_{INTERVAL}_{mon}.csv",
                         f"{ARCHIVE_BASE}/monthly/premiumIndexKlines/{sym}/"
                         f"{INTERVAL}/{sym}-{INTERVAL}-{mon}.zip"))
        for day in _days_of_partial_month(DAILY_END):
            plan.append((DATA_DIR / f"prem_{sym}_{INTERVAL}_{day}.csv",
                         f"{ARCHIVE_BASE}/daily/premiumIndexKlines/{sym}/"
                         f"{INTERVAL}/{sym}-{INTERVAL}-{day}.zip"))
    return plan


def _get(url: str) -> bytes:
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise HydrationError(f"HTTP {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise HydrationError(f"unreachable: {url} ({exc.reason})") from exc


def fetch_verified(url: str, dest: Path) -> None:
    blob = _get(url)
    sidecar = _get(url + ".CHECKSUM").decode("utf-8").split()
    if not sidecar:
        raise HydrationError(f"empty checksum sidecar for {url}")
    expected = sidecar[0].lower()
    actual = hashlib.sha256(blob).hexdigest()
    if actual != expected:
        raise HydrationError(
            f"checksum mismatch for {url}: archive says {expected}, got {actual}")
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = z.namelist()
        if len(names) != 1:
            raise HydrationError(f"{url} holds {len(names)} members, expected 1")
        text = z.read(names[0]).decode("utf-8")
    dest.write_text(text, encoding="utf-8")


def hydrate() -> tuple[int, int]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fetched = kept = 0
    for dest, url in planned_files():
        if dest.exists() and dest.stat().st_size > 0:
            kept += 1
            continue
        fetch_verified(url, dest)
        fetched += 1
    return fetched, kept


def digests() -> list[dict]:
    out = []
    for dest, url in planned_files():
        if not dest.exists():
            raise HydrationError(
                f"missing: {dest.name} — run hydrate_carry_basis.py first")
        out.append({
            "file": dest.name,
            "sha256": hashlib.sha256(dest.read_bytes()).hexdigest(),
            "lines": dest.read_text(encoding="utf-8").count("\n"),
            "source": url,
        })
    return out


def main() -> int:
    if "--digests" in sys.argv:
        for row in digests():
            print(f"{row['sha256']}  {row['lines']:>6}  {row['file']}")
        return 0
    fetched, kept = hydrate()
    print(f"done: {fetched} fetched, {kept} already present, "
          f"{fetched + kept} premium-index files under "
          f"{DATA_DIR.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
