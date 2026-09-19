"""
Hydrate the PROXY-VENUE history the perps feasibility gate reads. CREDENTIAL-FREE.

WHAT THIS PULLS
---------------
From `https://data.binance.vision` (public archive, no key, no account):

  - Binance USDT-margined perpetuals, BTCUSDT and ETHUSDT ONLY
  - 4h klines: monthly archives 2020-01 .. LAST_FULL_MONTH, then daily
    archives for the days of the current month up to KLINE_END (the archive
    publishes a day's file the day after it closes)
  - fundingRate history: monthly archives 2020-01 .. LAST_FULL_MONTH. The
    archive publishes NO daily funding files, so the funding series ends at
    the last closed month; the gate declares that boundary rather than
    filling it from a live endpoint.

It pulls no spot series, no other symbol and no other interval. Everything
lands under `data/perps_proxy/` (gitignored) as the extracted CSV member of
each zip, one file per archive, named `kl_{SYMBOL}_4h_{period}.csv` and
`fund_{SYMBOL}_{period}.csv`, where `period` is `YYYY-MM` for a monthly
archive and `YYYY-MM-DD` for a daily one.

DETERMINISM AND PROVENANCE
--------------------------
The set of archives is fixed by the constants below, not by the clock. A file
already on disk is NOT re-downloaded (the scoping task's cache is reused as
is). A file that is downloaded is verified against the archive's own
`.CHECKSUM` sidecar (sha256 of the zip) before it is extracted, and a
checksum mismatch is an error, not a warning. `--digests` prints the sha256
of every CSV the gate consumes; the gate's document records them.

Binance is NOT Coinbase. See `docs/research/2026-09-18-perps-scoping.md` §5 for
the venue-mismatch statement; nothing here changes it.

USAGE
    python backtesting/hydrate_perps_proxy.py             # fetch what is missing
    python backtesting/hydrate_perps_proxy.py --digests   # list files + sha256, no network
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

# Declared window. HISTORY_START is the first month the archive carries for
# either symbol's USDT-margined perp; KLINE_END is the last day whose daily
# archive existed when this gate was built (2026-09-18 is not yet published).
HISTORY_START = (2020, 1)
LAST_FULL_MONTH = (2026, 8)
KLINE_END = date(2026, 9, 17)


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
    """Every day from the first of `end`'s month through `end`, inclusive."""
    d = end.replace(day=1)
    out = []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def planned_files() -> list[tuple[Path, str]]:
    """(local path, archive url) for every file the gate consumes, in a fixed order."""
    plan: list[tuple[Path, str]] = []
    months = _months(HISTORY_START, LAST_FULL_MONTH)
    days = _days_of_partial_month(KLINE_END)
    for sym in SYMBOLS:
        for mon in months:
            plan.append((DATA_DIR / f"kl_{sym}_{INTERVAL}_{mon}.csv",
                         f"{ARCHIVE_BASE}/monthly/klines/{sym}/{INTERVAL}/"
                         f"{sym}-{INTERVAL}-{mon}.zip"))
        for day in days:
            plan.append((DATA_DIR / f"kl_{sym}_{INTERVAL}_{day}.csv",
                         f"{ARCHIVE_BASE}/daily/klines/{sym}/{INTERVAL}/"
                         f"{sym}-{INTERVAL}-{day}.zip"))
        for mon in months:
            plan.append((DATA_DIR / f"fund_{sym}_{mon}.csv",
                         f"{ARCHIVE_BASE}/monthly/fundingRate/{sym}/"
                         f"{sym}-fundingRate-{mon}.zip"))
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
    """Download one archive, check it against Binance's own sha256 sidecar, extract."""
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
        print(f"fetched {dest.name}")
    return fetched, kept


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def digests() -> list[dict]:
    """sha256 + line count of every consumed file; missing files are an error."""
    out = []
    for dest, url in planned_files():
        if not dest.exists():
            raise HydrationError(f"missing: {dest.name} — run hydrate_perps_proxy.py first")
        lines = dest.read_text(encoding="utf-8").count("\n")
        out.append({"file": dest.name, "sha256": sha256_of(dest),
                    "lines": lines, "source": url})
    return out


def main() -> int:
    if "--digests" in sys.argv:
        for row in digests():
            print(f"{row['sha256']}  {row['lines']:>6}  {row['file']}")
        return 0
    fetched, kept = hydrate()
    print(f"done: {fetched} fetched, {kept} already present, "
          f"{fetched + kept} files under {DATA_DIR.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
