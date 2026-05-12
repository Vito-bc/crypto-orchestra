"""
Scheduler — runs the full pipeline on a fixed interval.

Starts immediately, then repeats every INTERVAL_MINUTES (default 60).
Each run is wrapped in a try/except so a single failure never kills the loop.
All output is mirrored to logs/scheduler.log in addition to the console.

Usage:
    python pipeline/scheduler.py            # every 60 minutes
    python pipeline/scheduler.py 30         # every 30 minutes
    PIPELINE_INTERVAL_MINUTES=30 python pipeline/scheduler.py
"""

from __future__ import annotations

import io
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LOG_DIR       = ROOT / "logs"
SCHEDULER_LOG = LOG_DIR / "scheduler.log"

# ── Interval config ───────────────────────────────────────────────────────────
def _get_interval() -> int:
    """Return run interval in minutes from CLI arg or env var, default 60."""
    if len(sys.argv) > 1:
        try:
            return max(1, int(sys.argv[1]))
        except ValueError:
            pass
    try:
        return max(1, int(os.getenv("PIPELINE_INTERVAL_MINUTES", "60")))
    except ValueError:
        return 60


# ── Logging ───────────────────────────────────────────────────────────────────
class _Tee:
    """Write to both console and log file simultaneously."""
    def __init__(self, stream, log_path: Path):
        self._stream = stream
        self._log    = log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, msg: str) -> None:
        self._stream.write(msg)
        self._stream.flush()
        with self._log.open("a", encoding="utf-8") as f:
            f.write(msg)

    def flush(self) -> None:
        self._stream.flush()


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    interval_min = _get_interval()
    interval_sec = interval_min * 60

    # Force UTF-8 output so Unicode characters don't crash on Windows cp1252
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    sys.stdout = _Tee(sys.stdout, SCHEDULER_LOG)
    sys.stderr = _Tee(sys.stderr, SCHEDULER_LOG)

    print(f"\n{'='*65}")
    print(f"CRYPTO ORCHESTRA SCHEDULER")
    print(f"Started: {_now_utc()}")
    print(f"Interval: every {interval_min} minutes")
    print(f"Log: {SCHEDULER_LOG}")
    print(f"{'='*65}")
    print("Press Ctrl+C to stop.\n")

    # Import here so startup errors surface cleanly before the loop begins
    from pipeline.runner import run_all_assets

    run_number = 0

    while True:
        run_number += 1
        print(f"\n[Scheduler] -- Run #{run_number}  {_now_utc()} --")
        t0 = time.time()

        try:
            run_all_assets()
            elapsed = time.time() - t0
            print(f"[Scheduler] Run #{run_number} completed in {elapsed:.1f}s")
        except KeyboardInterrupt:
            raise
        except Exception:
            elapsed = time.time() - t0
            print(f"[Scheduler] Run #{run_number} FAILED after {elapsed:.1f}s — will retry next interval")
            traceback.print_exc()

        next_run = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        print(f"[Scheduler] Next run in {interval_min} minutes  (at approx "
              f"{_next_run_time(interval_sec)})")

        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            print(f"\n[Scheduler] Stopped by user at {_now_utc()}")
            sys.exit(0)


def _next_run_time(interval_sec: int) -> str:
    from datetime import timedelta
    t = datetime.now(timezone.utc) + timedelta(seconds=interval_sec)
    return t.strftime("%H:%M UTC")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[Scheduler] Stopped by user at {_now_utc()}")
        sys.exit(0)
