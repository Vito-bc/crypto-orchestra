from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notifications.telegram import send_telegram_message
from trading.daily_paper_summary import build_summary_text


def main() -> None:
    message = build_summary_text()
    sent = send_telegram_message(message)
    print("Daily Telegram Summary:", "sent" if sent else "skipped")


if __name__ == "__main__":
    main()
