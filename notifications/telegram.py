from __future__ import annotations

import json
import os
from pathlib import Path
from urllib import parse, request


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"


def load_env_file() -> None:
    if not ENV_PATH.exists():
        return

    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_telegram_config() -> tuple[str | None, str | None]:
    load_env_file()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    return token, chat_id


def send_telegram_message(text: str) -> bool:
    token, chat_id = get_telegram_config()
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    req = request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with request.urlopen(req, timeout=15) as response:
        body = response.read().decode("utf-8")
        data = json.loads(body)
        return bool(data.get("ok"))


def format_trade_event_message(event: dict) -> str:
    lines = [
        "Crypto Orchestra Paper Alert",
        f"Symbol: {event.get('symbol', '')}",
        f"Event: {event.get('event', '')}",
        f"Reason: {event.get('reason', '')}",
        f"Price: {event.get('price', 0):.2f}",
        f"Candle: {event.get('candle_time', '')}",
    ]
    if "entry_price" in event and event.get("entry_price") is not None:
        lines.append(f"Entry: {event['entry_price']:.2f}")
    if "pnl_pct" in event and event.get("pnl_pct") is not None:
        lines.append(f"PnL %: {event['pnl_pct']:.4f}")
    if "hold_hours" in event and event.get("hold_hours") is not None:
        lines.append(f"Hold Hours: {event['hold_hours']}")
    return "\n".join(lines)
