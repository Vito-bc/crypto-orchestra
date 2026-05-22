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


def format_limit_order_placed(asset: str, order: "PendingOrder", levels: dict) -> str:  # type: ignore[name-defined]
    current = levels.get("current_price", 0.0)
    dist    = levels.get("dist_to_support")
    lines = [
        "Crypto Orchestra — Limit Order Placed",
        f"Asset:       {asset}",
        f"Order ID:    #{order.id}",
        f"Limit price: ${order.limit_price:,.2f}" + (f"  ({dist:.1f}x ATR below market)" if dist else ""),
        f"Market now:  ${current:,.2f}",
        f"Stop loss:   ${order.stop_price:,.2f}",
        f"Take profit: ${order.target_price:,.2f}",
        "Fee:         0.2% maker (saves 0.4% vs market order)",
        "Expires:     24h",
    ]
    if order.reasoning:
        lines.append(f"Reason: {order.reasoning[:120]}")
    return "\n".join(lines)


def format_limit_order_filled(asset: str, order: "PendingOrder", fill_price: float) -> str:  # type: ignore[name-defined]
    lines = [
        "Crypto Orchestra — Limit Order FILLED",
        f"Asset:       {asset}",
        f"Order ID:    #{order.id}",
        f"Fill price:  ${fill_price:,.2f}",
        f"Limit was:   ${order.limit_price:,.2f}",
        f"Stop loss:   ${order.stop_price:,.2f}",
        f"Take profit: ${order.target_price:,.2f}",
        "Fee paid:    0.2% maker",
    ]
    return "\n".join(lines)


def format_position_opened(pos: "Position") -> str:  # type: ignore[name-defined]
    lines = [
        "Crypto Orchestra — Position OPENED",
        f"Asset:       {pos.asset}",
        f"Position ID: #{pos.id}",
        f"Entry price: ${pos.entry_price:,.2f}",
        f"Size:        ${pos.qty_usd:,.0f} USD",
        f"Stop loss:   ${pos.stop_price:,.2f}",
        f"Take profit: ${pos.target_price:,.2f}",
        f"Entry fee:   0.2% maker (${pos.qty_usd * 0.002:.2f})",
    ]
    return "\n".join(lines)


def format_position_closed(record: dict) -> str:
    pnl    = record.get("pnl_usd", 0.0)
    pnl_pct = record.get("pnl_pct", 0.0)
    result = "PROFIT" if pnl >= 0 else "LOSS"
    sign   = "+" if pnl >= 0 else ""
    lines = [
        f"Crypto Orchestra — Position CLOSED ({result})",
        f"Asset:       {record.get('asset', '')}",
        f"Position ID: #{record.get('id', '')}",
        f"Reason:      {record.get('reason', '')}",
        f"Entry:       ${record.get('entry_price', 0):,.2f}",
        f"Exit:        ${record.get('exit_price', 0):,.2f}",
        f"Net P&L:     {sign}${pnl:.2f}  ({sign}{pnl_pct:.2f}%)",
        f"Fees paid:   ${record.get('entry_fee_usd', 0):.2f} + ${record.get('exit_fee_usd', 0):.2f}",
        f"Hold time:   {record.get('hold_hours', 0):.1f}h",
    ]
    return "\n".join(lines)


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
