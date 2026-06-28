"""Generate realistic demo data for the Streamlit dashboard showcase."""
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

DEMO_DIR = Path(__file__).parent
START_DATE = datetime(2026, 3, 1, tzinfo=timezone.utc)
ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

ASSET_PRICES = {
    "BTC-USD": 82000,
    "ETH-USD": 2100,
    "SOL-USD": 140,
    "ZEC-USD": 580,
}

ASSET_ATR_PCT = {
    "BTC-USD": 0.025,
    "ETH-USD": 0.030,
    "SOL-USD": 0.045,
    "ZEC-USD": 0.055,
}


def gen_trades():
    trades = []
    current_date = START_DATE

    scenario = [
        ("BTC-USD", True,  "TAKE_PROFIT", 8),
        ("ZEC-USD", False, "STOP_LOSS",   6),
        ("ETH-USD", True,  "TAKE_PROFIT", 12),
        ("SOL-USD", False, "STOP_LOSS",   7),
        ("BTC-USD", True,  "TAKE_PROFIT", 9),
        ("ZEC-USD", True,  "TAKE_PROFIT", 5),
        ("ETH-USD", False, "STOP_LOSS",   8),
        ("BTC-USD", True,  "TAKE_PROFIT", 11),
        ("SOL-USD", True,  "MAX_HOLD",    12),
        ("ZEC-USD", False, "STOP_LOSS",   6),
        ("BTC-USD", True,  "TAKE_PROFIT", 7),
        ("ETH-USD", True,  "TAKE_PROFIT", 10),
        ("SOL-USD", False, "STOP_LOSS",   5),
        ("ZEC-USD", True,  "TAKE_PROFIT", 8),
        ("BTC-USD", False, "STOP_LOSS",   9),
        ("ETH-USD", True,  "TAKE_PROFIT", 6),
        ("SOL-USD", True,  "TAKE_PROFIT", 11),
        ("ZEC-USD", True,  "MAX_HOLD",    12),
        ("BTC-USD", True,  "TAKE_PROFIT", 8),
        ("ETH-USD", False, "STOP_LOSS",   7),
    ]

    for asset, is_win, reason, hold_h in scenario:
        base = ASSET_PRICES[asset]
        atr_pct = ASSET_ATR_PCT[asset]
        entry_price = base * (1 + random.uniform(-0.02, 0.02))

        if is_win:
            exit_price = entry_price * (1 + atr_pct * 4.0 * random.uniform(0.8, 1.0))
        else:
            exit_price = entry_price * (1 - atr_pct * 2.5 * random.uniform(0.8, 1.0))

        qty_usd = 200.0
        entry_fee = qty_usd * 0.002
        exit_fee = qty_usd * 0.004
        pnl_usd = qty_usd * (exit_price - entry_price) / entry_price - entry_fee - exit_fee
        pnl_pct = (exit_price - entry_price) / entry_price * 100

        entry_time = current_date + timedelta(hours=random.randint(2, 18))
        exit_time = entry_time + timedelta(hours=hold_h)
        current_date = exit_time + timedelta(hours=random.randint(6, 48))

        trades.append({
            "closed_at_utc": exit_time.isoformat(),
            "id": uuid.uuid4().hex[:8],
            "asset": asset,
            "entry_price": round(entry_price, 2),
            "exit_price": round(exit_price, 2),
            "entry_time": entry_time.isoformat(),
            "exit_time": exit_time.isoformat(),
            "reason": reason,
            "qty_usd": qty_usd,
            "entry_fee_usd": round(entry_fee, 2),
            "exit_fee_usd": round(exit_fee, 2),
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct, 4),
            "hold_hours": hold_h,
        })

    return trades


def gen_decisions(trades):
    decisions = []
    agents = ["technical", "macro", "sentiment", "whale", "risk"]
    weights = {"technical": 0.25, "macro": 0.30, "sentiment": 0.15, "whale": 0.20, "risk": 0.10}

    current = START_DATE
    end = START_DATE + timedelta(days=90)
    while current < end:
        for asset in ASSETS:
            action = "HOLD"
            decisions.append({
                "logged_at_utc": current.isoformat(),
                "asset": asset,
                "action": action,
                "confidence": round(random.uniform(0.30, 0.50), 2),
                "reasoning": "Market conditions do not meet entry thresholds.",
                "veto_triggered": False,
                "veto_reason": None,
                "overrides": [],
                "votes": [
                    {
                        "agent": a,
                        "signal": random.choice(["NEUTRAL", "NEUTRAL", "SELL"]),
                        "confidence": round(random.uniform(0.35, 0.65), 2),
                        "weight_applied": weights[a],
                    }
                    for a in agents
                ],
            })
        current += timedelta(hours=1)

    # Inject BUY decisions around trade entry times
    for trade in trades:
        entry = datetime.fromisoformat(trade["entry_time"])
        decisions.append({
            "logged_at_utc": (entry - timedelta(hours=1)).isoformat(),
            "asset": trade["asset"],
            "action": "BUY",
            "confidence": round(random.uniform(0.60, 0.82), 2),
            "reasoning": "Three agents align on BUY with solid momentum confirmation.",
            "veto_triggered": False,
            "veto_reason": None,
            "overrides": [],
            "votes": [
                {
                    "agent": "technical",
                    "signal": "BUY",
                    "confidence": round(random.uniform(0.60, 0.80), 2),
                    "weight_applied": 0.25,
                },
                {
                    "agent": "macro",
                    "signal": "BUY",
                    "confidence": round(random.uniform(0.65, 0.85), 2),
                    "weight_applied": 0.30,
                },
                {
                    "agent": "risk",
                    "signal": "BUY",
                    "confidence": 0.85,
                    "weight_applied": 0.10,
                },
                {
                    "agent": "sentiment",
                    "signal": random.choice(["NEUTRAL", "BUY"]),
                    "confidence": round(random.uniform(0.40, 0.65), 2),
                    "weight_applied": 0.15,
                },
                {
                    "agent": "whale",
                    "signal": "NEUTRAL",
                    "confidence": round(random.uniform(0.35, 0.55), 2),
                    "weight_applied": 0.20,
                },
            ],
        })

    decisions.sort(key=lambda d: d["logged_at_utc"])
    return decisions


def main():
    trades = gen_trades()
    decisions = gen_decisions(trades)

    trade_file = DEMO_DIR / "trade_history.jsonl"
    with trade_file.open("w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t) + "\n")

    decisions_file = DEMO_DIR / "agent_decisions.jsonl"
    with decisions_file.open("w", encoding="utf-8") as f:
        for d in decisions:
            f.write(json.dumps(d) + "\n")

    wins = sum(1 for t in trades if t["pnl_usd"] > 0)
    total_pnl = sum(t["pnl_usd"] for t in trades)
    print(f"Generated {len(trades)} trades — WR {wins/len(trades)*100:.0f}% — P&L ${total_pnl:+.2f}")
    print(f"Generated {len(decisions)} decisions")
    print(f"Saved to {DEMO_DIR}/")


if __name__ == "__main__":
    main()
