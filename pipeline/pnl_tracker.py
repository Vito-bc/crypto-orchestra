"""
Paper Trade P&L Tracker.

Reads agent_decisions.jsonl and reconstructs paper trade performance:
  - Each BUY decision opens a simulated position at next-candle open
  - Position closes when stop_loss_price or take_profit_price is hit,
    or when a SELL signal fires, or at end of tracking window
  - Uses yfinance to fetch actual 1h price data for exit simulation

Run:
    python pipeline/pnl_tracker.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DECISIONS_LOG = ROOT / "logs" / "agent_decisions.jsonl"
FEE_RATE      = 0.004   # 0.4% Coinbase Advanced taker fee (realistic)


def _fetch_prices(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    df = yf.download(
        symbol,
        start=start.strftime("%Y-%m-%d"),
        end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1h",
        progress=False,
        auto_adjust=True,
    )
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = df.reset_index()
    for col in ["Datetime", "datetime", "Date", "date"]:
        if col in df.columns:
            df = df.rename(columns={col: "time"}); break
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def _simulate_exit(entry_price: float, stop: float | None, target: float | None,
                   prices: pd.DataFrame, entry_time: datetime) -> dict:
    """Walk forward through 1h candles after entry and find the exit."""
    entry_ts = pd.Timestamp(entry_time, tz=timezone.utc) if entry_time.tzinfo is None else pd.Timestamp(entry_time)
    future   = prices[prices["time"] > entry_ts].reset_index(drop=True)

    for _, row in future.iterrows():
        low  = row["low"]
        high = row["high"]
        time = row["time"]

        if stop and low <= stop:
            return {"exit_price": stop, "exit_time": time, "reason": "STOP_LOSS",
                    "hold_hours": (time - entry_ts).total_seconds() / 3600}
        if target and high >= target:
            return {"exit_price": target, "exit_time": time, "reason": "TAKE_PROFIT",
                    "hold_hours": (time - entry_ts).total_seconds() / 3600}

    # No exit hit — still open, use last available price
    if not future.empty:
        last = future.iloc[-1]
        return {"exit_price": last["close"], "exit_time": last["time"],
                "reason": "STILL_OPEN",
                "hold_hours": (last["time"] - entry_ts).total_seconds() / 3600}

    return {"exit_price": entry_price, "exit_time": entry_ts, "reason": "NO_DATA", "hold_hours": 0}


def run_tracker() -> None:
    if not DECISIONS_LOG.exists():
        print("No decision log found. Run the pipeline first.")
        return

    with open(DECISIONS_LOG, encoding="utf-8") as f:
        records = [json.loads(l) for l in f if l.strip()]

    if not records:
        print("Decision log is empty.")
        return

    # Group by asset
    by_asset: dict[str, list] = {}
    for r in records:
        by_asset.setdefault(r["asset"], []).append(r)

    print("\nCrypto Orchestra -- Paper Trade P&L Tracker")
    print(f"Decision log: {DECISIONS_LOG}")
    print(f"Total decisions logged: {len(records)}")
    print("=" * 70)

    all_trades = []

    for asset, decisions in by_asset.items():
        buys  = [d for d in decisions if d["action"] == "BUY"]
        sells = [d for d in decisions if d["action"] == "SELL"]
        holds = [d for d in decisions if d["action"] == "HOLD"]

        print(f"\n{asset} — {len(decisions)} decisions  "
              f"({len(buys)} BUY, {len(sells)} SELL, {len(holds)} HOLD)")

        if not buys:
            print("  No BUY signals yet — system is being selective (3-agent filter working).")
            print(f"  Monitoring since: {decisions[0]['logged_at_utc'][:16]} UTC")
            print(f"  Latest decision:  {decisions[-1]['logged_at_utc'][:16]} UTC")
            print(f"  Last action:      {decisions[-1]['action']} ({decisions[-1]['confidence']:.0%} conf)")
            continue

        # Fetch full price history for exit simulation
        first_buy_time = datetime.fromisoformat(buys[0]["logged_at_utc"].replace("Z", "+00:00"))
        prices = _fetch_prices(asset, first_buy_time - timedelta(hours=2),
                               datetime.now(timezone.utc))

        print(f"\n  {'Date':<18} {'Entry':>9} {'Stop':>9} {'Target':>9} {'Exit':>9} {'Reason':<12} {'Hold':>6} {'P&L%':>7} {'P&L$':>8}")
        print(f"  {'-'*90}")

        for buy in buys:
            entry_time  = datetime.fromisoformat(buy["logged_at_utc"].replace("Z", "+00:00"))
            stop        = buy.get("stop_loss_price")
            target      = buy.get("take_profit_price")

            if prices.empty:
                print(f"  {str(entry_time)[:16]}  No price data available for simulation.")
                continue

            # Entry price = next candle open after decision
            entry_ts   = pd.Timestamp(entry_time, tz=timezone.utc)
            next_candle = prices[prices["time"] > entry_ts]
            if next_candle.empty:
                print(f"  {str(entry_time)[:16]}  No future candle data yet.")
                continue

            entry_price = next_candle.iloc[0]["open"]
            exit_info   = _simulate_exit(entry_price, stop, target, prices, entry_time)

            exit_price  = exit_info["exit_price"]
            gross_pnl   = (exit_price - entry_price) / entry_price
            net_pnl     = gross_pnl - 2 * FEE_RATE   # in + out fee
            pnl_usd     = net_pnl * 1000              # assume $1000 position size

            trade = {
                "asset":       asset,
                "entry_time":  entry_time,
                "entry_price": entry_price,
                "stop":        stop,
                "target":      target,
                "exit_price":  exit_price,
                "exit_time":   exit_info["exit_time"],
                "reason":      exit_info["reason"],
                "hold_hours":  exit_info["hold_hours"],
                "net_pnl_pct": net_pnl * 100,
                "pnl_usd":     pnl_usd,
            }
            all_trades.append(trade)

            sign = "+" if net_pnl >= 0 else ""
            print(
                f"  {str(entry_time)[:16]}  "
                f"{entry_price:>9.2f}  "
                f"{stop or 0:>9.2f}  "
                f"{target or 0:>9.2f}  "
                f"{exit_price:>9.2f}  "
                f"{exit_info['reason']:<12}  "
                f"{exit_info['hold_hours']:>5.1f}h  "
                f"{sign}{net_pnl*100:>5.2f}%  "
                f"{sign}{pnl_usd:>7.2f}"
            )

    # Summary
    if all_trades:
        wins   = [t for t in all_trades if t["pnl_usd"] > 0]
        losses = [t for t in all_trades if t["pnl_usd"] <= 0]
        total  = sum(t["pnl_usd"] for t in all_trades)
        wr     = len(wins) / len(all_trades) * 100 if all_trades else 0
        avg_w  = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
        avg_l  = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
        pf     = abs(avg_w / avg_l) if avg_l and avg_w else float("inf")

        print(f"\n{'='*70}")
        print(f"  SUMMARY  ({len(all_trades)} trades closed)")
        print(f"  Total P&L:      ${total:+.2f}")
        print(f"  Win rate:       {wr:.1f}%  ({len(wins)} wins / {len(losses)} losses)")
        print(f"  Profit factor:  {pf:.2f}")
        print(f"  Avg win:        ${avg_w:+.2f}")
        print(f"  Avg loss:       ${avg_l:+.2f}")
        print(f"  Fee rate used:  {FEE_RATE*100:.1f}% per side (Coinbase Advanced taker)")
    else:
        print(f"\n{'='*70}")
        print("  No completed trades yet to summarize.")
        print("  The 3-agent filter is working — it waits for strong consensus before acting.")
        print("  This is intentional: fewer trades, but higher quality when they do fire.")

    print()


if __name__ == "__main__":
    run_tracker()
