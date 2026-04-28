"""
Risk Management Agent.

Does NOT produce a directional signal — it answers "how much" and "with
what stops", not "whether to trade".

Reads:
  - Current ATR for stop/target calculation
  - Existing position state (to prevent over-exposure)
  - Portfolio-level daily loss limit from .env

Returns signal=NEUTRAL always, but populates metrics with:
  position_size_pct, stop_loss_price, take_profit_price, ok_to_trade
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.price_data import get_snapshot

_SYSTEM = """You are a risk management officer for a crypto trading system.

You receive current market data and portfolio constraints. Your job is to
determine whether it is safe to open a new position and, if so, at what size
and with what stop-loss and take-profit levels.

Output a JSON object with exactly these keys:
  ok_to_trade       : true | false
  position_size_pct : float (e.g. 0.02 = 2% of portfolio)
  stop_loss_price   : float
  take_profit_price : float
  confidence        : float between 0.0 and 1.0
  reasoning         : one sentence

Rules:
- Use ATR-based stops: stop = close - (2.0 x ATR), target = close + (4.0 x ATR)
- If daily loss limit is already hit: ok_to_trade=false, size=0
- If open positions >= max_positions: ok_to_trade=false, size=0
- If ATR is very high (> 3% of price): reduce size to 1% to limit exposure
- Normal conditions: position_size_pct = TRADE_SIZE_PCT from config
- confidence reflects how clean the risk picture is
- Return ONLY the JSON object, no markdown, no extra text."""


def _load_position_state(asset: str) -> dict:
    # asset-specific state file: BTC-USD → paper_position_btc.json
    base = asset.upper().replace("-USD", "").replace("/USDT", "").replace("/USD", "").lower()
    state_file = ROOT / "logs" / f"paper_position_{base}.json"
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {"status": "FLAT"}


class RiskAgent(BaseAgent):
    name = AgentName.RISK

    def analyze(self, asset: str) -> AgentSignal:
        snapshot = get_snapshot(asset)
        state    = _load_position_state(asset)

        trade_size   = float(os.getenv("TRADE_SIZE_PCT",   "0.02"))
        max_pos      = int(os.getenv("MAX_POSITIONS",       "5"))
        daily_limit  = float(os.getenv("DAILY_LOSS_LIMIT", "0.05"))

        if snapshot is None:
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=SignalType.NEUTRAL,
                confidence=0.0,
                reasoning="Cannot compute risk — price data unavailable.",
                metrics={"ok_to_trade": False},
            )

        close = snapshot["close"]
        atr   = snapshot["atr_1h"]
        atr_pct = atr / close if close else 0

        open_positions = 1 if state.get("status") == "LONG" else 0

        user_prompt = f"""Asset: {asset}

Current price: {close:.2f}
ATR (1h):      {atr:.4f}  ({atr_pct*100:.2f}% of price)

Portfolio constraints:
- TRADE_SIZE_PCT:   {trade_size} ({trade_size*100:.0f}% per trade)
- MAX_POSITIONS:    {max_pos}
- DAILY_LOSS_LIMIT: {daily_limit} ({daily_limit*100:.0f}%)
- Open positions:   {open_positions}
- Position status:  {state.get('status', 'FLAT')}

ATR-based levels if entering now:
- Stop loss:    {close - atr * 2.0:.2f}  (close - 2.0xATR)
- Take profit:  {close + atr * 4.0:.2f}  (close + 4.0xATR)
- Risk/reward:  1 : 2.0

Evaluate risk and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType.NEUTRAL,
            confidence=float(result.get("confidence", 0.5)),
            reasoning=result.get("reasoning", ""),
            metrics={
                "ok_to_trade":       result.get("ok_to_trade", False),
                "position_size_pct": result.get("position_size_pct", trade_size),
                "stop_loss_price":   result.get("stop_loss_price"),
                "take_profit_price": result.get("take_profit_price"),
                "atr":               atr,
                "atr_pct":           round(atr_pct * 100, 2),
            },
        )
