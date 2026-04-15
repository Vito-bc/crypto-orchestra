"""
Shared Pydantic schemas for inter-agent communication.

Every sub-agent returns an AgentSignal.
The orchestrator consumes a list of AgentSignal and returns a TradeDecision.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── Enums ─────────────────────────────────────────────────────────────────────

class SignalType(str, Enum):
    BUY     = "BUY"
    SELL    = "SELL"
    NEUTRAL = "NEUTRAL"


class AgentName(str, Enum):
    TECHNICAL   = "technical"
    MACRO       = "macro"
    SENTIMENT   = "sentiment"
    WHALE       = "whale"
    RISK        = "risk"


class MarketRegime(str, Enum):
    BULL    = "BULL"
    BEAR    = "BEAR"
    RANGING = "RANGING"
    UNKNOWN = "UNKNOWN"


class TradeAction(str, Enum):
    BUY   = "BUY"
    SELL  = "SELL"
    HOLD  = "HOLD"


# ── Sub-agent output ───────────────────────────────────────────────────────────

class AgentSignal(BaseModel):
    agent: AgentName
    asset: str                          # e.g. "BTC/USDT"
    timestamp: datetime
    signal: SignalType
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str                      # human-readable explanation
    ttl_minutes: int = 60               # how long this signal stays valid

    # Optional extras — agents fill what's relevant to their domain
    key_levels: Optional[dict] = None   # {"support": 80200, "resistance": 84500}
    regime: Optional[MarketRegime] = None
    metrics: Optional[dict] = None      # raw indicator values for logging


# ── Orchestrator output ────────────────────────────────────────────────────────

class AgentVote(BaseModel):
    agent: AgentName
    signal: SignalType
    confidence: float
    weight_applied: float               # how much the orchestrator weighted this agent


class TradeDecision(BaseModel):
    asset: str
    timestamp: datetime
    action: TradeAction
    confidence: float = Field(ge=0.0, le=1.0)

    # Position sizing from risk agent (None if HOLD)
    position_size_pct: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

    # Reasoning trail
    reasoning: str
    votes: list[AgentVote]
    overrides: list[str] = Field(default_factory=list)  # agents overridden + why
    veto_triggered: bool = False
    veto_reason: Optional[str] = None
