"""
Abstract base class for all Crypto Orchestra agents.

Every sub-agent inherits from BaseAgent and implements analyze().
The Claude client is shared and initialized once from the environment.
"""

from __future__ import annotations

import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import anthropic
from dotenv import load_dotenv

# Load .env from repo root
load_dotenv(Path(__file__).parent.parent / ".env")

from schemas.signals import AgentName, AgentSignal


class BaseAgent(ABC):
    """
    Shared foundation for all sub-agents.

    Subclasses must implement:
        - name  (AgentName)
        - analyze(asset: str) -> AgentSignal
    """

    name: AgentName

    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY is not set in .env")

        self.model = os.getenv("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")
        self.client = anthropic.Anthropic(api_key=api_key)

    # ── Helpers available to all agents ───────────────────────────────────────

    def _ask_claude(self, system: str, user: str, max_tokens: int = 1024) -> str:
        """
        Send a prompt to Claude and return the text response.
        temperature=0 for deterministic, reproducible decisions.
        """
        message = self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return message.content[0].text

    def _ask_claude_json(self, system: str, user: str, max_tokens: int = 1024) -> dict:
        """
        Ask Claude and parse the response as JSON.
        Strips markdown code fences if present.
        """
        raw = self._ask_claude(system, user, max_tokens)
        # Strip ```json ... ``` if Claude wraps output
        if raw.strip().startswith("```"):
            raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(raw)

    def _now(self) -> datetime:
        return datetime.utcnow()

    # ── Interface ──────────────────────────────────────────────────────────────

    @abstractmethod
    def analyze(self, asset: str) -> AgentSignal:
        """
        Run analysis for the given asset and return a structured signal.
        Must be implemented by every sub-agent.
        """
        ...

    def run(self, asset: str) -> AgentSignal:
        """
        Public entry point. Wraps analyze() with error handling so one
        failing agent never crashes the whole pipeline.
        """
        try:
            return self.analyze(asset)
        except Exception as exc:
            print(f"[{self.name}] ERROR: {exc}", file=sys.stderr)
            # Return a neutral fallback signal so the pipeline can continue
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=__import__("schemas.signals", fromlist=["SignalType"]).SignalType.NEUTRAL,
                confidence=0.0,
                reasoning=f"Agent failed with error: {exc}",
                ttl_minutes=0,
            )
