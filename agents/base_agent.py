"""
Abstract base class for all Crypto Orchestra agents.

Every sub-agent inherits from BaseAgent and implements analyze().
The Claude client is shared and initialized once from the environment.
"""

from __future__ import annotations

import json
import os
import re
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from schemas.signals import AgentName, AgentSignal, SignalType

_VALID_SIGNALS = {"BUY", "SELL", "NEUTRAL"}


class BaseAgent(ABC):
    name: AgentName

    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY is not set in .env")
        self.model  = os.getenv("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")
        self.client = anthropic.Anthropic(api_key=api_key)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _ask_claude(self, system: str, user: str, max_tokens: int = 1024) -> str:
        message = self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return message.content[0].text

    @staticmethod
    def _extract_json(raw: str) -> dict:
        """
        Extract a JSON object from Claude's response robustly.

        Handles three common formats:
          1. Bare JSON:              {"signal": "BUY", ...}
          2. Fenced with language:   ```json\n{...}\n```
          3. Fenced bare:            ```\n{...}\n```
          4. JSON embedded in prose: "Here is the result: {...}"
        """
        # Strip code fences first
        text = raw.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            text = text.strip()

        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Fall back: pull first {...} block from anywhere in the response
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        raise json.JSONDecodeError("No valid JSON object found in response", text, 0)

    @staticmethod
    def _sanitize(result: dict) -> dict:
        """
        Guarantee required keys exist with valid values.
        Called after every successful JSON parse.
        """
        # signal — must be BUY/SELL/NEUTRAL
        raw_signal = str(result.get("signal", "NEUTRAL")).upper().strip()
        if raw_signal not in _VALID_SIGNALS:
            raw_signal = "NEUTRAL"
        result["signal"] = raw_signal

        # confidence — must be float in [0.0, 1.0]
        try:
            conf = float(result.get("confidence", 0.3))
        except (TypeError, ValueError):
            conf = 0.3
        result["confidence"] = max(0.0, min(1.0, conf))

        # reasoning — must be a non-empty string
        reasoning = result.get("reasoning", "")
        if not isinstance(reasoning, str) or not reasoning.strip():
            result["reasoning"] = "No reasoning provided."

        return result

    def _ask_claude_json(self, system: str, user: str, max_tokens: int = 1024) -> dict:
        """
        Ask Claude and return a validated JSON dict.

        Strategy:
          - Attempt 1: parse normally
          - Attempt 2: retry with explicit JSON reminder (handles temporary
            model responses that include prose before the JSON)
          - Both fail: raise so run() can catch and return a neutral fallback
        """
        retry_suffix = (
            "\n\nIMPORTANT: Your previous response could not be parsed as JSON. "
            "Return ONLY a raw JSON object — no markdown, no explanation, just the JSON."
        )

        last_error: Exception = RuntimeError("No attempts made")

        for attempt in range(2):
            try:
                prompt = user if attempt == 0 else user + retry_suffix
                raw    = self._ask_claude(system, prompt, max_tokens)
                result = self._extract_json(raw)
                return self._sanitize(result)
            except (json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                if attempt == 0:
                    print(
                        f"[{self.name.value}] JSON parse failed (attempt 1), retrying...",
                        file=sys.stderr,
                    )

        raise ValueError(f"Claude returned invalid JSON after 2 attempts: {last_error}") from last_error

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    # ── Interface ──────────────────────────────────────────────────────────────

    @abstractmethod
    def analyze(self, asset: str) -> AgentSignal:
        """Run analysis and return a structured signal."""
        ...

    def run(self, asset: str) -> AgentSignal:
        """
        Public entry point. Wraps analyze() so one failing agent
        never crashes the whole pipeline.
        """
        try:
            return self.analyze(asset)
        except Exception as exc:
            print(f"[{self.name.value}] ERROR: {exc}", file=sys.stderr)
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=SignalType.NEUTRAL,
                confidence=0.0,
                reasoning=f"Agent failed: {exc}",
                ttl_minutes=0,
            )
