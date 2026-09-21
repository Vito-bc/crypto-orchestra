"""Persistent fail-closed monthly cost ceiling for shadow model calls."""

from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEND = ROOT / "logs" / "agent_shadow_spend.json"
DEFAULT_CEILING_USD = 5.0
CEILING_ENV = "AGENT_SHADOW_MONTHLY_CEILING_USD"

_PRICES_PER_MILLION = {
    "claude-haiku-4-5-20251001": (1.0, 5.0),
    "claude-sonnet-4-6": (3.0, 15.0),
}


class SpendCeilingExceeded(RuntimeError):
    pass


@dataclass
class CallTelemetry:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    api_calls: int = 0
    usage_available: bool = True
    refusal: str | None = None
    errors: list[str] = field(default_factory=list)


def token_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    try:
        input_price, output_price = _PRICES_PER_MILLION[model]
    except KeyError as exc:
        raise ValueError(f"no declared price for model {model!r}") from exc
    return (input_tokens * input_price + output_tokens * output_price) / 1_000_000


class MonthlyBudget:
    def __init__(
        self,
        path: Path = DEFAULT_SPEND,
        *,
        ceiling_usd: float | None = None,
        now: datetime | None = None,
    ) -> None:
        self.path = Path(path)
        raw_ceiling = os.getenv(CEILING_ENV, str(DEFAULT_CEILING_USD))
        self.ceiling_usd = float(raw_ceiling) if ceiling_usd is None else float(ceiling_usd)
        if self.ceiling_usd < 0:
            raise ValueError("monthly spend ceiling must be non-negative")
        self.now = now or datetime.now(timezone.utc)
        self.month = self.now.strftime("%Y-%m")
        self._lock = threading.Lock()

    def _fresh(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "month": self.month,
            "ceiling_usd": self.ceiling_usd,
            "spent_usd": 0.0,
            "reservations": {},
            "updated_at": self.now.isoformat(),
        }

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._fresh()
        state = json.loads(self.path.read_text(encoding="utf-8"))
        if state.get("month") != self.month:
            return self._fresh()
        state.setdefault("reservations", {})
        return state

    def _write(self, state: dict[str, Any]) -> None:
        state["ceiling_usd"] = self.ceiling_usd
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(state, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(self.path)

    @staticmethod
    def _request_upper_tokens(kwargs: dict[str, Any]) -> tuple[int, int]:
        payload = json.dumps(kwargs, sort_keys=True, default=str).encode("utf-8")
        input_upper = len(payload) + 4096
        output_upper = int(kwargs.get("max_tokens", 1024))
        return input_upper, output_upper

    def reserve(self, model: str, kwargs: dict[str, Any]) -> tuple[str, float]:
        input_upper, output_upper = self._request_upper_tokens(kwargs)
        reserve_usd = token_cost(model, input_upper, output_upper)
        with self._lock:
            state = self._read()
            reserved = sum(float(v) for v in state["reservations"].values())
            committed = float(state.get("spent_usd", 0.0)) + reserved
            if committed + reserve_usd > self.ceiling_usd:
                raise SpendCeilingExceeded(
                    "AGENT SHADOW SPEND CEILING: model call refused; "
                    f"month={self.month} committed=${committed:.6f} "
                    f"request_reserve=${reserve_usd:.6f} ceiling=${self.ceiling_usd:.2f}"
                )
            reservation_id = uuid.uuid4().hex
            state["reservations"][reservation_id] = reserve_usd
            self._write(state)
        return reservation_id, reserve_usd

    def settle(self, reservation_id: str, actual_usd: float | None) -> float:
        with self._lock:
            state = self._read()
            reserved = float(state["reservations"].pop(reservation_id))
            charged = reserved if actual_usd is None else float(actual_usd)
            state["spent_usd"] = float(state.get("spent_usd", 0.0)) + charged
            self._write(state)
        return charged

    def state(self) -> dict[str, Any]:
        with self._lock:
            return self._read()


class _BudgetedMessages:
    def __init__(self, inner, budget: MonthlyBudget, telemetry: CallTelemetry) -> None:
        self._inner = inner
        self._budget = budget
        self._telemetry = telemetry

    def create(self, **kwargs):
        model = str(kwargs["model"])
        try:
            reservation_id, _ = self._budget.reserve(model, kwargs)
        except SpendCeilingExceeded as exc:
            self._telemetry.refusal = str(exc)
            raise
        try:
            response = self._inner.create(**kwargs)
        except Exception as exc:
            charged = self._budget.settle(reservation_id, None)
            self._telemetry.cost_usd += charged
            self._telemetry.usage_available = False
            self._telemetry.errors.append(str(exc))
            raise
        usage = getattr(response, "usage", None)
        input_tokens = getattr(usage, "input_tokens", None)
        output_tokens = getattr(usage, "output_tokens", None)
        if input_tokens is None or output_tokens is None:
            charged = self._budget.settle(reservation_id, None)
            self._telemetry.usage_available = False
        else:
            input_tokens = int(input_tokens)
            output_tokens = int(output_tokens)
            charged = self._budget.settle(
                reservation_id, token_cost(model, input_tokens, output_tokens)
            )
            self._telemetry.input_tokens += input_tokens
            self._telemetry.output_tokens += output_tokens
        self._telemetry.cost_usd += charged
        self._telemetry.api_calls += 1
        return response


class BudgetedClient:
    def __init__(self, inner, budget: MonthlyBudget, telemetry: CallTelemetry) -> None:
        self.messages = _BudgetedMessages(inner.messages, budget, telemetry)
