"""Append-only JSONL storage for shadow observations and later price paths."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from agent_shadow import LOG_SCHEMA_VERSION

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = ROOT / "logs" / "agent_shadow.jsonl"


class ShadowLog:
    def __init__(self, path: Path = DEFAULT_LOG) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        item = dict(record)
        item.setdefault("schema_version", LOG_SCHEMA_VERSION)
        item.setdefault("logged_at", datetime.now(timezone.utc).isoformat())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(item, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded + "\n")
            handle.flush()
        return item

    def records(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        output = []
        for line_number, raw in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if not raw.strip():
                continue
            try:
                output.append(json.loads(raw))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid shadow log line {line_number}: {exc}") from exc
        return output

    def extend(self, records: Iterable[dict[str, Any]]) -> None:
        for record in records:
            self.append(record)
