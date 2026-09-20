"""Append-only declarations for forward agent-shadow variants."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "docs" / "operations" / "agent_shadow_variants.jsonl"

_REQUIRED = {
    "variant_id",
    "declared_date",
    "assets",
    "candidate_definition",
    "agents",
    "orchestrator_weighting",
    "models",
}


def validate_variant(value: dict[str, Any]) -> dict[str, Any]:
    missing = sorted(_REQUIRED - value.keys())
    if missing:
        raise ValueError(f"variant is missing required fields: {missing}")
    if not isinstance(value["variant_id"], str) or not value["variant_id"]:
        raise ValueError("variant_id must be a non-empty string")
    if not isinstance(value["assets"], list) or not value["assets"]:
        raise ValueError("assets must be a non-empty list")
    definition = value["candidate_definition"]
    if not isinstance(definition, dict) or definition.get("stage") != "wide":
        raise ValueError("candidate_definition.stage must be 'wide'")
    if not isinstance(value["agents"], list) or not value["agents"]:
        raise ValueError("agents must be a non-empty list")
    models = value["models"]
    if not isinstance(models, dict) or not models.get("subagents") or not models.get("orchestrator"):
        raise ValueError("models must declare subagents and orchestrator ids")
    if not isinstance(value["orchestrator_weighting"], dict):
        raise ValueError("orchestrator_weighting must be an object")
    return value


def load_variants(path: Path = DEFAULT_REGISTRY) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    seen: set[str] = set()
    if not path.exists():
        return variants
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            variant = validate_variant(json.loads(raw))
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"invalid variant at {path}:{line_number}: {exc}") from exc
        variant_id = variant["variant_id"]
        if variant_id in seen:
            raise ValueError(f"duplicate variant_id {variant_id!r} in {path}")
        seen.add(variant_id)
        variants.append(variant)
    return variants


def get_variant(variant_id: str, path: Path = DEFAULT_REGISTRY) -> dict[str, Any]:
    for variant in load_variants(path):
        if variant["variant_id"] == variant_id:
            return variant
    raise KeyError(f"unknown agent-shadow variant: {variant_id}")


def append_variant(variant: dict[str, Any], path: Path = DEFAULT_REGISTRY) -> None:
    """Append a new declaration; there is intentionally no update operation."""
    validate_variant(variant)
    existing = {item["variant_id"] for item in load_variants(path)}
    if variant["variant_id"] in existing:
        raise ValueError(
            f"variant {variant['variant_id']!r} already exists; changes require a new id"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(variant, sort_keys=True, separators=(",", ":")) + "\n")
