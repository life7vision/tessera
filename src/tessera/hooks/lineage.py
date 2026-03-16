"""Lineage hook implementation."""

from __future__ import annotations

from typing import Any

from tessera.hooks.base import BaseHook


class LineageHook(BaseHook):
    """Collect lineage events in memory or via callback."""

    name = "lineage"
    version = "0.1.0"

    def execute(self, event: str, context: dict[str, Any]) -> None:
        store = context.setdefault("lineage_events", [])
        store.append({"event": event, "context": dict(context)})
