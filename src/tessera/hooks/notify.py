"""Notification hook implementation."""

from __future__ import annotations

from typing import Any

from rich.console import Console

from tessera.hooks.base import BaseHook


class NotifyHook(BaseHook):
    """Emit console notifications for events."""

    name = "notify"
    version = "0.1.0"

    def __init__(self, config: dict):
        super().__init__(config)
        self.console = Console(record=True)

    def execute(self, event: str, context: dict[str, Any]) -> None:
        self.console.print(f"[bold blue]{event}[/] {context.get('message', '')}".strip())

