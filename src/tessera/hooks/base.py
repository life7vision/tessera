"""Base interfaces for hook plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseHook(ABC):
    """Base class for all hooks."""

    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def execute(self, event: str, context: dict[str, Any]) -> None:
        """Execute hook logic for an event."""

