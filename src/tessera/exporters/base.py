"""Base interfaces for exporter plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExportResult:
    """Output from an export operation."""

    success: bool
    exporter_name: str
    output_path: Path
    size_bytes: int
    duration_ms: int
    error_message: str | None = None


class BaseExporter(ABC):
    """Base class for all exporters."""

    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def export(self, version_id: str, target_path: Path, **kwargs) -> ExportResult:
        """Export a dataset version."""

