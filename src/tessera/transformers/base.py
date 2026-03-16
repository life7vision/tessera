"""Base interfaces for transformer plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TransformResult:
    """Output from a transformation step."""

    transformer_name: str
    success: bool
    input_path: Path
    output_path: Path
    input_checksum: str
    output_checksum: str
    input_size: int
    output_size: int
    duration_ms: int
    details: dict
    error_message: str | None = None


class BaseTransformer(ABC):
    """Base class for all transformers."""

    name: str = "base"
    version: str = "0.1.0"
    input_formats: list[str] = []
    output_format: str = ""

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def transform(
        self, input_path: Path, output_path: Path, **kwargs
    ) -> TransformResult:
        """Transform input data into an output artifact."""

    def can_handle(self, file_path: Path) -> bool:
        """Return whether this transformer can process a file."""

        suffix = file_path.suffix.lstrip(".")
        return not self.input_formats or suffix in self.input_formats

