"""Base interfaces for validator plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class ValidationLevel(Enum):
    """Severity level for validation outcomes."""

    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


@dataclass
class ValidationIssue:
    """Single validation issue."""

    level: ValidationLevel
    code: str
    message: str
    column: str | None = None
    details: dict = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Aggregate validation result."""

    validator_name: str
    level: ValidationLevel
    issues: list[ValidationIssue]
    duration_ms: int
    metadata: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """Return whether validation is considered successful."""

        return self.level != ValidationLevel.FAILED


class BaseValidator(ABC):
    """Base class for all validators."""

    name: str = "base"
    version: str = "0.1.0"
    supported_formats: list[str] = []

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def validate(
        self, file_path: Path, metadata: dict | None = None
    ) -> ValidationResult:
        """Validate a file."""

