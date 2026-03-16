"""Integrity validator implementation."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pyarrow.parquet as pq

from tessera.validators.base import (
    BaseValidator,
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
)


class IntegrityValidator(BaseValidator):
    """Validate file readability and basic integrity."""

    name = "integrity"
    version = "0.1.0"

    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
        issues: list[ValidationIssue] = []
        if file_path.stat().st_size <= 0:
            issues.append(
                ValidationIssue(
                    level=ValidationLevel.FAILED,
                    code="EMPTY_FILE",
                    message="Dosya boyutu sifirdan buyuk olmali.",
                )
            )

        if file_path.suffix == ".zip" and not zipfile.is_zipfile(file_path):
            issues.append(
                ValidationIssue(
                    level=ValidationLevel.FAILED,
                    code="CORRUPT_ZIP",
                    message="ZIP dosyasi bozuk veya okunamiyor.",
                )
            )

        if file_path.suffix == ".parquet":
            try:
                pq.read_metadata(file_path)
            except Exception:
                issues.append(
                    ValidationIssue(
                        level=ValidationLevel.FAILED,
                        code="INVALID_PARQUET",
                        message="Parquet metadata okunamadi.",
                    )
                )

        level = ValidationLevel.PASSED if not issues else max(
            (issue.level for issue in issues),
            key=lambda value: [ValidationLevel.PASSED, ValidationLevel.WARNING, ValidationLevel.FAILED].index(value),
        )
        return ValidationResult(
            validator_name=self.name,
            level=level,
            issues=issues,
            duration_ms=0,
        )

