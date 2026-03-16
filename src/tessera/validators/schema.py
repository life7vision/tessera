"""Schema validator implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow.parquet as pq

from tessera.validators.base import (
    BaseValidator,
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
)


class SchemaValidator(BaseValidator):
    """Validate basic tabular schema consistency."""

    name = "schema"
    version = "0.1.0"

    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
        issues: list[ValidationIssue] = []

        if file_path.suffix == ".csv":
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                rows = list(reader)
            if not rows or not rows[0]:
                issues.append(self._issue("MISSING_HEADER", "CSV baslik satiri icermiyor."))
            else:
                expected_columns = len(rows[0])
                for row in rows[1:]:
                    if len(row) != expected_columns:
                        issues.append(
                            self._issue("INCONSISTENT_COLUMNS", "CSV sutun sayisi tutarsiz.")
                        )
                        break

        elif file_path.suffix == ".json":
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
                if not isinstance(payload, (list, dict)):
                    issues.append(self._issue("INVALID_JSON_SHAPE", "JSON array veya object olmali."))
            except json.JSONDecodeError:
                issues.append(self._issue("INVALID_JSON", "JSON formatı gecersiz."))

        elif file_path.suffix == ".parquet":
            try:
                pq.read_schema(file_path)
            except Exception:
                issues.append(self._issue("INVALID_PARQUET_SCHEMA", "Parquet schema okunamadi."))

        level = ValidationLevel.FAILED if issues else ValidationLevel.PASSED
        return ValidationResult(
            validator_name=self.name,
            level=level,
            issues=issues,
            duration_ms=0,
        )

    def _issue(self, code: str, message: str) -> ValidationIssue:
        return ValidationIssue(level=ValidationLevel.FAILED, code=code, message=message)

