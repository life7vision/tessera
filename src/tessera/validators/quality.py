"""Quality validator implementation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tessera.validators.base import (
    BaseValidator,
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
)


class QualityValidator(BaseValidator):
    """Check simple quality indicators."""

    name = "quality"
    version = "0.1.0"

    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
        issues: list[ValidationIssue] = []
        dataframe = self._load_dataframe(file_path)
        if dataframe.empty:
            issues.append(
                ValidationIssue(
                    level=ValidationLevel.FAILED,
                    code="EMPTY_DATASET",
                    message="Veri seti bos.",
                )
            )
        duplicate_rows = int(dataframe.duplicated().sum())
        if duplicate_rows:
            issues.append(
                ValidationIssue(
                    level=ValidationLevel.WARNING,
                    code="DUPLICATE_ROWS",
                    message="Tam duplike satirlar bulundu.",
                    details={"duplicate_rows": duplicate_rows},
                )
            )

        if len(dataframe.index) > 0:
            null_rates = (dataframe.isna().mean() * 100).to_dict()
            for column, ratio in null_rates.items():
                if ratio > 50:
                    issues.append(
                        ValidationIssue(
                            level=ValidationLevel.WARNING,
                            code="HIGH_NULL_RATE",
                            message="Null oranı yuksek.",
                            column=str(column),
                            details={"null_ratio": ratio},
                        )
                    )

        level = ValidationLevel.PASSED
        if any(issue.level == ValidationLevel.FAILED for issue in issues):
            level = ValidationLevel.FAILED
        elif issues:
            level = ValidationLevel.WARNING

        return ValidationResult(
            validator_name=self.name,
            level=level,
            issues=issues,
            duration_ms=0,
            metadata={"row_count": len(dataframe.index), "column_count": len(dataframe.columns)},
        )

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == ".csv":
            return pd.read_csv(file_path)
        if file_path.suffix == ".json":
            return pd.read_json(file_path)
        if file_path.suffix == ".parquet":
            return pd.read_parquet(file_path)
        raise ValueError(f"Desteklenmeyen kalite dosya formati: {file_path.suffix}")

