"""Tests for the quality validator."""

from __future__ import annotations

from pathlib import Path

from tessera.validators.base import ValidationLevel
from tessera.validators.quality import QualityValidator


def test_quality_warns_on_duplicates_and_nulls(tmp_path: Path):
    path = tmp_path / "quality.csv"
    path.write_text(
        "id,name,value\n1,alpha,\n1,alpha,\n2,,\n",
        encoding="utf-8",
    )
    result = QualityValidator({}).validate(path)
    assert result.level == ValidationLevel.WARNING
    assert any(issue.code == "DUPLICATE_ROWS" for issue in result.issues)

