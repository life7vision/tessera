"""Tests for the quality validator."""

from __future__ import annotations

from pathlib import Path

import pytest

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


def test_quality_passes_on_clean_data(tmp_path: Path):
    path = tmp_path / "clean.csv"
    path.write_text("id,name\n1,alpha\n2,beta\n3,gamma\n", encoding="utf-8")

    result = QualityValidator({}).validate(path)

    assert result.level == ValidationLevel.PASSED
    assert result.issues == []


def test_quality_fails_on_empty_dataset(tmp_path: Path):
    path = tmp_path / "empty.csv"
    path.write_text("id,name\n", encoding="utf-8")

    result = QualityValidator({}).validate(path)

    assert result.level == ValidationLevel.FAILED
    assert any(issue.code == "EMPTY_DATASET" for issue in result.issues)


def test_quality_warns_on_high_null_rate(tmp_path: Path):
    # Column 'value' has 4 nulls out of 5 rows = 80% null
    path = tmp_path / "nulls.csv"
    path.write_text("id,value\n1,\n2,\n3,\n4,\n5,ok\n", encoding="utf-8")

    result = QualityValidator({}).validate(path)

    assert result.level == ValidationLevel.WARNING
    null_issues = [i for i in result.issues if i.code == "HIGH_NULL_RATE"]
    assert null_issues
    assert null_issues[0].column == "value"
    assert null_issues[0].details["null_ratio"] > 50


def test_quality_reports_metadata(tmp_path: Path):
    path = tmp_path / "data.csv"
    path.write_text("a,b,c\n1,2,3\n4,5,6\n", encoding="utf-8")

    result = QualityValidator({}).validate(path)

    assert result.metadata["row_count"] == 2
    assert result.metadata["column_count"] == 3


def test_quality_measures_duration(tmp_path: Path):
    path = tmp_path / "data.csv"
    path.write_text("x\n1\n2\n", encoding="utf-8")

    result = QualityValidator({}).validate(path)

    assert result.duration_ms >= 0


def test_quality_raises_on_unsupported_format(tmp_path: Path):
    path = tmp_path / "data.txt"
    path.write_text("some text\n")

    with pytest.raises(ValueError, match="Desteklenmeyen"):
        QualityValidator({}).validate(path)
