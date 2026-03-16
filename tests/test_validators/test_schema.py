"""Tests for the schema validator."""

from __future__ import annotations

from pathlib import Path

from tessera.validators.base import ValidationLevel
from tessera.validators.schema import SchemaValidator


def test_schema_accepts_valid_csv(sample_csv):
    result = SchemaValidator({}).validate(sample_csv)
    assert result.level == ValidationLevel.PASSED


def test_schema_rejects_inconsistent_csv(tmp_path: Path):
    path = tmp_path / "broken.csv"
    path.write_text("id,name\n1\n2,beta,extra\n", encoding="utf-8")
    result = SchemaValidator({}).validate(path)
    assert result.level == ValidationLevel.FAILED

