"""Tests for the integrity validator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tessera.validators.base import ValidationLevel
from tessera.validators.integrity import IntegrityValidator


def test_integrity_accepts_valid_file(sample_csv):
    result = IntegrityValidator({}).validate(sample_csv)
    assert result.level == ValidationLevel.PASSED


def test_integrity_rejects_invalid_parquet(tmp_path: Path):
    bad_file = tmp_path / "broken.parquet"
    bad_file.write_text("not parquet", encoding="utf-8")
    result = IntegrityValidator({}).validate(bad_file)
    assert result.level == ValidationLevel.FAILED

