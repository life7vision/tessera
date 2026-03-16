"""Tests for the format transformer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tessera.transformers.format import FormatTransformer


def test_format_transformer_converts_csv_to_parquet(sample_csv, tmp_path: Path):
    target = tmp_path / "output.parquet"
    result = FormatTransformer({"default_format": "parquet"}).transform(sample_csv, target)

    dataframe = pd.read_parquet(target)
    assert result.success is True
    assert list(dataframe.columns) == ["id", "name", "value"]

