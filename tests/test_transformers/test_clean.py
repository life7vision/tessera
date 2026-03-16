"""Tests for the clean transformer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tessera.transformers.clean import CleanTransformer


def test_clean_transformer_normalizes_columns(tmp_path: Path):
    source = tmp_path / "input.csv"
    source.write_text("First Name,Last Name\nAda,Lovelace\n", encoding="utf-8")
    target = tmp_path / "output.csv"

    result = CleanTransformer({}).transform(source, target)

    dataframe = pd.read_csv(target)
    assert result.success is True
    assert list(dataframe.columns) == ["first_name", "last_name"]

