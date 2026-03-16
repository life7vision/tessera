"""Tests for the clean transformer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tessera.core.exceptions import TransformError
from tessera.transformers.clean import CleanTransformer


def test_clean_transformer_normalizes_columns(tmp_path: Path):
    source = tmp_path / "input.csv"
    source.write_text("First Name,Last Name\nAda,Lovelace\n", encoding="utf-8")
    target = tmp_path / "output.csv"

    result = CleanTransformer({}).transform(source, target)

    dataframe = pd.read_csv(target)
    assert result.success is True
    assert list(dataframe.columns) == ["first_name", "last_name"]


def test_clean_transformer_normalizes_special_char_columns(tmp_path: Path):
    source = tmp_path / "input.csv"
    source.write_text("Column (A),My-Value,  Spaces  \n1,2,3\n", encoding="utf-8")
    target = tmp_path / "output.csv"

    result = CleanTransformer({}).transform(source, target)

    assert result.success is True
    assert "column_a" in result.details["cleaned_columns"]
    assert "my_value" in result.details["cleaned_columns"]
    assert "spaces" in result.details["cleaned_columns"]


def test_clean_transformer_trims_string_whitespace(tmp_path: Path):
    source = tmp_path / "input.csv"
    source.write_text("name,city\n  Alice  ,  Istanbul  \n Bob , Ankara \n", encoding="utf-8")
    target = tmp_path / "output.csv"

    CleanTransformer({}).transform(source, target)

    df = pd.read_csv(target)
    assert df["name"].tolist() == ["Alice", "Bob"]
    assert df["city"].tolist() == ["Istanbul", "Ankara"]


def test_clean_transformer_handles_parquet(tmp_path: Path, sample_csv: Path):
    # first create parquet
    import pyarrow as pa
    import pyarrow.parquet as pq
    df = pd.DataFrame({"A Column": [1, 2], "B Column": [3, 4]})
    parquet_path = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    target = tmp_path / "output.parquet"
    result = CleanTransformer({}).transform(parquet_path, target)

    assert result.success is True
    assert "a_column" in result.details["cleaned_columns"]
    assert "b_column" in result.details["cleaned_columns"]


def test_clean_transformer_raises_on_bad_parquet(tmp_path: Path):
    bad_file = tmp_path / "bad.parquet"
    bad_file.write_bytes(b"\x00\x01\x02\x03GARBAGE_NOT_PARQUET")  # corrupt parquet
    target = tmp_path / "out.parquet"

    with pytest.raises(TransformError):
        CleanTransformer({}).transform(bad_file, target)


def test_clean_transformer_details_has_row_count(tmp_path: Path):
    source = tmp_path / "input.csv"
    source.write_text("id,val\n1,a\n2,b\n3,c\n", encoding="utf-8")
    target = tmp_path / "output.csv"

    result = CleanTransformer({}).transform(source, target)

    assert result.details["row_count"] == 3
    assert result.details["original_columns"] == ["id", "val"]
