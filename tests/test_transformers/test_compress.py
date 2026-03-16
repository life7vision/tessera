"""Tests for the compression transformer."""

from __future__ import annotations

import gzip
from pathlib import Path

from tessera.transformers.compress import CompressTransformer


def test_compress_transformer_writes_gzip(sample_csv, tmp_path: Path):
    target = tmp_path / "output.csv.gz"
    result = CompressTransformer({"compression": "gzip"}).transform(sample_csv, target)

    with gzip.open(target, "rt", encoding="utf-8") as handle:
        content = handle.read()

    assert result.success is True
    assert "alpha" in content

