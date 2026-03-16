"""Cleaning transformer implementation."""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from tessera.core.hashing import compute_file_checksum
from tessera.transformers.base import BaseTransformer, TransformResult


class CleanTransformer(BaseTransformer):
    """Normalize text encodings and column names."""

    name = "clean"
    version = "0.1.0"
    input_formats = ["csv", "json", "parquet"]
    output_format = "cleaned"

    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
        start = time.perf_counter()
        dataframe = self._load_dataframe(input_path)
        dataframe.columns = [
            str(column).strip().lower().replace(" ", "_") for column in dataframe.columns
        ]
        if input_path.suffix == ".csv":
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(output_path, index=False, encoding="utf-8")
        elif input_path.suffix == ".json":
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dataframe.to_json(output_path, orient="records")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dataframe.to_parquet(output_path, index=False)

        return TransformResult(
            transformer_name=self.name,
            success=True,
            input_path=input_path,
            output_path=output_path,
            input_checksum=compute_file_checksum(input_path),
            output_checksum=compute_file_checksum(output_path),
            input_size=input_path.stat().st_size,
            output_size=output_path.stat().st_size,
            duration_ms=int((time.perf_counter() - start) * 1000),
            details={"columns": list(dataframe.columns)},
        )

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == ".csv":
            return pd.read_csv(file_path, encoding="utf-8-sig").dropna(how="all")
        if file_path.suffix == ".json":
            return pd.read_json(file_path).dropna(how="all")
        return pd.read_parquet(file_path).dropna(how="all")

