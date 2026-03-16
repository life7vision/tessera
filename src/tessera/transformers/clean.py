"""Cleaning transformer implementation."""

from __future__ import annotations

import re
import time
from pathlib import Path

import pandas as pd

from tessera.core.exceptions import TransformError
from tessera.core.hashing import compute_file_checksum
from tessera.transformers.base import BaseTransformer, TransformResult


class CleanTransformer(BaseTransformer):
    """Normalize text encodings, trim whitespace and standardize column names."""

    name = "clean"
    version = "0.1.0"
    input_formats = ["csv", "json", "parquet"]
    output_format = "cleaned"

    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
        start = time.perf_counter()

        try:
            dataframe = self._load_dataframe(input_path)
        except Exception as exc:
            raise TransformError(f"Dosya okunamadı: {input_path.name} — {exc}") from exc

        original_columns = list(dataframe.columns)
        dataframe = self._normalize_columns(dataframe)
        dataframe = self._trim_strings(dataframe)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if input_path.suffix == ".csv":
                dataframe.to_csv(output_path, index=False, encoding="utf-8")
            elif input_path.suffix == ".json":
                dataframe.to_json(output_path, orient="records")
            else:
                dataframe.to_parquet(output_path, index=False)
        except (OSError, ValueError) as exc:
            raise TransformError(f"Çıktı yazılamadı: {output_path.name} — {exc}") from exc

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
            details={
                "original_columns": original_columns,
                "cleaned_columns": list(dataframe.columns),
                "row_count": len(dataframe),
            },
        )

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == ".csv":
            return pd.read_csv(file_path, encoding="utf-8-sig").dropna(how="all")
        if file_path.suffix == ".json":
            return pd.read_json(file_path).dropna(how="all")
        return pd.read_parquet(file_path).dropna(how="all")

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase and replace non-alphanumeric characters with underscores."""
        def _clean(col: str) -> str:
            col = str(col).strip().lower()
            col = re.sub(r"[^\w]+", "_", col)
            col = col.strip("_")
            return col or "column"

        df = df.copy()
        df.columns = [_clean(c) for c in df.columns]
        return df

    def _trim_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip leading/trailing whitespace from all string columns."""
        df = df.copy()
        for col in df.select_dtypes(include=["object", "string"]).columns:
            df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
        return df

