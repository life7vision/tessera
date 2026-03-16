"""Format transformer implementation."""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from tessera.core.exceptions import TransformError
from tessera.core.hashing import compute_file_checksum
from tessera.transformers.base import BaseTransformer, TransformResult


class FormatTransformer(BaseTransformer):
    """Convert supported files into the target format."""

    name = "format"
    version = "0.1.0"
    input_formats = ["csv", "json", "parquet"]
    output_format = "parquet"

    _DATA_SUFFIXES = {".csv", ".json", ".parquet", ".tsv", ".jsonl"}

    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
        start = time.perf_counter()
        target_format = kwargs.get("target_format") or self.config.get("default_format", "parquet")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if input_path.suffix.lower() not in self._DATA_SUFFIXES or target_format == "original":
            try:
                output_path.write_bytes(input_path.read_bytes())
            except OSError as exc:
                raise TransformError(f"Dosya kopyalanamadı: {input_path.name} — {exc}") from exc
        else:
            try:
                dataframe = self._load_dataframe(input_path)
                dataframe.to_parquet(output_path, index=False)
            except Exception as exc:
                raise TransformError(f"Format dönüşümü başarısız: {input_path.name} — {exc}") from exc

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
            details={"target_format": target_format},
        )

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == ".csv":
            return pd.read_csv(file_path)
        if file_path.suffix == ".json":
            return pd.read_json(file_path)
        return pd.read_parquet(file_path)

