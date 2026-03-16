"""Profiling report exporter implementation."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from tessera.core.exceptions import TransformError
from tessera.exporters.base import BaseExporter, ExportResult


class ReportExporter(BaseExporter):
    """Write a lightweight dataset profiling report."""

    name = "report"
    version = "0.1.0"

    def export(self, version_id: str, target_path: Path, **kwargs) -> ExportResult:
        start = time.perf_counter()
        if "source_path" not in kwargs:
            raise TransformError("Rapor oluşturmak için 'source_path' parametresi gerekli.")
        source_path = Path(kwargs["source_path"])
        if not source_path.exists():
            raise TransformError(f"Kaynak dosya bulunamadı: {source_path}")
        try:
            dataframe = self._load_dataframe(source_path)
        except Exception as exc:
            raise TransformError(f"Rapor için dosya okunamadı: {source_path.name} — {exc}") from exc
        report = {
            "version_id": version_id,
            "row_count": len(dataframe.index),
            "column_count": len(dataframe.columns),
            "columns": [str(column) for column in dataframe.columns],
            "null_counts": dataframe.isna().sum().to_dict(),
        }
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return ExportResult(
            success=True,
            exporter_name=self.name,
            output_path=target_path,
            size_bytes=target_path.stat().st_size,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == ".csv":
            return pd.read_csv(file_path)
        if file_path.suffix == ".json":
            return pd.read_json(file_path)
        return pd.read_parquet(file_path)

