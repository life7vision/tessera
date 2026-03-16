"""Local exporter implementation."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from tessera.exporters.base import BaseExporter, ExportResult


class LocalExporter(BaseExporter):
    """Copy processed artifacts to a local target path."""

    name = "local"
    version = "0.1.0"

    def export(self, version_id: str, target_path: Path, **kwargs) -> ExportResult:
        start = time.perf_counter()
        source_path = Path(kwargs["source_path"])
        if source_path.is_dir():
            if target_path.exists():
                shutil.rmtree(target_path)
            shutil.copytree(source_path, target_path)
            size_bytes = sum(item.stat().st_size for item in target_path.rglob("*") if item.is_file())
        else:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
            size_bytes = target_path.stat().st_size
        return ExportResult(
            success=True,
            exporter_name=self.name,
            output_path=target_path,
            size_bytes=size_bytes,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )

