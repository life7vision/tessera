"""Kaggle connector implementation."""

from __future__ import annotations

import os
import time
from importlib import import_module
from pathlib import Path

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.hashing import compute_directory_checksum, compute_file_checksum


class KaggleConnector(BaseConnector):
    """Connector for Kaggle datasets."""

    name = "kaggle"
    version = "0.1.0"

    def validate_credentials(self) -> bool:
        try:
            self._api().authenticate()
            return True
        except (Exception, SystemExit):
            return False

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        results = self._api().dataset_list(search=query)
        return [self._to_dataset_info(item) for item in list(results)[:max_results]]

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        item = self._api().dataset_view(source_ref)
        return self._to_dataset_info(item, source_ref=source_ref)

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        start = time.perf_counter()
        target_dir.mkdir(parents=True, exist_ok=True)
        self._api().dataset_download_files(source_ref, path=target_dir, unzip=True)
        local_path = target_dir
        checksum = compute_directory_checksum(local_path)
        size_bytes, file_count = self._scan_path(local_path)
        return DownloadResult(
            success=True,
            local_path=local_path,
            checksum_sha256=checksum,
            size_bytes=size_bytes,
            file_count=file_count,
            duration_seconds=time.perf_counter() - start,
        )

    def _to_dataset_info(self, item, source_ref: str | None = None) -> DatasetInfo:
        return DatasetInfo(
            source="kaggle",
            source_ref=source_ref or getattr(item, "ref", ""),
            name=getattr(item, "title", None) or getattr(item, "slug", "unknown"),
            description=getattr(item, "subtitle", "") or "",
            size_bytes=getattr(item, "totalBytes", None),
            file_count=getattr(item, "fileCount", 1) or 1,
            format_hint=None,
            tags=[tag.name if hasattr(tag, "name") else str(tag) for tag in getattr(item, "tags", [])],
            license=getattr(getattr(item, "licenseName", None), "name", None)
            or getattr(item, "licenseName", None),
            last_updated=getattr(item, "lastUpdated", None),
            url=f"https://www.kaggle.com/datasets/{source_ref or getattr(item, 'ref', '')}",
            extra_metadata={},
        )

    def _api(self):
        return import_module("kaggle").api

    def _scan_path(self, path: Path) -> tuple[int, int]:
        files = [item for item in path.rglob("*") if item.is_file()]
        return sum(item.stat().st_size for item in files), len(files)
