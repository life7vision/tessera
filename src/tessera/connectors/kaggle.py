"""Kaggle connector implementation (kaggle >= 2.0 + kagglehub >= 0.4)."""

from __future__ import annotations

import json
import shutil
import tempfile
import time
from importlib import import_module
from pathlib import Path

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.hashing import compute_directory_checksum


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
        items = list(results)[:max_results]
        return [self._list_item_to_info(item) for item in items]

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        """Fetch dataset metadata using kaggle 2.0 dataset_metadata API."""
        with tempfile.TemporaryDirectory() as tmp:
            self._api().dataset_metadata(source_ref, path=tmp)
            meta_path = Path(tmp) / "dataset-metadata.json"
            raw = json.loads(meta_path.read_text(encoding="utf-8"))
        info = raw.get("info", raw)
        return DatasetInfo(
            source="kaggle",
            source_ref=source_ref,
            name=info.get("title") or info.get("datasetSlug", source_ref.split("/")[-1]),
            description=info.get("subtitle") or info.get("description", ""),
            size_bytes=info.get("totalBytes"),
            file_count=1,
            format_hint=None,
            tags=info.get("keywords", []),
            license=next(
                (lic.get("name") for lic in info.get("licenses", []) if isinstance(lic, dict)),
                None,
            ),
            last_updated=None,
            url=f"https://www.kaggle.com/datasets/{source_ref}",
            extra_metadata={
                "datasetId": info.get("datasetId"),
                "totalVotes": info.get("totalVotes"),
                "totalDownloads": info.get("totalDownloads"),
                "usabilityRating": info.get("usabilityRating"),
            },
        )

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        """Download using kagglehub (caching + automatic unzip)."""
        start = time.perf_counter()
        target_dir.mkdir(parents=True, exist_ok=True)
        kagglehub = import_module("kagglehub")
        cached_path = Path(kagglehub.dataset_download(source_ref))
        # Copy from kagglehub cache to our target directory
        for item in cached_path.iterdir():
            dest = target_dir / item.name
            if item.is_dir():
                shutil.copytree(item, dest, dirs_exist_ok=True)
            else:
                shutil.copy2(item, dest)
        checksum = compute_directory_checksum(target_dir)
        size_bytes, file_count = self._scan_path(target_dir)
        return DownloadResult(
            success=True,
            local_path=target_dir,
            checksum_sha256=checksum,
            size_bytes=size_bytes,
            file_count=file_count,
            duration_seconds=time.perf_counter() - start,
        )

    def _list_item_to_info(self, item) -> DatasetInfo:
        """Convert a dataset_list result item to DatasetInfo."""
        ref = getattr(item, "ref", "") or ""
        return DatasetInfo(
            source="kaggle",
            source_ref=ref,
            name=getattr(item, "title", None) or getattr(item, "slug", "unknown"),
            description=getattr(item, "subtitle", "") or "",
            size_bytes=getattr(item, "totalBytes", None),
            file_count=getattr(item, "fileCount", 1) or 1,
            format_hint=None,
            tags=[
                tag.name if hasattr(tag, "name") else str(tag)
                for tag in getattr(item, "tags", [])
            ],
            license=getattr(getattr(item, "licenseName", None), "name", None)
            or getattr(item, "licenseName", None),
            last_updated=getattr(item, "lastUpdated", None),
            url=f"https://www.kaggle.com/datasets/{ref}",
            extra_metadata={},
        )

    def _api(self):
        return import_module("kaggle").api

    def _scan_path(self, path: Path) -> tuple[int, int]:
        files = [f for f in path.rglob("*") if f.is_file()]
        return sum(f.stat().st_size for f in files), len(files)
