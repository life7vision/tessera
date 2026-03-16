"""Hugging Face connector implementation."""

from __future__ import annotations

import os
import time
from pathlib import Path

from huggingface_hub import dataset_info, list_datasets, snapshot_download, whoami

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.hashing import compute_directory_checksum


class HuggingFaceConnector(BaseConnector):
    """Connector for Hugging Face datasets."""

    name = "huggingface"
    version = "0.1.0"

    def validate_credentials(self) -> bool:
        token_env = self.config.get("token_env", "HF_TOKEN")
        token = os.getenv(token_env)
        if not token:
            return False
        try:
            whoami(token)
            return True
        except Exception:
            return False

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        results = list_datasets(search=query)
        return [self._to_dataset_info(item) for item in list(results)[:max_results]]

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        item = dataset_info(source_ref)
        return self._to_dataset_info(item, source_ref=source_ref)

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        start = time.perf_counter()
        target_dir.mkdir(parents=True, exist_ok=True)
        local_dir = Path(
            snapshot_download(repo_id=source_ref, repo_type="dataset", local_dir=target_dir)
        )
        checksum = compute_directory_checksum(local_dir)
        files = [item for item in local_dir.rglob("*") if item.is_file()]
        return DownloadResult(
            success=True,
            local_path=local_dir,
            checksum_sha256=checksum,
            size_bytes=sum(item.stat().st_size for item in files),
            file_count=len(files),
            duration_seconds=time.perf_counter() - start,
        )

    def _to_dataset_info(self, item, source_ref: str | None = None) -> DatasetInfo:
        dataset_id = source_ref or getattr(item, "id", "")
        tags = list(getattr(item, "tags", []) or [])
        return DatasetInfo(
            source="huggingface",
            source_ref=dataset_id,
            name=dataset_id.split("/")[-1] if dataset_id else "unknown",
            description=getattr(item, "description", "") or "",
            size_bytes=None,
            file_count=1,
            format_hint=None,
            tags=tags,
            license=getattr(item, "license", None),
            last_updated=str(getattr(item, "lastModified", None) or ""),
            url=f"https://huggingface.co/datasets/{dataset_id}" if dataset_id else None,
            extra_metadata={"downloads": getattr(item, "downloads", None)},
        )

