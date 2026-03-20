"""Upload connector — manually uploaded or locally registered datasets.

source_ref format: any user-provided slug, e.g. "my-football-analysis-2024"

This connector is intentionally minimal: files are provided by the user
(browser upload or server path), so there is nothing to fetch remotely.
"""

from __future__ import annotations

from pathlib import Path

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult


class UploadConnector(BaseConnector):
    """Connector for manually uploaded or server-side registered datasets.

    Handles two workflows:
      1. Browser upload — file arrives via multipart POST, already written to storage.
      2. Server registration — file already exists on disk, just needs catalog entry.

    In both cases download() is a no-op since the file is already in place.
    """

    name = "upload"
    version = "0.1.0"

    def validate_credentials(self) -> bool:
        return True

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        return []

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        return DatasetInfo(
            source="upload",
            source_ref=source_ref,
            name=source_ref,
            description="",
            size_bytes=None,
            file_count=1,
            format_hint=None,
            tags=[],
            license=None,
            last_updated=None,
            url=None,
            extra_metadata={},
        )

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        """No-op — file is already present at the registered path."""
        target_dir.mkdir(parents=True, exist_ok=True)
        return DownloadResult(
            success=True,
            local_path=target_dir,
            checksum_sha256="",
            size_bytes=0,
            file_count=0,
            duration_seconds=0.0,
        )
