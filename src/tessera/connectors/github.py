"""GitHub connector implementation."""

from __future__ import annotations

import io
import os
import time
import zipfile
from pathlib import Path

import requests

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.hashing import compute_directory_checksum


class GitHubConnector(BaseConnector):
    """Connector for GitHub repositories containing datasets."""

    name = "github"
    version = "0.1.0"
    base_url = "https://api.github.com"

    def validate_credentials(self) -> bool:
        response = requests.get(
            f"{self.base_url}/user",
            headers=self._headers(),
            timeout=self.config.get("download_timeout", 300),
        )
        return response.status_code == 200

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        response = requests.get(
            f"{self.base_url}/search/repositories",
            params={"q": f"{query} topic:dataset", "per_page": max_results},
            headers=self._headers(),
            timeout=self.config.get("download_timeout", 300),
        )
        response.raise_for_status()
        items = response.json().get("items", [])
        return [self._to_dataset_info(item) for item in items[:max_results]]

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        response = requests.get(
            f"{self.base_url}/repos/{source_ref}",
            headers=self._headers(),
            timeout=self.config.get("download_timeout", 300),
        )
        response.raise_for_status()
        return self._to_dataset_info(response.json(), source_ref=source_ref)

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        start = time.perf_counter()
        target_dir.mkdir(parents=True, exist_ok=True)

        release_response = requests.get(
            f"{self.base_url}/repos/{source_ref}/releases/latest",
            headers=self._headers(),
            timeout=self.config.get("download_timeout", 300),
        )
        if release_response.status_code == 200:
            release_data = release_response.json()
            download_url = release_data.get("zipball_url")
        else:
            download_url = f"https://github.com/{source_ref}/archive/refs/heads/main.zip"

        archive_response = requests.get(
            download_url,
            headers=self._headers(),
            timeout=self.config.get("download_timeout", 300),
        )
        archive_response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(archive_response.content)) as archive:
            archive.extractall(target_dir)

        checksum = compute_directory_checksum(target_dir)
        files = [item for item in target_dir.rglob("*") if item.is_file()]
        return DownloadResult(
            success=True,
            local_path=target_dir,
            checksum_sha256=checksum,
            size_bytes=sum(item.stat().st_size for item in files),
            file_count=len(files),
            duration_seconds=time.perf_counter() - start,
        )

    def _headers(self) -> dict[str, str]:
        token_env = self.config.get("token_env", "GITHUB_TOKEN")
        token = os.getenv(token_env)
        headers = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _to_dataset_info(self, item: dict, source_ref: str | None = None) -> DatasetInfo:
        repo_ref = source_ref or item.get("full_name", "")
        return DatasetInfo(
            source="github",
            source_ref=repo_ref,
            name=item.get("name", "unknown"),
            description=item.get("description") or "",
            size_bytes=None,
            file_count=1,
            format_hint=None,
            tags=[topic for topic in item.get("topics", [])],
            license=(item.get("license") or {}).get("spdx_id"),
            last_updated=item.get("updated_at"),
            url=item.get("html_url") or f"https://github.com/{repo_ref}",
            extra_metadata={"stars": item.get("stargazers_count")},
        )

