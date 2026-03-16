"""Base interfaces for connector plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatasetInfo:
    """Metadata about a dataset exposed by a connector."""

    source: str
    source_ref: str
    name: str
    description: str
    size_bytes: int | None
    file_count: int
    format_hint: str | None
    tags: list[str]
    license: str | None
    last_updated: str | None
    url: str | None
    extra_metadata: dict


@dataclass
class DownloadResult:
    """Download result returned by a connector."""

    success: bool
    local_path: Path
    checksum_sha256: str
    size_bytes: int
    file_count: int
    duration_seconds: float
    error_message: str | None = None


class BaseConnector(ABC):
    """Base class for all connectors."""

    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def validate_credentials(self) -> bool:
        """Validate connector credentials."""

    @abstractmethod
    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        """Search remote datasets."""

    @abstractmethod
    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        """Fetch dataset metadata."""

    @abstractmethod
    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        """Download a dataset into a target directory."""

