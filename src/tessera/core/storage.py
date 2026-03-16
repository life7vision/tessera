"""Storage management for Tessera data zones."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

from tessera.core.exceptions import QuarantineError, StorageError


class StorageManager:
    """Manage file placement across storage zones."""

    def __init__(self, config: dict):
        self.config = config
        self.base_path = Path(config["base_path"])
        self.zones = dict(config["zones"])

    def initialize(self) -> None:
        """Create base storage directories."""

        self.base_path.mkdir(parents=True, exist_ok=True)
        for zone_name in self.zones:
            self.get_zone_path(zone_name).mkdir(parents=True, exist_ok=True)

    def get_zone_path(self, zone: str) -> Path:
        """Return the path for a configured zone."""

        try:
            return self.base_path / self.zones[zone]
        except KeyError as exc:
            raise StorageError(f"Bilinmeyen depolama alanı: {zone}") from exc

    def store_raw(self, source_path: str | Path, dataset_name: str, version: str) -> Path:
        """Store content in the raw zone."""

        return self._store_in_zone("raw", source_path, dataset_name, version)

    def store_processed(
        self, source_path: str | Path, dataset_name: str, version: str
    ) -> Path:
        """Store content in the processed zone."""

        return self._store_in_zone("processed", source_path, dataset_name, version)

    def move_to_archive(
        self, processed_path: str | Path, dataset_name: str, version: str
    ) -> Path:
        """Move a processed file or directory to the archive zone."""

        source = Path(processed_path)
        if not source.exists():
            raise StorageError(f"Arşivlenecek yol bulunamadı: {source}")

        destination = self._version_target("archive", dataset_name, version, source.name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            self._remove_existing(destination)
        return source.replace(destination)

    def quarantine(self, file_path: str | Path, dataset_name: str, reason: str) -> Path:
        """Move a file or directory into quarantine with a timestamped name."""

        source = Path(file_path)
        if not source.exists():
            raise StorageError(f"Karantinaya alınacak yol bulunamadı: {source}")

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        safe_reason = reason.strip().replace(" ", "_") or "unknown"
        destination_name = f"{timestamp}_{safe_reason}_{source.name}"
        destination = self.get_zone_path("quarantine") / dataset_name / destination_name
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                self._remove_existing(destination)
            return source.replace(destination)
        except OSError as exc:
            raise QuarantineError(f"Karantina işlemi başarısız: {source} — {exc}") from exc

    def get_zone_size(self, zone: str) -> int:
        """Return the total size in bytes for a zone."""

        zone_path = self.get_zone_path(zone)
        if not zone_path.exists():
            return 0
        return sum(item.stat().st_size for item in zone_path.rglob("*") if item.is_file())

    def cleanup_old_versions(self, dataset_name: str, keep: int) -> list[Path]:
        """Remove old processed versions beyond the retention count."""

        processed_root = self.get_zone_path("processed") / dataset_name
        if not processed_root.exists():
            return []

        version_dirs = sorted(
            (path for path in processed_root.iterdir() if path.is_dir()),
            key=lambda path: path.name,
            reverse=True,
        )
        removed: list[Path] = []
        for stale in version_dirs[keep:]:
            shutil.rmtree(stale)
            removed.append(stale)
        return removed

    def _store_in_zone(
        self, zone: str, source_path: str | Path, dataset_name: str, version: str
    ) -> Path:
        source = Path(source_path)
        if not source.exists():
            raise StorageError(f"Taşınacak yol bulunamadı: {source}")

        destination = self._version_target(zone, dataset_name, version, source.name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._copy_path(source, destination)
        return destination

    def _version_target(
        self, zone: str, dataset_name: str, version: str, filename: str
    ) -> Path:
        return self.get_zone_path(zone) / dataset_name / f"v{version}" / filename

    def _copy_path(self, source: Path, destination: Path) -> None:
        if destination.exists():
            self._remove_existing(destination)

        try:
            if source.is_dir():
                shutil.copytree(source, destination)
                return

            temp_destination = destination.with_suffix(f"{destination.suffix}.tmp")
            shutil.copy2(source, temp_destination)
            temp_destination.replace(destination)
        except OSError as exc:
            # Covers disk full (ENOSPC), permission denied (EACCES), etc.
            raise StorageError(f"Dosya kopyalanamadı: {source} → {destination} — {exc}") from exc

    def _remove_existing(self, path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

