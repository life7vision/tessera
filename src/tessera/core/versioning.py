"""Semantic version utilities."""

from __future__ import annotations

from tessera.core.exceptions import VersionError


class VersionManager:
    """Manage semantic version strings."""

    def __init__(self, strategy: str = "semantic") -> None:
        if strategy != "semantic":
            raise VersionError(f"Desteklenmeyen versiyon stratejisi: {strategy}")
        self.strategy = strategy

    def parse(self, version: str) -> tuple[int, int, int]:
        """Parse a semantic version string."""

        try:
            major_str, minor_str, patch_str = version.split(".")
            return int(major_str), int(minor_str), int(patch_str)
        except (AttributeError, ValueError) as exc:
            raise VersionError(f"Geçersiz versiyon formatı: {version}") from exc

    def next_version(self, current: str | None, change_type: str = "minor") -> str:
        """Return the next semantic version for the requested change type."""

        if current is None:
            return "1.0.0"

        major, minor, patch = self.parse(current)
        if change_type == "major":
            return f"{major + 1}.0.0"
        if change_type == "minor":
            return f"{major}.{minor + 1}.0"
        if change_type == "patch":
            return f"{major}.{minor}.{patch + 1}"
        raise VersionError(f"Geçersiz değişiklik tipi: {change_type}")

    def compare(self, v1: str, v2: str) -> int:
        """Compare two semantic versions."""

        parsed_v1 = self.parse(v1)
        parsed_v2 = self.parse(v2)
        if parsed_v1 < parsed_v2:
            return -1
        if parsed_v1 > parsed_v2:
            return 1
        return 0

