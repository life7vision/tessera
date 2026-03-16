"""SQLite-backed catalog management."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from tessera.core.exceptions import CatalogError, DuplicateDatasetError


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class CatalogManager:
    """Manage dataset metadata and lineage records."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        """Initialize catalog database schema."""

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    current_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    tags TEXT DEFAULT '[]',
                    description TEXT DEFAULT '',
                    is_archived INTEGER DEFAULT 0,
                    UNIQUE(source, source_ref)
                );

                CREATE TABLE IF NOT EXISTS dataset_versions (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL REFERENCES datasets(id),
                    version TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    file_size_bytes INTEGER NOT NULL,
                    file_count INTEGER NOT NULL DEFAULT 1,
                    raw_path TEXT NOT NULL,
                    processed_path TEXT,
                    archive_path TEXT,
                    zone TEXT NOT NULL DEFAULT 'raw',
                    format TEXT,
                    compression TEXT,
                    row_count INTEGER,
                    column_count INTEGER,
                    profile_path TEXT,
                    metadata_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    UNIQUE(dataset_id, version)
                );

                CREATE TABLE IF NOT EXISTS lineage (
                    id TEXT PRIMARY KEY,
                    version_id TEXT NOT NULL REFERENCES dataset_versions(id),
                    operation TEXT NOT NULL,
                    plugin_name TEXT NOT NULL,
                    input_checksum TEXT,
                    output_checksum TEXT,
                    parameters_json TEXT DEFAULT '{}',
                    status TEXT NOT NULL,
                    error_message TEXT,
                    duration_ms INTEGER,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_datasets_source ON datasets(source);
                CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets(name);
                CREATE INDEX IF NOT EXISTS idx_datasets_tags ON datasets(tags);
                CREATE INDEX IF NOT EXISTS idx_versions_dataset ON dataset_versions(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_versions_checksum ON dataset_versions(checksum_sha256);
                CREATE INDEX IF NOT EXISTS idx_versions_zone ON dataset_versions(zone);
                CREATE INDEX IF NOT EXISTS idx_lineage_version ON lineage(version_id);
                CREATE INDEX IF NOT EXISTS idx_lineage_operation ON lineage(operation);
                """
            )

    def register_dataset(self, info: Any) -> str:
        """Insert a dataset record and return its identifier."""

        now = _utc_now()
        dataset_id = str(uuid4())
        payload = (
            dataset_id,
            info.name,
            info.source,
            info.source_ref,
            getattr(info, "current_version", None) or "1.0.0",
            now,
            now,
            json.dumps(getattr(info, "tags", [])),
            getattr(info, "description", "") or "",
        )
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO datasets (
                        id, name, source, source_ref, current_version,
                        created_at, updated_at, tags, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateDatasetError("Dataset kaydı zaten mevcut.") from exc
        return dataset_id

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        """Return a dataset record by id."""

        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM datasets WHERE id = ?",
                (dataset_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def search_datasets(
        self, query: str | None = None, source: str | None = None, tags: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Search datasets by name, source, and tags."""

        conditions: list[str] = []
        params: list[Any] = []
        if query:
            conditions.append("(name LIKE ? OR description LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
        if source:
            conditions.append("source = ?")
            params.append(source)
        if tags:
            for tag in tags:
                conditions.append("tags LIKE ?")
                params.append(f"%{tag}%")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM datasets {where_clause} ORDER BY updated_at DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def update_dataset(self, dataset_id: str, **kwargs: Any) -> None:
        """Update dataset fields."""

        if not kwargs:
            return

        fields: list[str] = []
        params: list[Any] = []
        for key, value in kwargs.items():
            if key == "tags" and isinstance(value, list):
                value = json.dumps(value)
            fields.append(f"{key} = ?")
            params.append(value)
        fields.append("updated_at = ?")
        params.append(_utc_now())
        params.append(dataset_id)

        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE datasets SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        if cursor.rowcount == 0:
            raise CatalogError(f"Dataset bulunamadı: {dataset_id}")

    def archive_dataset(self, dataset_id: str) -> None:
        """Mark a dataset as archived."""

        self.update_dataset(dataset_id, is_archived=1)

    def register_version(self, dataset_id: str, version_data: dict[str, Any]) -> str:
        """Insert a dataset version record."""

        version_id = str(uuid4())
        payload = {
            "id": version_id,
            "dataset_id": dataset_id,
            "version": version_data["version"],
            "checksum_sha256": version_data["checksum_sha256"],
            "file_size_bytes": version_data["file_size_bytes"],
            "file_count": version_data.get("file_count", 1),
            "raw_path": version_data["raw_path"],
            "processed_path": version_data.get("processed_path"),
            "archive_path": version_data.get("archive_path"),
            "zone": version_data.get("zone", "raw"),
            "format": version_data.get("format"),
            "compression": version_data.get("compression"),
            "row_count": version_data.get("row_count"),
            "column_count": version_data.get("column_count"),
            "profile_path": version_data.get("profile_path"),
            "metadata_json": json.dumps(version_data.get("metadata_json", {})),
            "created_at": version_data.get("created_at", _utc_now()),
        }
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO dataset_versions (
                        id, dataset_id, version, checksum_sha256, file_size_bytes,
                        file_count, raw_path, processed_path, archive_path, zone,
                        format, compression, row_count, column_count, profile_path,
                        metadata_json, created_at
                    ) VALUES (
                        :id, :dataset_id, :version, :checksum_sha256, :file_size_bytes,
                        :file_count, :raw_path, :processed_path, :archive_path, :zone,
                        :format, :compression, :row_count, :column_count, :profile_path,
                        :metadata_json, :created_at
                    )
                    """,
                    payload,
                )
                conn.execute(
                    "UPDATE datasets SET current_version = ?, updated_at = ? WHERE id = ?",
                    (payload["version"], _utc_now(), dataset_id),
                )
        except sqlite3.IntegrityError as exc:
            raise CatalogError("Versiyon kaydı eklenemedi.") from exc
        return version_id

    def get_version(self, version_id: str) -> dict[str, Any] | None:
        """Return a version record by id."""

        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dataset_versions WHERE id = ?",
                (version_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_versions(self, dataset_id: str) -> list[dict[str, Any]]:
        """Return all versions for a dataset."""

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM dataset_versions
                WHERE dataset_id = ?
                ORDER BY created_at DESC, version DESC
                """,
                (dataset_id,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_latest_version(self, dataset_id: str) -> dict[str, Any] | None:
        """Return the current version for a dataset."""

        dataset = self.get_dataset(dataset_id)
        if not dataset:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM dataset_versions
                WHERE dataset_id = ? AND version = ?
                """,
                (dataset_id, dataset["current_version"]),
            ).fetchone()
        return self._row_to_dict(row)

    def update_version_zone(self, version_id: str, zone: str, path: str | Path) -> None:
        """Update zone-related path fields for a version."""

        path_str = str(path)
        updates = {"zone": zone}
        if zone == "processed":
            updates["processed_path"] = path_str
        elif zone == "archive":
            updates["archive_path"] = path_str
        else:
            updates["raw_path"] = path_str

        fields = ", ".join(f"{key} = ?" for key in updates)
        params = list(updates.values()) + [version_id]
        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE dataset_versions SET {fields} WHERE id = ?",
                params,
            )
        if cursor.rowcount == 0:
            raise CatalogError(f"Versiyon bulunamadı: {version_id}")

    def record_lineage(
        self, version_id: str, operation: str, plugin_name: str, **kwargs: Any
    ) -> str:
        """Store a lineage event."""

        lineage_id = str(uuid4())
        payload = (
            lineage_id,
            version_id,
            operation,
            plugin_name,
            kwargs.get("input_checksum"),
            kwargs.get("output_checksum"),
            json.dumps(kwargs.get("parameters", {})),
            kwargs.get("status", "success"),
            kwargs.get("error_message"),
            kwargs.get("duration_ms"),
            _utc_now(),
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO lineage (
                    id, version_id, operation, plugin_name, input_checksum,
                    output_checksum, parameters_json, status, error_message,
                    duration_ms, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        return lineage_id

    def get_lineage(self, version_id: str) -> list[dict[str, Any]]:
        """Return lineage events for a version."""

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM lineage
                WHERE version_id = ?
                ORDER BY created_at ASC
                """,
                (version_id,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def check_duplicate(self, checksum: str) -> dict[str, Any] | None:
        """Return the latest version row matching a checksum."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM dataset_versions
                WHERE checksum_sha256 = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (checksum,),
            ).fetchone()
        return self._row_to_dict(row)

    def update_version_profile(self, version_id: str, profile_path: str, metadata_json: dict) -> None:
        """Update profile path and metadata for a version."""

        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE dataset_versions SET profile_path = ?, metadata_json = ? WHERE id = ?",
                (profile_path, json.dumps(metadata_json), version_id),
            )
        if cursor.rowcount == 0:
            raise CatalogError(f"Versiyon bulunamadı: {version_id}")

    def get_stats(self) -> dict[str, Any]:
        """Return top-level catalog statistics."""

        with self._connect() as conn:
            dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
            version_count = conn.execute("SELECT COUNT(*) FROM dataset_versions").fetchone()[0]
            archived_count = conn.execute(
                "SELECT COUNT(*) FROM datasets WHERE is_archived = 1"
            ).fetchone()[0]
            raw_bytes = conn.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) FROM dataset_versions"
            ).fetchone()[0]
        return {
            "dataset_count": dataset_count,
            "version_count": version_count,
            "archived_dataset_count": archived_count,
            "total_file_size_bytes": raw_bytes,
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None

        data = dict(row)
        for key in ("tags", "metadata_json", "parameters_json"):
            if key in data and isinstance(data[key], str):
                data[key] = json.loads(data[key])
        return data

