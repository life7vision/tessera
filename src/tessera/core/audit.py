"""Audit logging with SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class AuditLogger:
    """Persist audit events to SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        """Initialize audit database schema."""

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS audit_log (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT 'system',
                    action TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    resource_id TEXT,
                    details_json TEXT DEFAULT '{}',
                    status TEXT NOT NULL,
                    ip_address TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
                CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
                CREATE INDEX IF NOT EXISTS idx_audit_resource
                    ON audit_log(resource_type, resource_id);
                """
            )

    def log(
        self,
        action: str,
        resource_type: str,
        resource_id: str | None = None,
        actor: str = "system",
        details: dict[str, Any] | None = None,
        status: str = "success",
        ip_address: str | None = None,
    ) -> None:
        """Write an audit event."""

        timestamp = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (
                    id, timestamp, actor, action, resource_type,
                    resource_id, details_json, status, ip_address, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    timestamp,
                    actor,
                    action,
                    resource_type,
                    resource_id,
                    json.dumps(details or {}),
                    status,
                    ip_address,
                    timestamp,
                ),
            )

    def get_logs(
        self,
        action: str | None = None,
        resource_type: str | None = None,
        since: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return audit events filtered by basic criteria."""

        conditions: list[str] = []
        params: list[Any] = []
        if action:
            conditions.append("action = ?")
            params.append(action)
        if resource_type:
            conditions.append("resource_type = ?")
            params.append(resource_type)
        if since:
            conditions.append("timestamp >= ?")
            params.append(since)
        params.append(limit)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM audit_log
                {where_clause}
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["details_json"] = json.loads(data["details_json"])
        return data

