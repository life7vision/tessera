"""
Tessera Archiver — archiver.db SQLite katalog katmanı.

Tablolar: repos, versions, scan_reports, findings
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

from .models import (
    ArchiverStats,
    FindingRecord,
    RepoRecord,
    ScanReportRecord,
    VersionRecord,
)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS repos (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    key               TEXT    UNIQUE NOT NULL,
    provider          TEXT    NOT NULL,
    namespace         TEXT    NOT NULL,
    repo              TEXT    NOT NULL,
    current_version   TEXT,
    total_versions    INTEGER DEFAULT 0,
    first_archived_at TEXT,
    last_archived_at  TEXT,
    stars             INTEGER,
    language          TEXT,
    purpose           TEXT,
    app_type          TEXT,
    domain            TEXT,
    description       TEXT,
    size_bytes        INTEGER DEFAULT 0,
    checksum          TEXT,
    created_at        TEXT    DEFAULT (datetime('now')),
    updated_at        TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS versions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_key         TEXT    NOT NULL,
    version          TEXT    NOT NULL,
    archive_id       TEXT    UNIQUE NOT NULL,
    archived_at      TEXT,
    pushed_at        TEXT,
    file             TEXT,
    bundle           TEXT,
    size_bytes       INTEGER DEFAULT 0,
    checksum_sha256  TEXT,
    stars_at_archive INTEGER,
    UNIQUE(repo_key, version),
    FOREIGN KEY (repo_key) REFERENCES repos(key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS scan_reports (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_key       TEXT    NOT NULL,
    version        TEXT    NOT NULL,
    archive_id     TEXT    NOT NULL,
    risk_level     TEXT,
    is_clean       INTEGER DEFAULT 0,
    files_scanned  INTEGER DEFAULT 0,
    high_count     INTEGER DEFAULT 0,
    medium_count   INTEGER DEFAULT 0,
    low_count      INTEGER DEFAULT 0,
    total_findings INTEGER DEFAULT 0,
    scanned_at     TEXT,
    error          TEXT    DEFAULT '',
    FOREIGN KEY (repo_key) REFERENCES repos(key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS findings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id     INTEGER NOT NULL,
    severity    TEXT,
    category    TEXT,
    file        TEXT,
    line        INTEGER DEFAULT 0,
    description TEXT,
    snippet     TEXT    DEFAULT '',
    FOREIGN KEY (scan_id) REFERENCES scan_reports(id) ON DELETE CASCADE
);

-- İndeksler
CREATE INDEX IF NOT EXISTS idx_repos_provider    ON repos(provider);
CREATE INDEX IF NOT EXISTS idx_repos_language    ON repos(language);
CREATE INDEX IF NOT EXISTS idx_repos_domain      ON repos(domain);
CREATE INDEX IF NOT EXISTS idx_repos_last_arch   ON repos(last_archived_at DESC);
CREATE INDEX IF NOT EXISTS idx_versions_repo     ON versions(repo_key);
CREATE INDEX IF NOT EXISTS idx_scans_repo        ON scan_reports(repo_key);
CREATE INDEX IF NOT EXISTS idx_scans_risk        ON scan_reports(risk_level);
CREATE INDEX IF NOT EXISTS idx_findings_scan     ON findings(scan_id);
CREATE INDEX IF NOT EXISTS idx_findings_severity ON findings(severity);
"""


# ---------------------------------------------------------------------------
# ArchiverCatalog
# ---------------------------------------------------------------------------

class ArchiverCatalog:
    """archiver.db CRUD arayüzü."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # Bağlantı
    # ------------------------------------------------------------------

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self) -> None:
        with self._conn() as con:
            con.executescript(_DDL)

    # ------------------------------------------------------------------
    # Repo CRUD
    # ------------------------------------------------------------------

    def upsert_repo(self, rec: RepoRecord) -> None:
        """Repo kaydını ekle ya da güncelle."""
        sql = """
        INSERT INTO repos (
            key, provider, namespace, repo,
            current_version, total_versions,
            first_archived_at, last_archived_at,
            stars, language, purpose, app_type, domain,
            description, size_bytes, checksum, updated_at
        ) VALUES (
            :key, :provider, :namespace, :repo,
            :current_version, :total_versions,
            :first_archived_at, :last_archived_at,
            :stars, :language, :purpose, :app_type, :domain,
            :description, :size_bytes, :checksum, datetime('now')
        )
        ON CONFLICT(key) DO UPDATE SET
            current_version   = excluded.current_version,
            total_versions    = excluded.total_versions,
            first_archived_at = COALESCE(first_archived_at, excluded.first_archived_at),
            last_archived_at  = excluded.last_archived_at,
            stars             = excluded.stars,
            language          = excluded.language,
            purpose           = excluded.purpose,
            app_type          = excluded.app_type,
            domain            = excluded.domain,
            description       = excluded.description,
            size_bytes        = excluded.size_bytes,
            checksum          = excluded.checksum,
            updated_at        = datetime('now')
        """
        data = rec.model_dump()
        for k in ("first_archived_at", "last_archived_at", "created_at", "updated_at"):
            if isinstance(data.get(k), datetime):
                data[k] = data[k].isoformat()
        with self._conn() as con:
            con.execute(sql, data)

    def get_repo(self, key: str) -> RepoRecord | None:
        with self._conn() as con:
            row = con.execute("SELECT * FROM repos WHERE key = ?", (key,)).fetchone()
        return RepoRecord(**dict(row)) if row else None

    def list_repos(
        self,
        provider: str | None = None,
        language: str | None = None,
        domain: str | None = None,
        query: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RepoRecord]:
        conditions = []
        params: list = []

        if provider:
            conditions.append("provider = ?")
            params.append(provider)
        if language:
            conditions.append("language = ?")
            params.append(language)
        if domain:
            conditions.append("domain = ?")
            params.append(domain)
        if query:
            conditions.append("(repo LIKE ? OR namespace LIKE ? OR description LIKE ?)")
            q = f"%{query}%"
            params.extend([q, q, q])

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
        SELECT r.*,
               (SELECT s.risk_level FROM scan_reports s
                WHERE s.repo_key = r.key
                ORDER BY s.scanned_at DESC LIMIT 1) AS risk_level
        FROM repos r {where}
        ORDER BY r.last_archived_at DESC LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()
        return [RepoRecord(**dict(r)) for r in rows]

    def count_repos(self) -> int:
        with self._conn() as con:
            return con.execute("SELECT COUNT(*) FROM repos").fetchone()[0]

    def delete_repo(self, key: str) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM repos WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    # Version CRUD
    # ------------------------------------------------------------------

    def upsert_version(self, repo_key: str, ver: VersionRecord) -> None:
        sql = """
        INSERT INTO versions (
            repo_key, version, archive_id,
            archived_at, pushed_at, file, bundle,
            size_bytes, checksum_sha256, stars_at_archive
        ) VALUES (
            :repo_key, :version, :archive_id,
            :archived_at, :pushed_at, :file, :bundle,
            :size_bytes, :checksum_sha256, :stars_at_archive
        )
        ON CONFLICT(repo_key, version) DO UPDATE SET
            archived_at      = excluded.archived_at,
            file             = excluded.file,
            bundle           = excluded.bundle,
            size_bytes       = excluded.size_bytes,
            checksum_sha256  = excluded.checksum_sha256,
            stars_at_archive = excluded.stars_at_archive
        """
        data = ver.model_dump()
        data["repo_key"] = repo_key
        for k in ("archived_at", "pushed_at"):
            if isinstance(data.get(k), datetime):
                data[k] = data[k].isoformat()
        with self._conn() as con:
            con.execute(sql, data)

    def list_versions(self, repo_key: str) -> list[VersionRecord]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM versions WHERE repo_key = ? ORDER BY version DESC",
                (repo_key,),
            ).fetchall()
        return [VersionRecord(**{k: row[k] for k in row.keys() if k != "repo_key" and k != "id"}) for row in rows]

    # ------------------------------------------------------------------
    # Scan CRUD
    # ------------------------------------------------------------------

    def save_scan(self, scan: ScanReportRecord) -> int:
        """Scan raporunu ve bulgularını kaydet. Eklenen scan_id döner."""
        sql_report = """
        INSERT INTO scan_reports (
            repo_key, version, archive_id,
            risk_level, is_clean, files_scanned,
            high_count, medium_count, low_count, total_findings,
            scanned_at, error
        ) VALUES (
            :repo_key, :version, :archive_id,
            :risk_level, :is_clean, :files_scanned,
            :high_count, :medium_count, :low_count, :total_findings,
            :scanned_at, :error
        )
        """
        data = scan.model_dump(exclude={"findings"})
        if isinstance(data.get("scanned_at"), datetime):
            data["scanned_at"] = data["scanned_at"].isoformat()
        data["is_clean"] = int(data["is_clean"])

        with self._conn() as con:
            cur = con.execute(sql_report, data)
            scan_id = cur.lastrowid

            sql_finding = """
            INSERT INTO findings (scan_id, severity, category, file, line, description, snippet)
            VALUES (:scan_id, :severity, :category, :file, :line, :description, :snippet)
            """
            for f in scan.findings:
                fd = f.model_dump()
                fd["scan_id"] = scan_id
                con.execute(sql_finding, fd)

        return scan_id

    def get_latest_scan(self, repo_key: str, version: str | None = None) -> ScanReportRecord | None:
        with self._conn() as con:
            if version:
                row = con.execute(
                    "SELECT * FROM scan_reports WHERE repo_key=? AND version=? ORDER BY scanned_at DESC LIMIT 1",
                    (repo_key, version),
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT * FROM scan_reports WHERE repo_key=? ORDER BY scanned_at DESC LIMIT 1",
                    (repo_key,),
                ).fetchone()

            if not row:
                return None

            scan_id = row["id"]
            finding_rows = con.execute(
                "SELECT * FROM findings WHERE scan_id=?", (scan_id,)
            ).fetchall()

        findings = [
            FindingRecord(**{k: fr[k] for k in fr.keys() if k not in ("id", "scan_id")})
            for fr in finding_rows
        ]
        data = dict(row)
        data.pop("id", None)
        data["is_clean"] = bool(data["is_clean"])
        data["findings"] = findings
        return ScanReportRecord(**data)

    def list_unscanned(self) -> list[tuple[str, str]]:
        """(repo_key, version) çiftleri — scan_report'u olmayan versiyonlar."""
        sql = """
        SELECT v.repo_key, v.version
        FROM versions v
        LEFT JOIN scan_reports s ON s.repo_key = v.repo_key AND s.version = v.version
        WHERE s.id IS NULL
        ORDER BY v.archived_at DESC
        """
        with self._conn() as con:
            rows = con.execute(sql).fetchall()
        return [(r["repo_key"], r["version"]) for r in rows]

    # ------------------------------------------------------------------
    # İstatistikler
    # ------------------------------------------------------------------

    def get_stats(self) -> ArchiverStats:
        with self._conn() as con:
            total_repos = con.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
            total_versions = con.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
            size_row = con.execute("SELECT COALESCE(SUM(size_bytes),0) FROM repos").fetchone()
            total_size = size_row[0]
            last_row = con.execute(
                "SELECT MAX(last_archived_at) FROM repos"
            ).fetchone()
            last_archived = last_row[0]

            by_provider = {
                r["provider"]: r["cnt"]
                for r in con.execute(
                    "SELECT provider, COUNT(*) cnt FROM repos GROUP BY provider"
                ).fetchall()
            }
            by_language = {
                r["language"]: r["cnt"]
                for r in con.execute(
                    "SELECT language, COUNT(*) cnt FROM repos WHERE language IS NOT NULL GROUP BY language ORDER BY cnt DESC LIMIT 20"
                ).fetchall()
            }
            by_domain = {
                r["domain"]: r["cnt"]
                for r in con.execute(
                    "SELECT domain, COUNT(*) cnt FROM repos WHERE domain IS NOT NULL GROUP BY domain ORDER BY cnt DESC"
                ).fetchall()
            }
            by_risk = {
                r["risk_level"]: r["cnt"]
                for r in con.execute(
                    "SELECT risk_level, COUNT(*) cnt FROM scan_reports GROUP BY risk_level"
                ).fetchall()
            }

        return ArchiverStats(
            total_repos=total_repos,
            total_versions=total_versions,
            total_size_bytes=total_size,
            repos_by_provider=by_provider,
            repos_by_language=by_language,
            repos_by_domain=by_domain,
            repos_by_risk=by_risk,
            last_archived_at=last_archived,
        )
