"""
Tessera Archiver — Pydantic modelleri ve veri yapıları.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


def _human_bytes(n: int | None) -> str:
    if not n:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


# ---------------------------------------------------------------------------
# Kimlik / Yol yardımcısı
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RepoRef:
    """Bir repoyu benzersiz şekilde tanımlayan immutable değer nesnesi."""

    provider: str   # "github" | "gitlab"
    namespace: str  # "torvalds" | "group/subgroup"
    repo: str       # "linux"

    @property
    def key(self) -> str:
        """Birleşik anahtar: 'github:torvalds/linux'"""
        return f"{self.provider}:{self.namespace}/{self.repo}"

    @property
    def full_name(self) -> str:
        """'torvalds/linux'"""
        return f"{self.namespace}/{self.repo}"

    @classmethod
    def parse(cls, raw: str, default_provider: str = "github") -> "RepoRef":
        """
        Esnek girdi ayrıştırma:
          - "github:torvalds/linux"
          - "torvalds/linux"            → default_provider kullanılır
          - "https://github.com/a/b"   → github:a/b
        """
        raw = raw.strip()

        # URL formatı
        for prefix, prov in (
            ("https://github.com/", "github"),
            ("https://gitlab.com/", "gitlab"),
        ):
            if raw.startswith(prefix):
                rest = raw[len(prefix):].rstrip("/")
                parts = rest.split("/")
                # group/subgroup/repo → namespace = group/subgroup
                repo = parts[-1]
                namespace = "/".join(parts[:-1])
                return cls(provider=prov, namespace=namespace, repo=repo)

        # provider:namespace/repo
        if ":" in raw:
            provider, rest = raw.split(":", 1)
        else:
            provider = default_provider
            rest = raw

        parts = rest.split("/")
        if len(parts) < 2:
            raise ValueError(f"Geçersiz repo referansı: {raw!r}")

        repo = parts[-1]
        namespace = "/".join(parts[:-1])
        return cls(provider=provider, namespace=namespace, repo=repo)

    def __str__(self) -> str:
        return self.key


# ---------------------------------------------------------------------------
# DB / API response modelleri
# ---------------------------------------------------------------------------

class RepoRecord(BaseModel):
    """Bir reponun katalog kaydı (archiver.db → repos tablosu)."""

    key: str
    provider: str
    namespace: str
    repo: str
    current_version: str | None = None
    total_versions: int = 0
    first_archived_at: datetime | None = None
    last_archived_at: datetime | None = None
    stars: int | None = None
    language: str | None = None
    purpose: str | None = None
    app_type: str | None = None
    domain: str | None = None
    description: str | None = None
    size_bytes: int = 0
    checksum: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # Computed by catalog join (not stored in repos table)
    risk_level: str | None = None

    @property
    def size_human(self) -> str:
        return _human_bytes(self.size_bytes)

    @property
    def last_archived_str(self) -> str:
        if self.last_archived_at is None:
            return "—"
        return self.last_archived_at.strftime("%Y-%m-%d")


class VersionRecord(BaseModel):
    """Bir reponun tek bir arşiv versiyonu."""

    version: str
    archive_id: str
    archived_at: datetime
    pushed_at: datetime | None = None
    file: str
    bundle: str | None = None
    size_bytes: int = 0
    checksum_sha256: str
    stars_at_archive: int | None = None

    @property
    def size_human(self) -> str:
        return _human_bytes(self.size_bytes)


class FindingRecord(BaseModel):
    """Güvenlik taramasından tek bir bulgu."""

    severity: Literal["HIGH", "MEDIUM", "LOW"]
    category: str   # "NET001", "SEC002", ...
    file: str
    line: int = 0
    description: str
    snippet: str = ""


class ScanReportRecord(BaseModel):
    """Bir arşivin güvenlik tarama raporu."""

    repo_key: str
    version: str
    archive_id: str
    risk_level: Literal["HIGH", "MEDIUM", "LOW", "CLEAN"]
    is_clean: bool = False
    files_scanned: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    total_findings: int = 0
    scanned_at: datetime
    error: str = ""
    findings: list[FindingRecord] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# İş isteği modelleri (API request body)
# ---------------------------------------------------------------------------

class ArchiveJobRequest(BaseModel):
    """POST /api/archiver/jobs/archive için istek gövdesi."""

    repo: str = Field(
        ...,
        description="Repo referansı. Örn: 'github:torvalds/linux' veya 'torvalds/linux'",
    )
    force: bool = Field(False, description="Güncel olsa bile yeniden arşivle")
    include_heavy: bool = Field(
        False, description="node_modules, .venv gibi ağır dizinleri dahil et"
    )


class ScanJobRequest(BaseModel):
    """POST /api/archiver/jobs/scan için istek gövdesi."""

    repo: str = Field(
        "all",
        description="Repo referansı veya 'all' (taranmamış tümü)",
    )
    force: bool = Field(False, description="Zaten taranmışsa bile yeniden tara")


class PipelineJobRequest(BaseModel):
    """POST /api/archiver/jobs/pipeline için istek gövdesi."""

    repos: list[str] = Field(default_factory=list, description="Repo referansları listesi")
    force: bool = False
    include_heavy: bool = False


# ---------------------------------------------------------------------------
# İstatistik / özet modelleri
# ---------------------------------------------------------------------------

class ArchiverStats(BaseModel):
    """Dashboard için genel istatistikler."""

    total_repos: int = 0
    total_versions: int = 0
    total_size_bytes: int = 0
    repos_by_provider: dict[str, int] = Field(default_factory=dict)
    repos_by_language: dict[str, int] = Field(default_factory=dict)
    repos_by_domain: dict[str, int] = Field(default_factory=dict)
    repos_by_risk: dict[str, int] = Field(default_factory=dict)
    last_archived_at: datetime | None = None

    @property
    def total_size_human(self) -> str:
        return _human_bytes(self.total_size_bytes)


class JobStatus(BaseModel):
    """Async iş durumu."""

    job_id: str
    job_type: Literal["archive", "scan", "pipeline", "verify", "report"]
    status: Literal["pending", "running", "completed", "failed"]
    repo_key: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    message: str = ""
    error: str = ""
    result: dict | None = None
