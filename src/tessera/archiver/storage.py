"""
Tessera Archiver — Depolama zone yönetimi.

archive/
├── raw/{provider}/{namespace}/{repo}/v{N}/
├── metadata/{provider}/{namespace}/{repo}/
├── snapshots/{YYYY}/{MM}/{DD}/
├── structured/by-language|by-topic|by-org/
├── reports/daily|monthly|anomalies|verification/
└── _meta/audit/
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from .models import RepoRef


class ArchiverStorage:
    """archive/ altındaki tüm zone ve yolları yöneten sınıf."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._ensure_zones()

    # ------------------------------------------------------------------
    # Zone başlatma
    # ------------------------------------------------------------------

    def _ensure_zones(self) -> None:
        zones = [
            self.root / "raw",
            self.root / "metadata",
            self.root / "snapshots",
            self.root / "structured" / "by-language",
            self.root / "structured" / "by-topic",
            self.root / "structured" / "by-org",
            self.root / "reports" / "daily",
            self.root / "reports" / "monthly",
            self.root / "reports" / "anomalies",
            self.root / "reports" / "verification",
            self.root / "_meta" / "audit",
        ]
        for zone in zones:
            zone.mkdir(parents=True, exist_ok=True)

        schema_file = self.root / "_meta" / "schema_version.txt"
        if not schema_file.exists():
            schema_file.write_text("1.0.0")

    # ------------------------------------------------------------------
    # RAW zone — arşiv dosyaları
    # ------------------------------------------------------------------

    def raw_repo_dir(self, ref: RepoRef) -> Path:
        """archive/raw/{provider}/{namespace}/{repo}/"""
        return self.root / "raw" / ref.provider / ref.namespace / ref.repo

    def raw_version_dir(self, ref: RepoRef, version: str) -> Path:
        """archive/raw/{provider}/{namespace}/{repo}/{version}/"""
        d = self.raw_repo_dir(ref) / version
        d.mkdir(parents=True, exist_ok=True)
        return d

    def versions_json_path(self, ref: RepoRef) -> Path:
        """archive/raw/{provider}/{namespace}/{repo}/versions.json"""
        return self.raw_repo_dir(ref) / "versions.json"

    def update_latest_symlink(self, ref: RepoRef, version: str) -> None:
        """latest/ → version/ sembolik linkini güncelle."""
        repo_dir = self.raw_repo_dir(ref)
        latest = repo_dir / "latest"
        if latest.is_symlink():
            latest.unlink()
        try:
            latest.symlink_to(version)
        except OSError:
            pass  # Sembolik link desteklenmeyen ortamlarda sessizce geç

    # ------------------------------------------------------------------
    # METADATA zone
    # ------------------------------------------------------------------

    def metadata_repo_dir(self, ref: RepoRef) -> Path:
        """archive/metadata/{provider}/{namespace}/{repo}/"""
        d = self.root / "metadata" / ref.provider / ref.namespace / ref.repo
        d.mkdir(parents=True, exist_ok=True)
        return d

    def repo_info_path(self, ref: RepoRef) -> Path:
        return self.metadata_repo_dir(ref) / "repo_info.json"

    def contributors_path(self, ref: RepoRef) -> Path:
        return self.metadata_repo_dir(ref) / "contributors.json"

    def releases_path(self, ref: RepoRef) -> Path:
        return self.metadata_repo_dir(ref) / "releases.json"

    def languages_path(self, ref: RepoRef) -> Path:
        return self.metadata_repo_dir(ref) / "languages.json"

    def archive_log_path(self, ref: RepoRef) -> Path:
        return self.metadata_repo_dir(ref) / "archive_log.jsonl"

    # ------------------------------------------------------------------
    # SNAPSHOTS zone
    # ------------------------------------------------------------------

    def snapshot_dir(self, dt: datetime | None = None) -> Path:
        """archive/snapshots/{YYYY}/{MM}/{DD}/"""
        dt = dt or datetime.now(timezone.utc)
        d = self.root / "snapshots" / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def snapshot_path(self, ref: RepoRef, version: str, dt: datetime | None = None) -> Path:
        """archive/snapshots/{YYYY}/{MM}/{DD}/{namespace}__{repo}__{version}.tar.gz"""
        name = f"{ref.namespace.replace('/', '__')}__{ref.repo}__{version}.tar.gz"
        return self.snapshot_dir(dt) / name

    # ------------------------------------------------------------------
    # STRUCTURED zone — sembolik link ağacı
    # ------------------------------------------------------------------

    def create_structured_links(
        self,
        ref: RepoRef,
        language_category: str | None = None,
        topic_category: str | None = None,
    ) -> None:
        """by-language, by-topic ve by-org sembolik linklerini oluştur."""
        target = self.raw_repo_dir(ref)
        link_name = f"{ref.provider}__{ref.namespace.replace('/', '__')}__{ref.repo}"

        def _make_link(link_dir: Path) -> None:
            link_dir.mkdir(parents=True, exist_ok=True)
            link = link_dir / link_name
            if link.is_symlink():
                link.unlink()
            try:
                link.symlink_to(target)
            except OSError:
                pass

        if language_category:
            _make_link(self.root / "structured" / "by-language" / language_category)

        if topic_category:
            _make_link(self.root / "structured" / "by-topic" / topic_category)

        # by-org her zaman
        _make_link(
            self.root / "structured" / "by-org" / ref.provider / ref.namespace / ref.repo
        )

    # ------------------------------------------------------------------
    # REPORTS zone
    # ------------------------------------------------------------------

    def daily_report_path(self, dt: datetime | None = None) -> Path:
        dt = dt or datetime.now(timezone.utc)
        return self.root / "reports" / "daily" / f"{dt.strftime('%Y%m%d')}_report.json"

    def monthly_report_path(self, dt: datetime | None = None) -> Path:
        dt = dt or datetime.now(timezone.utc)
        return self.root / "reports" / "monthly" / f"{dt.strftime('%Y_%m')}_summary.json"

    def anomalies_report_path(self, dt: datetime | None = None) -> Path:
        dt = dt or datetime.now(timezone.utc)
        return self.root / "reports" / "anomalies" / f"{dt.strftime('%Y%m%d')}_anomalies.json"

    def verification_report_path(self, dt: datetime | None = None) -> Path:
        dt = dt or datetime.now(timezone.utc)
        return (
            self.root
            / "reports"
            / "verification"
            / f"{dt.strftime('%Y%m%d_%H%M%S')}_verify.json"
        )

    # ------------------------------------------------------------------
    # _META zone
    # ------------------------------------------------------------------

    @property
    def index_path(self) -> Path:
        return self.root / "_meta" / "index.json"

    @property
    def checksums_path(self) -> Path:
        return self.root / "_meta" / "checksums.sha256"

    @property
    def audit_log_path(self) -> Path:
        return self.root / "_meta" / "audit" / "audit_log.jsonl"

    @property
    def chain_state_path(self) -> Path:
        return self.root / "_meta" / "audit" / "chain_state.json"

    # ------------------------------------------------------------------
    # Yardımcılar
    # ------------------------------------------------------------------

    def exists(self, ref: RepoRef) -> bool:
        """Bu repo için en az bir arşiv versiyonu var mı?"""
        return self.raw_repo_dir(ref).exists()

    def list_versions(self, ref: RepoRef) -> list[str]:
        """Mevcut versiyon dizinlerini sıralı döner: ['v1', 'v2', ...]"""
        repo_dir = self.raw_repo_dir(ref)
        if not repo_dir.exists():
            return []
        return sorted(
            [d.name for d in repo_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
            key=lambda v: int(v[1:]) if v[1:].isdigit() else 0,
        )

    def next_version(self, ref: RepoRef) -> str:
        """Sıradaki versiyon etiketini döner: 'v1', 'v2', ..."""
        versions = self.list_versions(ref)
        if not versions:
            return "v1"
        last_num = max(int(v[1:]) for v in versions if v[1:].isdigit())
        return f"v{last_num + 1}"

    def total_size(self) -> int:
        """archive/raw altındaki tüm dosyaların toplam boyutu (byte)."""
        total = 0
        raw_dir = self.root / "raw"
        if raw_dir.exists():
            for f in raw_dir.rglob("*"):
                if f.is_file():
                    total += f.stat().st_size
        return total

    def iter_all_repos(self) -> list[tuple[str, str, str]]:
        """
        archive/raw altındaki tüm (provider, namespace, repo) üçlülerini döner.
        Çok seviyeli namespace'ler (gitlab group/subgroup) desteklenir.
        """
        results = []
        raw = self.root / "raw"
        if not raw.exists():
            return results

        for provider_dir in raw.iterdir():
            if not provider_dir.is_dir():
                continue
            provider = provider_dir.name
            # namespace en az 1 seviye, repo son seviye
            # Versiyon dizinleri vX formatındadır — bunları hariç tut
            for path in provider_dir.rglob("versions.json"):
                repo_dir = path.parent
                # repo_dir = archive/raw/{provider}/{namespace...}/{repo}
                rel = repo_dir.relative_to(provider_dir)
                parts = rel.parts
                if len(parts) < 2:
                    continue
                repo = parts[-1]
                namespace = "/".join(parts[:-1])
                results.append((provider, namespace, repo))
        return results
