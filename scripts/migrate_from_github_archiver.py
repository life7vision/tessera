#!/usr/bin/env python3
"""
Migration: github-archiver → Tessera Archiver

Kaynak: /run/media/life7vision/DataSSD/projects/github-archiver/archive/raw/
Hedef : /run/media/life7vision/DataSSD/projects/Tessera/archive/

Yapılanlar:
1. Tüm versions.json dosyalarını okur → archiver.db'ye RepoRecord + VersionRecord yazar
2. Tüm scan_report.json dosyalarını okur → archiver.db'ye ScanReportRecord + FindingRecord yazar
3. .tar.gz ve .git bundle dosyaları için Tessera'nın raw/ klasörüne symlink oluşturur

Veri KOPYALANMAz — 85 GB'ı tekrar kopyalamak yerine symlink kullanılır.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Tessera paketini bul
TESSERA_SRC = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(TESSERA_SRC))

from tessera.archiver.catalog import ArchiverCatalog
from tessera.archiver.models import (
    FindingRecord,
    RepoRecord,
    ScanReportRecord,
    VersionRecord,
)

# ---------------------------------------------------------------------------
# Yollar
# ---------------------------------------------------------------------------

SOURCE_ROOT = Path("/run/media/life7vision/DataSSD/projects/github-archiver/archive/raw")
TESSERA_ROOT = Path(__file__).parent.parent
TESSERA_ARCHIVE = TESSERA_ROOT / "archive"
TESSERA_RAW = TESSERA_ARCHIVE / "raw"
TESSERA_DB = TESSERA_ARCHIVE / "archiver.db"

PROVIDER = "github"


# ---------------------------------------------------------------------------
# Yardımcılar
# ---------------------------------------------------------------------------

def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def symlink_file(src: Path, dst: Path) -> bool:
    """src → dst symlink oluştur. Zaten varsa atla."""
    if dst.exists() or dst.is_symlink():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.symlink_to(src)
    return True


# ---------------------------------------------------------------------------
# Ana migration
# ---------------------------------------------------------------------------

def migrate() -> None:
    print(f"Kaynak : {SOURCE_ROOT}")
    print(f"Hedef  : {TESSERA_ARCHIVE}")
    print(f"Veritabanı: {TESSERA_DB}")
    print()

    catalog = ArchiverCatalog(TESSERA_DB)

    versions_files = sorted(SOURCE_ROOT.rglob("versions.json"))
    print(f"Bulunan versions.json sayısı: {len(versions_files)}")

    repo_count = 0
    version_count = 0
    scan_count = 0
    symlink_count = 0
    skip_count = 0

    for vf in versions_files:
        try:
            data = json.loads(vf.read_text())
        except Exception as e:
            print(f"  [HATA] {vf}: {e}")
            continue

        owner = data.get("owner", "")
        repo_name = data.get("repo", "")
        if not owner or not repo_name:
            # Yoldan çıkar
            parts = vf.parent.relative_to(SOURCE_ROOT).parts
            if len(parts) >= 2:
                owner = parts[0]
                repo_name = parts[1]
            else:
                print(f"  [ATLA] Eksik owner/repo: {vf}")
                skip_count += 1
                continue

        repo_key = f"{PROVIDER}:{owner}/{repo_name}"
        versions_data = data.get("versions", [])

        # Toplam boyut hesapla
        total_size = sum(v.get("size_bytes", 0) for v in versions_data)

        # RepoRecord oluştur
        rec = RepoRecord(
            key=repo_key,
            provider=PROVIDER,
            namespace=owner,
            repo=repo_name,
            current_version=data.get("current_version"),
            total_versions=data.get("total_versions", len(versions_data)),
            first_archived_at=parse_dt(data.get("first_archived_at")),
            last_archived_at=parse_dt(data.get("last_archived_at")),
            stars=versions_data[-1].get("stars_at_archive") if versions_data else None,
            language=versions_data[-1].get("language") if versions_data else None,
            size_bytes=total_size,
        )

        catalog.upsert_repo(rec)
        repo_count += 1

        # VersionRecord'ları işle
        repo_src_dir = vf.parent  # .../raw/{owner}/{repo}/
        repo_dst_dir = TESSERA_RAW / owner / repo_name

        for v in versions_data:
            ver_str = v.get("version", "v1")
            src_ver_dir = repo_src_dir / ver_str
            dst_ver_dir = repo_dst_dir / ver_str

            ver_rec = VersionRecord(
                version=ver_str,
                archive_id=v.get("archive_id", f"{owner}-{repo_name}-{ver_str}"),
                archived_at=parse_dt(v.get("archived_at")) or datetime.now(tz=timezone.utc),
                pushed_at=parse_dt(v.get("pushed_at")),
                file=v.get("file", ""),
                bundle=v.get("bundle"),
                size_bytes=v.get("size_bytes", 0),
                checksum_sha256=v.get("checksum_sha256", ""),
                stars_at_archive=v.get("stars_at_archive"),
            )

            catalog.upsert_version(repo_key, ver_rec)
            version_count += 1

            # Symlink'ler
            if src_ver_dir.exists():
                for f in src_ver_dir.iterdir():
                    if f.is_file():
                        dst_file = dst_ver_dir / f.name
                        if symlink_file(f, dst_file):
                            symlink_count += 1

            # scan_report.json varsa import et
            scan_file = src_ver_dir / "scan_report.json"
            if scan_file.exists():
                try:
                    sr = json.loads(scan_file.read_text())
                    summary = sr.get("summary", {})
                    findings_raw = sr.get("findings", [])

                    findings = []
                    for fr in findings_raw:
                        sev = fr.get("severity", "LOW").upper()
                        if sev not in ("HIGH", "MEDIUM", "LOW"):
                            sev = "LOW"
                        findings.append(FindingRecord(
                            severity=sev,
                            category=fr.get("category", "unknown"),
                            file=fr.get("file", ""),
                            line=fr.get("line", 0),
                            description=fr.get("description", ""),
                            snippet=fr.get("snippet", ""),
                        ))

                    risk = (sr.get("risk_level") or "CLEAN").upper()
                    if risk not in ("HIGH", "MEDIUM", "LOW", "CLEAN"):
                        risk = "CLEAN"

                    scan_rec = ScanReportRecord(
                        repo_key=repo_key,
                        version=ver_str,
                        archive_id=v.get("archive_id", f"{owner}-{repo_name}-{ver_str}"),
                        risk_level=risk,
                        is_clean=sr.get("is_clean", risk == "CLEAN"),
                        files_scanned=sr.get("files_scanned", 0),
                        high_count=summary.get("high", 0),
                        medium_count=summary.get("medium", 0),
                        low_count=summary.get("low", 0),
                        total_findings=summary.get("total", len(findings)),
                        scanned_at=parse_dt(sr.get("scanned_at")) or datetime.now(tz=timezone.utc),
                        error=sr.get("error", ""),
                        findings=findings,
                    )
                    catalog.save_scan(scan_rec)
                    scan_count += 1
                except Exception as e:
                    print(f"  [UYARI] scan_report import hatası {scan_file}: {e}")

        if repo_count % 50 == 0:
            print(f"  → {repo_count} repo işlendi...")

    print()
    print("=" * 50)
    print(f"✓ Repo kayıtları    : {repo_count}")
    print(f"✓ Versiyon kayıtları: {version_count}")
    print(f"✓ Scan raporu       : {scan_count}")
    print(f"✓ Symlink oluşturuldu: {symlink_count}")
    print(f"  Atlanan           : {skip_count}")
    print("=" * 50)

    # Son kontrol
    with catalog._conn() as con:
        r = con.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        v = con.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
        s = con.execute("SELECT COUNT(*) FROM scan_reports").fetchone()[0]
    print(f"\nDB son durum → repos: {r} | versions: {v} | scans: {s}")


if __name__ == "__main__":
    migrate()
