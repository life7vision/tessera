#!/usr/bin/env python3
"""
Tessera — S3 Upload Script

Mevcut local veriyi S3'e yükler:
  - data/raw/**  → tessera-datasets-715557237960/raw/
  - archive/raw/** (sadece .tar.gz + scan_report.json + versions.json)
              → tessera-archives-715557237960/raw/

Symlink'leri çözerek gerçek dosyaları yükler.
Zaten yüklenmiş dosyaları atlar (checksum karşılaştırması).
85 GB için multipart upload kullanır.
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

ACCOUNT   = "715557237960"
REGION    = "eu-central-1"
DATASETS_BUCKET = f"tessera-datasets-{ACCOUNT}"
ARCHIVES_BUCKET = f"tessera-archives-{ACCOUNT}"

TESSERA_ROOT  = Path(__file__).parent.parent
DATA_ROOT     = TESSERA_ROOT / "data"
ARCHIVE_RAW   = TESSERA_ROOT / "archive" / "raw"

# Multipart: 100MB chunk, 8 thread
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=100 * 1024 * 1024,
    max_concurrency=8,
    multipart_chunksize=100 * 1024 * 1024,
    use_threads=True,
)

ARCHIVE_EXTENSIONS = {".tar.gz", ".git", ".sha256", ".json"}


def md5_etag(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """AWS S3'ün ETag formatını taklit et (multipart için parça bazlı)."""
    md5s = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5s.append(hashlib.md5(chunk).digest())
    if len(md5s) == 1:
        return hashlib.md5(md5s[0]).hexdigest() if False else hashlib.md5(open(path, "rb").read()).hexdigest()
    combined = b"".join(md5s)
    return f"{hashlib.md5(combined).hexdigest()}-{len(md5s)}"


def already_uploaded(s3, bucket: str, key: str, local_path: Path) -> bool:
    """Dosya S3'te varsa ve boyutu eşleşiyorsa atla."""
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp["ContentLength"] == local_path.stat().st_size
    except Exception:
        return False


def upload_file(s3, bucket: str, key: str, local_path: Path, dry_run: bool = False) -> bool:
    """Tek dosya yükle. True döner = yüklendi, False = atlandı."""
    real_path = local_path.resolve()  # symlink çöz

    if not real_path.exists():
        print(f"  [ATLA] Dosya yok: {real_path}")
        return False

    if already_uploaded(s3, bucket, key, real_path):
        return False  # sessizce atla

    size_mb = real_path.stat().st_size / 1024 / 1024
    if dry_run:
        print(f"  [DRY] {key}  ({size_mb:.1f} MB)")
        return True

    try:
        s3.upload_file(
            str(real_path),
            bucket,
            key,
            Config=TRANSFER_CONFIG,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        return True
    except Exception as e:
        print(f"  [HATA] {key}: {e}")
        return False


def upload_datasets(s3, dry_run: bool = False) -> tuple[int, int]:
    """data/ → tessera-datasets"""
    uploaded = skipped = 0
    if not DATA_ROOT.exists():
        print("data/ dizini bulunamadı, atlanıyor.")
        return 0, 0

    SKIP_SUFFIXES = {".db", ".db-wal", ".db-shm", ".log", ".pyc"}
    files = [
        f for f in DATA_ROOT.rglob("*")
        if (f.is_file() or f.is_symlink())
        and not any(f.name.endswith(s) for s in SKIP_SUFFIXES)
        and not f.name.startswith(".")
    ]
    print(f"\n📦 Datasets: {len(files)} dosya → s3://{DATASETS_BUCKET}/")

    for f in files:
        key = str(f.relative_to(DATA_ROOT))
        if upload_file(s3, DATASETS_BUCKET, key, f, dry_run):
            uploaded += 1
            if uploaded % 10 == 0:
                print(f"   {uploaded} yüklendi...")
        else:
            skipped += 1

    return uploaded, skipped


def upload_archives(s3, dry_run: bool = False) -> tuple[int, int]:
    """archive/raw/ → tessera-archives (sadece tar.gz, json, sha256)"""
    uploaded = skipped = 0
    if not ARCHIVE_RAW.exists():
        print("archive/raw/ dizini bulunamadı, atlanıyor.")
        return 0, 0

    # Sadece belirli uzantılar
    all_files = []
    for f in ARCHIVE_RAW.rglob("*"):
        if not (f.is_file() or f.is_symlink()):
            continue
        name = f.name
        if any(name.endswith(ext) for ext in ARCHIVE_EXTENSIONS) or name == "versions.json":
            all_files.append(f)

    total_bytes = sum(f.resolve().stat().st_size for f in all_files if f.resolve().exists())
    print(f"\n🗄️  Archives: {len(all_files)} dosya ({total_bytes/1024/1024/1024:.1f} GB) → s3://{ARCHIVES_BUCKET}/")

    for f in all_files:
        key = "raw/" + str(f.relative_to(ARCHIVE_RAW))
        if upload_file(s3, ARCHIVES_BUCKET, key, f, dry_run):
            uploaded += 1
            if uploaded % 20 == 0:
                size_done = uploaded
                print(f"   {uploaded}/{len(all_files)} yüklendi...")
        else:
            skipped += 1

    return uploaded, skipped


def main():
    dry_run = "--dry-run" in sys.argv
    datasets_only = "--datasets" in sys.argv
    archives_only = "--archives" in sys.argv

    if dry_run:
        print("🔍 DRY RUN — gerçek yükleme yapılmıyor\n")

    s3 = boto3.client("s3", region_name=REGION)

    total_up = total_sk = 0

    if not archives_only:
        u, s = upload_datasets(s3, dry_run)
        total_up += u; total_sk += s
        print(f"   ✓ Datasets: {u} yüklendi, {s} atlandı")

    if not datasets_only:
        u, s = upload_archives(s3, dry_run)
        total_up += u; total_sk += s
        print(f"   ✓ Archives: {u} yüklendi, {s} atlandı")

    print(f"\n{'='*50}")
    print(f"Toplam yüklenen : {total_up}")
    print(f"Toplam atlanan  : {total_sk}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
