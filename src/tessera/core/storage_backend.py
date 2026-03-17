"""
Tessera — Storage Backend soyutlaması.

Desteklenen backend'ler:
  - LocalBackend  : dosya sistemi (geliştirme / tek makine)
  - S3Backend     : AWS S3 (production)

Kullanım:
    from tessera.core.storage_backend import get_backend
    backend = get_backend()               # config'den otomatik seçer
    backend.upload(local_path, key)
    backend.download(key, local_path)
    url = backend.presign(key, expires=3600)
"""

from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# Soyut arayüz
# ---------------------------------------------------------------------------

class StorageBackend(ABC):

    @abstractmethod
    def upload(self, local_path: Path, key: str) -> str:
        """Yerel dosyayı backend'e yükle. Backend URI döner."""

    @abstractmethod
    def download(self, key: str, local_path: Path) -> None:
        """Backend'den dosyayı indir."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """key mevcut mu?"""

    @abstractmethod
    def delete(self, key: str) -> None:
        """key'i sil."""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> Iterator[str]:
        """prefix altındaki tüm key'leri döner."""

    @abstractmethod
    def presign(self, key: str, expires: int = 3600) -> str:
        """İndirilebilir geçici URL döner (local backend dosya yolunu döner)."""

    @abstractmethod
    def public_url(self, key: str) -> str:
        """Tam URI: s3://... veya file://..."""


# ---------------------------------------------------------------------------
# Local backend
# ---------------------------------------------------------------------------

class LocalBackend(StorageBackend):
    """Dosya sistemi — geliştirme ve offline kullanım için."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _full(self, key: str) -> Path:
        return self.root / key

    def upload(self, local_path: Path, key: str) -> str:
        dst = self._full(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if local_path.resolve() != dst.resolve():
            shutil.copy2(local_path, dst)
        return f"file://{dst}"

    def download(self, key: str, local_path: Path) -> None:
        src = self._full(key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, local_path)

    def exists(self, key: str) -> bool:
        return self._full(key).exists()

    def delete(self, key: str) -> None:
        p = self._full(key)
        if p.exists():
            p.unlink()

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        base = self._full(prefix) if prefix else self.root
        if not base.exists():
            return
        for f in base.rglob("*"):
            if f.is_file():
                yield str(f.relative_to(self.root))

    def presign(self, key: str, expires: int = 3600) -> str:
        return f"file://{self._full(key)}"

    def public_url(self, key: str) -> str:
        return f"file://{self._full(key)}"


# ---------------------------------------------------------------------------
# S3 backend
# ---------------------------------------------------------------------------

class S3Backend(StorageBackend):
    """AWS S3 — production."""

    def __init__(self, bucket: str, prefix: str = "", region: str = "eu-central-1") -> None:
        import boto3
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.region = region
        self._s3 = boto3.client("s3", region_name=region)

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def upload(self, local_path: Path, key: str) -> str:
        s3_key = self._key(key)
        self._s3.upload_file(
            str(local_path),
            self.bucket,
            s3_key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        return f"s3://{self.bucket}/{s3_key}"

    def download(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, self._key(key), str(local_path))

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except self._s3.exceptions.ClientError:
            return False
        except Exception:
            return False

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=self._key(key))

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        full_prefix = self._key(prefix) if prefix else (self.prefix + "/" if self.prefix else "")
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                raw = obj["Key"]
                # prefix'i çıkar
                rel = raw[len(self.prefix) + 1:] if self.prefix and raw.startswith(self.prefix + "/") else raw
                yield rel

    def presign(self, key: str, expires: int = 3600) -> str:
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": self._key(key)},
            ExpiresIn=expires,
        )

    def public_url(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"

    def upload_multipart(self, local_path: Path, key: str, chunk_mb: int = 100) -> str:
        """100 MB üzeri dosyalar için multipart upload (daha hızlı + güvenilir)."""
        import boto3
        from boto3.s3.transfer import TransferConfig

        s3_key = self._key(key)
        config = TransferConfig(
            multipart_threshold=chunk_mb * 1024 * 1024,
            max_concurrency=8,
            multipart_chunksize=chunk_mb * 1024 * 1024,
            use_threads=True,
        )
        s3 = boto3.client("s3", region_name=self.region)
        s3.upload_file(
            str(local_path),
            self.bucket,
            s3_key,
            Config=config,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        return f"s3://{self.bucket}/{s3_key}"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_backend(config: dict | None = None) -> StorageBackend:
    """
    Config'e göre backend döner.

    Config örneği:
        storage_backend:
          type: s3          # "local" | "s3"
          s3_bucket_datasets: tessera-datasets-715557237960
          s3_bucket_archives: tessera-archives-715557237960
          s3_region: eu-central-1
          local_root: ./data
    """
    if config is None:
        try:
            from tessera.core.config import load_config
            cfg = load_config()
            config = getattr(cfg, "storage_backend", None) or {}
            if isinstance(config, object) and not isinstance(config, dict):
                config = config.__dict__ if hasattr(config, "__dict__") else {}
        except Exception:
            config = {}

    backend_type = config.get("type", os.getenv("TESSERA_STORAGE_BACKEND", "local"))

    if backend_type == "s3":
        bucket = config.get("s3_bucket_datasets", os.getenv("S3_BUCKET_DATASETS", ""))
        region = config.get("s3_region", os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
        return S3Backend(bucket=bucket, region=region)

    # default: local
    root = config.get("local_root", os.getenv("TESSERA_DATA_ROOT", "./data"))
    return LocalBackend(root=root)


def get_archive_backend(config: dict | None = None) -> StorageBackend:
    """Arşiv dosyaları için backend (tessera-archives bucket)."""
    if config is None:
        try:
            from tessera.core.config import load_config
            cfg = load_config()
            config = getattr(cfg, "storage_backend", None) or {}
            if isinstance(config, object) and not isinstance(config, dict):
                config = config.__dict__ if hasattr(config, "__dict__") else {}
        except Exception:
            config = {}

    backend_type = config.get("type", os.getenv("TESSERA_STORAGE_BACKEND", "local"))

    if backend_type == "s3":
        bucket = config.get("s3_bucket_archives", os.getenv("S3_BUCKET_ARCHIVES", ""))
        region = config.get("s3_region", os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
        return S3Backend(bucket=bucket, region=region)

    root = config.get("archive_root", os.getenv("TESSERA_ARCHIVE_ROOT", "./archive"))
    return LocalBackend(root=root)
