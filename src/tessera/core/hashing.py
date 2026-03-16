"""Checksum helpers for files and directories."""

from __future__ import annotations

import hashlib
from pathlib import Path

from tessera.core.exceptions import ValidationError


def _get_hasher(algorithm: str) -> "hashlib._Hash":
    try:
        return hashlib.new(algorithm)
    except ValueError as exc:
        raise ValidationError(f"Desteklenmeyen checksum algoritması: {algorithm}") from exc


def compute_file_checksum(
    file_path: str | Path, algorithm: str = "sha256", chunk_size: int = 8192
) -> str:
    """Compute a checksum for a file with chunked reads."""

    path = Path(file_path)
    if not path.is_file():
        raise ValidationError(f"Checksum için dosya bulunamadı: {path}")

    hasher = _get_hasher(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_directory_checksum(dir_path: str | Path, algorithm: str = "sha256") -> str:
    """Compute a deterministic checksum across directory contents."""

    path = Path(dir_path)
    if not path.is_dir():
        raise ValidationError(f"Checksum için dizin bulunamadı: {path}")

    hasher = _get_hasher(algorithm)
    for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        relative = item.relative_to(path).as_posix().encode("utf-8")
        hasher.update(relative)
        hasher.update(compute_file_checksum(item, algorithm).encode("ascii"))
    return hasher.hexdigest()


def verify_checksum(file_path: str | Path, expected: str, algorithm: str = "sha256") -> bool:
    """Verify that a file checksum matches the expected digest."""

    return compute_file_checksum(file_path, algorithm=algorithm) == expected

