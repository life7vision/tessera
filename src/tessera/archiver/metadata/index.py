"""
Tessera Archiver — Master index.json yönetimi.

archive/_meta/index.json: Tüm repoların hızlı arama için özet kaydı.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ..storage import ArchiverStorage

log = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MasterIndex:
    """archive/_meta/index.json okuma/yazma yöneticisi."""

    def __init__(self, storage: ArchiverStorage) -> None:
        self.storage = storage

    def _load(self) -> dict:
        path = self.storage.index_path
        if not path.exists():
            return {"repos": [], "total_repos": 0, "total_size_bytes": 0, "last_updated": ""}
        try:
            return json.loads(path.read_text())
        except Exception as exc:
            log.warning("index.json okunamadı, yeniden oluşturuluyor: %s", exc)
            return {"repos": [], "total_repos": 0, "total_size_bytes": 0, "last_updated": ""}

    def _save(self, index: dict) -> None:
        self.storage.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.storage.index_path.write_text(json.dumps(index, indent=2, default=str))

    def upsert(self, metadata: dict, version: str = "v1", total_versions: int = 1) -> None:
        """Repo kaydını index'e ekle veya güncelle."""
        index = self._load()
        source = metadata.get("source", {})
        provider = source.get("provider", "github")
        namespace = source.get("namespace") or source.get("owner", "")
        repo = source.get("repo", "")
        key = f"{provider}:{namespace}/{repo}"

        entry = {
            "key": key,
            "provider": provider,
            "namespace": namespace,
            "repo": repo,
            "archive_id": metadata.get("archive_id", ""),
            "archived_at": metadata.get("archived_at", ""),
            "current_version": version,
            "total_versions": total_versions,
            "stars": (metadata.get("stats") or {}).get("stars", 0),
            "language": (metadata.get("classification") or {}).get("language_primary"),
            "purpose": (metadata.get("analysis") or {}).get("purpose", "—"),
            "app_type": (metadata.get("analysis") or {}).get("app_type", "Repository"),
            "size_bytes": (metadata.get("archive") or {}).get("size_bytes", 0),
            "checksum": (metadata.get("archive") or {}).get("checksum_sha256", ""),
        }

        repos = index.get("repos", [])
        idx = next((i for i, r in enumerate(repos) if r.get("key") == key), None)
        if idx is not None:
            repos[idx] = entry
        else:
            repos.append(entry)

        index["repos"] = repos
        index["total_repos"] = len(repos)
        index["total_size_bytes"] = sum(r.get("size_bytes", 0) for r in repos)
        index["last_updated"] = _utcnow()
        self._save(index)
        log.debug("Master index güncellendi: %s (%s)", key, version)

    def get_all(self) -> list[dict]:
        return self._load().get("repos", [])

    def rebuild_from_metadata(self, metadata_list: list[dict]) -> None:
        """Tüm repo_info.json'lardan index'i yeniden oluştur."""
        index: dict = {
            "repos": [],
            "total_repos": 0,
            "total_size_bytes": 0,
            "last_updated": _utcnow(),
        }
        for meta in metadata_list:
            source = meta.get("source", {})
            provider = source.get("provider", "github")
            namespace = source.get("namespace") or source.get("owner", "")
            repo = source.get("repo", "")
            key = f"{provider}:{namespace}/{repo}"
            index["repos"].append({
                "key": key,
                "provider": provider,
                "namespace": namespace,
                "repo": repo,
                "archive_id": meta.get("archive_id", ""),
                "archived_at": meta.get("archived_at", ""),
                "stars": (meta.get("stats") or {}).get("stars", 0),
                "language": (meta.get("classification") or {}).get("language_primary"),
                "purpose": (meta.get("analysis") or {}).get("purpose", "—"),
                "app_type": (meta.get("analysis") or {}).get("app_type", "Repository"),
                "size_bytes": (meta.get("archive") or {}).get("size_bytes", 0),
                "checksum": (meta.get("archive") or {}).get("checksum_sha256", ""),
            })
        index["total_repos"] = len(index["repos"])
        index["total_size_bytes"] = sum(r.get("size_bytes", 0) for r in index["repos"])
        self._save(index)
        log.info("Master index yeniden oluşturuldu: %d repo", index["total_repos"])
