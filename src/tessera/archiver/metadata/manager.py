"""
Tessera Archiver — Metadata CRUD yönetimi.

github-archiver/metadata_manager.py'den taşındı ve Tessera storage mimarisine uyarlandı.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ..models import RepoRef
from ..pipeline.profiler import analyze_repository
from ..storage import ArchiverStorage

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def _classify_language(lang: str | None) -> str:
    if not lang:
        return "unknown"
    mapping = {
        "Python": "python", "JavaScript": "javascript", "TypeScript": "typescript",
        "Go": "go", "Rust": "rust", "Java": "java", "C": "c", "C++": "cpp",
        "C#": "csharp", "Ruby": "ruby", "PHP": "php", "Swift": "swift",
        "Kotlin": "kotlin", "Shell": "shell", "Dockerfile": "docker",
    }
    return mapping.get(lang, lang.lower().replace(" ", "-"))


def _classify_topic(topics: list[str] | None) -> str:
    if not topics:
        return "general"
    mapping = {
        ("ml", "machine-learning", "deep-learning", "ai", "llm", "nlp", "pytorch", "tensorflow"): "machine-learning",
        ("web", "frontend", "react", "vue", "angular", "css", "html"): "web",
        ("api", "backend", "server", "rest", "graphql", "microservice"): "backend",
        ("devops", "kubernetes", "docker", "k8s", "terraform", "ansible", "ci", "cd"): "devops",
        ("security", "pentest", "ctf", "vulnerability", "crypto"): "security",
        ("data", "analytics", "etl", "spark", "sql", "database"): "data",
        ("mobile", "android", "ios", "flutter", "react-native"): "mobile",
        ("game", "unity", "unreal", "godot"): "game",
        ("cli", "terminal", "command-line", "shell"): "cli",
    }
    topics_lower = {t.lower() for t in topics}
    for keys, category in mapping.items():
        if topics_lower & set(keys):
            return category
    return "general"


# ---------------------------------------------------------------------------
# Metadata builder
# ---------------------------------------------------------------------------

def build_repo_metadata(
    repo_info: dict,
    languages: dict,
    archive_path: Path,
    checksum: str,
    size_bytes: int,
    method: str,
    archive_id: str,
    provider: str = "github",
) -> dict:
    """GitHub API'den gelen repo_info'yu zengin metadata dict'ine dönüştürür."""
    lang = repo_info.get("language")
    topics = repo_info.get("topics") or []

    license_name = None
    lic = repo_info.get("license")
    if isinstance(lic, dict):
        license_name = lic.get("spdx_id") or lic.get("name")

    analysis = analyze_repository(repo_info, languages)

    return {
        "archive_id": archive_id,
        "archived_at": _utcnow(),
        "schema_version": "1.0.0",
        "source": {
            "provider": provider,
            "owner": (repo_info.get("owner") or {}).get("login", ""),
            "repo": repo_info.get("name", ""),
            "namespace": repo_info.get("namespace") or (repo_info.get("owner") or {}).get("login", ""),
            "url": repo_info.get("html_url", ""),
            "default_branch": repo_info.get("default_branch", "main"),
            "visibility": repo_info.get("visibility", "public"),
            "description": repo_info.get("description"),
            "created_at": repo_info.get("created_at"),
            "pushed_at": repo_info.get("pushed_at"),
            "archived": repo_info.get("archived", False),
        },
        "stats": {
            "stars": repo_info.get("stargazers_count", 0),
            "forks": repo_info.get("forks_count", 0),
            "watchers": repo_info.get("watchers_count", 0),
            "open_issues": repo_info.get("open_issues_count", 0),
            "size_kb": repo_info.get("size", 0),
        },
        "classification": {
            "language_primary": lang,
            "languages": list(languages.keys()),
            "language_bytes": languages,
            "topics": topics,
            "license": license_name,
            "category_language": _classify_language(lang),
            "category_topic": _classify_topic(topics),
        },
        "archive": {
            "method": method,
            "compression": "gzip",
            "file": archive_path.name,
            "size_bytes": size_bytes,
            "checksum_sha256": checksum,
        },
        "analysis": analysis,
    }


# ---------------------------------------------------------------------------
# Dosya CRUD
# ---------------------------------------------------------------------------

class MetadataManager:
    """ArchiverStorage üzerinde repo metadata dosyalarını yönetir."""

    def __init__(self, storage: ArchiverStorage) -> None:
        self.storage = storage

    def save_repo_metadata(self, ref: RepoRef, metadata: dict) -> None:
        _save_json(self.storage.repo_info_path(ref), metadata)
        log.debug("repo_info.json kaydedildi: %s", ref.key)

    def load_repo_metadata(self, ref: RepoRef) -> dict | None:
        path = self.storage.repo_info_path(ref)
        if not path.exists():
            return None
        try:
            return _load_json(path)
        except Exception as exc:
            log.warning("repo_info.json okunamadı (%s): %s", ref.key, exc)
            return None

    def save_contributors(self, ref: RepoRef, data: list[dict]) -> None:
        payload = {
            "count": len(data),
            "contributors": [
                {"login": c.get("login"), "contributions": c.get("contributions")}
                for c in data
            ],
        }
        _save_json(self.storage.contributors_path(ref), payload)

    def save_releases(self, ref: RepoRef, data: list[dict]) -> None:
        payload = {
            "count": len(data),
            "releases": [
                {"tag": r.get("tag_name"), "published": r.get("published_at")}
                for r in data
            ],
        }
        _save_json(self.storage.releases_path(ref), payload)

    def save_languages(self, ref: RepoRef, data: dict[str, int]) -> None:
        total = sum(data.values()) or 1
        payload = {
            "total_bytes": sum(data.values()),
            "breakdown": {
                lang: {"bytes": b, "pct": round(b / total * 100, 2)}
                for lang, b in data.items()
            },
        }
        _save_json(self.storage.languages_path(ref), payload)

    def append_archive_log(self, ref: RepoRef, record: dict) -> None:
        _append_jsonl(self.storage.archive_log_path(ref), record)

    def create_structured_links(self, ref: RepoRef, metadata: dict) -> None:
        """by-language, by-topic, by-org sembolik linkleri oluştur."""
        lang_cat = metadata.get("classification", {}).get("category_language")
        topic_cat = metadata.get("classification", {}).get("category_topic")
        self.storage.create_structured_links(ref, lang_cat, topic_cat)

    def all_repo_metadata(self) -> list[dict]:
        """archive/metadata altındaki tüm repo_info.json dosyalarını yükler."""
        results = []
        meta_root = self.storage.root / "metadata"
        if not meta_root.exists():
            return results
        for path in sorted(meta_root.rglob("repo_info.json")):
            try:
                results.append(_load_json(path))
            except Exception as exc:
                log.warning("repo_info.json okunamadı (%s): %s", path, exc)
        return results
