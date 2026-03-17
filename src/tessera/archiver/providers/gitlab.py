"""
Tessera Archiver — GitLab API istemcisi.

github-archiver/gitlab_client.py'den taşındı ve AbstractProvider'a uyarlandı.
"""
from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import quote

import requests

from ..config import ProviderConfig
from ..models import RepoRef
from .base import AbstractProvider

log = logging.getLogger(__name__)


class GitLabProvider(AbstractProvider):
    """GitLab REST API v4 istemcisi."""

    def __init__(self, cfg: ProviderConfig | None = None) -> None:
        if cfg is None:
            from ..config import get_archiver_config
            cfg = get_archiver_config().providers.gitlab

        self._cfg = cfg
        self.session = requests.Session()
        token = cfg.token
        if token:
            self.session.headers["PRIVATE-TOKEN"] = token
        else:
            log.warning("GITLAB_TOKEN tanımlanmamış.")

    @property
    def name(self) -> str:
        return "gitlab"

    # ------------------------------------------------------------------
    # İç yardımcı
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict | None = None) -> Any:
        url = f"{self._cfg.api_url}{endpoint}"
        for attempt in range(1, self._cfg.retry_count + 1):
            try:
                resp = self.session.get(
                    url, params=params, timeout=self._cfg.timeout_sec
                )
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429 or (
                    resp.status_code == 403 and "rate limit" in resp.text.lower()
                ):
                    wait = int(resp.headers.get("Retry-After", "60"))
                    log.warning("GitLab rate limit. %ds bekleniyor...", max(wait, 1))
                    time.sleep(max(wait, 1))
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                log.error("GitLab hata (%d/%d): %s", attempt, self._cfg.retry_count, exc)
                if attempt < self._cfg.retry_count:
                    time.sleep(self._cfg.retry_delay * attempt)
        raise RuntimeError(f"GitLab API isteği başarısız: {url}")

    @staticmethod
    def _norm_repo(project: dict) -> dict:
        """GitLab project → GitHub schema'sına normalize et."""
        ns = ((project.get("namespace") or {}).get("full_path") or "").strip()
        owner = ns.split("/")[0] if ns else ""
        return {
            "name": project.get("name"),
            "full_name": project.get("path_with_namespace"),
            "owner": {"login": owner or ns},
            "namespace": ns,
            "html_url": project.get("web_url"),
            "clone_url": project.get("http_url_to_repo"),
            "default_branch": project.get("default_branch") or "main",
            "visibility": project.get("visibility", "private"),
            "description": project.get("description"),
            "created_at": project.get("created_at"),
            "pushed_at": project.get("last_activity_at"),
            "archived": project.get("archived", False),
            "size": int(
                ((project.get("statistics") or {}).get("repository_size", 0)) / 1024
            ),
            "stargazers_count": project.get("star_count", 0),
            "forks_count": project.get("forks_count", 0),
            "watchers_count": 0,
            "open_issues_count": project.get("open_issues_count", 0),
            "language": project.get("language"),
            "topics": project.get("topics") or project.get("tag_list") or [],
            "license": None,
        }

    # ------------------------------------------------------------------
    # AbstractProvider implementasyonu
    # ------------------------------------------------------------------

    def get_repo(self, ref: RepoRef) -> dict | None:
        path = quote(f"{ref.namespace}/{ref.repo}", safe="")
        data = self._get(f"/projects/{path}", {"statistics": True})
        return self._norm_repo(data) if data else None

    def get_languages(self, ref: RepoRef) -> dict[str, int]:
        path = quote(f"{ref.namespace}/{ref.repo}", safe="")
        data = self._get(f"/projects/{path}/languages") or {}
        # GitLab yüzde döndürebilir — int'e normalize et
        out: dict[str, int] = {}
        for k, v in data.items():
            try:
                out[k] = int(float(v) * 1000)
            except (ValueError, TypeError):
                out[k] = 0
        return out

    def get_contributors(self, ref: RepoRef) -> list[dict]:
        path = quote(f"{ref.namespace}/{ref.repo}", safe="")
        data = self._get(f"/projects/{path}/repository/contributors") or []
        return [
            {
                "login": c.get("name") or c.get("email", ""),
                "contributions": c.get("commits", 0),
            }
            for c in data
        ]

    def get_releases(self, ref: RepoRef) -> list[dict]:
        path = quote(f"{ref.namespace}/{ref.repo}", safe="")
        data = self._get(f"/projects/{path}/releases") or []
        return [
            {"tag_name": r.get("tag_name"), "published_at": r.get("released_at")}
            for r in data
        ]

    # ------------------------------------------------------------------
    # GitLab'a özgü ekstra metodlar
    # ------------------------------------------------------------------

    def list_group_projects(self, group: str) -> list[tuple[str, str]]:
        """Grup ve alt gruplarındaki tüm (namespace, repo) çiftlerini döner."""
        g = quote(group, safe="")
        page, out = 1, []
        while True:
            data = (
                self._get(
                    f"/groups/{g}/projects",
                    {
                        "per_page": 100,
                        "page": page,
                        "include_subgroups": True,
                        "with_shared": False,
                    },
                )
                or []
            )
            if not data:
                break
            for p in data:
                full = p.get("path_with_namespace", "")
                parts = [x for x in full.split("/") if x]
                if len(parts) >= 2:
                    out.append(("/".join(parts[:-1]), parts[-1]))
            if len(data) < 100:
                break
            page += 1
        return out
