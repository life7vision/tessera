"""
Tessera Archiver — GitHub API istemcisi.

github-archiver/github_client.py'den taşındı ve AbstractProvider'a uyarlandı.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from ..config import ProviderConfig
from ..models import RepoRef
from .base import AbstractProvider

log = logging.getLogger(__name__)


class GitHubProvider(AbstractProvider):
    """GitHub REST API v3 istemcisi."""

    def __init__(self, cfg: ProviderConfig | None = None) -> None:
        if cfg is None:
            from ..config import get_archiver_config
            cfg = get_archiver_config().providers.github

        self._cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        token = cfg.token
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        else:
            log.warning("GITHUB_TOKEN tanımlanmamış — rate limit: 60 req/saat")

    @property
    def name(self) -> str:
        return "github"

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
                if resp.status_code == 403 and "rate limit" in resp.text.lower():
                    reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait = max(reset - int(time.time()), 1)
                    log.warning("GitHub rate limit. %ds bekleniyor...", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    log.warning("Bulunamadı: %s", url)
                    return None
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                log.error("GitHub hata (deneme %d/%d): %s", attempt, self._cfg.retry_count, exc)
                if attempt < self._cfg.retry_count:
                    time.sleep(self._cfg.retry_delay * attempt)
        raise RuntimeError(f"GitHub API isteği başarısız: {url}")

    # ------------------------------------------------------------------
    # AbstractProvider implementasyonu
    # ------------------------------------------------------------------

    def get_repo(self, ref: RepoRef) -> dict | None:
        return self._get(f"/repos/{ref.namespace}/{ref.repo}")

    def get_languages(self, ref: RepoRef) -> dict[str, int]:
        return self._get(f"/repos/{ref.namespace}/{ref.repo}/languages") or {}

    def get_contributors(self, ref: RepoRef) -> list[dict]:
        result, page = [], 1
        while True:
            data = self._get(
                f"/repos/{ref.namespace}/{ref.repo}/contributors",
                {"per_page": 100, "page": page, "anon": "true"},
            )
            if not data:
                break
            result.extend(data)
            if len(data) < 100:
                break
            page += 1
        return result

    def get_releases(self, ref: RepoRef) -> list[dict]:
        result, page = [], 1
        while True:
            data = self._get(
                f"/repos/{ref.namespace}/{ref.repo}/releases",
                {"per_page": 100, "page": page},
            )
            if not data:
                break
            result.extend(data)
            if len(data) < 100:
                break
            page += 1
        return result

    # ------------------------------------------------------------------
    # GitHub'a özgü ekstra metodlar
    # ------------------------------------------------------------------

    def list_org_repos(self, org: str) -> list[tuple[str, str]]:
        """Org'a ait tüm (namespace, repo) çiftlerini döner."""
        result, page = [], 1
        while True:
            data = self._get(
                f"/orgs/{org}/repos",
                {"per_page": 100, "page": page, "type": "all"},
            )
            if not data:
                break
            for r in data:
                full = r.get("full_name", "")
                parts = full.split("/")
                if len(parts) == 2:
                    result.append((parts[0], parts[1]))
            if len(data) < 100:
                break
            page += 1
        return result
