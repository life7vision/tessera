"""
Tessera Archiver — AbstractProvider ABC.

Tüm provider istemcileri bu ABC'yi implemente eder.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import RepoRef


class AbstractProvider(ABC):
    """Provider istemcisi için soyut temel sınıf."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider adı: 'github' | 'gitlab'"""

    @abstractmethod
    def get_repo(self, ref: RepoRef) -> dict | None:
        """
        Repo metadata'sını döner.
        GitHub schema'sına normalize edilmiş dict:
          name, full_name, owner.login, html_url, clone_url,
          default_branch, visibility, description, created_at,
          pushed_at, archived, size (KB), stargazers_count,
          forks_count, watchers_count, open_issues_count,
          language, topics, license
        """

    @abstractmethod
    def get_languages(self, ref: RepoRef) -> dict[str, int]:
        """Dil → byte sayısı eşlemesi döner."""

    @abstractmethod
    def get_contributors(self, ref: RepoRef) -> list[dict]:
        """[{login, contributions}, ...] listesi döner."""

    @abstractmethod
    def get_releases(self, ref: RepoRef) -> list[dict]:
        """[{tag_name, published_at}, ...] listesi döner."""
