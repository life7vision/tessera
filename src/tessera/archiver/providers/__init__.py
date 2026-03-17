"""Tessera Archiver — Provider istemcileri."""
from .base import AbstractProvider
from .github import GitHubProvider
from .gitlab import GitLabProvider


def get_provider(provider_name: str) -> AbstractProvider:
    """Provider adına göre istemci döner."""
    if provider_name == "github":
        return GitHubProvider()
    if provider_name == "gitlab":
        return GitLabProvider()
    raise ValueError(f"Bilinmeyen provider: {provider_name!r}. Desteklenenler: github, gitlab")


__all__ = ["AbstractProvider", "GitHubProvider", "GitLabProvider", "get_provider"]
