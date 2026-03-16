"""Tests for the GitHub connector."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

from tessera.connectors.github import GitHubConnector


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content=b""):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("request failed")


def build_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("repo/data.csv", "id\n1\n")
    return buffer.getvalue()


def test_github_search_and_metadata(mocker):
    mocker.patch(
        "tessera.connectors.github.requests.get",
        side_effect=[
            FakeResponse(payload={"items": [{"full_name": "org/repo", "name": "repo", "topics": ["dataset"]}]}),
            FakeResponse(payload={"full_name": "org/repo", "name": "repo", "topics": ["dataset"]}),
        ],
    )
    connector = GitHubConnector({})
    results = connector.search("repo")
    metadata = connector.fetch_metadata("org/repo")

    assert results[0].source_ref == "org/repo"
    assert metadata.name == "repo"


def test_github_download(mocker, tmp_path: Path):
    mocker.patch(
        "tessera.connectors.github.requests.get",
        side_effect=[
            FakeResponse(payload={"zipball_url": "https://example.com/archive.zip"}),
            FakeResponse(content=build_zip_bytes()),
        ],
    )

    connector = GitHubConnector({})
    result = connector.download("org/repo", tmp_path)

    assert result.success is True
    assert result.file_count == 1

