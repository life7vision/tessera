"""Tests for the Kaggle connector."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tessera.connectors.kaggle import KaggleConnector


def test_kaggle_search_and_metadata(mocker):
    fake_api = SimpleNamespace(
        dataset_list=lambda search: [
            SimpleNamespace(
                ref="owner/demo",
                title="Demo Dataset",
                subtitle="Description",
                totalBytes=10,
                fileCount=1,
                tags=[SimpleNamespace(name="demo")],
                licenseName="CC",
                lastUpdated="2026-01-01",
            )
        ],
        dataset_view=lambda source_ref: SimpleNamespace(
            ref="owner/demo",
            title="Demo Dataset",
            subtitle="Description",
            totalBytes=10,
            fileCount=1,
            tags=[],
            licenseName="CC",
            lastUpdated="2026-01-01",
        ),
    )
    mocker.patch.object(KaggleConnector, "_api", return_value=fake_api)

    connector = KaggleConnector({})
    results = connector.search("demo")
    metadata = connector.fetch_metadata("owner/demo")

    assert results[0].source_ref == "owner/demo"
    assert metadata.name == "Demo Dataset"


def test_kaggle_download(mocker, tmp_path: Path):
    def fake_download(source_ref: str, path: Path, unzip: bool = True):
        (Path(path) / "data.csv").write_text("id\n1\n", encoding="utf-8")

    mocker.patch.object(
        KaggleConnector,
        "_api",
        return_value=SimpleNamespace(dataset_download_files=fake_download),
    )

    connector = KaggleConnector({})
    result = connector.download("owner/demo", tmp_path)

    assert result.success is True
    assert result.file_count == 1
