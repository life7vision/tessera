"""Tests for the Kaggle connector (kaggle >= 2.0 + kagglehub)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from tessera.connectors.kaggle import KaggleConnector


def test_kaggle_search(mocker):
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
    )
    mocker.patch.object(KaggleConnector, "_api", return_value=fake_api)

    connector = KaggleConnector({})
    results = connector.search("demo")

    assert len(results) == 1
    assert results[0].source_ref == "owner/demo"
    assert results[0].name == "Demo Dataset"


def test_kaggle_fetch_metadata(mocker, tmp_path: Path):
    meta = {
        "info": {
            "datasetId": 123,
            "datasetSlug": "demo",
            "title": "Demo Dataset",
            "subtitle": "A demo",
            "keywords": ["demo"],
            "licenses": [{"name": "CC0-1.0"}],
            "totalVotes": 5,
            "totalDownloads": 50,
            "usabilityRating": 1.0,
        }
    }

    def fake_metadata(source_ref, path):
        (Path(path) / "dataset-metadata.json").write_text(
            json.dumps(meta), encoding="utf-8"
        )

    mocker.patch.object(
        KaggleConnector, "_api",
        return_value=SimpleNamespace(dataset_metadata=fake_metadata),
    )

    connector = KaggleConnector({})
    result = connector.fetch_metadata("owner/demo")

    assert result.name == "Demo Dataset"
    assert result.source_ref == "owner/demo"
    assert result.tags == ["demo"]
    assert result.license == "CC0-1.0"


def test_kaggle_download_via_kagglehub(mocker, tmp_path: Path):
    # kagglehub cache dizini simüle et
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "data.csv").write_text("id\n1\n2\n", encoding="utf-8")

    mocker.patch("kagglehub.dataset_download", return_value=str(cache_dir))

    connector = KaggleConnector({})
    result = connector.download("owner/demo", tmp_path / "target")

    assert result.success is True
    assert result.file_count == 1
    assert (tmp_path / "target" / "data.csv").exists()


def test_kaggle_validate_credentials_true(mocker):
    mocker.patch.object(
        KaggleConnector, "_api",
        return_value=SimpleNamespace(authenticate=lambda: None),
    )
    assert KaggleConnector({}).validate_credentials() is True


def test_kaggle_validate_credentials_false(mocker):
    def raise_error():
        raise ConnectionError("no auth")

    mocker.patch.object(
        KaggleConnector, "_api",
        return_value=SimpleNamespace(authenticate=raise_error),
    )
    assert KaggleConnector({}).validate_credentials() is False
