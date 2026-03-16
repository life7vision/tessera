"""Tests for the Hugging Face connector."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tessera.connectors.huggingface import HuggingFaceConnector


def test_huggingface_search_and_metadata(mocker):
    mocker.patch(
        "tessera.connectors.huggingface.list_datasets",
        return_value=[SimpleNamespace(id="user/demo", tags=["nlp"], description="demo")],
    )
    mocker.patch(
        "tessera.connectors.huggingface.dataset_info",
        return_value=SimpleNamespace(id="user/demo", tags=["nlp"], description="demo"),
    )

    connector = HuggingFaceConnector({})
    results = connector.search("demo")
    metadata = connector.fetch_metadata("user/demo")

    assert results[0].name == "demo"
    assert metadata.source == "huggingface"


def test_huggingface_download(mocker, tmp_path: Path):
    dataset_dir = tmp_path / "snapshot"
    dataset_dir.mkdir()
    (dataset_dir / "data.csv").write_text("id\n1\n", encoding="utf-8")
    mocker.patch("tessera.connectors.huggingface.snapshot_download", return_value=str(dataset_dir))

    connector = HuggingFaceConnector({})
    result = connector.download("user/demo", tmp_path / "target")

    assert result.success is True
    assert result.file_count == 1

