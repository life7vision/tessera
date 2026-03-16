"""Tests for catalog management."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from tessera.core.catalog import CatalogManager


@dataclass
class FakeDatasetInfo:
    source: str
    source_ref: str
    name: str
    description: str
    tags: list[str] = field(default_factory=list)


def make_catalog(tmp_path: Path) -> CatalogManager:
    catalog = CatalogManager(tmp_path / "catalog.db")
    catalog.initialize()
    return catalog


def test_register_and_search_dataset(tmp_path: Path):
    catalog = make_catalog(tmp_path)
    dataset_id = catalog.register_dataset(
        FakeDatasetInfo(
            source="kaggle",
            source_ref="owner/demo",
            name="demo-dataset",
            description="Demo catalog entry",
            tags=["tabular", "demo"],
        )
    )

    dataset = catalog.get_dataset(dataset_id)
    assert dataset is not None
    assert dataset["name"] == "demo-dataset"
    assert catalog.search_datasets(query="demo")[0]["id"] == dataset_id
    assert catalog.search_datasets(source="kaggle")[0]["id"] == dataset_id
    assert catalog.search_datasets(tags=["tabular"])[0]["id"] == dataset_id


def test_register_version_and_lineage(tmp_path: Path):
    catalog = make_catalog(tmp_path)
    dataset_id = catalog.register_dataset(
        FakeDatasetInfo(
            source="github",
            source_ref="org/repo",
            name="repo-data",
            description="Repository dataset",
        )
    )

    version_id = catalog.register_version(
        dataset_id,
        {
            "version": "1.1.0",
            "checksum_sha256": "abc123",
            "file_size_bytes": 128,
            "file_count": 1,
            "raw_path": "/tmp/raw.csv",
            "processed_path": "/tmp/processed.parquet",
            "zone": "processed",
            "format": "parquet",
            "metadata_json": {"rows": 10},
        },
    )
    catalog.record_lineage(
        version_id,
        "transform",
        "format",
        input_checksum="abc123",
        output_checksum="def456",
        parameters={"output_format": "parquet"},
        status="success",
        duration_ms=42,
    )

    latest = catalog.get_latest_version(dataset_id)
    lineage = catalog.get_lineage(version_id)
    duplicate = catalog.check_duplicate("abc123")

    assert latest is not None
    assert latest["version"] == "1.1.0"
    assert latest["metadata_json"] == {"rows": 10}
    assert lineage[0]["plugin_name"] == "format"
    assert lineage[0]["parameters_json"] == {"output_format": "parquet"}
    assert duplicate is not None
    assert duplicate["id"] == version_id


def test_update_zone_archive_and_stats(tmp_path: Path):
    catalog = make_catalog(tmp_path)
    dataset_id = catalog.register_dataset(
        FakeDatasetInfo(
            source="huggingface",
            source_ref="user/data",
            name="hf-data",
            description="HF dataset",
        )
    )
    version_id = catalog.register_version(
        dataset_id,
        {
            "version": "1.0.0",
            "checksum_sha256": "feedbeef",
            "file_size_bytes": 2048,
            "raw_path": "/tmp/raw",
        },
    )

    catalog.update_version_zone(version_id, "archive", "/tmp/archive")
    catalog.archive_dataset(dataset_id)

    version = catalog.get_version(version_id)
    dataset = catalog.get_dataset(dataset_id)
    stats = catalog.get_stats()

    assert version is not None
    assert version["zone"] == "archive"
    assert version["archive_path"] == "/tmp/archive"
    assert dataset is not None
    assert dataset["is_archived"] == 1
    assert stats["dataset_count"] == 1
    assert stats["version_count"] == 1
    assert stats["archived_dataset_count"] == 1
    assert stats["total_file_size_bytes"] == 2048


def test_get_versions_returns_latest_first(tmp_path: Path):
    catalog = make_catalog(tmp_path)
    dataset_id = catalog.register_dataset(
        FakeDatasetInfo(
            source="kaggle",
            source_ref="owner/demo",
            name="demo-dataset",
            description="Demo",
        )
    )
    catalog.register_version(
        dataset_id,
        {
            "version": "1.0.0",
            "checksum_sha256": "one",
            "file_size_bytes": 100,
            "raw_path": "/tmp/raw-1",
        },
    )
    catalog.register_version(
        dataset_id,
        {
            "version": "1.1.0",
            "checksum_sha256": "two",
            "file_size_bytes": 100,
            "raw_path": "/tmp/raw-2",
        },
    )

    versions = catalog.get_versions(dataset_id)

    assert [version["version"] for version in versions] == ["1.1.0", "1.0.0"]
