"""Tests for the lineage hook."""

from __future__ import annotations

from pathlib import Path

from tessera.hooks.lineage import LineageHook


def test_lineage_hook_appends_event():
    context = {}
    LineageHook({}).execute("transform", context)

    assert context["lineage_events"][0]["event"] == "transform"


def test_lineage_hook_stores_multiple_events():
    context = {}
    hook = LineageHook({})
    hook.execute("pre_ingest", context)
    hook.execute("post_ingest", context)

    assert len(context["lineage_events"]) == 2
    assert context["lineage_events"][1]["event"] == "post_ingest"


def test_lineage_hook_does_not_include_events_list_in_stored_context():
    context = {}
    LineageHook({}).execute("transform", context)

    stored_ctx = context["lineage_events"][0]["context"]
    assert "lineage_events" not in stored_ctx


def test_lineage_hook_records_to_catalog(tmp_path: Path):
    from tessera.core.catalog import CatalogManager

    db_path = tmp_path / "catalog.db"
    catalog = CatalogManager(db_path)
    catalog.initialize()

    # Register a dataset and version to get a valid version_id
    dataset_id = catalog.register_dataset(
        type("D", (), {"name": "test", "source": "kaggle", "source_ref": "test/ds"})()
    )
    version_id = catalog.register_version(
        dataset_id,
        {
            "version": "1.0.0",
            "checksum_sha256": "abc",
            "file_size_bytes": 100,
            "raw_path": str(tmp_path / "raw.csv"),
        },
    )

    context = {
        "version_id": version_id,
        "dataset_id": dataset_id,
        "source": "kaggle",
        "source_ref": "test/ds",
        "catalog_db": str(db_path),
    }
    LineageHook({}).execute("post_ingest", context)

    records = catalog.get_lineage(version_id)
    assert len(records) == 1
    assert records[0]["operation"] == "post_ingest"
    assert records[0]["plugin_name"] == "lineage"


def test_lineage_hook_skips_catalog_without_version_id():
    """Should not crash when version_id is absent."""
    context = {"source": "kaggle"}
    LineageHook({}).execute("post_ingest", context)

    assert "lineage_events" in context
