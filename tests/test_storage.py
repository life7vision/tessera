"""Tests for storage management."""

from __future__ import annotations

from pathlib import Path

from tessera.core.storage import StorageManager


def make_storage(tmp_path: Path) -> StorageManager:
    storage = StorageManager(
        {
            "base_path": str(tmp_path / "data"),
            "zones": {
                "raw": "raw",
                "processed": "processed",
                "archive": "archive",
                "quarantine": "quarantine",
            },
        }
    )
    storage.initialize()
    return storage


def test_initialize_creates_zone_directories(tmp_path: Path):
    storage = make_storage(tmp_path)

    assert storage.get_zone_path("raw").is_dir()
    assert storage.get_zone_path("processed").is_dir()
    assert storage.get_zone_path("archive").is_dir()
    assert storage.get_zone_path("quarantine").is_dir()


def test_store_raw_and_processed(sample_csv, tmp_path: Path):
    storage = make_storage(tmp_path)

    raw_path = storage.store_raw(sample_csv, "demo", "1.0.0")
    processed_path = storage.store_processed(sample_csv, "demo", "1.0.0")

    assert raw_path.exists()
    assert processed_path.exists()
    assert raw_path.parts[-4:] == ("raw", "demo", "v1.0.0", "sample.csv")
    assert processed_path.parts[-4:] == ("processed", "demo", "v1.0.0", "sample.csv")


def test_move_to_archive_and_quarantine(sample_csv, tmp_path: Path):
    storage = make_storage(tmp_path)
    processed_path = storage.store_processed(sample_csv, "demo", "1.0.0")

    archive_path = storage.move_to_archive(processed_path, "demo", "1.0.0")
    assert archive_path.exists()
    assert not processed_path.exists()

    raw_path = storage.store_raw(sample_csv, "demo", "1.0.0")
    quarantine_path = storage.quarantine(raw_path, "demo", "schema_fail")
    assert quarantine_path.exists()
    assert "schema_fail" in quarantine_path.name


def test_zone_size_and_cleanup(sample_csv, tmp_path: Path):
    storage = make_storage(tmp_path)
    storage.store_processed(sample_csv, "demo", "1.0.0")
    storage.store_processed(sample_csv, "demo", "1.1.0")
    storage.store_processed(sample_csv, "demo", "1.2.0")

    assert storage.get_zone_size("processed") > 0
    removed = storage.cleanup_old_versions("demo", keep=2)

    assert len(removed) == 1
    assert removed[0].name == "v1.0.0"


def test_store_raw_directory(sample_csv, tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    nested = source_dir / "nested.csv"
    nested.write_text(sample_csv.read_text(encoding="utf-8"), encoding="utf-8")
    storage = make_storage(tmp_path)

    stored = storage.store_raw(source_dir, "demo", "1.0.0")

    assert stored.is_dir()
    assert (stored / "nested.csv").exists()
