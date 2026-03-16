"""Tests for the local exporter."""

from __future__ import annotations

from pathlib import Path

from tessera.exporters.local import LocalExporter


def test_local_exporter_copies_file(sample_csv, tmp_path: Path):
    target = tmp_path / "export.csv"
    result = LocalExporter({}).export("version-1", target, source_path=sample_csv)

    assert result.success is True
    assert target.exists()

