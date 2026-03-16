"""CLI tests for export command."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from tessera.cli.main import cli


def _fake_version(zone: str = "processed", path: str | None = None) -> dict:
    return {
        "id": "v-1",
        "dataset_id": "ds-1",
        "version": "1.0.0",
        "zone": zone,
        "raw_path": path or "/data/raw/file.csv",
        "processed_path": path or "/data/processed/file.parquet",
        "archive_path": None,
    }


def _make_exporter(success: bool = True):
    class FakeResult:
        success = True
        output_path = Path("/tmp/exported")
        size_bytes = 512
        duration_ms = 5
        exporter_name = "local"

    class FakeExporter:
        def export(self, version_id, target_path, **kwargs):
            return FakeResult()

    return FakeExporter()


def _make_catalog(version=None):
    ver = version or _fake_version()

    class FakeCatalog:
        def get_versions(self, _id):
            return [ver]

        def get_latest_version(self, _id):
            return ver

    return FakeCatalog()


def _make_registry():
    class FakeRegistry:
        def get_exporter(self, name):
            return _make_exporter()

    return FakeRegistry()


def test_export_latest_version(mocker, tmp_path: Path):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={
            "catalog": _make_catalog(),
            "registry": _make_registry(),
        },
    )
    result = CliRunner().invoke(cli, ["export", "ds-1", str(tmp_path / "out")])

    assert result.exit_code == 0
    assert "local" in result.output or "export" in result.output.lower()


def test_export_missing_zone_path(mocker, tmp_path: Path):
    version_no_path = _fake_version()
    version_no_path["archive_path"] = None
    version_no_path["processed_path"] = None

    class FakeCatalog:
        def get_versions(self, _id):
            return [version_no_path]

        def get_latest_version(self, _id):
            return version_no_path

    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={
            "catalog": FakeCatalog(),
            "registry": _make_registry(),
        },
    )
    result = CliRunner().invoke(
        cli, ["export", "ds-1", str(tmp_path / "out"), "--zone", "archive"]
    )

    assert result.exit_code != 0
    assert "zone" in result.output.lower() or "yok" in result.output.lower()


def test_export_specific_version(mocker, tmp_path: Path):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={
            "catalog": _make_catalog(),
            "registry": _make_registry(),
        },
    )
    result = CliRunner().invoke(
        cli, ["export", "ds-1", str(tmp_path / "out"), "--version", "1.0.0"]
    )

    assert result.exit_code == 0
