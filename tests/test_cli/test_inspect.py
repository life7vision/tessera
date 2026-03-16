"""CLI tests for inspect command."""

from __future__ import annotations

from click.testing import CliRunner

from tessera.cli.main import cli


def _fake_dataset(dataset_id: str = "ds-1") -> dict:
    return {
        "id": dataset_id,
        "name": "test-ds",
        "source": "kaggle",
        "source_ref": "owner/test-ds",
        "current_version": "1.0.0",
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:00+00:00",
        "tags": "[]",
        "description": "",
        "is_archived": 0,
    }


def _fake_version(version_id: str = "v-1") -> dict:
    return {
        "id": version_id,
        "dataset_id": "ds-1",
        "version": "1.0.0",
        "checksum_sha256": "abc123",
        "file_size_bytes": 1024,
        "file_count": 1,
        "raw_path": "/data/raw",
        "zone": "raw",
    }


def _make_catalog(dataset=None, version=None, versions=None, lineage=None):
    ds = dataset or _fake_dataset()
    ver = version or _fake_version()

    class FakeCatalog:
        def get_dataset(self, _id):
            return ds

        def get_versions(self, _id):
            return versions or [ver]

        def get_latest_version(self, _id):
            return ver

        def get_lineage(self, _id):
            return lineage or []

    return FakeCatalog()


def _make_audit():
    class FakeAudit:
        def get_logs(self, **kwargs):
            return []

    return FakeAudit()


def test_inspect_shows_dataset(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog(), "audit": _make_audit()},
    )
    result = CliRunner().invoke(cli, ["inspect", "ds-1"])

    assert result.exit_code == 0
    assert "test-ds" in result.output


def test_inspect_not_found(mocker):
    class EmptyCatalog:
        def get_dataset(self, _id):
            return None

        def get_latest_version(self, _id):
            return None

    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": EmptyCatalog(), "audit": _make_audit()},
    )
    result = CliRunner().invoke(cli, ["inspect", "missing-id"])

    assert result.exit_code != 0
    assert "bulunamadi" in result.output.lower()


def test_inspect_with_lineage_flag(mocker):
    lineage = [{"operation": "ingest", "plugin_name": "kaggle", "status": "success"}]
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog(lineage=lineage), "audit": _make_audit()},
    )
    result = CliRunner().invoke(cli, ["inspect", "ds-1", "--lineage"])

    assert result.exit_code == 0
    assert "ingest" in result.output


def test_inspect_json_format(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog(), "audit": _make_audit()},
    )
    result = CliRunner().invoke(cli, ["inspect", "ds-1", "--format", "json"])

    assert result.exit_code == 0
    assert "test-ds" in result.output
