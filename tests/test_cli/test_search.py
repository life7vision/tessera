"""CLI tests for search, inspect, plugin, config, export, and stats commands."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from tessera.cli.main import cli


class FakeCatalog:
    def search_datasets(self, query=None, source=None, tags=None):
        return [
            {
                "id": "dataset-1",
                "name": "demo",
                "source": "kaggle",
                "source_ref": "owner/demo",
                "current_version": "1.0.0",
                "created_at": "2026-01-01",
                "updated_at": "2026-01-01",
                "tags": ["demo"],
                "description": "demo",
                "is_archived": 0,
            }
        ]

    def get_dataset(self, dataset_id):
        if dataset_id == "dataset-1":
            return self.search_datasets()[0]
        return None

    def get_latest_version(self, dataset_id):
        return {
            "id": "version-1",
            "version": "1.0.0",
            "zone": "processed",
            "raw_path": "/tmp/raw.csv",
            "processed_path": "/tmp/processed.csv",
            "archive_path": None,
            "metadata_json": {"rows": 2},
        }

    def get_versions(self, dataset_id):
        return [self.get_latest_version(dataset_id)]

    def get_lineage(self, version_id):
        return [{"plugin_name": "format", "status": "success"}]

    def get_stats(self):
        return {"dataset_count": 1, "version_count": 1}


class FakeAudit:
    def get_logs(self, **kwargs):
        return [{"action": "ingest", "resource_type": "dataset_version"}]


class FakeRegistry:
    def list_plugins(self):
        return {"connectors": ["kaggle"], "validators": [], "transformers": [], "exporters": ["local"], "hooks": []}

    def get_exporter(self, name):
        return FakeExporter()


class FakeExporter:
    def export(self, version_id, target_path, **kwargs):
        Path(target_path).write_text("demo", encoding="utf-8")
        return type(
            "ExportResult",
            (),
            {
                "__dict__": {
                    "success": True,
                    "exporter_name": "local",
                    "output_path": str(target_path),
                    "size_bytes": 4,
                    "duration_ms": 1,
                }
            },
        )()


class FakeConfig:
    def __init__(self):
        self.project = type("Project", (), {"name": "demo-project"})()

    def to_dict(self):
        return {"project": {"name": "demo-project"}}


def fake_runtime():
    return {
        "catalog": FakeCatalog(),
        "audit": FakeAudit(),
        "registry": FakeRegistry(),
        "config": FakeConfig(),
    }


def test_search_command(mocker):
    mocker.patch("tessera.cli.common.get_runtime", return_value=fake_runtime())
    runner = CliRunner()
    result = runner.invoke(cli, ["search", "demo", "--format", "json"])
    assert result.exit_code == 0
    assert "dataset-1" in result.output


def test_inspect_command(mocker):
    mocker.patch("tessera.cli.common.get_runtime", return_value=fake_runtime())
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", "dataset-1", "--lineage", "--audit"])
    assert result.exit_code == 0
    assert "dataset-1" in result.output


def test_plugin_and_config_commands(mocker):
    mocker.patch("tessera.cli.common.get_runtime", return_value=fake_runtime())
    runner = CliRunner()
    plugin_result = runner.invoke(cli, ["plugin", "list"])
    config_result = runner.invoke(cli, ["config", "show"])
    assert plugin_result.exit_code == 0
    assert config_result.exit_code == 0
    assert "kaggle" in plugin_result.output
    assert "demo-project" in config_result.output


def test_export_and_stats_commands(mocker, tmp_path: Path):
    mocker.patch("tessera.cli.common.get_runtime", return_value=fake_runtime())
    runner = CliRunner()
    export_target = tmp_path / "export.csv"
    export_result = runner.invoke(cli, ["export", "dataset-1", str(export_target)])
    stats_result = runner.invoke(cli, ["stats"])
    assert export_result.exit_code == 0
    assert export_target.exists()
    assert stats_result.exit_code == 0
    assert "dataset_count" in stats_result.output


def test_list_and_config_validate_commands(mocker):
    mocker.patch("tessera.cli.common.get_runtime", return_value=fake_runtime())
    runner = CliRunner()
    list_result = runner.invoke(cli, ["list", "--format", "json"])
    validate_result = runner.invoke(cli, ["config", "validate"])

    assert list_result.exit_code == 0
    assert "dataset-1" in list_result.output
    assert validate_result.exit_code == 0
    assert "valid" in validate_result.output
