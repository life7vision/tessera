"""CLI tests for config command."""

from __future__ import annotations

from click.testing import CliRunner

from tessera.cli.main import cli


def _make_config(name: str = "test-archive"):
    class FakeProject:
        def __init__(self):
            self.name = name

    class FakeConfig:
        project = FakeProject()

        def to_dict(self):
            return {"project": {"name": name}, "storage": {"base_path": "./data"}}

    return FakeConfig()


def test_config_show(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"config": _make_config()},
    )
    result = CliRunner().invoke(cli, ["config", "show"])

    assert result.exit_code == 0
    assert "test-archive" in result.output


def test_config_show_json(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"config": _make_config()},
    )
    result = CliRunner().invoke(cli, ["config", "show", "--format", "json"])

    assert result.exit_code == 0
    assert "test-archive" in result.output


def test_config_validate_passes(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"config": _make_config("my-archive")},
    )
    result = CliRunner().invoke(cli, ["config", "validate"])

    assert result.exit_code == 0
    assert "my-archive" in result.output or "valid" in result.output.lower()
