"""CLI tests for plugin command."""

from __future__ import annotations

from click.testing import CliRunner

from tessera.cli.main import cli


def _make_registry():
    class FakeRegistry:
        def list_plugins(self):
            return {
                "connectors": ["kaggle", "huggingface", "github"],
                "validators": ["integrity", "schema", "quality"],
                "transformers": ["clean", "format", "compress"],
                "exporters": ["local", "report"],
                "hooks": ["lineage", "notify"],
            }

    return FakeRegistry()


def test_plugin_list(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"registry": _make_registry()},
    )
    result = CliRunner().invoke(cli, ["plugin", "list"])

    assert result.exit_code == 0
    assert "kaggle" in result.output
    assert "integrity" in result.output


def test_plugin_list_json(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"registry": _make_registry()},
    )
    result = CliRunner().invoke(cli, ["plugin", "list", "--format", "json"])

    assert result.exit_code == 0
    assert "connectors" in result.output


def test_plugin_info_found(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"registry": _make_registry()},
    )
    result = CliRunner().invoke(cli, ["plugin", "info", "kaggle"])

    assert result.exit_code == 0
    assert "kaggle" in result.output
    assert "connectors" in result.output


def test_plugin_info_not_found(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"registry": _make_registry()},
    )
    result = CliRunner().invoke(cli, ["plugin", "info", "nonexistent-plugin"])

    assert result.exit_code != 0
    assert "bulunamadi" in result.output.lower()
