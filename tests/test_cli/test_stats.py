"""CLI tests for stats command."""

from __future__ import annotations

from click.testing import CliRunner

from tessera.cli.main import cli


def _make_catalog(stats=None):
    default_stats = {
        "dataset_count": 5,
        "version_count": 12,
        "total_size_bytes": 1024 * 1024,
        "source_breakdown": {"kaggle": 3, "huggingface": 2},
    }

    class FakeCatalog:
        def get_stats(self):
            return stats or default_stats

    return FakeCatalog()


def test_stats_shows_counts(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog()},
    )
    result = CliRunner().invoke(cli, ["stats"])

    assert result.exit_code == 0
    assert "5" in result.output or "dataset_count" in result.output


def test_stats_json_format(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog()},
    )
    result = CliRunner().invoke(cli, ["stats", "--format", "json"])

    assert result.exit_code == 0
    assert "dataset_count" in result.output
