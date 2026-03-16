"""CLI tests for list command."""

from __future__ import annotations

from click.testing import CliRunner

from tessera.cli.main import cli


def _fake_datasets():
    return [
        {
            "id": "ds-1",
            "name": "alpha",
            "source": "kaggle",
            "source_ref": "owner/alpha",
            "current_version": "1.0.0",
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:00:00+00:00",
            "tags": "[]",
            "description": "",
            "is_archived": 0,
        },
        {
            "id": "ds-2",
            "name": "beta",
            "source": "huggingface",
            "source_ref": "owner/beta",
            "current_version": "2.0.0",
            "created_at": "2024-01-02T00:00:00+00:00",
            "updated_at": "2024-01-02T00:00:00+00:00",
            "tags": "[]",
            "description": "",
            "is_archived": 1,
        },
    ]


def _make_catalog(datasets=None, version_zone="raw"):
    dsets = datasets if datasets is not None else _fake_datasets()

    class FakeCatalog:
        def search_datasets(self, **kwargs):
            return dsets

        def get_latest_version(self, dataset_id):
            return {"id": f"v-{dataset_id}", "zone": version_zone}

    return FakeCatalog()


def test_list_shows_all_datasets(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog()},
    )
    result = CliRunner().invoke(cli, ["list"])

    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output


def test_list_filters_archived(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog()},
    )
    result = CliRunner().invoke(cli, ["list", "--archived"])

    assert result.exit_code == 0
    assert "beta" in result.output
    assert "alpha" not in result.output


def test_list_filters_by_zone(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog(version_zone="processed")},
    )
    result = CliRunner().invoke(cli, ["list", "--zone", "processed"])

    assert result.exit_code == 0
    assert "ds-1" in result.output


def test_list_empty_catalog(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog(datasets=[])},
    )
    result = CliRunner().invoke(cli, ["list"])

    assert result.exit_code == 0
    assert "bulunamadi" in result.output.lower()


def test_list_json_format(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"catalog": _make_catalog()},
    )
    result = CliRunner().invoke(cli, ["list", "--format", "json"])

    assert result.exit_code == 0
    assert "alpha" in result.output
