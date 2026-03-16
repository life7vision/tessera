"""CLI tests for ingest and init commands."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from tessera.cli.main import cli


class FakePipeline:
    def ingest(self, source, source_ref, tags=None, force=False):
        return type(
            "Result",
            (),
            {
                "success": True,
                "dataset_id": "dataset-1",
                "version": "1.0.0",
                "stages": [1, 2],
                "error_message": None,
            },
        )()


def test_ingest_command(mocker):
    mocker.patch(
        "tessera.cli.common.get_runtime",
        return_value={"pipeline": FakePipeline()},
    )
    runner = CliRunner()

    result = runner.invoke(cli, ["ingest", "kaggle", "owner/demo", "--tags", "demo,test"])

    assert result.exit_code == 0
    assert "dataset-1" in result.output


def test_init_command_creates_project(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--path", str(tmp_path / "archive")])

    assert result.exit_code == 0
    assert (tmp_path / "archive" / "config" / "default.yaml").exists()


def test_init_command_copies_env_example(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--path", str(tmp_path / "archive")])

    assert result.exit_code == 0
    assert (tmp_path / "archive" / ".env.example").exists()


def test_init_command_creates_data_dirs(tmp_path: Path):
    runner = CliRunner()
    target = tmp_path / "myarchive"
    runner.invoke(cli, ["init", "--path", str(target)])

    for zone in ("raw", "processed", "archive", "quarantine"):
        assert (target / "data" / zone).exists()


def test_init_command_shows_next_steps(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--path", str(tmp_path / "archive")])

    assert result.exit_code == 0
    assert "Başlarken" in result.output or "cd" in result.output

