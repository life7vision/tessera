"""CLI command for dataset export."""

from __future__ import annotations

from pathlib import Path

import click

from tessera.cli import common


@click.command("export")
@click.argument("dataset_id")
@click.argument("target_path", type=click.Path(path_type=Path))
@click.option("--version", "version_name", default=None)
@click.option("--zone", default="processed", type=click.Choice(["raw", "processed", "archive"]))
@click.pass_context
def export_cmd(
    ctx: click.Context, dataset_id: str, target_path: Path, version_name: str | None, zone: str
) -> None:
    """Export a dataset version."""

    runtime = common.get_runtime(ctx)
    catalog = runtime["catalog"]
    versions = catalog.get_versions(dataset_id)
    version = None
    if version_name:
        version = next((item for item in versions if item["version"] == version_name), None)
    else:
        version = catalog.get_latest_version(dataset_id)
    if not version:
        raise click.ClickException("Versiyon bulunamadi.")

    source_key = f"{zone}_path"
    source_path = version.get(source_key)
    if not source_path:
        raise click.ClickException(f"Bu zone icin dosya yok: {zone}")

    exporter = runtime["registry"].get_exporter("local")
    result = exporter.export(version["id"], target_path, source_path=source_path)
    common.print_output(result.__dict__, title="Export Result")
