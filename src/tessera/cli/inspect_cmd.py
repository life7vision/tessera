"""CLI command for dataset inspection."""

from __future__ import annotations

import click

from tessera.cli import common


@click.command("inspect")
@click.argument("dataset_id")
@click.option("--version", "version_name", default=None)
@click.option("--lineage", "show_lineage", is_flag=True, default=False)
@click.option("--audit", "show_audit", is_flag=True, default=False)
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def inspect_cmd(
    ctx: click.Context,
    dataset_id: str,
    version_name: str | None,
    show_lineage: bool,
    show_audit: bool,
    output_format: str,
) -> None:
    """Inspect dataset and version details."""

    runtime = common.get_runtime(ctx)
    catalog = runtime["catalog"]
    audit = runtime["audit"]
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise click.ClickException(f"Dataset bulunamadi: {dataset_id}")

    version = None
    if version_name:
        versions = catalog.get_versions(dataset_id)
        version = next((item for item in versions if item["version"] == version_name), None)
    else:
        version = catalog.get_latest_version(dataset_id)

    payload = {"dataset": dataset, "version": version}
    if show_lineage and version:
        payload["lineage"] = catalog.get_lineage(version["id"])
    if show_audit:
        payload["audit"] = audit.get_logs(resource_type="dataset_version", limit=100)
    common.print_output(payload, output_format=output_format, title="Inspect")
