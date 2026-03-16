"""CLI command for dataset listing."""

from __future__ import annotations

import click

from tessera.cli import common


@click.command("list")
@click.option("--zone", default=None)
@click.option("--archived", is_flag=True, default=False)
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def list_cmd(ctx: click.Context, zone: str | None, archived: bool, output_format: str) -> None:
    """List datasets in the catalog."""

    runtime = common.get_runtime(ctx)
    results = runtime["catalog"].search_datasets()
    if archived:
        results = [item for item in results if item["is_archived"] == 1]
    if zone:
        filtered = []
        for item in results:
            latest = runtime["catalog"].get_latest_version(item["id"])
            if latest and latest["zone"] == zone:
                filtered.append(item | {"zone": latest["zone"]})
        results = filtered
    common.print_output(results, output_format=output_format, title="Datasets")
