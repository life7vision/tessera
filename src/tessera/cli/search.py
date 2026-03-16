"""CLI command for catalog search."""

from __future__ import annotations

import click

from tessera.cli import common


@click.command("search")
@click.argument("query", required=False)
@click.option("--source", default=None)
@click.option("--tags", default="")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def search_cmd(
    ctx: click.Context,
    query: str | None,
    source: str | None,
    tags: str,
    output_format: str,
) -> None:
    """Search datasets in the catalog."""

    runtime = common.get_runtime(ctx)
    tag_list = [item.strip() for item in tags.split(",") if item.strip()]
    results = runtime["catalog"].search_datasets(query=query, source=source, tags=tag_list or None)
    common.print_output(results, output_format=output_format, title="Search Results")
