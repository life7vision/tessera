"""CLI command for archive statistics."""

from __future__ import annotations

import click

from tessera.cli import common


@click.command("stats")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def stats_cmd(ctx: click.Context, output_format: str) -> None:
    """Show catalog statistics."""

    runtime = common.get_runtime(ctx)
    stats = runtime["catalog"].get_stats()
    common.print_output(stats, output_format=output_format, title="Stats")
