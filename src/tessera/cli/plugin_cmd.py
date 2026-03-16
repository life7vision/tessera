"""CLI commands for plugin inspection."""

from __future__ import annotations

import click

from tessera.cli import common


@click.group("plugin")
def plugin_cli() -> None:
    """Plugin commands."""


@plugin_cli.command("list")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def plugin_list(ctx: click.Context, output_format: str) -> None:
    """List discovered plugins."""

    runtime = common.get_runtime(ctx)
    common.print_output(runtime["registry"].list_plugins(), output_format=output_format, title="Plugins")


@plugin_cli.command("info")
@click.argument("plugin_name")
@click.pass_context
def plugin_info(ctx: click.Context, plugin_name: str) -> None:
    """Show plugin information."""

    runtime = common.get_runtime(ctx)
    all_plugins = runtime["registry"].list_plugins()
    for group, names in all_plugins.items():
        if plugin_name in names:
            common.print_output({"group": group, "name": plugin_name}, title="Plugin Info")
            return
    raise click.ClickException(f"Plugin bulunamadi: {plugin_name}")
