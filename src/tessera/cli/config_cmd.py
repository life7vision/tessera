"""CLI commands for configuration operations."""

from __future__ import annotations

import click

from tessera.cli import common


@click.group("config")
def config_cli() -> None:
    """Configuration commands."""


@config_cli.command("show")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
@click.pass_context
def config_show(ctx: click.Context, output_format: str) -> None:
    """Show effective configuration."""

    runtime = common.get_runtime(ctx)
    common.print_output(runtime["config"].to_dict(), output_format=output_format, title="Config")


@config_cli.command("validate")
@click.pass_context
def config_validate(ctx: click.Context) -> None:
    """Validate effective configuration."""

    runtime = common.get_runtime(ctx)
    common.print_output({"valid": True, "project": runtime["config"].project.name}, title="Config Validation")
