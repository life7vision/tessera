"""Shared helpers for CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from tessera.cli.main import create_runtime

console = Console()


def get_runtime(ctx: click.Context) -> dict[str, Any]:
    """Return cached CLI runtime."""

    ctx.obj = ctx.obj or {}
    if "runtime" not in ctx.obj:
        ctx.obj["runtime"] = create_runtime(ctx.obj.get("config_path"))
    return ctx.obj["runtime"]


def print_table(title: str, columns: list[str], rows: list[list[Any]]) -> None:
    """Render a rich table."""

    table = Table(title=title)
    for column in columns:
        table.add_column(column)
    for row in rows:
        table.add_row(*[str(value) if value is not None else "" for value in row])
    console.print(table)


def print_output(data: Any, output_format: str = "table", title: str = "Result") -> None:
    """Render data as table or JSON."""

    if output_format == "json":
        console.print_json(json.dumps(data, default=str))
        return

    if isinstance(data, list):
        if not data:
            console.print("[yellow]Kayit bulunamadi.[/yellow]")
            return
        columns = list(data[0].keys())
        rows = [[item.get(column) for column in columns] for item in data]
        print_table(title, columns, rows)
        return

    if isinstance(data, dict):
        print_table(title, ["Alan", "Deger"], [[key, value] for key, value in data.items()])
        return

    console.print(str(data))


def ensure_directory(path: Path) -> None:
    """Create a directory if needed."""

    path.mkdir(parents=True, exist_ok=True)

