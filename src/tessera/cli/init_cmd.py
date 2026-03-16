"""CLI command for project initialization."""

from __future__ import annotations

import shutil
from pathlib import Path

import click

from tessera.cli.common import console, ensure_directory


@click.command("init")
@click.option("--path", "target_path", type=click.Path(file_okay=False, path_type=Path), default=Path("."))
def init_cmd(target_path: Path) -> None:
    """Initialize a new archive project."""

    ensure_directory(target_path)
    ensure_directory(target_path / "config")
    ensure_directory(target_path / "data" / "raw")
    ensure_directory(target_path / "data" / "processed")
    ensure_directory(target_path / "data" / "archive")
    ensure_directory(target_path / "data" / "quarantine")

    source_config = Path(__file__).resolve().parents[3] / "config" / "default.yaml"
    target_config = target_path / "config" / "default.yaml"
    if not target_config.exists():
        shutil.copy2(source_config, target_config)

    console.print(f"[green]Proje baslatildi:[/green] {target_path}")

