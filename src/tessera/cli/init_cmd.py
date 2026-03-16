"""CLI command for project initialization."""

from __future__ import annotations

import shutil
from pathlib import Path

import click
from rich.panel import Panel

from tessera.cli.common import console, ensure_directory

_PKG_ROOT = Path(__file__).resolve().parents[3]


@click.command("init")
@click.option("--path", "target_path", type=click.Path(file_okay=False, path_type=Path), default=Path("."))
@click.option("--git", "git_init", is_flag=True, default=False, help="Run git init in the project directory.")
def init_cmd(target_path: Path, git_init: bool) -> None:
    """Initialize a new archive project."""

    ensure_directory(target_path)
    ensure_directory(target_path / "config")
    ensure_directory(target_path / "data" / "raw")
    ensure_directory(target_path / "data" / "processed")
    ensure_directory(target_path / "data" / "archive")
    ensure_directory(target_path / "data" / "quarantine")

    # Copy default config
    source_config = _PKG_ROOT / "config" / "default.yaml"
    target_config = target_path / "config" / "default.yaml"
    if not target_config.exists() and source_config.exists():
        shutil.copy2(source_config, target_config)
        console.print(f"  [cyan]✓[/cyan] config/default.yaml oluşturuldu")

    # Copy .env.example
    source_env = _PKG_ROOT / ".env.example"
    target_env = target_path / ".env.example"
    if not target_env.exists() and source_env.exists():
        shutil.copy2(source_env, target_env)
        console.print(f"  [cyan]✓[/cyan] .env.example oluşturuldu")

    # Git init
    if git_init:
        import subprocess
        try:
            subprocess.run(["git", "init", str(target_path)], check=True, capture_output=True)
            console.print(f"  [cyan]✓[/cyan] git deposu başlatıldı")
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print(f"  [yellow]![/yellow] git init başarısız (git kurulu mu?)")

    console.print(f"\n[green bold]Proje başlatıldı:[/green bold] {target_path.resolve()}")
    console.print(
        Panel(
            "\n".join([
                "Sonraki adımlar:",
                f"  1. [bold]cd {target_path}[/bold]",
                "  2. [bold]cp .env.example .env[/bold]  → API anahtarlarını gir",
                "  3. [bold]tessera --config config/default.yaml ingest kaggle owner/dataset[/bold]",
            ]),
            title="Başlarken",
            border_style="blue",
        )
    )

