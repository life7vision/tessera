"""Command-line interface entrypoint."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console

from tessera import __version__
from tessera.core.audit import AuditLogger
from tessera.core.catalog import CatalogManager
from tessera.core.config import load_config
from tessera.core.pipeline import Pipeline
from tessera.core.registry import PluginRegistry

console = Console()


def create_runtime(config_path: str | None = None) -> dict:
    """Create shared runtime objects for CLI commands."""

    config = load_config(config_path, force_reload=True)
    registry = PluginRegistry(config.to_dict())
    registry.discover_plugins()
    storage = config.storage
    base_path = Path(storage.base_path)
    catalog = CatalogManager(base_path / storage.catalog_db)
    audit = AuditLogger(base_path / storage.audit_db)
    pipeline = Pipeline(config, registry, catalog, audit)
    return {
        "config": config,
        "registry": registry,
        "catalog": catalog,
        "audit": audit,
        "pipeline": pipeline,
    }


@click.group(help="Kurumsal Veri Arsivleme Sistemi")
@click.version_option(version=__version__, prog_name="tessera")
@click.option("--config", "config_path", type=click.Path(dir_okay=False, path_type=Path))
@click.pass_context
def cli(ctx: click.Context, config_path: Path | None) -> None:
    """Tessera command group."""

    ctx.ensure_object(dict)
    ctx.obj["config_path"] = str(config_path) if config_path else None


from tessera.cli.config_cmd import config_cli  # noqa: E402
from tessera.cli.export import export_cmd  # noqa: E402
from tessera.cli.ingest import ingest_cmd  # noqa: E402
from tessera.cli.inspect_cmd import inspect_cmd  # noqa: E402
from tessera.cli.list_cmd import list_cmd  # noqa: E402
from tessera.cli.plugin_cmd import plugin_cli  # noqa: E402
from tessera.cli.search import search_cmd  # noqa: E402
from tessera.cli.stats import stats_cmd  # noqa: E402
from tessera.cli.init_cmd import init_cmd  # noqa: E402
from tessera.cli.web_cmd import web_cmd  # noqa: E402
from tessera.archiver.cli.commands import archiver_cli  # noqa: E402

cli.add_command(init_cmd)
cli.add_command(ingest_cmd)
cli.add_command(search_cmd)
cli.add_command(inspect_cmd)
cli.add_command(list_cmd)
cli.add_command(export_cmd)
cli.add_command(config_cli)
cli.add_command(plugin_cli)
cli.add_command(stats_cmd)
cli.add_command(web_cmd)
cli.add_command(archiver_cli)
