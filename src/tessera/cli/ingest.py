"""CLI command for dataset ingestion."""

from __future__ import annotations

import click

from tessera.cli import common


@click.command("ingest")
@click.argument("source")
@click.argument("source_ref")
@click.option("--tags", default="", help="Virgulle ayrilmis etiket listesi.")
@click.option("--force", is_flag=True, default=False, help="Mevcut checksum olsa bile yeniden isler.")
@click.pass_context
def ingest_cmd(ctx: click.Context, source: str, source_ref: str, tags: str, force: bool) -> None:
    """Download and archive a dataset."""

    runtime = common.get_runtime(ctx)
    tag_list = [item.strip() for item in tags.split(",") if item.strip()]
    result = runtime["pipeline"].ingest(source, source_ref, tags=tag_list, force=force)
    common.print_output(
        {
            "success": result.success,
            "dataset_id": result.dataset_id,
            "version": result.version,
            "stages": len(result.stages),
            "error_message": result.error_message,
        },
        title="Ingest Result",
    )
