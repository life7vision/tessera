"""CLI command for running the web frontend."""

from __future__ import annotations

import click


@click.command("web")
@click.option("--host", default="127.0.0.1")
@click.option("--port", default=8000, type=int)
@click.option("--reload", is_flag=True, help="Dev mode auto reload.")
def web_cmd(host: str, port: int, reload: bool) -> None:
    """Start the Tessera web interface."""

    import uvicorn

    uvicorn.run("tessera.web.app:create_app", host=host, port=port, reload=reload, factory=True)

