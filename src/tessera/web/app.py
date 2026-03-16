"""FastAPI application factory for Tessera web UI."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from tessera.core.audit import AuditLogger
from tessera.core.catalog import CatalogManager
from tessera.core.config import load_config
from tessera.core.credentials import CredentialManager
from tessera.core.ingest_jobs import IngestJobStore
from tessera.core.registry import PluginRegistry


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    # Load .env into os.environ before anything else so credentials are available.
    # Check project root from TESSERA_CONFIG env var, then cwd.
    import os as _os
    _cfg_path = _os.getenv("TESSERA_CONFIG", "")
    _env_candidates = [
        Path(_cfg_path).parent / ".env" if _cfg_path else None,
        Path(".env"),
        Path(__file__).parent.parent.parent.parent / ".env",  # src/../../../.env
    ]
    for _env_file in _env_candidates:
        if _env_file and _env_file.exists():
            try:
                from dotenv import load_dotenv
                load_dotenv(_env_file, override=False)
            except ImportError:
                pass
            break

    app = FastAPI(title="Tessera", version="0.1.0")

    base_dir = Path(__file__).parent
    templates = Jinja2Templates(directory=base_dir / "templates")
    app.mount("/static", StaticFiles(directory=base_dir / "static"), name="static")

    config = load_config()
    catalog = CatalogManager(Path(config.storage.base_path) / config.storage.catalog_db)
    catalog.initialize()
    audit = AuditLogger(Path(config.storage.base_path) / config.storage.audit_db)
    audit.initialize()

    registry = PluginRegistry(config.to_dict())
    registry.discover_plugins()

    credential_manager = CredentialManager(Path(".env"))
    job_store = IngestJobStore()

    app.state.config = config
    app.state.catalog = catalog
    app.state.audit = audit
    app.state.registry = registry
    app.state.credential_manager = credential_manager
    app.state.job_store = job_store
    app.state.templates = templates

    from tessera.web.routes.api import router as api_router
    from tessera.web.routes.pages import router as pages_router

    app.include_router(pages_router)
    app.include_router(api_router, prefix="/api/v1")
    return app
