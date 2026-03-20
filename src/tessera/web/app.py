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

    from tessera.web.routes.home_api import router as api_router
    from tessera.web.routes.home_pages import router as pages_router
    from tessera.web.routes.archiver_pages import router as archiver_pages_router
    from tessera.web.routes.archiver_api import router as archiver_api_router
    from tessera.web.routes.forge_pages import router as forge_pages_router
    from tessera.web.routes.forge_api import router as forge_api_router

    # Archiver state — lazy init so startup doesn't fail if archiver is unconfigured
    try:
        from tessera.archiver.config import get_archiver_config
        from tessera.archiver.catalog import ArchiverCatalog
        from tessera.archiver.storage import ArchiverStorage
        _archiver_cfg = get_archiver_config()
        _archiver_catalog = ArchiverCatalog(_archiver_cfg.database)
        _archiver_storage = ArchiverStorage(_archiver_cfg.storage_root)
        app.state.archiver_config  = _archiver_cfg   # pages routes use this name
        app.state.archiver_cfg     = _archiver_cfg   # API routes use this name
        app.state.archiver_catalog = _archiver_catalog
        app.state.archiver_storage = _archiver_storage
    except Exception:
        app.state.archiver_config  = None
        app.state.archiver_cfg     = None
        app.state.archiver_catalog = None
        app.state.archiver_storage = None

    # Startup'ta policy'i arka planda otomatik değerlendir
    if app.state.archiver_catalog is not None:
        try:
            from tessera.archiver.pipeline.policy_cache import refresh_async
            refresh_async(app.state.archiver_catalog)
        except Exception:
            pass

    app.include_router(pages_router)
    app.include_router(archiver_pages_router)
    app.include_router(forge_pages_router)
    app.include_router(api_router, prefix="/api/v1")
    app.include_router(archiver_api_router, prefix="/api/v1/archiver")
    app.include_router(forge_api_router, prefix="/api/v1/forge")
    return app
