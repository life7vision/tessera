"""HTML page routes for the Tessera web app."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.responses import RedirectResponse

from tessera.web.routes.api import _build_stats_payload

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the home page."""

    catalog = request.app.state.catalog
    stats = _build_stats_payload(catalog.get_stats(), catalog.search_datasets())
    return request.app.state.templates.TemplateResponse(
        request,
        "home.html",
        {"stats": stats, "active": "home"},
    )


@router.get("/datasets", response_class=HTMLResponse)
async def datasets_page(request: Request):
    """Render the datasets explorer (split-panel) page."""

    catalog = request.app.state.catalog
    all_datasets = catalog.search_datasets()
    sources = sorted({ds["source"] for ds in all_datasets})

    enriched = []
    for ds in all_datasets:
        versions = catalog.get_versions(ds["id"])
        enriched.append({**ds, "version_count": len(versions)})

    return request.app.state.templates.TemplateResponse(
        request,
        "datasets.html",
        {
            "active": "datasets",
            "datasets": enriched,
            "sources": sources,
        },
    )


@router.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = "", source: str = "", tag: str = "", zone: str = ""):
    """Render search results."""

    catalog = request.app.state.catalog
    tags = [item.strip() for item in tag.split(",") if item.strip()] if tag else None
    results = catalog.search_datasets(query=q or None, source=source or None, tags=tags)
    if zone:
        filtered = []
        for dataset in results:
            latest = catalog.get_latest_version(dataset["id"])
            if latest and latest["zone"] == zone:
                dataset = dict(dataset)
                dataset["zone"] = latest["zone"]
                filtered.append(dataset)
        results = filtered

    return request.app.state.templates.TemplateResponse(
        request,
        "search.html",
        {
            "query": q,
            "source": source,
            "tag": tag,
            "zone": zone,
            "results": results,
            "count": len(results),
            "active": "catalog",
        },
    )


@router.get("/dataset/{dataset_id}", response_class=HTMLResponse)
async def detail(request: Request, dataset_id: str):
    """Render a dataset detail page."""

    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadi.")
    versions = catalog.get_versions(dataset_id)
    latest = catalog.get_latest_version(dataset_id)
    lineage = catalog.get_lineage(latest["id"]) if latest else []
    return request.app.state.templates.TemplateResponse(
        request,
        "detail.html",
        {
            "dataset": dataset,
            "versions": versions,
            "latest": latest,
            "lineage": lineage,
            "active": "catalog",
        },
    )


@router.get("/detail", response_class=HTMLResponse)
async def detail_index():
    """Redirect legacy detail path to search."""

    return RedirectResponse(url="/search", status_code=307)


@router.get("/detail/{dataset_id}", response_class=HTMLResponse)
async def detail_legacy(dataset_id: str):
    """Redirect legacy detail path with id to the canonical route."""

    return RedirectResponse(url=f"/dataset/{dataset_id}", status_code=307)


@router.get("/ingest", response_class=HTMLResponse)
async def ingest_page(request: Request):
    """Render the ingest form page."""

    registry = request.app.state.registry
    connectors = sorted(registry._connectors.keys())
    jobs = request.app.state.job_store.all_jobs()[:10]
    return request.app.state.templates.TemplateResponse(
        request,
        "ingest.html",
        {
            "active": "ingest",
            "connectors": connectors,
            "jobs": [j.to_dict() for j in jobs],
        },
    )


@router.get("/pipeline", response_class=HTMLResponse)
async def pipeline(request: Request):
    """Render recent pipeline activity."""

    audit = request.app.state.audit
    entries = audit.get_logs(limit=20)
    return request.app.state.templates.TemplateResponse(
        request,
        "pipeline.html",
        {"entries": entries, "active": "pipeline"},
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings(request: Request):
    """Render the settings page."""

    from tessera.web.routes.api import _human_size
    from pathlib import Path

    config = request.app.state.config
    registry = request.app.state.registry
    cm = request.app.state.credential_manager

    # Plugins
    plugins = registry.list_plugins()
    enriched_plugins: dict[str, list[dict]] = {}
    for group, names in plugins.items():
        store = getattr(registry, f"_{group}", {})
        enriched_plugins[group] = [
            {
                "name": n,
                "version": getattr(store.get(n), "version", "—") if store.get(n) else "—",
            }
            for n in names
        ]

    # Storage
    base = Path(config.storage.base_path)
    zone_stats = {}
    total_zone_bytes = 0
    for zone_name, zone_dir in config.storage.zones.items():
        zone_path = base / zone_dir
        size = sum(f.stat().st_size for f in zone_path.rglob("*") if f.is_file()) if zone_path.exists() else 0
        total_zone_bytes += size
        zone_stats[zone_name] = {"size_bytes": size, "size_human": _human_size(size)}

    # Percent bars (avoid division by zero)
    for zone_name in zone_stats:
        pct = int(zone_stats[zone_name]["size_bytes"] / total_zone_bytes * 100) if total_zone_bytes else 0
        zone_stats[zone_name]["pct"] = pct

    db_stats = {}
    for attr, label in [("catalog_db", "catalog"), ("audit_db", "audit")]:
        p = base / getattr(config.storage, attr)
        size = p.stat().st_size if p.exists() else 0
        db_stats[label] = {"size_human": _human_size(size)}

    return request.app.state.templates.TemplateResponse(
        request,
        "settings.html",
        {
            "active": "settings",
            "config": config,
            "credentials": cm.all_services(),
            "plugins": enriched_plugins,
            "zone_stats": zone_stats,
            "db_stats": db_stats,
        },
    )
