"""Forge module page routes — /forge/*"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.responses import RedirectResponse

router = APIRouter()


@router.get("/forge", response_class=HTMLResponse)
async def forge_dashboard(request: Request):
    catalog = request.app.state.catalog
    job_store = request.app.state.job_store

    from tessera.web.routes.forge_api import _build_forge_stats
    stats = _build_forge_stats(catalog)
    recent_jobs = [j.to_dict() for j in job_store.all_jobs()[:8]]

    return request.app.state.templates.TemplateResponse(
        request,
        "forge/dashboard.html",
        {"active": "forge", "stats": stats, "recent_jobs": recent_jobs},
    )


@router.get("/forge/datasets", response_class=HTMLResponse)
async def forge_datasets(request: Request):
    catalog = request.app.state.catalog
    all_datasets = catalog.search_datasets()
    sources = sorted({ds["source"] for ds in all_datasets})

    enriched = []
    for ds in all_datasets:
        versions = catalog.get_versions(ds["id"])
        enriched.append({**ds, "version_count": len(versions)})

    return request.app.state.templates.TemplateResponse(
        request,
        "forge/datasets.html",
        {"active": "forge", "datasets": enriched, "sources": sources},
    )


@router.get("/forge/ingest", response_class=HTMLResponse)
async def forge_ingest(request: Request):
    registry = request.app.state.registry
    connectors = sorted(registry._connectors.keys())
    jobs = request.app.state.job_store.all_jobs()[:10]

    return request.app.state.templates.TemplateResponse(
        request,
        "forge/ingest.html",
        {"active": "forge", "connectors": connectors, "jobs": [j.to_dict() for j in jobs]},
    )


@router.get("/forge/pipeline", response_class=HTMLResponse)
async def forge_pipeline(request: Request):
    audit = request.app.state.audit
    job_store = request.app.state.job_store
    entries = audit.get_logs(limit=30)
    jobs = [j.to_dict() for j in job_store.all_jobs()[:20]]

    return request.app.state.templates.TemplateResponse(
        request,
        "forge/pipeline.html",
        {"active": "forge", "entries": entries, "jobs": jobs},
    )


# ── Legacy redirects ───────────────────────────────────────────────

@router.get("/datasets", response_class=HTMLResponse)
async def legacy_datasets():
    return RedirectResponse(url="/forge/datasets", status_code=301)


@router.get("/ingest", response_class=HTMLResponse)
async def legacy_ingest():
    return RedirectResponse(url="/forge/ingest", status_code=301)


@router.get("/pipeline", response_class=HTMLResponse)
async def legacy_pipeline():
    return RedirectResponse(url="/forge/pipeline", status_code=301)
