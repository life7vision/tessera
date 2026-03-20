"""Forge module API routes — /api/v1/forge/*"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()


# ── Stats ─────────────────────────────────────────────────────────

@router.get("/stats")
async def forge_stats(request: Request):
    catalog = request.app.state.catalog
    return _build_forge_stats(catalog)


def _build_forge_stats(catalog) -> dict:
    raw = catalog.get_stats()
    datasets = catalog.search_datasets()
    sources = sorted({ds["source"] for ds in datasets})
    total_size = int(raw.get("total_file_size_bytes", 0))

    # Per-source breakdown
    source_counts: dict[str, int] = {}
    for ds in datasets:
        source_counts[ds["source"]] = source_counts.get(ds["source"], 0) + 1

    return {
        "total_datasets": int(raw.get("dataset_count", 0)),
        "total_versions": int(raw.get("version_count", 0)),
        "archived_datasets": int(raw.get("archived_dataset_count", 0)),
        "total_file_size_bytes": total_size,
        "total_size_human": _human_size(total_size),
        "sources": sources,
        "source_counts": source_counts,
    }


# ── Datasets ──────────────────────────────────────────────────────

@router.get("/datasets")
async def list_datasets(
    request: Request,
    q: str = "",
    source: str = "",
    tag: str = "",
    limit: int = 200,
):
    catalog = request.app.state.catalog
    tags = [t.strip() for t in tag.split(",") if t.strip()] if tag else None
    results = catalog.search_datasets(query=q or None, source=source or None, tags=tags)
    return {"count": len(results), "datasets": results[:limit]}


@router.get("/datasets/{dataset_id}")
async def get_dataset(request: Request, dataset_id: str):
    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadi.")
    versions = catalog.get_versions(dataset_id)
    return {"dataset": dataset, "versions": versions}


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(request: Request, dataset_id: str):
    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadi.")
    catalog.delete_dataset(dataset_id)
    return {"ok": True}


@router.get("/datasets/{dataset_id}/preview")
async def preview_dataset(request: Request, dataset_id: str, version_id: str = ""):
    import asyncio
    from tessera.web.routes.api import _build_preview

    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadı.")
    version = (
        catalog.get_version(version_id) if version_id
        else catalog.get_latest_version(dataset_id)
    )
    if not version:
        raise HTTPException(status_code=404, detail="Versiyon bulunamadı.")

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _build_preview, dataset_id, version)


@router.get("/datasets/{dataset_id}/lineage")
async def get_lineage(request: Request, dataset_id: str):
    catalog = request.app.state.catalog
    latest = catalog.get_latest_version(dataset_id)
    if not latest:
        raise HTTPException(status_code=404, detail="Versiyon bulunamadi.")
    lineage = catalog.get_lineage(latest["id"])
    return {"lineage": lineage}


@router.post("/datasets/{dataset_id}/refresh-metadata")
async def refresh_metadata(request: Request, dataset_id: str):
    catalog = request.app.state.catalog
    registry = request.app.state.registry

    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadı.")

    try:
        connector = registry.get_connector(dataset["source"])
        info = connector.fetch_metadata(dataset["source_ref"])
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    import json as _json
    catalog.update_dataset(
        dataset_id,
        name=info.name,
        description=info.description,
        tags=_json.dumps(info.tags) if isinstance(info.tags, list) else (info.tags or "[]"),
        license=info.license or "",
    )
    return {"ok": True, "dataset": catalog.get_dataset(dataset_id)}


# ── Ingest jobs ───────────────────────────────────────────────────

class IngestPayload(BaseModel):
    source: str
    source_ref: str
    tags: list[str] = []
    force: bool = False


@router.post("/ingest")
async def start_ingest(request: Request, payload: IngestPayload, background_tasks: BackgroundTasks):
    from tessera.web.routes.api import _run_ingest

    job_store = request.app.state.job_store
    job = job_store.create_job(payload.source, payload.source_ref, payload.tags, payload.force)
    background_tasks.add_task(
        _run_ingest,
        request.app.state,
        job.id,
        payload.source,
        payload.source_ref,
        payload.tags,
        payload.force,
    )
    return {"job_id": job.id, "status": job.status}


@router.get("/ingest")
async def list_ingest_jobs(request: Request, limit: int = 20):
    jobs = request.app.state.job_store.all_jobs()[:limit]
    return {"jobs": [j.to_dict() for j in jobs]}


@router.get("/ingest/{job_id}")
async def get_ingest_job(request: Request, job_id: str):
    job = request.app.state.job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadi.")
    return job.to_dict()


# ── Metadata preview (before ingest) ─────────────────────────────

@router.get("/preview")
async def preview_metadata(request: Request, source: str, ref: str):
    registry = request.app.state.registry
    try:
        connector = registry.get_connector(source)
        info = connector.fetch_metadata(ref)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {
        "name": info.name,
        "description": info.description,
        "tags": info.tags,
        "license": info.license,
        "size_bytes": info.size_bytes,
        "url": info.url,
        "extra": info.extra_metadata,
    }


# ── Helpers ───────────────────────────────────────────────────────

def _human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    return f"{size / (1024 * 1024 * 1024):.1f} GB"
