"""Forge module API routes — /api/v1/forge/*"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Request, UploadFile
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
    from tessera.web.routes.home_api import _build_preview

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
    from tessera.web.routes.home_api import _run_ingest

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


# ── Upload (browser file → catalog) ──────────────────────────────

@router.post("/upload")
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    name: str = Form(""),
    description: str = Form(""),
    tags: str = Form(""),
    source_ref: str = Form(""),
):
    """Accept a file upload, save to storage, register in catalog."""
    from tessera.core.config import load_config
    from tessera.core.catalog import CatalogManager
    from tessera.core.hashing import compute_file_checksum
    from tessera.core.exceptions import DuplicateDatasetError

    catalog = request.app.state.catalog
    config = request.app.state.config

    # Determine storage path
    upload_id = str(uuid4())
    filename = file.filename or "upload"
    ext = Path(filename).suffix.lower()
    safe_name = "".join(c if c.isalnum() or c in "-_." else "_" for c in filename)
    dest_dir = Path(config.storage.base_path) / "raw" / "uploads" / upload_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / safe_name

    # Stream to disk
    start = time.perf_counter()
    with dest_path.open("wb") as f:
        while chunk := await file.read(1024 * 256):
            f.write(chunk)
    duration = time.perf_counter() - start

    size_bytes = dest_path.stat().st_size
    checksum = compute_file_checksum(dest_path)

    # Row/col count for tabular formats
    row_count = col_count = None
    if ext in (".csv", ".tsv"):
        try:
            import pandas as pd
            df = pd.read_csv(dest_path, encoding="latin1", nrows=0)
            col_count = len(df.columns)
            row_count = sum(1 for _ in dest_path.open(encoding="latin1")) - 1
        except Exception:
            pass
    elif ext in (".parquet",):
        try:
            import pandas as pd
            df = pd.read_parquet(dest_path)
            row_count, col_count = df.shape
        except Exception:
            pass
    elif ext in (".json", ".jsonl"):
        try:
            import pandas as pd
            df = pd.read_json(dest_path, lines=(ext == ".jsonl"))
            row_count, col_count = df.shape
        except Exception:
            pass

    # Build metadata
    final_name = name.strip() or Path(filename).stem
    final_ref = source_ref.strip() or f"upload/{upload_id[:8]}"
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    format_hint = ext.lstrip(".") if ext else None

    from tessera.connectors.base import DatasetInfo
    info = DatasetInfo(
        source="upload",
        source_ref=final_ref,
        name=final_name,
        description=description.strip(),
        size_bytes=size_bytes,
        file_count=1,
        format_hint=format_hint,
        tags=tag_list,
        license=None,
        last_updated=None,
        url=None,
        extra_metadata={"original_filename": filename, "upload_id": upload_id},
    )

    try:
        dataset_id = catalog.register_dataset(info)
    except DuplicateDatasetError:
        raise HTTPException(status_code=409, detail="Bu source_ref zaten kayıtlı.")

    catalog.register_version(dataset_id, {
        "version": "1.0.0",
        "checksum_sha256": checksum,
        "file_size_bytes": size_bytes,
        "file_count": 1,
        "raw_path": str(dest_path),
        "processed_path": str(dest_path),
        "zone": "raw",
        "format": format_hint,
        "compression": None,
        "row_count": row_count,
        "column_count": col_count,
        "metadata_json": json.dumps({"upload_id": upload_id}),
    })

    return {
        "ok": True,
        "dataset_id": dataset_id,
        "name": final_name,
        "source_ref": final_ref,
        "size_bytes": size_bytes,
        "duration_seconds": round(duration, 2),
    }


# ── Register (server path → catalog, no upload) ───────────────────

class RegisterPayload(BaseModel):
    name: str
    path: str
    description: str = ""
    tags: list[str] = []
    source_ref: str = ""
    format: str = ""


@router.post("/register")
async def register_dataset(request: Request, payload: RegisterPayload):
    """Register a file that already exists on the server into the catalog."""
    from tessera.core.hashing import compute_file_checksum
    from tessera.core.exceptions import DuplicateDatasetError
    from tessera.connectors.base import DatasetInfo

    catalog = request.app.state.catalog
    file_path = Path(payload.path)

    if not file_path.exists():
        raise HTTPException(status_code=422, detail=f"Dosya bulunamadı: {payload.path}")

    size_bytes = file_path.stat().st_size
    checksum = compute_file_checksum(file_path)
    ext = file_path.suffix.lower().lstrip(".")
    fmt = payload.format or ext or None
    source_ref = payload.source_ref or f"local/{file_path.name}"

    # Row/col count
    row_count = col_count = None
    if ext in ("csv", "tsv"):
        try:
            import pandas as pd
            df = pd.read_csv(file_path, encoding="latin1", nrows=0)
            col_count = len(df.columns)
            row_count = sum(1 for _ in file_path.open(encoding="latin1")) - 1
        except Exception:
            pass

    info = DatasetInfo(
        source="upload",
        source_ref=source_ref,
        name=payload.name,
        description=payload.description,
        size_bytes=size_bytes,
        file_count=1,
        format_hint=fmt,
        tags=payload.tags,
        license=None,
        last_updated=None,
        url=None,
        extra_metadata={"server_path": str(file_path)},
    )

    try:
        dataset_id = catalog.register_dataset(info)
    except DuplicateDatasetError:
        raise HTTPException(status_code=409, detail="Bu source_ref zaten kayıtlı.")

    catalog.register_version(dataset_id, {
        "version": "1.0.0",
        "checksum_sha256": checksum,
        "file_size_bytes": size_bytes,
        "file_count": 1,
        "raw_path": str(file_path),
        "processed_path": str(file_path),
        "zone": "raw",
        "format": fmt,
        "compression": None,
        "row_count": row_count,
        "column_count": col_count,
        "metadata_json": json.dumps({"server_path": str(file_path)}),
    })

    return {"ok": True, "dataset_id": dataset_id, "name": payload.name, "source_ref": source_ref}


# ── Helpers ───────────────────────────────────────────────────────

def _human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    return f"{size / (1024 * 1024 * 1024):.1f} GB"
