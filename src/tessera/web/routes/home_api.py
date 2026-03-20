"""JSON API endpoints for the Tessera web app."""

from __future__ import annotations

import threading
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()


# ── Metadata preview endpoint ─────────────────────────────────────

@router.get("/datasets/preview")
async def preview_metadata(request: Request, source: str, ref: str):
    """Fetch remote metadata for a dataset ref without ingesting it."""

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


# ── Dataset endpoints ──────────────────────────────────────────────

@router.get("/datasets")
async def list_datasets(
    request: Request,
    q: str = "",
    source: str = "",
    tag: str = "",
    limit: int = 50,
):
    """Return filtered datasets as JSON."""

    catalog = request.app.state.catalog
    tags = [item.strip() for item in tag.split(",") if item.strip()] if tag else None
    results = catalog.search_datasets(query=q or None, source=source or None, tags=tags)
    return {"count": len(results), "datasets": results[:limit]}


@router.get("/datasets/{dataset_id}")
async def get_dataset(request: Request, dataset_id: str):
    """Return a dataset and its versions."""

    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset bulunamadi.")
    versions = catalog.get_versions(dataset_id)
    return {"dataset": dataset, "versions": versions}


@router.get("/datasets/{dataset_id}/preview")
async def preview_dataset(request: Request, dataset_id: str, version_id: str = ""):
    """Return a sample of rows from the dataset's raw files."""

    import asyncio
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
    result = await loop.run_in_executor(None, _build_preview, dataset_id, version)
    return result


def _find_readable_file(raw_path: str) -> tuple[Path, str] | None:
    """Return (path, format) for the best readable file in raw_path."""
    if not raw_path:
        return None
    p = Path(raw_path)
    if p.is_file():
        fmt = p.suffix.lstrip(".")
        if fmt in {"parquet", "csv", "tsv", "json", "jsonl"}:
            return p, fmt
        return None
    if not p.is_dir():
        return None
    _SKIP_STEMS = {"license", "readme", "changelog", "notice", "authors", "contributing"}

    def _is_data_file(fp: Path) -> bool:
        stem = fp.stem.lower()
        return (
            not stem.startswith(".")
            and not any(stem == s or stem.startswith(s + "_") or stem.startswith(s + "-") for s in _SKIP_STEMS)
        )

    for pattern, fmt in [
        ("*.parquet", "parquet"),
        ("*.csv", "csv"),
        ("*.tsv", "tsv"),
        ("*.jsonl", "jsonl"),
        ("*.json", "json"),
    ]:
        found = [f for f in sorted(p.rglob(pattern)) if _is_data_file(f)]
        if found:
            return found[0], fmt
    return None


def _build_preview(dataset_id: str, version: dict, max_rows: int = 100, max_cols: int = 30) -> dict:
    base = {
        "dataset_id": dataset_id,
        "version_id": version["id"],
        "version": version.get("version"),
    }
    try:
        import pandas as pd
    except ImportError:
        return {**base, "error": "pandas_missing"}

    found = _find_readable_file(version.get("raw_path", ""))
    if not found:
        return {**base, "error": "no_readable_file"}

    file_path, fmt = found
    try:
        if fmt == "parquet":
            df = pd.read_parquet(file_path)
        elif fmt in {"csv", "tsv"}:
            sep = "\t" if fmt == "tsv" else ","
            df = pd.read_csv(file_path, nrows=max_rows, sep=sep, low_memory=False)
        elif fmt in {"jsonl", "json"}:
            df = pd.read_json(file_path, lines=(fmt == "jsonl"), nrows=max_rows if fmt == "jsonl" else None)
        else:
            return {**base, "error": "unsupported_format"}
    except Exception as exc:
        return {**base, "error": "read_failed", "error_detail": str(exc)}

    total_rows = len(df)
    total_cols = len(df.columns)
    truncated_cols = total_cols > max_cols

    df = df.head(max_rows)
    if truncated_cols:
        df = df.iloc[:, :max_cols]

    # Sanitize: inf → None, NaN → None
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notna(df), None)

    # Truncate long string cells
    def _trunc(v):
        if isinstance(v, str) and len(v) > 120:
            return v[:120] + "…"
        return v

    rows = [
        {col: _trunc(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]

    return {
        **base,
        "source_file": file_path.name,
        "format": fmt,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "preview_rows": len(rows),
        "truncated_columns": truncated_cols,
        "columns": df.columns.tolist(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "rows": rows,
        "error": None,
    }


@router.get("/datasets/{dataset_id}/lineage")
async def get_lineage(request: Request, dataset_id: str):
    """Return lineage for the latest dataset version."""

    catalog = request.app.state.catalog
    latest = catalog.get_latest_version(dataset_id)
    if not latest:
        raise HTTPException(status_code=404, detail="Versiyon bulunamadi.")
    lineage = catalog.get_lineage(latest["id"])
    return {"lineage": lineage}


# ── Stats ──────────────────────────────────────────────────────────

@router.get("/stats")
async def get_stats(request: Request):
    """Return high-level archive statistics."""

    catalog = request.app.state.catalog
    return _build_stats_payload(catalog.get_stats(), catalog.search_datasets())


# ── Config ─────────────────────────────────────────────────────────

@router.get("/config")
async def get_config(request: Request):
    """Return current application configuration."""

    config = request.app.state.config
    return config.to_dict()


# ── Plugins ────────────────────────────────────────────────────────

@router.get("/plugins")
async def get_plugins(request: Request):
    """Return discovered plugins grouped by type."""

    registry = request.app.state.registry
    raw = registry.list_plugins()

    # Enrich with version info
    enriched: dict[str, list[dict]] = {}
    for group, names in raw.items():
        enriched[group] = []
        store = getattr(registry, f"_{group}", {})
        for name in names:
            cls = store.get(name)
            enriched[group].append({
                "name": name,
                "version": getattr(cls, "version", "—") if cls else "—",
                "module": cls.__module__ if cls else "—",
            })
    return enriched


# ── Storage ────────────────────────────────────────────────────────

@router.get("/storage")
async def get_storage(request: Request):
    """Return zone-level disk usage."""

    config = request.app.state.config
    base = Path(config.storage.base_path)
    zones = config.storage.zones

    def dir_size(path: Path) -> int:
        if not path.exists():
            return 0
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    result = {}
    for zone_name, zone_dir in zones.items():
        zone_path = base / zone_dir
        size = dir_size(zone_path)
        result[zone_name] = {
            "path": str(zone_path),
            "size_bytes": size,
            "size_human": _human_size(size),
            "exists": zone_path.exists(),
        }

    # DB files
    dbs = {}
    for db_attr, label in [("catalog_db", "catalog"), ("audit_db", "audit")]:
        db_path = base / getattr(config.storage, db_attr)
        size = db_path.stat().st_size if db_path.exists() else 0
        dbs[label] = {"path": str(db_path), "size_bytes": size, "size_human": _human_size(size)}

    return {"zones": result, "databases": dbs}


# ── Credentials ────────────────────────────────────────────────────

@router.get("/credentials")
async def get_credentials(request: Request):
    """Return masked credential status for all services."""

    cm = request.app.state.credential_manager
    return {"services": cm.all_services()}


class CredentialPayload(BaseModel):
    value: str


@router.post("/credentials/{service}/{env_var}")
async def save_credential(request: Request, service: str, env_var: str, body: CredentialPayload):
    """Save a credential to the .env file."""

    from tessera.core.credentials import SERVICE_KEYS
    if service not in SERVICE_KEYS or env_var not in SERVICE_KEYS.get(service, []):
        raise HTTPException(status_code=400, detail="Geçersiz servis veya env_var.")

    if not body.value.strip():
        raise HTTPException(status_code=400, detail="Değer boş olamaz.")

    cm = request.app.state.credential_manager
    cm.set_key(env_var, body.value.strip())
    raw = cm.get_raw(env_var)
    from tessera.core.credentials import _mask
    return {"ok": True, "masked": _mask(raw) if raw else None}


@router.delete("/credentials/{service}/{env_var}")
async def delete_credential(request: Request, service: str, env_var: str):
    """Remove a credential from the .env file."""

    from tessera.core.credentials import SERVICE_KEYS
    if service not in SERVICE_KEYS or env_var not in SERVICE_KEYS.get(service, []):
        raise HTTPException(status_code=400, detail="Geçersiz servis veya env_var.")

    cm = request.app.state.credential_manager
    cm.delete_key(env_var)
    return {"ok": True}


@router.post("/credentials/{service}/test")
async def test_credential(request: Request, service: str):
    """Test connector credentials by calling validate_credentials()."""

    try:
        registry = request.app.state.registry
        connector = registry.get_connector(service)
        ok = connector.validate_credentials()
        return {"ok": bool(ok), "message": "Bağlantı başarılı." if ok else "Kimlik doğrulama başarısız."}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


# ── Metadata refresh ───────────────────────────────────────────

@router.post("/datasets/{dataset_id}/refresh-metadata")
async def refresh_metadata(request: Request, dataset_id: str):
    """Re-fetch metadata from source and update catalog entry."""

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

    updated = catalog.get_dataset(dataset_id)
    return {"ok": True, "dataset": updated}


# ── Ingest jobs ────────────────────────────────────────────────────

class IngestPayload(BaseModel):
    source: str
    source_ref: str
    tags: list[str] = []
    force: bool = False


def _run_ingest(app_state, job_id: str, source: str, source_ref: str, tags: list[str], force: bool) -> None:
    """Execute pipeline in a background thread and stream stage updates into job_store."""
    import concurrent.futures
    from tessera.core.pipeline import Pipeline

    job_store = app_state.job_store
    job_store.update_status(job_id, "running")

    def _do_ingest():
        config = app_state.config
        registry = app_state.registry
        catalog = app_state.catalog
        audit = app_state.audit

        # Quick credential check before kicking off the full pipeline
        try:
            connector = registry.get_connector(source)
            if not connector.validate_credentials():
                raise RuntimeError(
                    f"{source} için API anahtarı eksik veya geçersiz. "
                    "Lütfen Ayarlar sayfasından credential ekle."
                )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"{source} credential doğrulaması başarısız: {exc}"
            ) from exc

        pipeline = Pipeline(config, registry, catalog, audit)
        return pipeline.ingest(source, source_ref, tags=tags or None, force=force)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_do_ingest)
            result = future.result(timeout=600)  # 10 dakika max

        for stage in result.stages:
            job_store.append_stage(job_id, {
                "stage": stage.stage,
                "plugin": stage.plugin_name,
                "status": stage.status,
                "duration_ms": stage.duration_ms,
            })
        job_store.finish_job(
            job_id,
            success=result.success,
            dataset_id=result.dataset_id,
            version=result.version,
            error_message=result.error_message,
        )
    except concurrent.futures.TimeoutError:
        job_store.finish_job(job_id, success=False, error_message="Zaman aşımı (10 dk).")
    except BaseException as exc:
        job_store.append_stage(job_id, {
            "stage": "pipeline",
            "plugin": "system",
            "status": "failed",
            "duration_ms": 0,
        })
        job_store.finish_job(job_id, success=False, error_message=str(exc))


@router.post("/ingest")
async def start_ingest(request: Request, payload: IngestPayload, background_tasks: BackgroundTasks):
    """Start a background ingest job and return its ID immediately."""

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
    """Return recent ingest jobs."""

    job_store = request.app.state.job_store
    jobs = job_store.all_jobs()[:limit]
    return {"jobs": [j.to_dict() for j in jobs]}


@router.get("/ingest/{job_id}")
async def get_ingest_job(request: Request, job_id: str):
    """Return status of a specific ingest job."""

    job = request.app.state.job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadi.")
    return job.to_dict()


# ── Helpers ────────────────────────────────────────────────────────

def _build_stats_payload(raw_stats: dict, datasets: list[dict]) -> dict:
    sources = sorted({dataset["source"] for dataset in datasets})
    total_size = int(raw_stats.get("total_file_size_bytes", 0))
    return {
        "total_datasets": int(raw_stats.get("dataset_count", 0)),
        "total_versions": int(raw_stats.get("version_count", 0)),
        "archived_datasets": int(raw_stats.get("archived_dataset_count", 0)),
        "total_file_size_bytes": total_size,
        "total_size_human": _human_size(total_size),
        "sources": sources,
    }


def _human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    return f"{size / (1024 * 1024 * 1024):.1f} GB"
