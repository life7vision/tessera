"""JSON API endpoints for the Tessera web app."""

from __future__ import annotations

import threading
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()


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
