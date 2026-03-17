"""
Tessera Archiver — REST API endpointleri.

Prefix: /api/v1/archiver
"""
from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)


# ---------------------------------------------------------------------------
# Yardımcılar
# ---------------------------------------------------------------------------

def _archiver_state(request: Request):
    """app.state'ten archiver nesnelerini çıkarır."""
    state = request.app.state
    if not hasattr(state, "archiver_catalog"):
        raise HTTPException(status_code=503, detail="Archiver modülü başlatılmamış.")
    return state


def _human(b: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {u}"
        b //= 1024
    return f"{b:.1f} PB"


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def archiver_stats(request: Request):
    """Archiver genel istatistikleri."""
    state = _archiver_state(request)
    stats = state.archiver_catalog.get_stats()
    return {
        "total_repos": stats.total_repos,
        "total_versions": stats.total_versions,
        "total_size_bytes": stats.total_size_bytes,
        "total_size_human": _human(stats.total_size_bytes),
        "repos_by_provider": stats.repos_by_provider,
        "repos_by_language": stats.repos_by_language,
        "repos_by_domain": stats.repos_by_domain,
        "repos_by_risk": stats.repos_by_risk,
        "last_archived_at": stats.last_archived_at,
    }


# ---------------------------------------------------------------------------
# Repos
# ---------------------------------------------------------------------------

@router.get("/repos")
async def list_repos(
    request: Request,
    provider: str = "",
    language: str = "",
    domain: str = "",
    q: str = "",
    limit: int = 50,
    offset: int = 0,
):
    """Repo listesi (filtreli, sayfalı)."""
    state = _archiver_state(request)
    repos = state.archiver_catalog.list_repos(
        provider=provider or None,
        language=language or None,
        domain=domain or None,
        query=q or None,
        limit=limit,
        offset=offset,
    )
    total = state.archiver_catalog.count_repos()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "repos": [r.model_dump() for r in repos],
    }


@router.get("/repos/{provider}/{namespace:path}/{repo}")
async def get_repo(request: Request, provider: str, namespace: str, repo: str):
    """Tek repo detayı ve versiyonları."""
    state = _archiver_state(request)
    from tessera.archiver.models import RepoRef
    ref = RepoRef(provider=provider, namespace=namespace, repo=repo)

    rec = state.archiver_catalog.get_repo(ref.key)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Repo bulunamadı: {ref.key}")

    versions = state.archiver_catalog.list_versions(ref.key)
    scan = state.archiver_catalog.get_latest_scan(ref.key)

    return {
        "repo": rec.model_dump(),
        "versions": [v.model_dump() for v in versions],
        "latest_scan": scan.model_dump(exclude={"findings"}) if scan else None,
    }


@router.get("/repos/{provider}/{namespace:path}/{repo}/scan")
async def get_repo_scan(request: Request, provider: str, namespace: str, repo: str):
    """Reponun güncel scan raporu (bulgular dahil)."""
    state = _archiver_state(request)
    from tessera.archiver.models import RepoRef
    key = RepoRef(provider=provider, namespace=namespace, repo=repo).key
    scan = state.archiver_catalog.get_latest_scan(key)
    if not scan:
        raise HTTPException(status_code=404, detail="Scan raporu bulunamadı.")
    return scan.model_dump()


# ---------------------------------------------------------------------------
# Jobs — Archive
# ---------------------------------------------------------------------------

class ArchiveRequest(BaseModel):
    repo: str
    force: bool = False
    include_heavy: bool = False


@router.post("/jobs/archive")
async def start_archive(
    request: Request,
    payload: ArchiveRequest,
    background_tasks: BackgroundTasks,
):
    """Arşiv işi başlatır."""
    state = _archiver_state(request)
    from tessera.archiver.models import RepoRef
    from tessera.archiver.jobs import get_job_store

    try:
        ref = RepoRef.parse(payload.repo)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    job_store = get_job_store()
    job = job_store.create("archive", repo_key=ref.key, params=payload.model_dump())

    background_tasks.add_task(
        _run_archive, state, job.id, ref, payload.force, payload.include_heavy
    )
    return {"job_id": job.id, "status": job.status, "repo_key": ref.key}


def _run_archive(state, job_id: str, ref, force: bool, include_heavy: bool) -> None:
    from tessera.archiver.jobs import get_job_store
    from tessera.archiver.providers import get_provider
    from tessera.archiver.pipeline.archiver import archive_repo

    job_store = get_job_store()
    job_store.start(job_id)
    job_store.append_log(job_id, f"Arşivleniyor: {ref.key}")
    try:
        prov = get_provider(ref.provider)
        result = archive_repo(
            ref=ref,
            provider=prov,
            storage=state.archiver_storage,
            catalog=state.archiver_catalog,
            cfg=state.archiver_cfg,
            force=force,
            include_heavy=include_heavy,
        )
        job_store.finish(job_id, success=result["success"], result=result,
                         error=result.get("error") or None)
    except Exception as exc:
        job_store.finish(job_id, success=False, error=str(exc))


# ---------------------------------------------------------------------------
# Jobs — Scan
# ---------------------------------------------------------------------------

class ScanRequest(BaseModel):
    repo: str = "all"
    force: bool = False


@router.post("/jobs/scan")
async def start_scan(
    request: Request,
    payload: ScanRequest,
    background_tasks: BackgroundTasks,
):
    """Güvenlik tarama işi başlatır."""
    state = _archiver_state(request)
    from tessera.archiver.jobs import get_job_store

    job_store = get_job_store()
    repo_key = None if payload.repo == "all" else payload.repo
    job = job_store.create("scan", repo_key=repo_key, params=payload.model_dump())

    background_tasks.add_task(_run_scan, state, job.id, payload.repo, payload.force)
    return {"job_id": job.id, "status": job.status}


def _run_scan(state, job_id: str, repo: str, force: bool) -> None:
    from tessera.archiver.jobs import get_job_store
    from tessera.archiver.models import RepoRef
    from tessera.archiver.pipeline.scanner import scan_archive, save_scan_report
    from pathlib import Path

    job_store = get_job_store()
    job_store.start(job_id)
    scanned = failed = 0

    if repo == "all":
        unscanned = state.archiver_catalog.list_unscanned() if not force else [
            (r.key, state.archiver_catalog.list_versions(r.key)[0].version)
            for r in state.archiver_catalog.list_repos()
            if state.archiver_catalog.list_versions(r.key)
        ]
    else:
        try:
            ref = RepoRef.parse(repo)
        except ValueError as exc:
            job_store.finish(job_id, success=False, error=str(exc))
            return
        versions = state.archiver_catalog.list_versions(ref.key)
        unscanned = [(ref.key, versions[0].version)] if versions else []

    for repo_key, version in unscanned:
        try:
            ref = RepoRef.parse(repo_key)
            version_dir = state.archiver_storage.raw_version_dir(ref, version)
            archives = sorted(version_dir.glob("*.tar.gz"))
            if not archives:
                failed += 1
                continue
            yara_dir = Path(state.archiver_cfg.scanner.yara_rules_dir)
            report = scan_archive(archives[0], yara_rules_dir=yara_dir if yara_dir.exists() else None)
            report.repo_key = repo_key
            report.version = version
            versions_list = state.archiver_catalog.list_versions(repo_key)
            report.archive_id = versions_list[0].archive_id if versions_list else ""
            state.archiver_catalog.save_scan(report)
            save_scan_report(report, version_dir / "scan_report.json")
            job_store.append_log(job_id, f"Tarandı: {repo_key} [{version}] → {report.risk_level}")
            scanned += 1
        except Exception as exc:
            job_store.append_log(job_id, f"HATA: {repo_key}: {exc}")
            failed += 1

    job_store.finish(
        job_id,
        success=failed == 0,
        result={"scanned": scanned, "failed": failed},
        error=f"{failed} tarama başarısız" if failed else None,
    )


# ---------------------------------------------------------------------------
# Jobs — Pipeline
# ---------------------------------------------------------------------------

class PipelineRequest(BaseModel):
    repos: list[str] = []
    force: bool = False
    include_heavy: bool = False


@router.post("/jobs/pipeline")
async def start_pipeline(
    request: Request,
    payload: PipelineRequest,
    background_tasks: BackgroundTasks,
):
    """Tam pipeline: archive → scan → policy."""
    state = _archiver_state(request)
    from tessera.archiver.jobs import get_job_store

    job_store = get_job_store()
    job = job_store.create("pipeline", params=payload.model_dump())
    background_tasks.add_task(_run_pipeline, state, job.id, payload)
    return {"job_id": job.id, "status": job.status}


def _run_pipeline(state, job_id: str, payload: PipelineRequest) -> None:
    from tessera.archiver.jobs import get_job_store
    from tessera.archiver.models import RepoRef
    from tessera.archiver.providers import get_provider
    from tessera.archiver.pipeline.archiver import archive_repo
    from tessera.archiver.pipeline.scanner import scan_archive, save_scan_report
    from pathlib import Path

    job_store = get_job_store()
    job_store.start(job_id)
    archived = scanned = failed = 0

    for repo_str in payload.repos:
        try:
            ref = RepoRef.parse(repo_str)
            prov = get_provider(ref.provider)

            result = archive_repo(ref=ref, provider=prov,
                                   storage=state.archiver_storage,
                                   catalog=state.archiver_catalog,
                                   cfg=state.archiver_cfg,
                                   force=payload.force,
                                   include_heavy=payload.include_heavy)
            if result["success"] and not result["skipped"]:
                archived += 1
                job_store.append_log(job_id, f"Arşivlendi: {ref.key} → {result.get('version')}")

                version = result["version"]
                version_dir = state.archiver_storage.raw_version_dir(ref, version)
                archives = sorted(version_dir.glob("*.tar.gz"))
                if archives:
                    yara_dir = Path(state.archiver_cfg.scanner.yara_rules_dir)
                    scan = scan_archive(archives[0], yara_rules_dir=yara_dir if yara_dir.exists() else None)
                    scan.repo_key = ref.key
                    scan.version = version
                    scan.archive_id = result["archive_id"]
                    state.archiver_catalog.save_scan(scan)
                    save_scan_report(scan, version_dir / "scan_report.json")
                    scanned += 1
                    job_store.append_log(job_id, f"Tarandı: {ref.key} → {scan.risk_level}")
        except Exception as exc:
            failed += 1
            job_store.append_log(job_id, f"HATA: {repo_str}: {exc}")

    job_store.finish(
        job_id,
        success=failed == 0,
        result={"archived": archived, "scanned": scanned, "failed": failed},
        error=f"{failed} işlem başarısız" if failed else None,
    )


# ---------------------------------------------------------------------------
# Jobs — Status
# ---------------------------------------------------------------------------

@router.get("/jobs/{job_id}")
async def get_job(request: Request, job_id: str):
    """İş durumunu sorgula."""
    from tessera.archiver.jobs import get_job_store
    job = get_job_store().get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadı.")
    return job.to_dict()


@router.get("/jobs")
async def list_jobs(
    request: Request,
    job_type: str = "",
    limit: int = 20,
):
    """Son işleri listele."""
    from tessera.archiver.jobs import get_job_store
    jtype = job_type or None  # type: ignore[assignment]
    jobs = get_job_store().all_jobs(jtype)[:limit]
    return {"jobs": [j.to_dict() for j in jobs]}


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

@router.get("/reports/daily")
async def daily_report(request: Request):
    state = _archiver_state(request)
    from tessera.archiver.reporting.daily import generate_daily_report
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, generate_daily_report, state.archiver_storage)


@router.get("/reports/monthly")
async def monthly_report(request: Request):
    state = _archiver_state(request)
    from tessera.archiver.reporting.monthly import generate_monthly_report
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, generate_monthly_report, state.archiver_storage)


@router.get("/reports/anomalies")
async def anomalies_report(request: Request):
    state = _archiver_state(request)
    from tessera.archiver.reporting.anomalies import detect_anomalies
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, detect_anomalies, state.archiver_storage)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@router.get("/policy/check")
async def policy_check(request: Request, allow_missing: bool = False):
    """Güvenlik politikasını değerlendir."""
    state = _archiver_state(request)
    from tessera.archiver.pipeline.policy import evaluate_policy
    result = evaluate_policy(state.archiver_catalog, allow_missing=allow_missing)
    return {
        "passed": result.passed,
        "summary": result.summary,
        "total_repos": result.total_repos,
        "scanned_repos": result.scanned_repos,
        "missing_scans": result.missing_scans,
        "total_high": result.total_high,
        "total_medium": result.total_medium,
        "total_low": result.total_low,
        "violations": [v.__dict__ for v in result.violations[:100]],
    }


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------

@router.post("/verify")
async def start_verify(request: Request, background_tasks: BackgroundTasks, limit: int = 0):
    """Checksum doğrulama işi başlatır."""
    state = _archiver_state(request)
    from tessera.archiver.jobs import get_job_store
    job_store = get_job_store()
    job = job_store.create("verify")
    background_tasks.add_task(_run_verify, state, job.id, limit or None)
    return {"job_id": job.id, "status": job.status}


def _run_verify(state, job_id: str, limit) -> None:
    from tessera.archiver.jobs import get_job_store
    from tessera.archiver.verification.periodic import run_verification
    job_store = get_job_store()
    job_store.start(job_id)
    try:
        report = run_verification(state.archiver_storage, limit=limit)
        s = report["summary"]
        job_store.finish(
            job_id,
            success=s["fail"] == 0,
            result=s,
            error=f"{s['fail']} checksum hatası" if s["fail"] > 0 else None,
        )
    except Exception as exc:
        job_store.finish(job_id, success=False, error=str(exc))


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

@router.get("/audit")
async def get_audit(request: Request, limit: int = 50, offset: int = 0):
    """Audit log olaylarını döner."""
    state = _archiver_state(request)
    import json as _json
    audit_path = state.archiver_storage.audit_log_path
    if not audit_path.exists():
        return {"events": [], "total": 0}

    lines = [l.strip() for l in audit_path.read_text().splitlines() if l.strip()]
    total = len(lines)
    page = lines[offset: offset + limit]
    events = []
    for line in reversed(page):
        try:
            events.append(_json.loads(line))
        except Exception:
            pass
    return {"total": total, "limit": limit, "offset": offset, "events": events}


# ---------------------------------------------------------------------------
# Master Index
# ---------------------------------------------------------------------------

@router.get("/index")
async def get_index(request: Request):
    """Master index.json içeriğini döner."""
    state = _archiver_state(request)
    from tessera.archiver.metadata.index import MasterIndex
    idx = MasterIndex(state.archiver_storage)
    repos = idx.get_all()
    return {"total": len(repos), "repos": repos}
