"""HTML page routes for the Tessera Archiver web UI."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/archiver")


def _get_archiver_catalog(request: Request):
    """Retrieve ArchiverCatalog from app state (lazy-initialized)."""
    return getattr(request.app.state, "archiver_catalog", None)


def _get_archiver_config(request: Request):
    """Retrieve ArchiverConfig from app state."""
    return getattr(request.app.state, "archiver_config", None)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def _build_activity_timeline(catalog) -> list[dict]:
    """Son 7 günün arşiv aktivitesini hesapla."""
    from datetime import datetime, timezone, timedelta

    days = []
    today = datetime.now(timezone.utc).date()
    versions_by_date: dict[str, int] = {}

    if catalog:
        for repo in catalog.list_repos():
            for ver in catalog.list_versions(repo.key):
                if ver.archived_at:
                    date_key = str(ver.archived_at)[:10]
                    versions_by_date[date_key] = versions_by_date.get(date_key, 0) + 1

    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        day_str = d.strftime("%d/%m")
        days.append({"label": day_str, "value": versions_by_date.get(str(d), 0)})

    return days


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def archiver_dashboard(request: Request):
    """Archiver dashboard — stats overview."""

    catalog = _get_archiver_catalog(request)
    stats = catalog.get_stats() if catalog else None

    from tessera.archiver.jobs import get_job_store
    job_store = get_job_store()
    recent_jobs = [j.to_dict() for j in job_store.all_jobs()[:8]]
    activity = _build_activity_timeline(catalog)

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/dashboard.html",
        {
            "active": "archiver",
            "stats": stats,
            "recent_jobs": recent_jobs,
            "activity": activity,
        },
    )


# ---------------------------------------------------------------------------
# Repos
# ---------------------------------------------------------------------------


@router.get("/repos", response_class=HTMLResponse)
async def archiver_repos(
    request: Request,
    provider: str = "",
    risk: str = "",
    q: str = "",
    page: int = 1,
):
    """Archived repos list with filter/search."""

    catalog = _get_archiver_catalog(request)
    repos = catalog.list_repos(limit=2000) if catalog else []

    # Client-side filtering (lightweight — catalog holds O(1k) repos at most)
    if provider:
        repos = [r for r in repos if (r.provider or "") == provider]
    if risk:
        repos = [r for r in repos if (r.risk_level or "").upper() == risk.upper()]
    if q:
        q_lower = q.lower()
        repos = [
            r for r in repos
            if q_lower in (r.repo or "").lower()
            or q_lower in (r.namespace or "").lower()
            or q_lower in (r.description or "").lower()
        ]

    all_repos = catalog.list_repos() if catalog else []
    providers = sorted({r.provider or "" for r in all_repos} - {""})
    languages = sorted({r.language or "" for r in all_repos} - {""})

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/repos.html",
        {
            "active": "archiver",
            "repos": repos,
            "providers": providers,
            "languages": languages,
            "filter_provider": provider,
            "filter_risk": risk,
            "filter_q": q,
            "total": len(repos),
        },
    )


# ---------------------------------------------------------------------------
# Repo detail
# ---------------------------------------------------------------------------


@router.get("/repos/{provider}/{namespace:path}/{repo}", response_class=HTMLResponse)
async def archiver_repo_detail(
    request: Request,
    provider: str,
    namespace: str,
    repo: str,
):
    """Single repo detail: versions, latest scan, profile info."""

    catalog = _get_archiver_catalog(request)
    if catalog is None:
        raise HTTPException(status_code=503, detail="Archiver catalog başlatılmadı.")

    repo_key = f"{provider}:{namespace}/{repo}"
    repo_data = catalog.get_repo(repo_key)
    if repo_data is None:
        raise HTTPException(status_code=404, detail=f"Repo bulunamadı: {repo_key}")

    versions = catalog.list_versions(repo_key)
    scan = catalog.get_latest_scan(repo_key)

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/repo_detail.html",
        {
            "active": "archiver",
            "repo": repo_data,
            "versions": versions,
            "scan": scan,
        },
    )


# ---------------------------------------------------------------------------
# Archive job form
# ---------------------------------------------------------------------------


@router.get("/archive", response_class=HTMLResponse)
async def archiver_archive_form(request: Request):
    """Archive submission form."""

    from tessera.archiver.jobs import get_job_store
    job_store = get_job_store()
    archive_jobs = [j.to_dict() for j in job_store.all_jobs("archive")]

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/archive_job.html",
        {
            "active": "archiver",
            "jobs": archive_jobs,
        },
    )


# ---------------------------------------------------------------------------
# Scan centre
# ---------------------------------------------------------------------------


@router.get("/scan", response_class=HTMLResponse)
async def archiver_scan(request: Request):
    """Scan centre — trigger scans and view results."""

    catalog = _get_archiver_catalog(request)
    repos = catalog.list_repos(limit=2000) if catalog else []

    from tessera.archiver.jobs import get_job_store
    job_store = get_job_store()
    scan_jobs = [j.to_dict() for j in job_store.all_jobs("scan")]

    # Latest scan with actual findings for accordion display
    latest_findings: list[dict] = []
    latest_scan_repo = ""
    latest_risk = ""

    if catalog:
        for repo in repos:
            scan = catalog.get_latest_scan(repo.key)
            if scan and scan.findings:
                # Group findings by severity
                groups: dict[str, list] = {}
                for f in scan.findings:
                    sev = f.severity or "LOW"
                    groups.setdefault(sev, []).append(f)
                order = ["HIGH", "MEDIUM", "LOW"]
                latest_findings = [
                    {
                        "severity": sev,
                        "count": len(groups[sev]),
                        "findings": [f.model_dump() for f in groups[sev][:20]],
                    }
                    for sev in order if sev in groups
                ]
                latest_scan_repo = repo.key
                latest_risk = scan.risk_level or ""
                break

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/scan_results.html",
        {
            "active": "archiver",
            "repos": repos,
            "scan_jobs": scan_jobs,
            "latest_findings": latest_findings,
            "latest_scan_repo": latest_scan_repo,
            "latest_risk": latest_risk,
        },
    )


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


@router.get("/reports", response_class=HTMLResponse)
async def archiver_reports(request: Request):
    """Reports overview — daily, monthly, anomaly tabs."""

    from tessera.archiver.jobs import get_job_store
    job_store = get_job_store()
    report_jobs = [j.to_dict() for j in job_store.all_jobs("report")]

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/reports.html",
        {
            "active": "archiver",
            "report_jobs": report_jobs,
        },
    )


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@router.get("/policy", response_class=HTMLResponse)
async def archiver_policy(request: Request):
    """Policy gate status page."""

    catalog = _get_archiver_catalog(request)
    archiver_config = _get_archiver_config(request)

    repos = catalog.list_repos(limit=2000) if catalog else []
    policy_cfg = archiver_config.policy.model_dump() if archiver_config else {}

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/policy.html",
        {
            "active": "archiver",
            "repos": repos,
            "policy": policy_cfg,
        },
    )


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------


@router.get("/audit", response_class=HTMLResponse)
async def archiver_audit(request: Request, limit: int = 50):
    """Audit log viewer."""

    from tessera.archiver.storage import ArchiverStorage
    from tessera.core.config import load_config
    import json

    cfg = load_config()
    archiver_cfg_dict = cfg.archiver or {}
    storage_root = archiver_cfg_dict.get("storage_root", "archive")

    storage = ArchiverStorage(storage_root)
    audit_path = storage.audit_log_path

    entries: list[dict] = []
    if audit_path.exists():
        with open(audit_path) as f:
            lines = f.readlines()
        for line in reversed(lines[-limit:]):
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    return request.app.state.templates.TemplateResponse(
        request,
        "archiver/audit.html",
        {
            "active": "archiver",
            "entries": entries,
            "limit": limit,
        },
    )
