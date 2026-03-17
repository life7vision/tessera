"""
Tessera Archiver — Arşivleme pipeline motoru.

github-archiver/archiver.py'den taşındı ve Tessera mimarisine uyarlandı.
Pipeline: clone → bundle → tar.gz → checksum → metadata → catalog → snapshot
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..catalog import ArchiverCatalog
from ..config import ArchiverConfig, get_archiver_config
from ..models import RepoRecord, RepoRef, VersionRecord
from ..providers.base import AbstractProvider
from ..storage import ArchiverStorage

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------

_SEPARATOR = "=" * 60

_DEFAULT_EXCLUDE_GLOBS = [
    "**/node_modules/**", "**/.pnpm-store/**", "**/.npm/**", "**/.yarn/**",
    "**/.venv/**", "**/venv/**", "**/__pycache__/**", "**/.mypy_cache/**",
    "**/.pytest_cache/**", "**/.tox/**", "**/.ruff_cache/**",
    "**/dist/**", "**/build/**", "**/out/**", "**/target/**",
    "**/.next/**", "**/.nuxt/**", "**/coverage/**", "**/.nyc_output/**",
    "**/.cache/**", "**/.gradle/**", "**/.terraform/**",
    "**/.idea/**", "**/.vscode/**",
]

_PROG_RE = re.compile(
    r"(Receiving objects|Resolving deltas|Counting objects|Compressing objects)"
    r":\s+(\d+)%\s+\((\d+)/(\d+)\)"
    r"(?:,\s*([\d.]+ \w+))?(?:\s+\|\s*([\d.]+\s*\w+/s))?"
)


# ---------------------------------------------------------------------------
# Yardımcı fonksiyonlar
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _is_up_to_date(last_archived_at: str | None, pushed_at: str | None) -> bool:
    """Repo son push'tan sonra arşivlendiyse True döner."""
    if not last_archived_at or not pushed_at:
        return False
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    try:
        archived = datetime.strptime(last_archived_at, fmt).replace(tzinfo=timezone.utc)
        pushed = datetime.strptime(pushed_at, fmt).replace(tzinfo=timezone.utc)
        return pushed <= archived
    except ValueError:
        return False


def _build_pathspec_excludes(
    owner: str,
    repo: str,
    include_heavy: bool = False,
    policy_path: Path | None = None,
) -> list[str]:
    """git archive için :(exclude) pathspec listesi döner."""
    if include_heavy:
        return []

    excludes = list(_DEFAULT_EXCLUDE_GLOBS)

    if policy_path and policy_path.exists():
        try:
            policy = json.loads(policy_path.read_text())
            global_cfg = policy.get("global", {})
            repo_cfg = policy.get("repos", {}).get(f"{owner}/{repo}", {})

            lean = repo_cfg.get("lean_archive", global_cfg.get("lean_archive", True))
            if not lean:
                return []

            if isinstance(global_cfg.get("exclude_globs"), list):
                excludes.extend(global_cfg["exclude_globs"])
            if isinstance(repo_cfg.get("exclude_globs"), list):
                excludes.extend(repo_cfg["exclude_globs"])
        except Exception as exc:
            log.warning("archive_policy.json okunamadı: %s", exc)

    # Dedup + sıralama
    seen: set[str] = set()
    result = []
    for p in excludes:
        if p not in seen:
            seen.add(p)
            result.append(f":(exclude){p}")
    return result


# ---------------------------------------------------------------------------
# Git işlemleri
# ---------------------------------------------------------------------------

def _git_clone_bare(
    clone_url: str,
    dest: Path,
    token: str = "",
    provider: str = "github",
) -> bool:
    """Bare clone. Token varsa URL'ye embed eder."""
    if token:
        if provider == "github":
            clone_url = clone_url.replace("https://", f"https://{token}@")
        elif provider == "gitlab":
            clone_url = clone_url.replace("https://", f"https://oauth2:{token}@")

    display_url = clone_url.split("@")[-1]
    log.info("Klonlanıyor: %s", display_url)

    proc = subprocess.Popen(
        ["git", "clone", "--bare", "--progress", clone_url, str(dest)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0,
    )

    buf = ""
    err_lines: list[str] = []

    for chunk in iter(lambda: proc.stderr.read(256), ""):
        buf += chunk
        while buf:
            cr, nl = buf.find("\r"), buf.find("\n")
            if cr == -1 and nl == -1:
                break
            if cr != -1 and (nl == -1 or cr < nl):
                line, buf = buf[:cr], buf[cr + 1:]
            else:
                line, buf = buf[:nl], buf[nl + 1:]
            line = line.strip()
            if line:
                err_lines.append(line)
                if len(err_lines) > 20:
                    err_lines.pop(0)

    proc.wait()

    if proc.returncode != 0:
        for line in err_lines[-5:]:
            log.error("Clone stderr: %s", line)
        return False
    return True


def _git_bundle(bare_repo: Path, bundle_path: Path) -> bool:
    r = subprocess.run(
        ["git", "-C", str(bare_repo), "bundle", "create", str(bundle_path), "--all"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        log.error("Bundle hatası: %s", r.stderr.strip())
        return False
    log.info("Bundle: %s (%s)", bundle_path.name, _human_size(bundle_path.stat().st_size))
    return True


def _git_archive(
    bare_repo: Path,
    dest: Path,
    owner: str,
    repo: str,
    branch: str = "HEAD",
    include_heavy: bool = False,
    policy_path: Path | None = None,
) -> bool:
    excludes = _build_pathspec_excludes(owner, repo, include_heavy, policy_path)
    cmd = [
        "git", "-C", str(bare_repo),
        "archive", "--format=tar.gz", f"--output={dest}", branch,
    ] + excludes

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 and excludes:
        log.warning("Pathspec exclude desteklenmiyor, filtresiz deneniyor...")
        r = subprocess.run(
            ["git", "-C", str(bare_repo), "archive",
             "--format=tar.gz", f"--output={dest}", branch],
            capture_output=True, text=True,
        )

    if r.returncode != 0:
        log.error("git archive hatası: %s", r.stderr.strip())
        return False

    log.info("Arşiv: %s (%s)", dest.name, _human_size(dest.stat().st_size))
    return True


def _check_disk(
    repo_size_kb: int,
    target_dir: Path,
    multiplier: float = 3.0,
) -> tuple[bool, str]:
    needed = int(repo_size_kb * 1024 * multiplier)
    free = shutil.disk_usage(target_dir).free
    if free < needed:
        return False, (
            f"Yetersiz disk alanı! Gereken: {_human_size(needed)}, "
            f"Mevcut: {_human_size(free)}"
        )
    if free < needed * 2:
        log.warning("Disk alanı az: %s boş, %s gerekli", _human_size(free), _human_size(needed))
    return True, ""


# ---------------------------------------------------------------------------
# Audit log (basit hash-chain)
# ---------------------------------------------------------------------------

def _append_audit(storage: ArchiverStorage, event_type: str, payload: dict) -> None:
    try:
        import json as _json
        chain_path = storage.chain_state_path
        audit_path = storage.audit_log_path

        prev_hash = "GENESIS"
        if chain_path.exists():
            state = _json.loads(chain_path.read_text())
            prev_hash = state.get("last_hash", "GENESIS")

        event = {
            "id": _new_uuid(),
            "ts": _utcnow(),
            "event_type": event_type,
            "payload": payload,
            "prev_hash": prev_hash,
        }
        chain_hash = hashlib.sha256(
            _json.dumps(event, sort_keys=True).encode()
        ).hexdigest()
        event["chain_hash"] = chain_hash

        with audit_path.open("a") as f:
            f.write(_json.dumps(event) + "\n")
        chain_path.write_text(_json.dumps({"last_hash": chain_hash}))
    except Exception as exc:
        log.warning("Audit log yazılamadı: %s", exc)


# ---------------------------------------------------------------------------
# Ana arşivleme fonksiyonu
# ---------------------------------------------------------------------------

def archive_repo(
    ref: RepoRef,
    provider: AbstractProvider,
    storage: ArchiverStorage,
    catalog: ArchiverCatalog,
    cfg: ArchiverConfig | None = None,
    force: bool = False,
    include_heavy: bool = False,
) -> dict[str, Any]:
    """
    Tek repo için arşivleme pipeline'ı çalıştırır.

    Returns:
        dict with keys: success (bool), version (str), archive_id (str),
                        skipped (bool), error (str)
    """
    if cfg is None:
        cfg = get_archiver_config()

    display = ref.key
    log.info(_SEPARATOR)
    log.info("İşleniyor: %s", display)

    # -- Repo bilgilerini al ---------------------------------------------------
    repo_info = provider.get_repo(ref)
    if not repo_info:
        msg = f"Repo bulunamadı: {display}"
        log.error(msg)
        _append_audit(storage, "ARCHIVE_FAILED", {"repo": display, "reason": "repo_not_found"})
        return {"success": False, "skipped": False, "error": msg}

    if repo_info.get("size", 0) == 0:
        msg = f"Boş repo, atlanıyor: {display}"
        log.warning(msg)
        _append_audit(storage, "ARCHIVE_SKIPPED", {"repo": display, "reason": "empty_repo"})
        return {"success": True, "skipped": True, "error": ""}

    # -- Disk kontrolü --------------------------------------------------------
    ok, disk_msg = _check_disk(
        repo_info.get("size", 0),
        storage.root,
        cfg.pipeline.disk_space_multiplier,
    )
    if not ok:
        log.error(disk_msg)
        _append_audit(storage, "ARCHIVE_FAILED", {"repo": display, "reason": "insufficient_disk"})
        return {"success": False, "skipped": False, "error": disk_msg}

    # -- Versiyon hesapla -----------------------------------------------------
    existing = catalog.get_repo(ref.key)
    if existing and not force and _is_up_to_date(
        existing.last_archived_at.isoformat() if existing.last_archived_at else None,
        repo_info.get("pushed_at"),
    ):
        log.info("ATLANDI: %s zaten güncel (%s)", display, existing.current_version)
        _append_audit(storage, "ARCHIVE_SKIPPED", {
            "repo": display,
            "reason": "already_up_to_date",
            "current_version": existing.current_version,
        })
        return {"success": True, "skipped": True, "version": existing.current_version, "error": ""}

    next_ver = storage.next_version(ref)
    log.info("Yeni versiyon: %s", next_ver)

    # -- Dosya yolları --------------------------------------------------------
    version_dir = storage.raw_version_dir(ref, next_ver)
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_path = version_dir / f"{ref.repo}_{now_str}.tar.gz"
    bundle_path = version_dir / f"{ref.repo}_bundle_{now_str[:8]}.git"
    archive_id = _new_uuid()

    log.info("Arşiv modu: %s", "full (include-heavy)" if include_heavy else "lean")

    # -- Git işlemleri (geçici dizin) -----------------------------------------
    token = cfg.providers.github.token if ref.provider == "github" else cfg.providers.gitlab.token

    with tempfile.TemporaryDirectory(prefix="tessera_arch_") as tmp:
        bare = Path(tmp) / f"{ref.repo}.git"

        if not _git_clone_bare(repo_info["clone_url"], bare, token=token, provider=ref.provider):
            _append_audit(storage, "ARCHIVE_FAILED", {"repo": display, "reason": "git_clone_failed"})
            return {"success": False, "skipped": False, "error": "git clone başarısız"}

        if cfg.pipeline.include_git_bundle:
            if not _git_bundle(bare, bundle_path):
                _append_audit(storage, "ARCHIVE_FAILED", {"repo": display, "reason": "git_bundle_failed"})
                return {"success": False, "skipped": False, "error": "git bundle başarısız"}
        else:
            bundle_path = None

        policy_path = storage.root.parent / "archive_policy.json"
        if not _git_archive(
            bare, archive_path,
            ref.namespace, ref.repo,
            repo_info.get("default_branch", "HEAD"),
            include_heavy,
            policy_path if policy_path.exists() else None,
        ):
            _append_audit(storage, "ARCHIVE_FAILED", {"repo": display, "reason": "git_archive_failed"})
            return {"success": False, "skipped": False, "error": "git archive başarısız"}

    # -- Checksum --------------------------------------------------------------
    checksum = _sha256(archive_path)
    (version_dir / f"{archive_path.name}.sha256").write_text(
        f"{checksum}  {archive_path.name}\n"
    )

    # Global checksums.sha256 manifestine ekle
    try:
        with storage.checksums_path.open("a") as f:
            f.write(f"{checksum}  {archive_path.relative_to(storage.root)}\n")
    except Exception:
        pass

    # -- Catalog güncelle -------------------------------------------------------
    now_utc = _utcnow()
    pushed_at = repo_info.get("pushed_at", "")
    stars = repo_info.get("stargazers_count", 0)
    language = repo_info.get("language") or ""

    ver_rec = VersionRecord(
        version=next_ver,
        archive_id=archive_id,
        archived_at=datetime.now(timezone.utc),
        pushed_at=datetime.fromisoformat(pushed_at.replace("Z", "+00:00")) if pushed_at else None,
        file=archive_path.name,
        bundle=bundle_path.name if bundle_path else None,
        size_bytes=archive_path.stat().st_size,
        checksum_sha256=checksum,
        stars_at_archive=stars,
    )
    catalog.upsert_version(ref.key, ver_rec)

    # Versions listesini yeniden say
    all_versions = catalog.list_versions(ref.key)
    total_versions = len(all_versions)

    repo_rec = RepoRecord(
        key=ref.key,
        provider=ref.provider,
        namespace=ref.namespace,
        repo=ref.repo,
        current_version=next_ver,
        total_versions=total_versions,
        last_archived_at=datetime.now(timezone.utc),
        stars=stars,
        language=language,
        size_bytes=archive_path.stat().st_size,
        checksum=checksum,
        description=repo_info.get("description") or "",
    )
    catalog.upsert_repo(repo_rec)
    storage.update_latest_symlink(ref, next_ver)

    # -- versions.json (dosya bazlı, geriye uyumluluk için) --------------------
    _write_versions_json(storage, ref, all_versions, repo_info, now_utc)

    # -- Snapshot ---------------------------------------------------------------
    snap_path = storage.snapshot_path(ref, next_ver)
    if not snap_path.exists():
        shutil.copy2(archive_path, snap_path)
        log.info("Snapshot: %s", snap_path)

    # -- Audit -----------------------------------------------------------------
    _append_audit(storage, "ARCHIVE_CREATED", {
        "repo": display,
        "version": next_ver,
        "archive_id": archive_id,
        "file": archive_path.name,
        "size_bytes": archive_path.stat().st_size,
        "checksum": checksum,
    })

    log.info("TAMAMLANDI: %s → %s (%s)", display, next_ver, _human_size(archive_path.stat().st_size))
    return {
        "success": True,
        "skipped": False,
        "version": next_ver,
        "archive_id": archive_id,
        "file": str(archive_path),
        "size_bytes": archive_path.stat().st_size,
        "checksum": checksum,
        "error": "",
    }


# ---------------------------------------------------------------------------
# versions.json yazar (geriye uyumluluk)
# ---------------------------------------------------------------------------

def _write_versions_json(
    storage: ArchiverStorage,
    ref: RepoRef,
    versions: list[VersionRecord],
    repo_info: dict,
    now_utc: str,
) -> None:
    try:
        path = storage.versions_json_path(ref)
        data: dict[str, Any] = {
            "provider": ref.provider,
            "namespace": ref.namespace,
            "owner": ref.namespace.split("/")[0],
            "repo": ref.repo,
            "current_version": versions[-1].version if versions else "v1",
            "total_versions": len(versions),
            "last_archived_at": now_utc,
            "versions": [
                {
                    "version": v.version,
                    "archived_at": v.archived_at.isoformat() if v.archived_at else now_utc,
                    "pushed_at": v.pushed_at.isoformat() if v.pushed_at else "",
                    "archive_id": v.archive_id,
                    "file": v.file,
                    "bundle": v.bundle,
                    "size_bytes": v.size_bytes,
                    "checksum_sha256": v.checksum_sha256,
                    "stars_at_archive": v.stars_at_archive,
                }
                for v in versions
            ],
        }
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str))
    except Exception as exc:
        log.warning("versions.json yazılamadı: %s", exc)
