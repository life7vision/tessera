"""
Tessera Archiver — Async job tracking.

Tessera'nın core/ingest_jobs.py pattern'ini izler.
Archive, scan, pipeline, verify ve report işlerini thread-safe in-memory store'da takip eder.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

MAX_JOBS = 100

JobType = Literal["archive", "scan", "pipeline", "verify", "report"]
JobStatus = Literal["pending", "running", "done", "failed"]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# ArchiveJob — tek bir iş birimi
# ---------------------------------------------------------------------------

class ArchiveJob:
    """Archiver modülündeki tek bir iş çalıştırmasını temsil eder."""

    def __init__(
        self,
        job_id: str,
        job_type: JobType,
        repo_key: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        self.id = job_id
        self.job_type = job_type
        self.repo_key = repo_key
        self.params: dict[str, Any] = params or {}

        self.status: JobStatus = "pending"
        self.logs: list[str] = []
        self.result: dict[str, Any] | None = None
        self.error: str | None = None

        self.started_at: str = _now()
        self.finished_at: str | None = None

    def log(self, message: str) -> None:
        self.logs.append(f"[{_now()}] {message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "repo_key": self.repo_key,
            "params": self.params,
            "status": self.status,
            "logs": self.logs,
            "result": self.result,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


# ---------------------------------------------------------------------------
# ArchiveJobStore — thread-safe in-memory registry
# ---------------------------------------------------------------------------

class ArchiveJobStore:
    """Thread-safe in-memory archiver job kaydedicisi."""

    def __init__(self) -> None:
        self._jobs: dict[str, ArchiveJob] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()

    def create(
        self,
        job_type: JobType,
        repo_key: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> ArchiveJob:
        job = ArchiveJob(str(uuid4()), job_type, repo_key, params)
        with self._lock:
            self._jobs[job.id] = job
            self._order.append(job.id)
            # En eski işleri temizle
            while len(self._order) > MAX_JOBS:
                oldest = self._order.pop(0)
                self._jobs.pop(oldest, None)
        return job

    def get(self, job_id: str) -> ArchiveJob | None:
        return self._jobs.get(job_id)

    def all_jobs(self, job_type: JobType | None = None) -> list[ArchiveJob]:
        with self._lock:
            jobs = [self._jobs[jid] for jid in reversed(self._order) if jid in self._jobs]
        if job_type:
            jobs = [j for j in jobs if j.job_type == job_type]
        return jobs

    def start(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = "running"
            job.log("İş başlatıldı")

    def finish(
        self,
        job_id: str,
        *,
        success: bool,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return
        job.status = "done" if success else "failed"
        job.result = result
        job.error = error
        job.finished_at = _now()
        job.log("İş tamamlandı" if success else f"İş başarısız: {error}")

    def append_log(self, job_id: str, message: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.log(message)

    def running_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.status == "running")


# ---------------------------------------------------------------------------
# Modül düzeyinde singleton (FastAPI app ömrü boyunca yaşar)
# ---------------------------------------------------------------------------

_store: ArchiveJobStore | None = None
_store_lock = threading.Lock()


def get_job_store() -> ArchiveJobStore:
    """Singleton job store döner (lazy init)."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = ArchiveJobStore()
    return _store
