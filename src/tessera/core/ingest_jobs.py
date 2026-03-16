"""In-memory store for tracking background ingest job status."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

MAX_JOBS = 50


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class IngestJob:
    """Represents a single ingest pipeline execution."""

    def __init__(self, job_id: str, source: str, source_ref: str, tags: list[str], force: bool) -> None:
        self.id = job_id
        self.source = source
        self.source_ref = source_ref
        self.tags = tags
        self.force = force
        self.status: str = "pending"   # pending | running | done | failed
        self.stages: list[dict[str, Any]] = []
        self.dataset_id: str | None = None
        self.version: str | None = None
        self.error_message: str | None = None
        self.started_at: str = _now()
        self.finished_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "source_ref": self.source_ref,
            "tags": self.tags,
            "force": self.force,
            "status": self.status,
            "stages": self.stages,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class IngestJobStore:
    """Thread-safe in-memory job registry."""

    def __init__(self) -> None:
        self._jobs: dict[str, IngestJob] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()

    def create_job(self, source: str, source_ref: str, tags: list[str], force: bool) -> IngestJob:
        job = IngestJob(str(uuid4()), source, source_ref, tags, force)
        with self._lock:
            self._jobs[job.id] = job
            self._order.append(job.id)
            # Evict oldest if over limit
            while len(self._order) > MAX_JOBS:
                oldest = self._order.pop(0)
                self._jobs.pop(oldest, None)
        return job

    def get_job(self, job_id: str) -> IngestJob | None:
        return self._jobs.get(job_id)

    def all_jobs(self) -> list[IngestJob]:
        with self._lock:
            return [self._jobs[jid] for jid in reversed(self._order) if jid in self._jobs]

    def update_status(self, job_id: str, status: str) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.status = status
            if status in ("done", "failed"):
                job.finished_at = _now()

    def append_stage(self, job_id: str, stage: dict[str, Any]) -> None:
        job = self._jobs.get(job_id)
        if job:
            job.stages.append(stage)

    def finish_job(
        self,
        job_id: str,
        *,
        success: bool,
        dataset_id: str | None = None,
        version: str | None = None,
        error_message: str | None = None,
    ) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return
        job.status = "done" if success else "failed"
        job.dataset_id = dataset_id
        job.version = version
        job.error_message = error_message
        job.finished_at = _now()
