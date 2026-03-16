"""Tests for audit logging."""

from __future__ import annotations

from pathlib import Path

from tessera.core.audit import AuditLogger


def make_audit(tmp_path: Path) -> AuditLogger:
    logger = AuditLogger(tmp_path / "audit.db")
    logger.initialize()
    return logger


def test_log_and_filter_audit_events(tmp_path: Path):
    logger = make_audit(tmp_path)
    logger.log(
        "ingest",
        "dataset",
        resource_id="dataset-1",
        actor="tester",
        details={"source": "kaggle"},
        status="success",
    )
    logger.log(
        "archive",
        "dataset",
        resource_id="dataset-1",
        details={"version": "1.0.0"},
        status="success",
    )

    logs = logger.get_logs(limit=10)
    ingest_logs = logger.get_logs(action="ingest", limit=10)
    dataset_logs = logger.get_logs(resource_type="dataset", limit=10)

    assert len(logs) == 2
    assert ingest_logs[0]["action"] == "ingest"
    assert ingest_logs[0]["details_json"] == {"source": "kaggle"}
    assert len(dataset_logs) == 2
