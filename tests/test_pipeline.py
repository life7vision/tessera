"""Tests for the ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.audit import AuditLogger
from tessera.core.catalog import CatalogManager
from tessera.core.pipeline import Pipeline
from tessera.exporters.base import BaseExporter, ExportResult
from tessera.hooks.base import BaseHook
from tessera.transformers.base import BaseTransformer, TransformResult
from tessera.validators.base import (
    BaseValidator,
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
)


class FakeConnector(BaseConnector):
    name = "fake"
    version = "1.0.0"

    def validate_credentials(self) -> bool:
        return True

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        return []

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        return DatasetInfo(
            source="fake",
            source_ref=source_ref,
            name="demo_dataset",
            description="Demo",
            size_bytes=10,
            file_count=1,
            format_hint="csv",
            tags=["demo"],
            license=None,
            last_updated=None,
            url=None,
            extra_metadata={},
        )

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / "input.csv"
        content = "id,name\n1,alpha\n2,beta\n"
        file_path.write_text(content, encoding="utf-8")
        return DownloadResult(
            success=True,
            local_path=file_path,
            checksum_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            size_bytes=file_path.stat().st_size,
            file_count=1,
            duration_seconds=0.01,
        )


class PassValidator(BaseValidator):
    name = "pass_validator"
    version = "1.0.0"

    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
        return ValidationResult(
            validator_name=self.name,
            level=ValidationLevel.PASSED,
            issues=[],
            duration_ms=1,
            metadata={"row_count": 2, "column_count": 2},
        )


class FailValidator(BaseValidator):
    name = "fail_validator"
    version = "1.0.0"

    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
        return ValidationResult(
            validator_name=self.name,
            level=ValidationLevel.FAILED,
            issues=[
                ValidationIssue(
                    level=ValidationLevel.FAILED,
                    code="BROKEN",
                    message="broken",
                )
            ],
            duration_ms=1,
        )


class CopyTransformer(BaseTransformer):
    name = "copy_transformer"
    version = "1.0.0"

    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
        output_path.write_bytes(input_path.read_bytes())
        return TransformResult(
            transformer_name=self.name,
            success=True,
            input_path=input_path,
            output_path=output_path,
            input_checksum="in",
            output_checksum="out",
            input_size=input_path.stat().st_size,
            output_size=output_path.stat().st_size,
            duration_ms=1,
            details={},
        )


class FakeReportExporter(BaseExporter):
    name = "report"
    version = "1.0.0"

    def export(self, version_id: str, target_path: Path, **kwargs) -> ExportResult:
        target_path.write_text("{}", encoding="utf-8")
        return ExportResult(
            success=True,
            exporter_name=self.name,
            output_path=target_path,
            size_bytes=target_path.stat().st_size,
            duration_ms=1,
        )


class RecorderHook(BaseHook):
    name = "recorder"
    version = "1.0.0"
    events: list[tuple[str, dict]] = []

    def execute(self, event: str, context: dict[str, object]) -> None:
        self.__class__.events.append((event, dict(context)))


class FakeRegistry:
    def __init__(self, validator_cls=PassValidator):
        self.validator_cls = validator_cls

    def get_connector(self, name: str):
        return FakeConnector({})

    def get_validator(self, name: str):
        return self.validator_cls({})

    def get_transformer(self, name: str):
        return CopyTransformer({})

    def get_exporter(self, name: str):
        return FakeReportExporter({})

    def get_hook(self, name: str):
        return RecorderHook({})


def make_config(tmp_path: Path, validator_name: str) -> dict:
    return {
        "storage": {
            "base_path": str(tmp_path / "data"),
            "zones": {
                "raw": "raw",
                "processed": "processed",
                "archive": "archive",
                "quarantine": "quarantine",
            },
            "catalog_db": "catalog.db",
            "audit_db": "audit.db",
        },
        "ingestion": {
            "default_connector": "fake",
            "checksum_algorithm": "sha256",
            "skip_existing": True,
            "quarantine_on_fail": True,
        },
        "processing": {
            "default_format": "original",
            "compression": "gzip",
            "compression_level": 3,
            "auto_profile": True,
        },
        "versioning": {"strategy": "semantic", "keep_versions": 5, "archive_older": True},
        "validators": [validator_name],
        "transformers": ["copy_transformer"],
        "hooks": {
            "pre_ingest": ["recorder"],
            "post_ingest": ["recorder"],
            "on_error": ["recorder"],
        },
    }


def test_pipeline_happy_path(tmp_path: Path):
    RecorderHook.events = []
    config = make_config(tmp_path, "pass_validator")
    catalog = CatalogManager(tmp_path / "catalog.db")
    audit = AuditLogger(tmp_path / "audit.db")
    pipeline = Pipeline(config, FakeRegistry(PassValidator), catalog, audit)

    result = pipeline.ingest("fake", "source/demo")

    assert result.success is True
    assert result.version == "1.0.0"
    assert any(stage.stage == "transform" for stage in result.stages)
    latest = catalog.get_latest_version(result.dataset_id)
    assert latest is not None
    assert latest["zone"] == "processed"
    assert RecorderHook.events[0][0] == "pre_ingest"
    assert RecorderHook.events[-1][0] == "post_ingest"


def test_pipeline_duplicate_skips_second_ingest(tmp_path: Path):
    config = make_config(tmp_path, "pass_validator")
    catalog = CatalogManager(tmp_path / "catalog.db")
    audit = AuditLogger(tmp_path / "audit.db")
    pipeline = Pipeline(config, FakeRegistry(PassValidator), catalog, audit)

    first = pipeline.ingest("fake", "source/demo")
    second = pipeline.ingest("fake", "source/demo")

    assert first.success is True
    assert second.success is True
    assert second.version == "1.0.0"
    assert any(stage.status == "skipped" for stage in second.stages)
    assert len(catalog.get_versions(first.dataset_id)) == 1


def test_pipeline_validation_failure_moves_to_quarantine(tmp_path: Path):
    RecorderHook.events = []
    config = make_config(tmp_path, "fail_validator")
    catalog = CatalogManager(tmp_path / "catalog.db")
    audit = AuditLogger(tmp_path / "audit.db")
    pipeline = Pipeline(config, FakeRegistry(FailValidator), catalog, audit)

    result = pipeline.ingest("fake", "source/demo")

    quarantine_root = Path(config["storage"]["base_path"]) / "quarantine" / "demo_dataset"
    assert result.success is False
    assert quarantine_root.exists()
    assert any(stage.stage == "quarantine" for stage in result.stages)
    assert RecorderHook.events[-1][0] == "on_error"


def test_pipeline_records_lineage_and_audit(tmp_path: Path):
    RecorderHook.events = []
    config = make_config(tmp_path, "pass_validator")
    catalog = CatalogManager(tmp_path / "catalog.db")
    audit = AuditLogger(tmp_path / "audit.db")
    pipeline = Pipeline(config, FakeRegistry(PassValidator), catalog, audit)

    result = pipeline.ingest("fake", "source/demo")
    latest = catalog.get_latest_version(result.dataset_id)
    lineage = catalog.get_lineage(latest["id"])
    audit_logs = audit.get_logs(resource_type="dataset_version")

    assert result.success is True
    assert any(entry["plugin_name"] == "fake" for entry in lineage)
    assert any(entry["plugin_name"] == "pass_validator" for entry in lineage)
    assert audit_logs[0]["action"] == "ingest"


def test_pipeline_archives_old_versions(tmp_path: Path):
    config = make_config(tmp_path, "pass_validator")
    config["versioning"]["keep_versions"] = 1
    catalog = CatalogManager(tmp_path / "catalog.db")
    audit = AuditLogger(tmp_path / "audit.db")
    pipeline = Pipeline(config, FakeRegistry(PassValidator), catalog, audit)

    first = pipeline.ingest("fake", "source/demo", force=True)
    second = pipeline.ingest("fake", "source/demo", force=True)

    versions = catalog.get_versions(first.dataset_id)
    archived = [version for version in versions if version["zone"] == "archive"]
    processed = [version for version in versions if version["zone"] == "processed"]

    assert second.success is True
    assert len(archived) == 1
    assert len(processed) == 1
    assert Path(archived[0]["archive_path"]).exists()
