"""Pipeline orchestration for dataset ingestion."""

from __future__ import annotations

import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tessera.connectors.base import DatasetInfo
from tessera.core.audit import AuditLogger
from tessera.core.catalog import CatalogManager
from tessera.core.exceptions import PipelineError
from tessera.core.hashing import compute_directory_checksum, compute_file_checksum
from tessera.core.storage import StorageManager
from tessera.core.versioning import VersionManager
from tessera.exporters.base import BaseExporter
from tessera.transformers.base import BaseTransformer
from tessera.validators.base import BaseValidator, ValidationLevel


@dataclass
class StageResult:
    """Single pipeline stage outcome."""

    stage: str
    plugin_name: str
    status: str
    duration_ms: int
    details: dict


@dataclass
class PipelineResult:
    """Aggregate pipeline result."""

    success: bool
    dataset_id: str
    version: str
    stages: list[StageResult]
    total_duration_ms: int
    error_message: str | None = None


class Pipeline:
    """Coordinate connector, validation, transformation, and catalog flows."""

    def __init__(self, config, registry, catalog: CatalogManager, audit: AuditLogger):
        self.config = config
        self.registry = registry
        self.catalog = catalog
        self.audit = audit
        self.storage = StorageManager(self._section("storage"))
        self.storage.initialize()
        self.catalog.initialize()
        self.audit.initialize()
        self.version_manager = VersionManager(self._section("versioning").get("strategy", "semantic"))

    def ingest(
        self, source: str, source_ref: str, tags: list[str] | None = None, force: bool = False
    ) -> PipelineResult:
        """Run the ingest pipeline for a dataset."""

        started_at = time.perf_counter()
        stages: list[StageResult] = []
        connector = self.registry.get_connector(source)
        pre_context = {"source": source, "source_ref": source_ref, "tags": tags or []}
        self._run_hooks("pre_ingest", "pre_ingest", pre_context, stages)

        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            download_dir = temp_dir / "download"
            try:
                download_result = connector.download(source_ref, download_dir)
                stages.append(
                    StageResult(
                        stage="download",
                        plugin_name=connector.name,
                        status="success" if download_result.success else "failed",
                        duration_ms=int(download_result.duration_seconds * 1000),
                        details={
                            "path": str(download_result.local_path),
                            "checksum": download_result.checksum_sha256,
                        },
                    )
                )
                self.audit.log(
                    "download",
                    "dataset",
                    resource_id=source_ref,
                    details={"source": source, "checksum": download_result.checksum_sha256},
                    status="success",
                )

                duplicate = self.catalog.check_duplicate(download_result.checksum_sha256)
                if duplicate and self._section("ingestion").get("skip_existing", True) and not force:
                    stages.append(
                        StageResult(
                            stage="duplicate_check",
                            plugin_name="catalog",
                            status="skipped",
                            duration_ms=0,
                            details={"version_id": duplicate["id"], "checksum": download_result.checksum_sha256},
                        )
                    )
                    self.audit.log(
                        "duplicate_skip",
                        "dataset_version",
                        resource_id=duplicate["id"],
                        details={"checksum": download_result.checksum_sha256},
                        status="success",
                    )
                    return PipelineResult(
                        success=True,
                        dataset_id=duplicate["dataset_id"],
                        version=duplicate["version"],
                        stages=stages,
                        total_duration_ms=int((time.perf_counter() - started_at) * 1000),
                    )

                metadata = connector.fetch_metadata(source_ref)
                dataset = self._resolve_dataset(metadata)

                # Always refresh catalog metadata (name, description, tags, license)
                # for existing datasets so stale/empty entries get updated.
                if dataset:
                    import json as _json
                    self.catalog.update_dataset(
                        dataset["id"],
                        name=metadata.name,
                        description=metadata.description or dataset.get("description") or "",
                        tags=(
                            _json.dumps(metadata.tags)
                            if isinstance(metadata.tags, list)
                            else (metadata.tags or "[]")
                        ),
                    )

                next_version = self.version_manager.next_version(
                    dataset["current_version"] if dataset else None,
                    "minor" if dataset else "minor",
                )

                raw_path = self.storage.store_raw(
                    download_result.local_path, metadata.name, next_version
                )
                checksum = self._compute_checksum(raw_path)
                version_path = self._primary_artifact(raw_path)
                validation_metadata: dict[str, Any] = {}

                validation_failed = False
                for validator_name in self._section("validators", []):
                    validator: BaseValidator = self.registry.get_validator(validator_name)
                    result = validator.validate(version_path, metadata=validation_metadata or None)
                    stages.append(
                        StageResult(
                            stage="validate",
                            plugin_name=validator.name,
                            status=result.level.value,
                            duration_ms=result.duration_ms,
                            details={"issues": [issue.code for issue in result.issues]},
                        )
                    )
                    self.audit.log(
                        "validate",
                        "dataset",
                        resource_id=source_ref,
                        details={"validator": validator.name, "status": result.level.value},
                        status="success" if result.passed else "failed",
                    )
                    validation_metadata.update(result.metadata)
                    if not result.passed:
                        quarantined = self.storage.quarantine(
                            raw_path, metadata.name, validator.name
                        )
                        stages.append(
                            StageResult(
                                stage="quarantine",
                                plugin_name="storage",
                                status="success",
                                duration_ms=0,
                                details={"path": str(quarantined)},
                            )
                        )
                        validation_failed = True
                        break

                if validation_failed:
                    self._run_hooks(
                        "on_error",
                        "on_error",
                        {"source": source, "source_ref": source_ref, "message": "validation_failed"},
                        stages,
                    )
                    return PipelineResult(
                        success=False,
                        dataset_id=dataset["id"] if dataset else "",
                        version=next_version,
                        stages=stages,
                        total_duration_ms=int((time.perf_counter() - started_at) * 1000),
                        error_message="Validation failed",
                    )

                processed_artifact = self._run_transformers(
                    raw_artifact=version_path,
                    dataset_name=metadata.name,
                    version=next_version,
                    stages=stages,
                )
                processed_path = self.storage.store_processed(
                    processed_artifact, metadata.name, next_version
                )
                final_processed_artifact = self._primary_artifact(processed_path)

                from tessera.core.temporal import detect_temporal_coverage
                temporal = detect_temporal_coverage(raw_path)
                enriched_metadata = {**validation_metadata, **temporal}

                dataset_id = dataset["id"] if dataset else self.catalog.register_dataset(metadata)
                version_id = self.catalog.register_version(
                    dataset_id,
                    {
                        "version": next_version,
                        "checksum_sha256": checksum,
                        "file_size_bytes": self._path_size(raw_path),
                        "file_count": self._path_file_count(raw_path),
                        "raw_path": str(raw_path),
                        "processed_path": str(processed_path),
                        "zone": "processed",
                        "format": final_processed_artifact.suffix.lstrip(".") or None,
                        "compression": self._compression_name(final_processed_artifact),
                        "row_count": validation_metadata.get("row_count"),
                        "column_count": validation_metadata.get("column_count"),
                        "metadata_json": enriched_metadata,
                    },
                )
                self.catalog.record_lineage(
                    version_id,
                    "download",
                    connector.name,
                    output_checksum=download_result.checksum_sha256,
                    parameters={"source_ref": source_ref},
                    status="success",
                    duration_ms=int(download_result.duration_seconds * 1000),
                )
                for stage in stages:
                    if stage.stage in {"validate", "transform"}:
                        self.catalog.record_lineage(
                            version_id,
                            stage.stage,
                            stage.plugin_name,
                            parameters=stage.details,
                            status=stage.status,
                            duration_ms=stage.duration_ms,
                        )

                profile_path = None
                if self._section("processing").get("auto_profile", True):
                    try:
                        exporter: BaseExporter = self.registry.get_exporter("report")
                        profile_path = raw_path.parent / "profile.json"
                        exporter.export(version_id, profile_path, source_path=final_processed_artifact)
                        stages.append(
                            StageResult(
                                stage="profile",
                                plugin_name=exporter.name,
                                status="success",
                                duration_ms=0,
                                details={"path": str(profile_path)},
                            )
                        )
                    except Exception:
                        profile_path = None

                if profile_path:
                    version_record = self.catalog.get_version(version_id)
                    metadata_json = dict(version_record["metadata_json"])
                    metadata_json["profile_path"] = str(profile_path)
                    self.catalog.update_version_profile(version_id, str(profile_path), metadata_json)

                # AI enrichment: generate description if missing or very short
                ai_cfg = self._section("ai_enrichment")
                if ai_cfg.get("enabled", False):
                    current_desc = (
                        metadata.description
                        or (dataset.get("description") if dataset else None)
                        or ""
                    )
                    min_len = int(ai_cfg.get("min_description_length", 80))
                    if len(current_desc.strip()) < min_len:
                        try:
                            from tessera.core.ai_enrichment import enrich_description
                            ai_desc = enrich_description(raw_path, metadata, config=ai_cfg)
                            if ai_desc:
                                import json as _json
                                self.catalog.update_dataset(
                                    dataset_id, description=ai_desc
                                )
                                stages.append(StageResult(
                                    stage="ai_enrich",
                                    plugin_name="claude",
                                    status="success",
                                    duration_ms=0,
                                    details={"chars": len(ai_desc)},
                                ))
                        except Exception:
                            pass

                self._run_hooks(
                    "post_ingest",
                    "post_ingest",
                    {
                        "dataset_id": dataset_id,
                        "version_id": version_id,
                        "source": source,
                        "source_ref": source_ref,
                    },
                    stages,
                )
                self.audit.log(
                    "ingest",
                    "dataset_version",
                    resource_id=version_id,
                    details={"dataset_id": dataset_id, "version": next_version},
                    status="success",
                )
                self._archive_old_versions(dataset_id, metadata.name)
                return PipelineResult(
                    success=True,
                    dataset_id=dataset_id,
                    version=next_version,
                    stages=stages,
                    total_duration_ms=int((time.perf_counter() - started_at) * 1000),
                )
            except Exception as exc:
                self.audit.log(
                    "ingest",
                    "dataset",
                    resource_id=source_ref,
                    details={"error": str(exc)},
                    status="failed",
                )
                self._run_hooks(
                    "on_error",
                    "on_error",
                    {"source": source, "source_ref": source_ref, "message": str(exc)},
                    stages,
                )
                raise PipelineError(f"Pipeline hatasi: {exc}") from exc

    def reingest(self, dataset_id: str) -> PipelineResult:
        """Reingest a dataset by identifier."""

        dataset = self.catalog.get_dataset(dataset_id)
        if not dataset:
            raise PipelineError(f"Dataset bulunamadi: {dataset_id}")
        return self.ingest(dataset["source"], dataset["source_ref"])

    def _run_transformers(
        self, raw_artifact: Path, dataset_name: str, version: str, stages: list[StageResult]
    ) -> Path:
        current_path = raw_artifact
        for transformer_name in self._section("transformers", []):
            transformer: BaseTransformer = self.registry.get_transformer(transformer_name)
            suffix = current_path.suffix or ".bin"
            output_path = current_path.parent / f"{current_path.stem}_{transformer.name}{self._output_suffix(transformer, suffix)}"
            result = transformer.transform(current_path, output_path)
            stages.append(
                StageResult(
                    stage="transform",
                    plugin_name=transformer.name,
                    status="success" if result.success else "failed",
                    duration_ms=result.duration_ms,
                    details={"output_path": str(result.output_path)},
                )
            )
            current_path = result.output_path
        return current_path

    def _run_hooks(
        self, config_key: str, event: str, context: dict[str, Any], stages: list[StageResult]
    ) -> None:
        for hook_name in self._section("hooks").get(config_key, []):
            hook = self.registry.get_hook(hook_name)
            started = time.perf_counter()
            hook.execute(event, context)
            stages.append(
                StageResult(
                    stage="hook",
                    plugin_name=hook.name,
                    status="success",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    details={"event": event},
                )
            )

    def _resolve_dataset(self, metadata: DatasetInfo) -> dict[str, Any] | None:
        matches = self.catalog.search_datasets(source=metadata.source)
        for dataset in matches:
            if dataset["source_ref"] == metadata.source_ref:
                return dataset
        return None

    def _section(self, key: str, default: Any = None) -> Any:
        if hasattr(self.config, key):
            value = getattr(self.config, key)
            return value.model_dump() if hasattr(value, "model_dump") else value
        if isinstance(self.config, dict):
            return self.config.get(key, default if default is not None else {})
        return default if default is not None else {}

    _DATA_SUFFIXES = {".csv", ".parquet", ".json", ".tsv", ".xlsx", ".jsonl"}
    _SKIP_SUFFIXES = {".md", ".txt", ".yaml", ".yml", ".toml", ".rst", ".html", ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".zip", ".tar", ".gz", ".zst"}

    def _primary_artifact(self, path: Path) -> Path:
        if path.is_file():
            return path
        all_files = sorted(item for item in path.rglob("*") if item.is_file())
        if not all_files:
            raise PipelineError(f"Islenecek dosya bulunamadi: {path}")
        data_files = [f for f in all_files if f.suffix.lower() in self._DATA_SUFFIXES]
        return data_files[0] if data_files else all_files[0]

    def _compute_checksum(self, path: Path) -> str:
        return compute_file_checksum(path) if path.is_file() else compute_directory_checksum(path)

    def _path_size(self, path: Path) -> int:
        if path.is_file():
            return path.stat().st_size
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())

    def _path_file_count(self, path: Path) -> int:
        if path.is_file():
            return 1
        return len([item for item in path.rglob("*") if item.is_file()])

    def _compression_name(self, path: Path) -> str | None:
        if path.suffix == ".gz":
            return "gzip"
        if path.suffix == ".zst":
            return "zstd"
        return None

    def _output_suffix(self, transformer: BaseTransformer, current_suffix: str) -> str:
        if transformer.name == "format":
            if self._section("processing").get("default_format", "parquet") == "parquet":
                return ".parquet"
        if transformer.name == "compress":
            compression = self._section("processing").get("compression", "zstd")
            return ".gz" if compression == "gzip" else ".zst"
        return current_suffix

    def _archive_old_versions(self, dataset_id: str, dataset_name: str) -> None:
        versioning = self._section("versioning")
        if not versioning.get("archive_older", True):
            return
        keep = int(versioning.get("keep_versions", 5))
        versions = self.catalog.get_versions(dataset_id)
        for stale in versions[keep:]:
            processed_path = stale.get("processed_path")
            if not processed_path:
                continue
            path = Path(processed_path)
            if not path.exists():
                continue
            archive_path = self.storage.move_to_archive(path, dataset_name, stale["version"])
            self.catalog.update_version_zone(stale["id"], "archive", archive_path)

