"""Pydantic models for application configuration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TesseraBaseModel(BaseModel):
    """Shared pydantic configuration."""

    model_config = {"extra": "forbid", "validate_assignment": True}


class StorageConfig(TesseraBaseModel):
    """Storage layer configuration."""

    base_path: str = "./data"
    zones: dict[str, str] = {
        "raw": "raw",
        "processed": "processed",
        "archive": "archive",
        "quarantine": "quarantine",
    }
    catalog_db: str = "catalog.db"
    audit_db: str = "audit.db"


class IngestionConfig(TesseraBaseModel):
    """Ingestion behaviour configuration."""

    default_connector: str = "kaggle"
    checksum_algorithm: str = "sha256"
    skip_existing: bool = True
    quarantine_on_fail: bool = True


class ProcessingConfig(TesseraBaseModel):
    """Processing pipeline configuration."""

    default_format: str = "parquet"
    compression: str = "zstd"
    compression_level: int = Field(default=3, ge=1, le=22)
    auto_profile: bool = True


class VersioningConfig(TesseraBaseModel):
    """Version management configuration."""

    strategy: str = "semantic"
    keep_versions: int = Field(default=5, ge=1)
    archive_older: bool = True


class ConnectorConfig(TesseraBaseModel):
    """Connector-level configuration."""

    enabled: bool = True
    credentials_env: str | None = None
    token_env: str | None = None
    download_timeout: int = 300
    max_retries: int = 3


class LoggingConfig(TesseraBaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    file: str | None = None


class ProjectConfig(TesseraBaseModel):
    """Project metadata configuration."""

    name: str = "my-data-archive"
    version: str = "0.1.0"


class AppConfig(TesseraBaseModel):
    """Root application configuration."""

    project: ProjectConfig = Field(default_factory=ProjectConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    versioning: VersioningConfig = Field(default_factory=VersioningConfig)
    connectors: dict[str, ConnectorConfig] = Field(default_factory=dict)
    validators: list[str] = Field(
        default_factory=lambda: ["integrity", "schema", "quality"]
    )
    transformers: list[str] = Field(
        default_factory=lambda: ["clean", "format", "compress"]
    )
    hooks: dict[str, list[str]] = Field(default_factory=dict)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation."""

        return self.model_dump()

