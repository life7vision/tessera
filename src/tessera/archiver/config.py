"""
Tessera Archiver — Konfigürasyon modelleri.

Tessera'nın core/models.py'deki AppConfig'e ArchiverConfig eklenir.
Bu modül kendi başına da kullanılabilir.
"""
from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


class ProviderConfig(BaseModel):
    model_config = {"extra": "ignore"}

    token_env: str = ""
    api_url: str = ""
    timeout_sec: int = 30
    retry_count: int = 3
    retry_delay: int = 2

    @property
    def token(self) -> str:
        """Token'ı ortam değişkeninden oku."""
        if not self.token_env:
            return ""
        return os.environ.get(self.token_env, "")


class ProvidersConfig(BaseModel):
    model_config = {"extra": "ignore"}

    github: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(
            token_env="GITHUB_TOKEN",
            api_url="https://api.github.com",
        )
    )
    gitlab: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(
            token_env="GITLAB_TOKEN",
            api_url="https://gitlab.com/api/v4",
        )
    )


class PipelineConfig(BaseModel):
    model_config = {"extra": "ignore"}

    lean_archive: bool = True
    include_git_bundle: bool = True
    disk_space_multiplier: float = 3.0


class ScannerConfig(BaseModel):
    model_config = {"extra": "ignore"}

    max_file_size_mb: int = 5
    max_total_mb: int = 256
    max_uncompressed_gb: int = 5
    yara_rules_dir: str = "rules/yara"


class PolicyConfig(BaseModel):
    model_config = {"extra": "ignore"}

    max_high: int = 0
    max_medium: int = 0
    max_risk_level: str = "LOW"
    max_scan_age_hours: int = 24


class ArchiverAiConfig(BaseModel):
    model_config = {"extra": "ignore"}

    enabled: bool = True
    min_description_length: int = 50


class ArchiverConfig(BaseModel):
    """Archiver modülü için konfigürasyon."""

    model_config = {"extra": "ignore"}

    enabled: bool = True
    storage_root: str = "./archive"
    database: str = "./archive/archiver.db"

    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    scanner: ScannerConfig = Field(default_factory=ScannerConfig)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    ai_enrichment: ArchiverAiConfig = Field(default_factory=ArchiverAiConfig)

    @property
    def storage_path(self) -> Path:
        return Path(self.storage_root)

    @property
    def db_path(self) -> Path:
        return Path(self.database)


def get_archiver_config() -> ArchiverConfig:
    """
    Tessera'nın mevcut config sisteminden archiver section'ı yükle.
    Config yüklü değilse direkt YAML'dan oku.
    """
    try:
        from tessera.core.config import get_config
        cfg = get_config()
        # AppConfig'de archiver varsa kullan
        archiver_dict = getattr(cfg, "archiver", None)
        if archiver_dict is not None:
            if isinstance(archiver_dict, dict):
                return ArchiverConfig(**archiver_dict)
            return archiver_dict
    except Exception:
        pass

    # Fallback: direkt YAML oku
    try:
        import yaml
        cfg_path = Path("config/default.yaml")
        if cfg_path.exists():
            raw = yaml.safe_load(cfg_path.read_text())
            archiver_raw = raw.get("archiver", {})
            return ArchiverConfig(**archiver_raw)
    except Exception:
        pass

    return ArchiverConfig()
