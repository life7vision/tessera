"""Shared pytest fixtures."""

from __future__ import annotations

import json

import pytest

from tessera.core.config import clear_config_cache
from tessera.core.models import AppConfig


@pytest.fixture(autouse=True)
def reset_config_cache():
    clear_config_cache()
    yield
    clear_config_cache()


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "sample.csv"
    path.write_text("id,name,value\n1,alpha,100\n2,beta,200\n", encoding="utf-8")
    return path


@pytest.fixture
def sample_json(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text(
        json.dumps(
            [
                {"id": 1, "name": "alpha", "value": 100},
                {"id": 2, "name": "beta", "value": 200},
            ]
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def sample_config_dict(tmp_path):
    return {
        "project": {"name": "test-archive", "version": "0.1.0"},
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
            "default_format": "parquet",
            "compression": "zstd",
            "compression_level": 3,
            "auto_profile": True,
        },
        "versioning": {"strategy": "semantic", "keep_versions": 2, "archive_older": True},
        "connectors": {},
        "validators": ["integrity"],
        "transformers": ["clean"],
        "hooks": {"pre_ingest": [], "post_ingest": [], "on_error": []},
        "logging": {"level": "INFO", "format": "%(message)s", "file": None},
    }


@pytest.fixture
def sample_app_config(sample_config_dict):
    return AppConfig.model_validate(sample_config_dict)
