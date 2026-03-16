"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tessera.core.config import get_config, load_config
from tessera.core.exceptions import ConfigError


def test_load_default_config():
    config = load_config()

    # base_path değeri ortama göre değişebilir — sadece dolu olduğunu doğrula
    assert config.storage.base_path
    assert config.ingestion.default_connector == "kaggle"
    assert config.processing.compression == "zstd"


def test_env_override(monkeypatch):
    monkeypatch.setenv("TESSERA_STORAGE__BASE_PATH", "/tmp/tessera-data")
    monkeypatch.setenv("TESSERA_PROCESSING__COMPRESSION_LEVEL", "5")

    config = load_config(force_reload=True)

    assert config.storage.base_path == "/tmp/tessera-data"
    assert config.processing.compression_level == 5


def test_explicit_config_path(tmp_path: Path):
    config_path = tmp_path / "custom.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "project": {"name": "custom-archive", "version": "0.9.0"},
                "storage": {"base_path": "./custom-data"},
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path, force_reload=True)

    assert config.project.name == "custom-archive"
    assert config.storage.base_path == "./custom-data"


def test_invalid_config_raises(tmp_path: Path):
    config_path = tmp_path / "broken.yaml"
    config_path.write_text("[]", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config(config_path, force_reload=True)


def test_config_singleton_cache():
    first = load_config()
    second = get_config()

    assert first is second


def test_env_override_list_parsing(monkeypatch):
    monkeypatch.setenv("TESSERA_VALIDATORS", "integrity,schema,quality")

    config = load_config(force_reload=True)

    assert config.validators == ["integrity", "schema", "quality"]
