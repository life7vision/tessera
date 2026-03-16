"""Configuration loading and validation helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from tessera.core.exceptions import ConfigError
from tessera.core.models import AppConfig

DEFAULT_CONFIG_PATH = Path("config/default.yaml")
ENV_CONFIG_PATH = "TESSERA_CONFIG"
ENV_PREFIX = "TESSERA_"

_CONFIG_CACHE: AppConfig | None = None


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _parse_env_value(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"

    for caster in (int, float):
        try:
            return caster(value)
        except ValueError:
            continue

    if "," in value:
        return [item.strip() for item in value.split(",") if item.strip()]

    return value


def _env_overrides(prefix: str = ENV_PREFIX) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix) or key == ENV_CONFIG_PATH:
            continue

        path_parts = key[len(prefix) :].lower().split("__")
        current = overrides
        for part in path_parts[:-1]:
            current = current.setdefault(part, {})
        current[path_parts[-1]] = _parse_env_value(value)
    return overrides


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Konfigürasyon dosyası bulunamadı: {path}")

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Konfigürasyon YAML formatı geçersiz: {path}") from exc

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError("Konfigürasyonun kök yapısı sözlük olmalıdır.")
    return data


def resolve_config_path(config_path: str | Path | None = None) -> Path:
    """Resolve the effective configuration path."""

    if config_path is not None:
        return Path(config_path)

    env_path = os.getenv(ENV_CONFIG_PATH)
    if env_path:
        return Path(env_path)

    return DEFAULT_CONFIG_PATH


def load_config(config_path: str | Path | None = None, *, force_reload: bool = False) -> AppConfig:
    """Load, merge, validate, and cache the application configuration."""

    global _CONFIG_CACHE

    if _CONFIG_CACHE is not None and not force_reload:
        return _CONFIG_CACHE

    resolved_path = resolve_config_path(config_path)
    raw_config = _load_yaml_file(resolved_path)
    merged_config = _deep_merge(raw_config, _env_overrides())

    try:
        _CONFIG_CACHE = AppConfig.model_validate(merged_config)
    except PydanticValidationError as exc:
        raise ConfigError(f"Konfigürasyon doğrulaması başarısız: {exc}") from exc

    return _CONFIG_CACHE


def get_config() -> AppConfig:
    """Return the cached configuration, loading defaults if necessary."""

    return load_config()


def clear_config_cache() -> None:
    """Clear the singleton configuration cache."""

    global _CONFIG_CACHE
    _CONFIG_CACHE = None
