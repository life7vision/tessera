"""Plugin discovery and lookup."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

from tessera.core.exceptions import ConfigError, PluginNotFoundError
from tessera.connectors.base import BaseConnector
from tessera.exporters.base import BaseExporter
from tessera.hooks.base import BaseHook
from tessera.transformers.base import BaseTransformer
from tessera.validators.base import BaseValidator


class PluginRegistry:
    """Discover and instantiate plugin classes."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self._connectors: dict[str, type[BaseConnector]] = {}
        self._validators: dict[str, type[BaseValidator]] = {}
        self._transformers: dict[str, type[BaseTransformer]] = {}
        self._exporters: dict[str, type[BaseExporter]] = {}
        self._hooks: dict[str, type[BaseHook]] = {}

    def discover_plugins(self) -> None:
        """Discover plugins from package namespaces."""

        self._discover_namespace("tessera.connectors", BaseConnector, self._connectors)
        self._discover_namespace("tessera.validators", BaseValidator, self._validators)
        self._discover_namespace("tessera.transformers", BaseTransformer, self._transformers)
        self._discover_namespace("tessera.exporters", BaseExporter, self._exporters)
        self._discover_namespace("tessera.hooks", BaseHook, self._hooks)

    def get_connector(self, name: str) -> BaseConnector:
        """Instantiate a connector plugin by name."""

        return self._create_plugin(name, self._connectors, "connectors")

    def get_validator(self, name: str) -> BaseValidator:
        """Instantiate a validator plugin by name."""

        return self._create_plugin(name, self._validators, "validators")

    def get_transformer(self, name: str) -> BaseTransformer:
        """Instantiate a transformer plugin by name."""

        return self._create_plugin(name, self._transformers, "transformers")

    def get_exporter(self, name: str) -> BaseExporter:
        """Instantiate an exporter plugin by name."""

        return self._create_plugin(name, self._exporters, "exporters")

    def get_hook(self, name: str) -> BaseHook:
        """Instantiate a hook plugin by name."""

        return self._create_plugin(name, self._hooks, "hooks")

    def list_plugins(self) -> dict[str, list[str]]:
        """Return discovered plugin names grouped by type."""

        return {
            "connectors": sorted(self._connectors),
            "validators": sorted(self._validators),
            "transformers": sorted(self._transformers),
            "exporters": sorted(self._exporters),
            "hooks": sorted(self._hooks),
        }

    def _create_plugin(self, name: str, registry: dict[str, type], group: str):
        plugin_class = registry.get(name)
        if plugin_class is None:
            raise PluginNotFoundError(f"Plugin bulunamadı: {group}.{name}")
        group_config = self.config.get(group, {})
        # group_config may be a list (e.g. validators: [integrity, schema]) or a dict
        plugin_config = group_config.get(name, {}) if isinstance(group_config, dict) else {}
        return plugin_class(plugin_config)

    def _discover_namespace(self, package_name: str, base_class: type, target: dict[str, type]) -> None:
        package = importlib.import_module(package_name)
        modules = self._iter_modules(package)
        for module in modules:
            for _, member in inspect.getmembers(module, inspect.isclass):
                if not issubclass(member, base_class) or member is base_class:
                    continue
                plugin_name = getattr(member, "name", None)
                plugin_version = getattr(member, "version", None)
                if not plugin_name or not plugin_version:
                    raise ConfigError(
                        f"Plugin sınıfında name ve version zorunludur: {member.__module__}.{member.__name__}"
                    )
                target[plugin_name] = member

    def _iter_modules(self, package: ModuleType) -> Iterable[ModuleType]:
        package_paths = getattr(package, "__path__", None)
        if package_paths is None:
            return []

        modules: list[ModuleType] = []
        for module_info in pkgutil.iter_modules(package_paths):
            if module_info.name == "base":
                continue
            module = importlib.import_module(f"{package.__name__}.{module_info.name}")
            modules.append(module)
        return modules

