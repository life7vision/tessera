"""Tests for plugin registry behaviour."""

from __future__ import annotations

from pathlib import Path

import pytest

from tessera.core.exceptions import PluginNotFoundError
from tessera.core.registry import PluginRegistry
from tessera.transformers.base import BaseTransformer, TransformResult
from tessera.validators.base import BaseValidator, ValidationLevel, ValidationResult


def test_registry_discovers_phase_plugins():
    registry = PluginRegistry()
    registry.discover_plugins()

    plugins = registry.list_plugins()

    assert plugins == {
        "connectors": ["football_data", "github", "huggingface", "kaggle", "upload"],
        "validators": ["integrity", "quality", "schema"],
        "transformers": ["clean", "compress", "format"],
        "exporters": ["local", "report"],
        "hooks": ["lineage", "notify"],
    }


def test_registry_raises_for_missing_plugin():
    registry = PluginRegistry()
    registry.discover_plugins()

    with pytest.raises(PluginNotFoundError):
        registry.get_connector("missing")


def test_transformer_can_handle_by_extension(tmp_path: Path):
    class CsvTransformer(BaseTransformer):
        name = "csv_transformer"
        version = "1.0.0"
        input_formats = ["csv"]
        output_format = "parquet"

        def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
            return TransformResult(
                transformer_name=self.name,
                success=True,
                input_path=input_path,
                output_path=output_path,
                input_checksum="in",
                output_checksum="out",
                input_size=1,
                output_size=1,
                duration_ms=1,
                details={},
            )

    transformer = CsvTransformer({})
    assert transformer.can_handle(tmp_path / "data.csv") is True
    assert transformer.can_handle(tmp_path / "data.json") is False


def test_validation_result_passed_property():
    class DummyValidator(BaseValidator):
        name = "dummy"
        version = "1.0.0"

        def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult:
            return ValidationResult(
                validator_name=self.name,
                level=ValidationLevel.WARNING,
                issues=[],
                duration_ms=1,
            )

    result = DummyValidator({}).validate(Path("demo.csv"))
    assert result.passed is True
