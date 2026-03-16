"""Tests for the FastAPI web frontend."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path

from starlette.requests import Request

from tessera.core.audit import AuditLogger
from tessera.core.catalog import CatalogManager
from tessera.core.config import clear_config_cache
from tessera.web.app import create_app
from tessera.web.routes import api as api_routes
from tessera.web.routes import pages as page_routes


@dataclass
class FakeDatasetInfo:
    source: str
    source_ref: str
    name: str
    description: str
    tags: list[str] = field(default_factory=list)


def seed_web_data(base_path: Path) -> str:
    catalog = CatalogManager(base_path / "catalog.db")
    audit = AuditLogger(base_path / "audit.db")
    catalog.initialize()
    audit.initialize()

    dataset_id = catalog.register_dataset(
        FakeDatasetInfo(
            source="kaggle",
            source_ref="owner/demo",
            name="demo-dataset",
            description="Demo dataset for web",
            tags=["demo", "tabular"],
        )
    )
    version_id = catalog.register_version(
        dataset_id,
        {
            "version": "1.0.0",
            "checksum_sha256": "abc123abc123",
            "file_size_bytes": 2048,
            "file_count": 1,
            "raw_path": str(base_path / "raw.csv"),
            "processed_path": str(base_path / "processed.parquet"),
            "zone": "processed",
            "format": "parquet",
            "compression": "zstd",
            "row_count": 2,
            "column_count": 3,
        },
    )
    catalog.record_lineage(version_id, "download", "kaggle", status="success", duration_ms=10)
    audit.log("ingest", "dataset_version", resource_id=version_id, details={"dataset_id": dataset_id})
    return dataset_id


def build_config_yaml(data_path: Path) -> str:
    return "\n".join(
        [
            "project:",
            '  name: "web-test"',
            '  version: "0.1.0"',
            "storage:",
            f'  base_path: "{data_path}"',
            "  zones:",
            '    raw: "raw"',
            '    processed: "processed"',
            '    archive: "archive"',
            '    quarantine: "quarantine"',
            '  catalog_db: "catalog.db"',
            '  audit_db: "audit.db"',
            "ingestion:",
            '  default_connector: "kaggle"',
            '  checksum_algorithm: "sha256"',
            "  skip_existing: true",
            "  quarantine_on_fail: true",
            "processing:",
            '  default_format: "parquet"',
            '  compression: "zstd"',
            "  compression_level: 3",
            "  auto_profile: true",
            "versioning:",
            '  strategy: "semantic"',
            "  keep_versions: 5",
            "  archive_older: true",
            "connectors: {}",
            "validators: [integrity, schema, quality]",
            "transformers: [clean, format, compress]",
            "hooks:",
            "  pre_ingest: []",
            "  post_ingest: []",
            "  on_error: []",
            "logging:",
            '  level: "INFO"',
            '  format: "%(message)s"',
            "  file: null",
        ]
    )


def make_request(app, path: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("testclient", 123),
        "server": ("testserver", 80),
        "scheme": "http",
        "root_path": "",
        "http_version": "1.1",
        "app": app,
    }
    return Request(scope)


def test_create_app_and_routes(monkeypatch, tmp_path: Path):
    data_path = tmp_path / "data"
    data_path.mkdir()
    dataset_id = seed_web_data(data_path)
    config_path = tmp_path / "web-config.yaml"
    config_path.write_text(build_config_yaml(data_path), encoding="utf-8")
    monkeypatch.setenv("TESSERA_CONFIG", str(config_path))
    clear_config_cache()

    app = create_app()
    route_paths = {route.path for route in app.routes}

    assert app.title == "Tessera"
    assert "/" in route_paths
    assert "/search" in route_paths
    assert "/detail" in route_paths
    assert "/detail/{dataset_id}" in route_paths
    assert "/dataset/{dataset_id}" in route_paths
    assert "/pipeline" in route_paths
    assert "/api/v1/datasets" in route_paths
    assert "/api/v1/stats" in route_paths
    assert dataset_id


def test_page_and_api_routes(monkeypatch, tmp_path: Path):
    data_path = tmp_path / "data"
    data_path.mkdir()
    dataset_id = seed_web_data(data_path)
    config_path = tmp_path / "web-config.yaml"
    config_path.write_text(build_config_yaml(data_path), encoding="utf-8")
    monkeypatch.setenv("TESSERA_CONFIG", str(config_path))
    clear_config_cache()

    app = create_app()
    request = make_request(app, "/")

    home = asyncio.run(page_routes.home(request))
    search = asyncio.run(page_routes.search(request, q="demo"))
    detail = asyncio.run(page_routes.detail(request, dataset_id=dataset_id))
    pipeline = asyncio.run(page_routes.pipeline(request))
    detail_index = asyncio.run(page_routes.detail_index())
    detail_legacy = asyncio.run(page_routes.detail_legacy(dataset_id=dataset_id))
    api_list = asyncio.run(api_routes.list_datasets(request, q="demo"))
    api_detail = asyncio.run(api_routes.get_dataset(request, dataset_id=dataset_id))
    api_lineage = asyncio.run(api_routes.get_lineage(request, dataset_id=dataset_id))
    api_stats = asyncio.run(api_routes.get_stats(request))

    assert home.status_code == 200
    assert "Tessera" in home.body.decode("utf-8")
    assert search.status_code == 200
    assert "demo-dataset" in search.body.decode("utf-8")
    assert detail.status_code == 200
    assert "Lineage" in detail.body.decode("utf-8")
    assert pipeline.status_code == 200
    assert "ingest" in pipeline.body.decode("utf-8")
    assert detail_index.status_code == 307
    assert detail_index.headers["location"] == "/search"
    assert detail_legacy.status_code == 307
    assert detail_legacy.headers["location"] == f"/dataset/{dataset_id}"
    assert api_list["count"] == 1
    assert api_detail["dataset"]["id"] == dataset_id
    assert api_lineage["lineage"][0]["plugin_name"] == "kaggle"
    assert api_stats["total_datasets"] == 1
