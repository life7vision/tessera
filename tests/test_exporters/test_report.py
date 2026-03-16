"""Tests for the report exporter."""

from __future__ import annotations

import json
from pathlib import Path

from tessera.exporters.report import ReportExporter


def test_report_exporter_writes_json(sample_csv, tmp_path: Path):
    target = tmp_path / "report.json"
    result = ReportExporter({}).export("version-1", target, source_path=sample_csv)

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert result.success is True
    assert payload["row_count"] == 2

