"""Tessera Archiver — Anomali tespiti ve raporu."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ..storage import ArchiverStorage
from .daily import _utcnow

log = logging.getLogger(__name__)

_10GB = 10 * 1024 ** 3


def detect_anomalies(storage: ArchiverStorage) -> dict:
    """
    archive/raw altındaki tüm arşivleri tarar ve anomalileri raporlar:
      - missing_checksum: .sha256 dosyası olmayan arşivler
      - missing_metadata: repo_info.json olmayan repolar
      - empty_archives: 0 byte arşivler
      - large_gt_10gb: 10 GB üzeri arşivler
    """
    anomalies: dict[str, list] = {
        "missing_checksum": [],
        "missing_metadata": [],
        "empty_archives": [],
        "large_gt_10gb": [],
    }

    raw_dir = storage.root / "raw"
    meta_dir = storage.root / "metadata"

    if not raw_dir.exists():
        report = {
            "report_type": "anomalies",
            "generated_at": _utcnow(),
            "total": 0,
            "anomalies": anomalies,
        }
        _write(storage, report)
        return report

    seen_missing_meta: set[str] = set()

    for arc in sorted(raw_dir.rglob("*.tar.gz")):
        # Repo key çıkar
        try:
            rel = arc.relative_to(raw_dir)
            parts = rel.parts  # (provider, ns..., repo, version, file.tar.gz)
            if len(parts) < 4:
                repo_key = str(rel)
            else:
                provider = parts[0]
                # version dir = parts[-2], file = parts[-1]
                # ns+repo = parts[1:-2]
                ns_repo = parts[1:-2]
                repo = ns_repo[-1]
                namespace = "/".join(ns_repo[:-1]) if len(ns_repo) > 1 else ""
                repo_key = f"{provider}:{namespace}/{repo}" if namespace else f"{provider}:{repo}"
        except Exception:
            repo_key = arc.name

        # Checksum kontrolü
        if not arc.with_name(f"{arc.name}.sha256").exists():
            anomalies["missing_checksum"].append({"repo": repo_key, "file": arc.name})

        # Boş arşiv
        size = arc.stat().st_size
        if size == 0:
            anomalies["empty_archives"].append({"repo": repo_key, "file": arc.name})

        # Aşırı büyük
        if size > _10GB:
            anomalies["large_gt_10gb"].append({
                "repo": repo_key,
                "file": arc.name,
                "size_gb": round(size / 1024 ** 3, 2),
            })

    # Metadata eksikliği: versions.json varsa ama repo_info.json yoksa
    for vfile in raw_dir.rglob("versions.json"):
        try:
            rel = vfile.parent.relative_to(raw_dir)
            parts = rel.parts  # (provider, ns..., repo)
            if len(parts) < 2:
                continue
            provider = parts[0]
            repo = parts[-1]
            namespace = "/".join(parts[1:-1]) if len(parts) > 2 else parts[1] if len(parts) == 2 else ""
            repo_key = f"{provider}:{namespace}/{repo}" if namespace else f"{provider}:{repo}"
        except Exception:
            continue

        if repo_key in seen_missing_meta:
            continue

        meta_path = meta_dir / "/".join(vfile.parent.relative_to(raw_dir).parts) / "repo_info.json"
        if not meta_path.exists():
            anomalies["missing_metadata"].append(repo_key)
            seen_missing_meta.add(repo_key)

    total = sum(len(v) for v in anomalies.values())
    report = {
        "report_type": "anomalies",
        "generated_at": _utcnow(),
        "total": total,
        "anomalies": anomalies,
    }
    _write(storage, report)
    log.info("Anomali raporu: %d anomali tespit edildi", total)
    return report


def _write(storage: ArchiverStorage, report: dict) -> None:
    out = storage.anomalies_report_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info("Anomali raporu: %s", out)
