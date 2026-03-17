"""Tessera Archiver — Günlük rapor üretici."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ..storage import ArchiverStorage

log = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _human(b: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {u}"
        b //= 1024
    return f"{b:.1f} PB"


def _load_all_meta(storage: ArchiverStorage) -> list[dict]:
    records = []
    for f in (storage.root / "metadata").rglob("repo_info.json"):
        try:
            records.append(json.loads(f.read_text()))
        except Exception as exc:
            log.warning("Metadata okunamadı (%s): %s", f, exc)
    return records


def _load_all_logs(storage: ArchiverStorage) -> list[dict]:
    records = []
    for f in (storage.root / "metadata").rglob("archive_log.jsonl"):
        try:
            for line in f.open(encoding="utf-8"):
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        except Exception as exc:
            log.warning("Log okunamadı (%s): %s", f, exc)
    return records


def generate_daily_report(storage: ArchiverStorage) -> dict:
    """Bugünün arşivleme istatistiklerini içeren günlük rapor üretir."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_meta = _load_all_meta(storage)
    all_logs = _load_all_logs(storage)

    today_logs = [r for r in all_logs if r.get("archived_at", "").startswith(today)]
    total_size = sum(
        (r.get("archive") or {}).get("size_bytes", 0) for r in all_meta
    )

    lang_dist: dict[str, int] = {}
    topic_dist: dict[str, int] = {}
    for r in all_meta:
        cl = r.get("classification") or {}
        lang = cl.get("category_language") or "other"
        topic = cl.get("category_topic") or "other"
        lang_dist[lang] = lang_dist.get(lang, 0) + 1
        topic_dist[topic] = topic_dist.get(topic, 0) + 1

    top10 = sorted(
        [
            {
                "repo": (
                    f"{(r.get('source') or {}).get('provider', 'github')}"
                    f":{(r.get('source') or {}).get('namespace', (r.get('source') or {}).get('owner', ''))}"
                    f"/{(r.get('source') or {}).get('repo', '')}"
                ),
                "stars": (r.get("stats") or {}).get("stars", 0),
            }
            for r in all_meta
            if "source" in r and "stats" in r
        ],
        key=lambda x: x["stars"],
        reverse=True,
    )[:10]

    report = {
        "report_type": "daily",
        "generated_at": _utcnow(),
        "date": today,
        "summary": {
            "total_repos": len(all_meta),
            "archived_today": len(today_logs),
            "total_size_bytes": total_size,
            "total_size": _human(total_size),
        },
        "today_archives": [
            {
                "file": r.get("file"),
                "size": _human(r.get("size_bytes", 0)),
                "at": r.get("archived_at"),
            }
            for r in today_logs
        ],
        "distribution": {"by_language": lang_dist, "by_topic": topic_dist},
        "top10_stars": top10,
    }

    out = storage.daily_report_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info("Günlük rapor: %s", out)
    return report
