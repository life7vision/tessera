"""Tessera Archiver — Aylık rapor üretici."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ..storage import ArchiverStorage
from .daily import _load_all_logs, _load_all_meta, _utcnow, _human

log = logging.getLogger(__name__)


def generate_monthly_report(storage: ArchiverStorage) -> dict:
    """Bu ayki arşivleme istatistiklerini içeren aylık rapor üretir."""
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    all_meta = _load_all_meta(storage)
    all_logs = _load_all_logs(storage)

    month_logs = [r for r in all_logs if r.get("archived_at", "").startswith(month)]
    month_size = sum(r.get("size_bytes", 0) for r in month_logs)

    # Domain ve dil dağılımı
    lang_dist: dict[str, int] = {}
    domain_dist: dict[str, int] = {}
    for r in all_meta:
        cl = r.get("classification") or {}
        lang = cl.get("category_language") or "other"
        domain = (r.get("analysis") or {}).get("domain") or "other"
        lang_dist[lang] = lang_dist.get(lang, 0) + 1
        domain_dist[domain] = domain_dist.get(domain, 0) + 1

    report = {
        "report_type": "monthly",
        "generated_at": _utcnow(),
        "month": month,
        "summary": {
            "total_repos": len(all_meta),
            "archived_this_month": len(month_logs),
            "size_this_month_bytes": month_size,
            "size_this_month": _human(month_size),
        },
        "distribution": {"by_language": lang_dist, "by_domain": domain_dist},
        "monthly_archives": [
            {
                "file": r.get("file"),
                "size": _human(r.get("size_bytes", 0)),
                "at": r.get("archived_at"),
            }
            for r in month_logs
        ],
    }

    out = storage.monthly_report_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info("Aylık rapor: %s", out)
    return report
