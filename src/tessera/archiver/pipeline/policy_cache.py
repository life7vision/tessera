"""
Tessera Archiver — Policy result cache.

Startup'ta, her scan/pipeline bitişinde otomatik güncellenir.
Policy sayfası ve dashboard bu cache'den okur.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_lock = threading.Lock()
_cached: Optional[dict] = None


def get() -> Optional[dict]:
    """Son policy sonucunu döner. Henüz çalışmadıysa None."""
    return _cached


def refresh(catalog) -> dict:
    """Policy'i evaluate et ve cache'i güncelle."""
    global _cached
    from .policy import evaluate_policy

    try:
        result = evaluate_policy(catalog)
        data = {
            "passed": result.passed,
            "summary": result.summary,
            "total_repos": result.total_repos,
            "scanned_repos": result.scanned_repos,
            "missing_scans": result.missing_scans,
            "total_high": result.total_high,
            "total_medium": result.total_medium,
            "total_low": result.total_low,
            "violations": [v.__dict__ for v in result.violations[:100]],
            "violations_count": len(result.violations),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        with _lock:
            _cached = data
        log.info("Policy cache güncellendi: %s", result.summary)
        return data
    except Exception as exc:
        log.warning("Policy cache güncellenemedi: %s", exc)
        return {}


def refresh_async(catalog) -> None:
    """Policy'i arka planda (thread) evaluate et."""
    t = threading.Thread(target=refresh, args=(catalog,), daemon=True)
    t.start()
