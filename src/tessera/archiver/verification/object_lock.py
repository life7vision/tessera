"""
Tessera Archiver — AWS S3 Object Lock önkontrol.

Immutability konfigürasyonunu doğrular.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def check_object_lock() -> dict:
    """
    Ortam değişkenlerinden S3 Object Lock konfigürasyonunu doğrular.
    CI/CD pipeline'larında kullanılmak üzere tasarlanmıştır.
    """
    issues: list[str] = []
    warnings: list[str] = []

    provider = os.environ.get("ARCHIVE_PROVIDER", "").lower()
    immutability = os.environ.get("IMMUTABILITY_ENABLED", "").lower()
    retention_days = os.environ.get("RETENTION_DAYS", "")

    if not provider:
        warnings.append("ARCHIVE_PROVIDER tanımlanmamış (aws/gcp/azure bekleniyor)")
    elif provider not in {"aws", "gcp", "azure"}:
        issues.append(f"Geçersiz ARCHIVE_PROVIDER: {provider!r}")

    if immutability not in {"true", "1", "yes"}:
        warnings.append("IMMUTABILITY_ENABLED=true değil — Object Lock aktif olmayabilir")

    if retention_days:
        try:
            days = int(retention_days)
            if days < 1:
                issues.append(f"RETENTION_DAYS geçersiz: {days}")
            elif days < 365:
                warnings.append(f"RETENTION_DAYS={days} — kurumsal politika genellikle ≥365 gün gerektirir")
        except ValueError:
            issues.append(f"RETENTION_DAYS sayısal değil: {retention_days!r}")
    else:
        warnings.append("RETENTION_DAYS tanımlanmamış")

    if provider == "aws":
        s3_lock = os.environ.get("S3_OBJECT_LOCK_ENABLED", "").lower()
        s3_mode = os.environ.get("S3_OBJECT_LOCK_MODE", "").upper()

        if s3_lock not in {"true", "1", "yes"}:
            issues.append("S3_OBJECT_LOCK_ENABLED=true değil")
        if s3_mode not in {"GOVERNANCE", "COMPLIANCE"}:
            issues.append(f"S3_OBJECT_LOCK_MODE geçersiz: {s3_mode!r} (GOVERNANCE veya COMPLIANCE bekleniyor)")

    passed = len(issues) == 0
    result = {
        "passed": passed,
        "provider": provider or "not-set",
        "issues": issues,
        "warnings": warnings,
    }

    if passed:
        log.info("Object Lock kontrolü geçildi%s", f" ({len(warnings)} uyarı)" if warnings else "")
    else:
        log.error("Object Lock kontrolü başarısız: %d sorun", len(issues))
        for issue in issues:
            log.error("  [FAIL] %s", issue)
    for w in warnings:
        log.warning("  [WARN] %s", w)

    return result
