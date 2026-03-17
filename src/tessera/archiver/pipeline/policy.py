"""
Tessera Archiver — Güvenlik politika kapısı (CI/CD gate).

github-archiver/security_gate.py'den taşındı ve Tessera catalog'una entegre edildi.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from ..catalog import ArchiverCatalog
from ..config import PolicyConfig, get_archiver_config
from ..models import ScanReportRecord

log = logging.getLogger(__name__)


@dataclass
class PolicyViolation:
    repo_key: str
    version: str
    reason: str


@dataclass
class PolicyResult:
    passed: bool
    total_repos: int
    scanned_repos: int
    missing_scans: int
    total_high: int
    total_medium: int
    total_low: int
    violations: list[PolicyViolation]

    @property
    def summary(self) -> str:
        if self.passed:
            return (
                f"PASS: Güvenlik politikası geçildi. "
                f"({self.scanned_repos} repo, "
                f"HIGH={self.total_high} MEDIUM={self.total_medium} LOW={self.total_low})"
            )
        return (
            f"FAIL: {len(self.violations)} ihlal. "
            f"HIGH={self.total_high} MEDIUM={self.total_medium} LOW={self.total_low}"
        )


def _risk_score(level: str) -> int:
    return {"CLEAN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(level.upper(), 99)


def evaluate_policy(
    catalog: ArchiverCatalog,
    policy: PolicyConfig | None = None,
    allow_missing: bool = False,
) -> PolicyResult:
    """
    Tüm repoların güncel scan raporlarını değerlendirir.
    policy=None ise config/default.yaml'daki archiver.policy kullanılır.
    """
    if policy is None:
        policy = get_archiver_config().policy

    repos = catalog.list_repos(limit=10_000)
    violations: list[PolicyViolation] = []
    missing_scans = 0
    scans: list[ScanReportRecord] = []

    now = datetime.now(timezone.utc)

    for repo in repos:
        scan = catalog.get_latest_scan(repo.key)

        if scan is None:
            missing_scans += 1
            if not allow_missing:
                violations.append(PolicyViolation(
                    repo_key=repo.key,
                    version=repo.current_version or "?",
                    reason="Scan raporu yok",
                ))
            continue

        scans.append(scan)

        # Scan yaş kontrolü
        if scan.scanned_at:
            age_hours = (now - scan.scanned_at).total_seconds() / 3600
            if age_hours > policy.max_scan_age_hours:
                violations.append(PolicyViolation(
                    repo_key=repo.key,
                    version=scan.version,
                    reason=f"Scan raporu eski: {age_hours:.1f}h > {policy.max_scan_age_hours}h",
                ))

        # HIGH/MEDIUM bulgu sayıları
        if scan.high_count > policy.max_high:
            violations.append(PolicyViolation(
                repo_key=repo.key,
                version=scan.version,
                reason=f"HIGH={scan.high_count} > max {policy.max_high}",
            ))

        if scan.medium_count > policy.max_medium:
            violations.append(PolicyViolation(
                repo_key=repo.key,
                version=scan.version,
                reason=f"MEDIUM={scan.medium_count} > max {policy.max_medium}",
            ))

        # Risk seviyesi
        if _risk_score(scan.risk_level) > _risk_score(policy.max_risk_level):
            violations.append(PolicyViolation(
                repo_key=repo.key,
                version=scan.version,
                reason=f"risk_level={scan.risk_level} > max {policy.max_risk_level}",
            ))

    total_high = sum(s.high_count for s in scans)
    total_medium = sum(s.medium_count for s in scans)
    total_low = sum(s.low_count for s in scans)

    result = PolicyResult(
        passed=len(violations) == 0,
        total_repos=len(repos),
        scanned_repos=len(scans),
        missing_scans=missing_scans,
        total_high=total_high,
        total_medium=total_medium,
        total_low=total_low,
        violations=violations,
    )

    if result.passed:
        log.info(result.summary)
    else:
        log.warning(result.summary)
        for v in violations[:20]:
            log.warning("  [FAIL] %s [%s]: %s", v.repo_key, v.version, v.reason)

    return result


def evaluate_single(
    scan: ScanReportRecord,
    policy: PolicyConfig | None = None,
) -> PolicyResult:
    """Tek bir scan raporunu politikaya göre değerlendirir."""
    if policy is None:
        policy = get_archiver_config().policy

    violations: list[PolicyViolation] = []
    now = datetime.now(timezone.utc)

    if scan.scanned_at:
        age_hours = (now - scan.scanned_at).total_seconds() / 3600
        if age_hours > policy.max_scan_age_hours:
            violations.append(PolicyViolation(
                repo_key=scan.repo_key, version=scan.version,
                reason=f"Scan yaşı {age_hours:.1f}h > {policy.max_scan_age_hours}h",
            ))

    if scan.high_count > policy.max_high:
        violations.append(PolicyViolation(
            repo_key=scan.repo_key, version=scan.version,
            reason=f"HIGH={scan.high_count} > max {policy.max_high}",
        ))

    if scan.medium_count > policy.max_medium:
        violations.append(PolicyViolation(
            repo_key=scan.repo_key, version=scan.version,
            reason=f"MEDIUM={scan.medium_count} > max {policy.max_medium}",
        ))

    if _risk_score(scan.risk_level) > _risk_score(policy.max_risk_level):
        violations.append(PolicyViolation(
            repo_key=scan.repo_key, version=scan.version,
            reason=f"risk_level={scan.risk_level} > max {policy.max_risk_level}",
        ))

    return PolicyResult(
        passed=len(violations) == 0,
        total_repos=1,
        scanned_repos=1,
        missing_scans=0,
        total_high=scan.high_count,
        total_medium=scan.medium_count,
        total_low=scan.low_count,
        violations=violations,
    )
