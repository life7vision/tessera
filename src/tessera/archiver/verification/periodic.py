"""
Tessera Archiver — Periyodik bütünlük doğrulama işi.

github-archiver/periodic_verify.py'den taşındı ve Tessera storage/audit entegre edildi.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..storage import ArchiverStorage

log = logging.getLogger(__name__)


@dataclass
class VerifyResult:
    file: str
    status: str          # OK | MISMATCH | MISSING_SHA | INVALID_SHA_FILE
    expected: str = ""
    actual: str = ""
    note: str = ""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_expected(sha_path: Path) -> str | None:
    try:
        return sha_path.read_text().strip().splitlines()[0].split()[0]
    except Exception:
        return None


def run_verification(
    storage: ArchiverStorage,
    limit: int | None = None,
) -> dict:
    """
    archive/raw altındaki tüm .tar.gz dosyalarını checksum karşılaştırmasıyla doğrular.
    Sonuçları reports/verification/ altına yazar ve audit log'a ekler.
    """
    results: list[VerifyResult] = []
    raw_dir = storage.root / "raw"

    archives = sorted(raw_dir.rglob("*.tar.gz")) if raw_dir.exists() else []
    if limit and limit > 0:
        archives = archives[:limit]

    ok = fail = missing_sha = 0

    for arc in archives:
        sha_path = Path(str(arc) + ".sha256")
        try:
            rel = str(arc.relative_to(storage.root))
        except Exception:
            rel = arc.name

        if not sha_path.exists():
            missing_sha += 1
            fail += 1
            results.append(VerifyResult(
                file=rel, status="MISSING_SHA", note="sha256 sidecar dosyası yok"
            ))
            continue

        expected = _read_expected(sha_path)
        if not expected:
            fail += 1
            results.append(VerifyResult(
                file=rel, status="INVALID_SHA_FILE", note="sha256 dosyası parse edilemedi"
            ))
            continue

        actual = _sha256(arc)
        if actual == expected:
            ok += 1
            results.append(VerifyResult(file=rel, status="OK", expected=expected, actual=actual))
        else:
            fail += 1
            results.append(VerifyResult(
                file=rel, status="MISMATCH",
                expected=expected, actual=actual,
                note="Checksum uyuşmazlığı!",
            ))

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    report = {
        "report_type": "verification",
        "generated_at": now_str,
        "summary": {
            "total_checked": len(archives),
            "ok": ok,
            "fail": fail,
            "missing_sha": missing_sha,
        },
        "results": [r.__dict__ for r in results],
    }

    # Dosyaya yaz
    out = storage.verification_report_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info(
        "Verification tamamlandı: total=%d ok=%d fail=%d missing_sha=%d → %s",
        len(archives), ok, fail, missing_sha, out.name,
    )

    # Audit log
    try:
        from ..pipeline.archiver import _append_audit
        _append_audit(storage, "PERIODIC_VERIFY", {
            "report": out.name,
            "total_checked": len(archives),
            "ok": ok,
            "fail": fail,
            "missing_sha": missing_sha,
        })
    except Exception as exc:
        log.warning("Audit log yazılamadı: %s", exc)

    return report
