"""
Tessera Archiver — Güvenlik tarama motoru.

github-archiver/scanner.py'den taşındı ve Tessera mimarisine uyarlandı.
Harici bağımlılık gerektirmez; yalnızca stdlib + opsiyonel yara kullanır.
"""
from __future__ import annotations

import json
import logging
import math
import re
import tarfile
from datetime import datetime, timezone
from pathlib import Path

from ..models import FindingRecord, RepoRef, ScanReportRecord

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tespit Kuralları (20+)
# ---------------------------------------------------------------------------

_RULES: list[dict] = [
    # ── Yüksek Risk ─────────────────────────────────────────────────────────
    {
        "id": "NET001", "severity": "HIGH", "category": "download-execute",
        "description": "curl/wget pipe to shell — uzaktan kod çalıştırma",
        "pattern": r"(curl|wget)\s+['\"]?https?://[^\s'\"]+['\"]?\s*\|\s*(bash|sh|python3?|perl|ruby)",
    },
    {
        "id": "NET002", "severity": "HIGH", "category": "reverse-shell",
        "description": "Bash TCP reverse shell (/dev/tcp)",
        "pattern": r"bash\s+-i\s+>&?\s*/dev/tcp/[\d.]+/\d+",
    },
    {
        "id": "NET003", "severity": "HIGH", "category": "reverse-shell",
        "description": "Python socket reverse shell",
        "pattern": r"socket\.connect\s*\(\s*\(['\"][0-9.]+['\"]",
    },
    {
        "id": "NET004", "severity": "HIGH", "category": "reverse-shell",
        "description": "Netcat reverse shell (nc -e /bin/sh)",
        "pattern": r"\bnc\b.{0,20}-e\s+/bin/(bash|sh)",
    },
    {
        "id": "NET006", "severity": "HIGH", "category": "download-execute",
        "description": "PowerShell encoded command",
        "pattern": r"powershell(?:\.exe)?\s+.*(?:-enc|-encodedcommand)\s+[A-Za-z0-9+/=]{20,}",
        "flags": re.IGNORECASE,
    },
    {
        "id": "NET007", "severity": "HIGH", "category": "download-execute",
        "description": "Invoke-WebRequest + iex ile uzaktan kod çalıştırma",
        "pattern": r"(invoke-webrequest|iwr).{0,120}\|\s*(iex|invoke-expression)",
        "flags": re.IGNORECASE,
    },
    {
        "id": "CM001", "severity": "HIGH", "category": "cryptominer",
        "description": "Kripto madenci referansı (xmrig/stratum/coinhive)",
        "pattern": r"(xmrig|stratum\+tcp://|coinhive\.min|cryptonight|minexmr)",
        "flags": re.IGNORECASE,
    },
    {
        "id": "SC001", "severity": "HIGH", "category": "supply-chain",
        "description": "package.json postinstall/preinstall ile shell komutu",
        "files": ["package.json"],
        "pattern": r'"(pre|post)install"\s*:\s*"[^"]*?(curl |wget |bash |sh -c|python )[^"]*"',
    },
    {
        "id": "OB001", "severity": "HIGH", "category": "obfuscation",
        "description": "exec(base64.decode) — gizlenmiş Python yükleyici",
        "pattern": r"exec\s*\(\s*(base64\.b64decode|__import__\(['\"]base64['\"])[\s\.\(]",
    },
    # ── Orta Risk ────────────────────────────────────────────────────────────
    {
        "id": "OB002", "severity": "MEDIUM", "category": "obfuscation",
        "description": "eval(base64.b64decode) — gizlenmiş Python kodu",
        "pattern": r"eval\s*\(\s*base64\.b64decode\s*\(",
    },
    {
        "id": "OB003", "severity": "MEDIUM", "category": "obfuscation",
        "description": "JavaScript eval(atob()) — gizlenmiş JS kodu",
        "pattern": r"eval\s*\(\s*atob\s*\(",
    },
    {
        "id": "SEC001", "severity": "MEDIUM", "category": "hardcoded-secret",
        "description": "PEM özel anahtarı gömülü",
        "pattern": r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----",
    },
    {
        "id": "SEC002", "severity": "MEDIUM", "category": "hardcoded-secret",
        "description": "Hardcoded AWS Secret Access Key",
        "pattern": r"(?i)aws_secret_access_key\s*[=:]\s*['\"]?[A-Za-z0-9/+]{30,}",
    },
    {
        "id": "SEC003", "severity": "MEDIUM", "category": "hardcoded-secret",
        "description": "GitHub token benzeri dize",
        "pattern": r"\b(?:ghp|gho|ghu|ghs|github_pat)_[A-Za-z0-9_]{20,}\b",
    },
    {
        "id": "SEC004", "severity": "MEDIUM", "category": "hardcoded-secret",
        "description": "Slack token benzeri dize",
        "pattern": r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
    },
    {
        "id": "DEF001", "severity": "MEDIUM", "category": "destructive",
        "description": "rm -rf / — yıkıcı dosya silme",
        "pattern": r"\brm\b\s+-[rRfF]{1,3}\s+(/\s|/\*|/$)",
    },
    {
        "id": "SC002", "severity": "MEDIUM", "category": "supply-chain",
        "description": "setup.py içinde subprocess çağrısı",
        "files": ["setup.py"],
        "pattern": r"subprocess\.(run|Popen|call|check_output)\s*\(\s*\[?['\"]",
    },
    # ── Düşük Risk ───────────────────────────────────────────────────────────
    {
        "id": "SPY001", "severity": "LOW", "category": "spyware",
        "description": "Keylogger kütüphanesi (pynput)",
        "pattern": r"(from\s+pynput|import\s+pynput)",
    },
    {
        "id": "SPY002", "severity": "LOW", "category": "sensitive-file",
        "description": "/etc/passwd veya /etc/shadow erişimi",
        "pattern": r"/etc/(passwd|shadow|sudoers)",
    },
    {
        "id": "NET005", "severity": "LOW", "category": "network",
        "description": "Yaygın reverse shell portu (4444/5555/1337/31337)",
        "pattern": (
            r"(?i)("
            r"/dev/tcp/[0-9.]+/(4444|5555|1337|31337)|"
            r"\b(?:nc|ncat|netcat|socat)\b[^\n]{0,80}\b(4444|5555|1337|31337)\b|"
            r"\bsocket\.connect\s*\([^)]{0,80}\b(4444|5555|1337|31337)\b"
            r")"
        ),
    },
]

# ---------------------------------------------------------------------------
# Dosya filtresi sabitleri
# ---------------------------------------------------------------------------

_TEXT_EXTS = frozenset({
    ".py", ".js", ".ts", ".jsx", ".tsx", ".mjs", ".cjs",
    ".sh", ".bash", ".zsh", ".fish",
    ".rb", ".pl", ".php", ".go", ".java", ".c", ".cpp", ".h",
    ".rs", ".swift", ".kt", ".cs", ".scala",
    ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".env",
    ".md", ".txt", ".dockerfile", ".gemspec",
})

_ALWAYS_SCAN = frozenset({
    "package.json", "setup.py", "setup.cfg", "pyproject.toml",
    "makefile", "dockerfile", ".env", ".env.example",
    "gemfile", "cargo.toml", "go.mod",
})

_MAX_FILE_SIZE = 5 * 1024 * 1024        # 5 MB
_MAX_MEMBERS = 100_000
_MAX_UNCOMPRESSED = 5 * 1024 * 1024 * 1024   # 5 GB
_MAX_SCANNED_BYTES = 256 * 1024 * 1024  # 256 MB
_YARA_TIMEOUT = 5


# ---------------------------------------------------------------------------
# Dosya filtresi
# ---------------------------------------------------------------------------

def _is_scannable(path: Path, size: int) -> bool:
    if size == 0 or size > _MAX_FILE_SIZE:
        return False
    name = path.name.lower()
    if name in _ALWAYS_SCAN:
        return True
    return path.suffix.lower() in _TEXT_EXTS


def _is_likely_binary(raw: bytes) -> bool:
    if not raw:
        return False
    if b"\x00" in raw:
        return True
    ctrl = sum(1 for b in raw if b < 9 or (13 < b < 32))
    return (ctrl / max(len(raw), 1)) > 0.3


# ---------------------------------------------------------------------------
# Entropi & heuristik
# ---------------------------------------------------------------------------

def _shannon_entropy(text: str) -> float:
    if not text:
        return 0.0
    freq: dict[str, int] = {}
    for c in text:
        freq[c] = freq.get(c, 0) + 1
    n = len(text)
    return -sum((cnt / n) * math.log2(cnt / n) for cnt in freq.values())


_SECRET_KW_RX = re.compile(
    r"(?i)\b(secret|token|api[_-]?key|password|passwd|auth|credential|bearer)\b"
)
_SECRET_CAND_RX = re.compile(
    r"(?<![A-Za-z0-9+/=_-])([A-Za-z0-9+/=_-]{32,})(?![A-Za-z0-9+/=_-])"
)


def _heuristic_secret_findings(content: str, filepath: str) -> list[FindingRecord]:
    results = []
    for idx, line in enumerate(content.splitlines(), start=1):
        if not _SECRET_KW_RX.search(line):
            continue
        for m in _SECRET_CAND_RX.finditer(line):
            candidate = m.group(1)
            if len(set(candidate)) < 8:
                continue
            if _shannon_entropy(candidate) < 3.5:
                continue
            results.append(FindingRecord(
                severity="MEDIUM",
                category="hardcoded-secret",
                file=filepath,
                line=idx,
                description="Yüksek entropy'li olası sır/token",
                snippet=line.strip()[:120],
            ))
            break
    return results


def _dependency_policy_findings(content: str, filepath: str) -> list[FindingRecord]:
    results = []
    name = Path(filepath).name.lower()
    suffix = Path(filepath).suffix.lower()

    # requirements*.txt
    if name.startswith("requirements") and suffix == ".txt":
        for i, raw in enumerate(content.splitlines(), start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith(("-r ", "--requirement ", "--index-url", "--extra-index-url", "--trusted-host")):
                continue
            if line.startswith(("-e ", "git+", "http://", "https://")):
                results.append(FindingRecord(
                    severity="HIGH", category="supply-chain",
                    file=filepath, line=i,
                    description="Doğrudan URL/VCS bağımlılığı (yüksek supply-chain riski)",
                    snippet=raw.strip()[:120],
                ))
                continue
            if "==" not in line:
                results.append(FindingRecord(
                    severity="MEDIUM", category="dependency-policy",
                    file=filepath, line=i,
                    description="Pinlenmemiş Python bağımlılığı",
                    snippet=raw.strip()[:120],
                ))

    # package.json
    if name == "package.json":
        try:
            data = json.loads(content)
        except Exception:
            return results

        for section in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
            for dep, ver in (data.get(section) or {}).items():
                if not isinstance(ver, str):
                    continue
                v = ver.strip()
                if not v:
                    continue
                if v in {"*", "latest"} or "x" in v.lower():
                    results.append(FindingRecord(
                        severity="MEDIUM", category="dependency-policy",
                        file=filepath, line=1,
                        description=f"Gevşek npm sürüm aralığı: {dep}={v}",
                        snippet=f"{dep}: {v}"[:120],
                    ))
                elif v.startswith(("^", "~", ">", "<")):
                    results.append(FindingRecord(
                        severity="LOW", category="dependency-policy",
                        file=filepath, line=1,
                        description=f"Semver aralığı kullanımı: {dep}={v}",
                        snippet=f"{dep}: {v}"[:120],
                    ))

        for key in ("preinstall", "install", "postinstall"):
            cmd = (data.get("scripts") or {}).get(key)
            if not isinstance(cmd, str):
                continue
            if any(k in cmd.lower() for k in ("curl ", "wget ", "powershell", "invoke-webrequest", "iwr ")):
                results.append(FindingRecord(
                    severity="HIGH", category="supply-chain",
                    file=filepath, line=1,
                    description=f"{key} scriptinde uzaktan kod indirme/çalıştırma",
                    snippet=cmd.strip()[:120],
                ))

    return results


# ---------------------------------------------------------------------------
# YARA (opsiyonel)
# ---------------------------------------------------------------------------

_YARA_COMPILED: list[tuple[str, object]] | None = None
_YARA_ATTEMPTED = False


def _load_yara_rules(rules_dir: Path | None = None) -> list[tuple[str, object]]:
    global _YARA_COMPILED, _YARA_ATTEMPTED
    if _YARA_ATTEMPTED:
        return _YARA_COMPILED or []
    _YARA_ATTEMPTED = True
    _YARA_COMPILED = []

    if rules_dir is None:
        rules_dir = Path("rules/yara")
    if not rules_dir.exists():
        return []

    try:
        import yara  # type: ignore
    except ImportError:
        log.debug("yara modülü bulunamadı; YARA taraması pas geçildi.")
        return []

    for rule_file in sorted(rules_dir.glob("*.yar")):
        try:
            _YARA_COMPILED.append((rule_file.name, yara.compile(filepath=str(rule_file))))
        except Exception as exc:
            log.warning("YARA kuralı yüklenemedi (%s): %s", rule_file.name, exc)
    return _YARA_COMPILED


def _yara_findings(raw: bytes, filepath: str, rules_dir: Path | None = None) -> list[FindingRecord]:
    results = []
    for _, compiled in _load_yara_rules(rules_dir):
        try:
            matches = compiled.match(data=raw, timeout=_YARA_TIMEOUT)
        except Exception:
            continue
        for m in matches:
            meta = getattr(m, "meta", {}) or {}
            tags = {str(t).lower() for t in getattr(m, "tags", [])}
            sev = str(meta.get("severity", "")).upper()
            if sev not in {"HIGH", "MEDIUM", "LOW"}:
                sev = "HIGH" if "high" in tags else ("MEDIUM" if "medium" in tags else "LOW")
            results.append(FindingRecord(
                severity=sev,  # type: ignore[arg-type]
                category="yara",
                file=filepath,
                line=0,
                description=str(meta.get("description") or f"YARA: {getattr(m, 'rule', 'unknown')}"),
                snippet=str(getattr(m, "rule", ""))[:120],
            ))
    return results


# ---------------------------------------------------------------------------
# İçerik tarama motoru
# ---------------------------------------------------------------------------

def _scan_content(content: str, filepath: str) -> list[FindingRecord]:
    results: list[FindingRecord] = []
    fname = Path(filepath).name.lower()
    lines = content.splitlines()
    seen: set[tuple[str, int, str]] = set()

    for rule in _RULES:
        allowed = rule.get("files")
        if allowed and fname not in allowed:
            continue
        try:
            rx = re.compile(rule["pattern"], rule.get("flags", 0))
        except re.error:
            continue
        for m in rx.finditer(content):
            line_no = content[: m.start()].count("\n") + 1
            snippet = lines[line_no - 1].strip()[:120] if line_no <= len(lines) else ""
            key = (rule["id"], line_no, snippet)
            if key in seen:
                continue
            seen.add(key)
            results.append(FindingRecord(
                severity=rule["severity"],  # type: ignore[arg-type]
                category=rule["category"],
                file=filepath,
                line=line_no,
                description=rule["description"],
                snippet=snippet,
            ))

    results.extend(_heuristic_secret_findings(content, filepath))
    results.extend(_dependency_policy_findings(content, filepath))
    return results


def _is_safe_member(member: tarfile.TarInfo) -> bool:
    p = Path(member.name)
    if p.is_absolute():
        return False
    if any(part == ".." for part in p.parts):
        return False
    if member.issym() or member.islnk():
        return False
    return True


def _engine_finding(severity: str, category: str, file: str, description: str, snippet: str = "") -> FindingRecord:
    return FindingRecord(
        severity=severity,  # type: ignore[arg-type]
        category=category,
        file=file,
        line=0,
        description=description,
        snippet=snippet[:120],
    )


# ---------------------------------------------------------------------------
# Ana tarama fonksiyonu
# ---------------------------------------------------------------------------

def scan_archive(
    archive_path: Path,
    yara_rules_dir: Path | None = None,
) -> ScanReportRecord:
    """
    tar.gz arşivini güvenli ve limitli şekilde stream ederek tarar.
    Tessera ScanReportRecord modeli döner.
    """
    findings: list[FindingRecord] = []
    files_scanned = 0
    error = ""

    try:
        total_uncompressed = 0
        total_scanned = 0

        with tarfile.open(archive_path, "r:gz") as tf:
            members = tf.getmembers()

            if len(members) > _MAX_MEMBERS:
                error = f"Tar üye sayısı limitini aşıyor ({len(members)} > {_MAX_MEMBERS})"
                findings.append(_engine_finding(
                    "HIGH", "archive-safety", archive_path.name,
                    "Aşırı tar üye sayısı (olası archive bomb)", f"members={len(members)}",
                ))
                return _build_report(archive_path.name, findings, files_scanned, error)

            for member in members:
                if not _is_safe_member(member) or not member.isreg():
                    continue

                total_uncompressed += member.size
                if total_uncompressed > _MAX_UNCOMPRESSED:
                    error = f"Açılmış boyut limiti aşıldı ({total_uncompressed})"
                    findings.append(_engine_finding(
                        "HIGH", "archive-safety", member.name,
                        "Aşırı açılmış boyut (archive bomb)", f"total={total_uncompressed}",
                    ))
                    return _build_report(archive_path.name, findings, files_scanned, error)

                p = Path(member.name)
                if not _is_scannable(p, member.size):
                    continue

                fobj = tf.extractfile(member)
                if fobj is None:
                    continue
                raw = fobj.read(_MAX_FILE_SIZE + 1)
                if not raw or len(raw) > _MAX_FILE_SIZE:
                    continue

                # YARA (binary dahil)
                findings.extend(_yara_findings(raw, member.name, yara_rules_dir))

                if _is_likely_binary(raw[:2048]):
                    continue

                total_scanned += len(raw)
                if total_scanned > _MAX_SCANNED_BYTES:
                    error = f"Tarama byte limiti aşıldı ({total_scanned})"
                    findings.append(_engine_finding(
                        "MEDIUM", "scan-limit", member.name,
                        "Tarama byte limiti aşıldı, analiz kısmi", f"scanned={total_scanned}",
                    ))
                    return _build_report(archive_path.name, findings, files_scanned, error)

                files_scanned += 1
                try:
                    content = raw.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                findings.extend(_scan_content(content, member.name))

    except Exception as exc:
        error = str(exc)
        log.error("Tarama hatası: %s", exc)

    return _build_report(archive_path.name, findings, files_scanned, error)


def _build_report(
    target: str,
    findings: list[FindingRecord],
    files_scanned: int,
    error: str,
) -> ScanReportRecord:
    """Finding listesinden ScanReportRecord üretir."""
    high = sum(1 for f in findings if f.severity == "HIGH")
    medium = sum(1 for f in findings if f.severity == "MEDIUM")
    low = sum(1 for f in findings if f.severity == "LOW")

    if high > 0:
        risk_level = "HIGH"
    elif medium > 0:
        risk_level = "MEDIUM"
    elif low > 0:
        risk_level = "LOW"
    else:
        risk_level = "CLEAN"

    return ScanReportRecord(
        repo_key="",      # Çağıran tarafından doldurulur
        version="",       # Çağıran tarafından doldurulur
        archive_id="",    # Çağıran tarafından doldurulur
        risk_level=risk_level,  # type: ignore[arg-type]
        is_clean=(risk_level == "CLEAN"),
        files_scanned=files_scanned,
        high_count=high,
        medium_count=medium,
        low_count=low,
        total_findings=len(findings),
        scanned_at=datetime.now(timezone.utc),
        error=error,
        findings=findings,
    )


# ---------------------------------------------------------------------------
# Scan raporunu dosyaya kaydet (geriye uyumluluk — versions/scan_report.json)
# ---------------------------------------------------------------------------

def save_scan_report(report: ScanReportRecord, dest: Path) -> None:
    """scan_report.json formatında diske yazar."""
    data = {
        "target": dest.parent.parent.name,
        "risk_level": report.risk_level,
        "is_clean": report.is_clean,
        "files_scanned": report.files_scanned,
        "error": report.error,
        "scanned_at": report.scanned_at.isoformat() if report.scanned_at else "",
        "summary": {
            "high": report.high_count,
            "medium": report.medium_count,
            "low": report.low_count,
            "total": report.total_findings,
        },
        "findings": [f.model_dump() for f in report.findings],
    }
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(data, indent=2))
