"""
Tessera Archiver — Repository profil analizi (deterministik, LLM'siz).

github-archiver/repo_profiler.py'den taşındı.
Opsiyonel olarak Tessera'nın AWS Bedrock entegrasyonunu da kullanabilir.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Rule:
    domain: str
    app_type: str
    use_case: str
    keywords: tuple[str, ...]
    weight: int = 1


_RULES: tuple[_Rule, ...] = (
    _Rule("AI/ML", "SDK/Library",
          "AI model veya inference geliştirme",
          ("llm", "gpt", "transformer", "inference", "prompt", "rag",
           "agent", "ml", "machine-learning"), 2),
    _Rule("Web", "Frontend App",
          "Web arayüzü/SPA geliştirme",
          ("react", "vue", "angular", "nextjs", "next.js", "svelte",
           "frontend", "ui", "storybook"), 2),
    _Rule("Backend", "API/Service",
          "Backend API veya servis geliştirme",
          ("api", "backend", "server", "microservice", "fastapi",
           "flask", "django", "spring", "express"), 2),
    _Rule("DevOps", "Platform Tool",
          "CI/CD, container veya altyapı otomasyonu",
          ("kubernetes", "k8s", "docker", "terraform", "ansible",
           "helm", "devops", "ci", "cd"), 2),
    _Rule("Security", "Security Tool",
          "Güvenlik testi/tarama/analiz",
          ("security", "pentest", "vulnerability", "exploit", "owasp",
           "malware", "forensic", "ctf"), 2),
    _Rule("Data", "Data Tool",
          "Veri işleme/analitik/ETL",
          ("data", "etl", "analytics", "pipeline", "spark",
           "hadoop", "warehouse", "sql"), 2),
    _Rule("Mobile", "Mobile App",
          "Mobil uygulama geliştirme",
          ("android", "ios", "react-native", "flutter", "swiftui", "mobile"), 2),
    _Rule("Game", "Game/Engine",
          "Oyun veya oyun motoru geliştirme",
          ("game", "unity", "unreal", "godot", "phaser"), 2),
    _Rule("CLI", "CLI Tool",
          "Komut satırı aracı",
          ("cli", "command-line", "terminal", "shell"), 1),
    _Rule("Docs/Education", "Knowledge Base",
          "Dokümantasyon/eğitim içeriği",
          ("tutorial", "guide", "roadmap", "awesome", "book", "course", "docs"), 1),
)


def _normalize(repo_info: dict, languages: dict) -> str:
    name = str(repo_info.get("name", "")).lower()
    description = str(repo_info.get("description") or "").lower()
    topics = " ".join(str(t).lower() for t in (repo_info.get("topics") or []))
    lang_keys = " ".join(str(k).lower() for k in (languages or {}).keys())
    return f"{name} {description} {topics} {lang_keys}".strip()


def _top_language(languages: dict) -> str | None:
    if not languages:
        return None
    try:
        return max(languages.items(), key=lambda kv: int(kv[1] or 0))[0]
    except Exception:
        return next(iter(languages.keys()), None)


def analyze_repository(repo_info: dict, languages: dict) -> dict:
    """
    Deterministik kural tabanlı repo profil analizi.

    Returns:
        {
          purpose, domain, app_type, primary_use_case,
          confidence, signals, summary
        }
    """
    blob = _normalize(repo_info, languages)
    hits: list[tuple[_Rule, list[str]]] = []

    for rule in _RULES:
        matched = [kw for kw in rule.keywords if kw in blob]
        if matched:
            hits.append((rule, matched))

    if hits:
        best, matched = sorted(
            hits, key=lambda x: (x[0].weight, len(x[1])), reverse=True
        )[0]
        domain = best.domain
        app_type = best.app_type
        primary_use_case = best.use_case
        signals = matched[:6]
        confidence = min(95, 55 + len(matched) * 10 + (best.weight - 1) * 10)
    else:
        domain = "General"
        app_type = "Repository"
        primary_use_case = "Genel amaçlı yazılım veya kaynak kod deposu"
        signals = []
        confidence = 35

    lang = _top_language(languages) or repo_info.get("language") or "Unknown"
    repo_name = repo_info.get("name", "repo")
    purpose = f"{domain} odaklı {app_type.lower()} ({lang})"

    return {
        "purpose": purpose,
        "domain": domain,
        "app_type": app_type,
        "primary_use_case": primary_use_case,
        "confidence": confidence,
        "signals": signals,
        "summary": f"{repo_name}: {primary_use_case}.",
    }
