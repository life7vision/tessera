# TESSERA — Faz 9: Web Frontend Spesifikasyonu
## FastAPI + Jinja2 + Vanilla CSS — Minimal Arama Arayüzü

> Google Dataset Search sadeliği + Anthropic tasarım dili

---

## 1. TEKNOLOJİ KARARI

**Backend:** FastAPI (REST API + HTML serving)
**Template:** Jinja2
**Frontend:** Vanilla HTML/CSS/JS — framework yok
**Stil:** Özel CSS — Anthropic Sans, flat, beyaz, bol boşluk
**Neden React/Vue değil:** Arama + katalog tarama + detay görüntüleme için overkill. 4 sayfa, sıfır build step.

---

## 2. DİZİN YAPISI (Mevcut projeye eklenen dosyalar)

```
src/tessera/
├── web/
│   ├── __init__.py
│   ├── app.py                # FastAPI app factory
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── pages.py          # HTML sayfaları (Jinja2)
│   │   └── api.py            # JSON API endpoints
│   ├── templates/
│   │   ├── base.html         # Ana layout (nav, footer)
│   │   ├── home.html         # Ana sayfa — arama
│   │   ├── search.html       # Arama sonuçları
│   │   ├── detail.html       # Dataset detay sayfası
│   │   └── pipeline.html     # Pipeline durumu / son ingest'ler
│   └── static/
│       ├── style.css         # Tek CSS dosyası
│       └── app.js            # Minimal JS (search, filter)
```

---

## 3. BAĞIMLILIKLAR (pyproject.toml'a ekle)

```toml
# Mevcut dependencies'e ekle:
"fastapi>=0.109",
"uvicorn[standard]>=0.27",
"jinja2>=3.1",
```

---

## 4. CLI KOMUTU (cli/main.py'ye ekle)

```bash
tessera web [--host 0.0.0.0] [--port 8000] [--reload]
```

```python
@cli.command()
@click.option("--host", default="127.0.0.1")
@click.option("--port", default=8000, type=int)
@click.option("--reload", is_flag=True, help="Dev mode — auto reload")
def web(host, port, reload):
    """Web arayüzünü başlat."""
    import uvicorn
    uvicorn.run("tessera.web.app:create_app", host=host, port=port, reload=reload, factory=True)
```

---

## 5. FastAPI App (web/app.py)

```python
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

def create_app() -> FastAPI:
    app = FastAPI(title="Tessera", version="0.1.0")

    # Static files
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # Templates
    templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

    # Tessera core bağlantısı
    from tessera.core.config import load_config
    from tessera.core.catalog import CatalogManager
    from tessera.core.storage import StorageManager
    from tessera.core.audit import AuditLogger

    config = load_config()
    catalog = CatalogManager(Path(config.storage.base_path) / config.storage.catalog_db)
    catalog.initialize()

    # State'e ekle — route'lardan erişim için
    app.state.config = config
    app.state.catalog = catalog
    app.state.templates = templates

    # Routes
    from tessera.web.routes.pages import router as pages_router
    from tessera.web.routes.api import router as api_router
    app.include_router(pages_router)
    app.include_router(api_router, prefix="/api/v1")

    return app
```

---

## 6. SAYFALAR (routes/pages.py)

```python
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Ana sayfa — arama kutusu + istatistikler."""
    catalog = request.app.state.catalog
    stats = catalog.get_stats()
    return request.app.state.templates.TemplateResponse("home.html", {
        "request": request,
        "stats": stats,
    })

@router.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = "", source: str = "", tag: str = "", zone: str = ""):
    """Arama sonuçları sayfası."""
    catalog = request.app.state.catalog
    tags = [t.strip() for t in tag.split(",") if t.strip()] if tag else None
    results = catalog.search_datasets(
        query=q or None,
        source=source or None,
        tags=tags,
    )
    return request.app.state.templates.TemplateResponse("search.html", {
        "request": request,
        "query": q,
        "source": source,
        "tag": tag,
        "zone": zone,
        "results": results,
        "count": len(results),
    })

@router.get("/dataset/{dataset_id}", response_class=HTMLResponse)
async def detail(request: Request, dataset_id: str):
    """Dataset detay sayfası — versiyonlar, lineage, metadata."""
    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    versions = catalog.get_versions(dataset_id)
    latest = catalog.get_latest_version(dataset_id)
    lineage = catalog.get_lineage(latest["id"]) if latest else []
    return request.app.state.templates.TemplateResponse("detail.html", {
        "request": request,
        "dataset": dataset,
        "versions": versions,
        "latest": latest,
        "lineage": lineage,
    })

@router.get("/pipeline", response_class=HTMLResponse)
async def pipeline(request: Request):
    """Son pipeline çalışmaları ve durum."""
    catalog = request.app.state.catalog
    audit = request.app.state.catalog  # Audit log'dan son ingest'leri çek
    # Son 20 audit kaydı
    return request.app.state.templates.TemplateResponse("pipeline.html", {
        "request": request,
    })
```

---

## 7. API ENDPOINTS (routes/api.py)

```python
from fastapi import APIRouter, Request

router = APIRouter()

@router.get("/datasets")
async def list_datasets(request: Request, q: str = "", source: str = "", tag: str = "", limit: int = 50):
    """JSON API — dataset listesi."""
    catalog = request.app.state.catalog
    tags = [t.strip() for t in tag.split(",") if t.strip()] if tag else None
    results = catalog.search_datasets(query=q or None, source=source or None, tags=tags)
    return {"count": len(results), "datasets": results[:limit]}

@router.get("/datasets/{dataset_id}")
async def get_dataset(request: Request, dataset_id: str):
    """JSON API — dataset detay."""
    catalog = request.app.state.catalog
    dataset = catalog.get_dataset(dataset_id)
    versions = catalog.get_versions(dataset_id)
    return {"dataset": dataset, "versions": versions}

@router.get("/datasets/{dataset_id}/lineage")
async def get_lineage(request: Request, dataset_id: str):
    """JSON API — lineage zinciri."""
    catalog = request.app.state.catalog
    latest = catalog.get_latest_version(dataset_id)
    lineage = catalog.get_lineage(latest["id"]) if latest else []
    return {"lineage": lineage}

@router.get("/stats")
async def get_stats(request: Request):
    """JSON API — arşiv istatistikleri."""
    return request.app.state.catalog.get_stats()
```

---

## 8. TEMPLATE YAPISI

### 8.1 base.html — Ana Layout

```html
<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Tessera{% endblock %}</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <nav class="nav">
        <a href="/" class="nav-brand">
            <svg class="nav-logo" viewBox="0 0 20 20" width="20" height="20">
                <rect x="2" y="2" width="7" height="7" rx="1.5" fill="#534AB7" opacity="0.9"/>
                <rect x="11" y="2" width="7" height="7" rx="1.5" fill="#1D9E75" opacity="0.9"/>
                <rect x="2" y="11" width="7" height="7" rx="1.5" fill="#D85A30" opacity="0.9"/>
                <rect x="11" y="11" width="7" height="7" rx="1.5" fill="#378ADD" opacity="0.9"/>
            </svg>
            <span>Tessera</span>
        </a>
        {% block nav_search %}{% endblock %}
        <div class="nav-links">
            <a href="/" class="{% if active == 'home' %}active{% endif %}">Ara</a>
            <a href="/search" class="{% if active == 'catalog' %}active{% endif %}">Katalog</a>
            <a href="/pipeline" class="{% if active == 'pipeline' %}active{% endif %}">Pipeline</a>
        </div>
    </nav>
    <main>
        {% block content %}{% endblock %}
    </main>
    <script src="/static/app.js"></script>
</body>
</html>
```

### 8.2 home.html — Ana Sayfa

Google Dataset Search tarzı: Ortada logo, arama kutusu, örnek sorgular, altta istatistik kartları.

```html
{% extends "base.html" %}
{% block title %}Tessera — Veri Arşivi{% endblock %}
{% block content %}
<div class="home">
    <div class="home-hero">
        <div class="home-brand">
            <svg viewBox="0 0 20 20" width="36" height="36"><!-- tessera logo --></svg>
            <h1>Tessera</h1>
        </div>
        <p class="home-subtitle">Veri setlerini ara, kesfet, arsivle</p>

        <form action="/search" method="get" class="search-form">
            <input type="text" name="q" placeholder="Dataset ara..." class="search-input" autofocus>
            <button type="submit" class="search-btn">
                <!-- search icon SVG -->
            </button>
        </form>

        <div class="search-hints">
            <a href="/search?source=kaggle">kaggle</a>
            <a href="/search?tag=classification">classification</a>
            <a href="/search?tag=nlp">nlp</a>
        </div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{{ stats.total_datasets }}</div>
            <div class="stat-label">Dataset</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{{ stats.total_versions }}</div>
            <div class="stat-label">Versiyon</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{{ stats.total_size_human }}</div>
            <div class="stat-label">Toplam</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{{ stats.sources|length }}</div>
            <div class="stat-label">Kaynak</div>
        </div>
    </div>
</div>
{% endblock %}
```

### 8.3 search.html — Arama Sonuçları

Her sonuç bir kart: isim, versiyon badge, format badge, açıklama, taglar, boyut, kaynak, tarih.

### 8.4 detail.html — Dataset Detay

Üstte dataset bilgisi (isim, kaynak, açıklama, taglar).
Ortada versiyon listesi (tablo: versiyon, zone, boyut, tarih, checksum kısaltması).
Altta lineage zinciri (pipeline adımları — dikey timeline).
Sağda metadata kartı (row_count, column_count, format, compression).

---

## 9. CSS TASARIM PRENSİPLERİ (static/style.css)

```css
/* Tek font, iki ağırlık */
:root {
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    --text-primary: #1a1a1a;
    --text-secondary: #6b6b6b;
    --text-tertiary: #999;
    --bg-primary: #ffffff;
    --bg-secondary: #f7f7f5;
    --border: #e5e5e3;
    --border-hover: #d0d0ce;
    --accent: #534AB7;       /* Tessera purple */
    --radius-sm: 6px;
    --radius-md: 8px;
    --radius-lg: 12px;
    --radius-pill: 999px;
}

/* Dark mode */
@media (prefers-color-scheme: dark) {
    :root {
        --text-primary: #e8e6e0;
        --text-secondary: #9c9a92;
        --text-tertiary: #73726c;
        --bg-primary: #1a1a18;
        --bg-secondary: #242422;
        --border: #333331;
        --border-hover: #444442;
    }
}

body {
    font-family: var(--font);
    color: var(--text-primary);
    background: var(--bg-primary);
    margin: 0;
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
}

/* --- Kurallar --- */
/* Max-width: 720px (dar, odaklı, Anthropic tarzı)
 * Spacing: 16px grid
 * Font sizes: 13px (küçük), 15px (normal), 20px (büyük rakam), 28px (hero başlık)
 * Weights: 400 (normal), 500 (medium) — 600+ YASAK
 * Borders: 0.5px solid var(--border)
 * Radius: 6-12px, badge'ler 999px pill
 * Shadows: YOK — flat tasarım
 * Transitions: 0.15s ease (hover efektleri için)
 * Kartlar: bg-primary + 0.5px border + radius-lg + 16-20px padding
 * Badge'ler: 11px font, 2-8px padding, pill radius, renkli bg + koyu text
 */
```

### Renk Kodlaması (Badge'ler)
- **Kaynak badge:**
  - kaggle → amber tonu (#FAEEDA bg, #633806 text)
  - huggingface → teal tonu (#E1F5EE bg, #085041 text)
  - github → gray tonu (#F1EFE8 bg, #444441 text)
- **Versiyon badge:** purple (#EEEDFE bg, #3C3489 text)
- **Format badge:** teal (#E1F5EE bg, #085041 text)
- **Tag badge:** neutral gray (bg-secondary bg, text-tertiary text)
- **Zone badge:**
  - raw → coral (#FAECE7 bg, #712B13 text)
  - processed → green (#EAF3DE bg, #27500A text)
  - archive → gray (#F1EFE8 bg, #444441 text)
- **Lineage status:**
  - success → green
  - warning → amber
  - failed → red

---

## 10. JAVASCRIPT (static/app.js)

Minimal — sadece:

1. **Arama debounce:** Input'a yazarken 300ms bekle, sonra /api/v1/datasets?q=... ile JSON çek, sonuçları güncelle (opsiyonel, form submit de yeterli)
2. **Filter toggle:** Kaynak/zone/format filtre badge'lerine tıklayınca URL parametresi ekle/çıkar
3. **Copy to clipboard:** Checksum, dataset ID kopyalama
4. **Humanize:** Byte → KB/MB/GB dönüşümü, tarih formatı

```javascript
// Byte formatı
function humanSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    return (bytes / 1073741824).toFixed(1) + ' GB';
}

// Checksum kopyala
function copyText(text) {
    navigator.clipboard.writeText(text);
}
```

---

## 11. SAYFA AKIŞI

```
[Ana Sayfa /]
  └── Arama kutusu + Enter
       └── [Arama Sonuçları /search?q=titanic]
            └── Kart'a tıkla
                 └── [Dataset Detay /dataset/{id}]
                      ├── Versiyon listesi (tablo)
                      ├── Lineage zinciri (timeline)
                      └── Metadata kartı

[Pipeline /pipeline]
  └── Son ingest'ler (tablo + durum badge)
```

---

## 12. İMPLEMENTASYON SIRASI

```
1. pyproject.toml'a fastapi, uvicorn, jinja2 ekle
2. src/tessera/web/__init__.py
3. src/tessera/web/app.py — FastAPI factory
4. src/tessera/web/routes/api.py — JSON endpoints
5. src/tessera/web/routes/pages.py — HTML routes
6. src/tessera/web/static/style.css — Tüm CSS
7. src/tessera/web/templates/base.html — Layout
8. src/tessera/web/templates/home.html — Ana sayfa
9. src/tessera/web/templates/search.html — Sonuçlar
10. src/tessera/web/templates/detail.html — Detay
11. src/tessera/web/templates/pipeline.html — Pipeline durumu
12. src/tessera/web/static/app.js — Minimal JS
13. cli/main.py'ye "tessera web" komutu ekle
14. Test: web endpoint'leri + template rendering
```

---

## 13. TASARIM REFERANSLARI

- **Layout:** Google Dataset Search — ortada arama, minimal, beyaz
- **Tipografi:** Anthropic claude.ai — system font, iki ağırlık (400/500)
- **Kartlar:** Anthropic docs — ince border, büyük radius, bol padding
- **Badge'ler:** GitHub — pill shape, renkli bg + koyu text, 11px font
- **Timeline (lineage):** Linear.app — dikey çizgi + durum noktaları
- **Boşluk:** Notion — cömert whitespace, dar max-width (720px)

---

## 14. ÖNEMLİ NOTLAR

- Dark mode zorunlu — tüm renkler CSS variable ile
- 0 JavaScript framework — vanilla ES6 yeterli
- Sayfalar server-side render (Jinja2) — SEO ve hız için
- API endpoints ayrıca var — ileride ayrı frontend istenirse hazır
- Mobile responsive — max-width: 720px zaten dar, telefonda da çalışır
- Logo: 4 renkli kare mozaik (purple, teal, coral, blue) — tessera metaforu
