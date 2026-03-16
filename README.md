# Tessera

Tessera, kurumsal veri arşivleme akışları için tasarlanmış modüler bir dataset ingest ve kataloglama sistemidir. Kaggle, Hugging Face ve GitHub kaynaklarından veri çekmek; doğrulamak; dönüştürmek; versiyonlamak; kataloglamak ve audit kaydı tutmak için katmanlı bir Python mimarisi sunar.

## Özellikler

- Plugin tabanlı connector, validator, transformer, exporter ve hook mimarisi
- SQLite tabanlı katalog, lineage ve audit kayıtları
- Immutable raw zone, processed/archive/quarantine zone ayrımı
- `click` + `rich` tabanlı CLI
- Pydantic ile doğrulanan YAML konfigürasyon yapısı
- Mock tabanlı, network gerektirmeyen test kapsamı
- Claude Console ilhamı neurodesign dataset arayüzü (grid → detay geçişi)
- AWS Bedrock (Claude Haiku) ile otomatik AI dataset description üretimi
- Otomatik temporal coverage tespiti (`date_start`, `date_end`, `date_column`)
- Dataset içeriği önizleme (ilk 100 satır, tablo görünümü)
- AWS EC2 + nginx + systemd ile production deploy

## Kurulum

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
```

## Hızlı Başlangıç

Yeni bir arşiv dizini başlat:

```bash
.venv/bin/tessera init --path ./my-archive
```

Dataset ingest et:

```bash
.venv/bin/tessera ingest kaggle owner/dataset-name --tags demo,tabular
```

Katalogda ara:

```bash
.venv/bin/tessera search titanic
```

İstatistikleri göster:

```bash
.venv/bin/tessera stats
```

Web arayüzünü başlat:

```bash
.venv/bin/tessera web --host 127.0.0.1 --port 8000
```

Sonra tarayıcıda `http://127.0.0.1:8000` adresini açabilirsiniz.

## Konfigürasyon

Varsayılan konfigürasyon dosyası `config/default.yaml` içindedir. `--config` seçeneği veya `TESSERA_CONFIG` ortam değişkeni ile farklı bir dosya kullanabilirsiniz.

Nested env override örnekleri:

```bash
export TESSERA_STORAGE__BASE_PATH=/mnt/data/archive
export TESSERA_PROCESSING__COMPRESSION=gzip
export TESSERA_LOGGING__LEVEL=DEBUG
```

### AI Zenginleştirme (AWS Bedrock)

Dataset description'ları çok kısa veya boş olduğunda Claude Haiku ile otomatik üretilebilir. `config/default.yaml` içinde:

```yaml
ai_enrichment:
  enabled: true
  model: eu.anthropic.claude-haiku-4-5-20251001-v1:0
  region: eu-central-1
  max_tokens: 1024
  min_description_length: 80
```

AWS kimlik bilgileri `~/.aws/credentials` veya ortam değişkenleri (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) üzerinden okunur.

## CLI Komutları

| Komut | Açıklama |
|---|---|
| `tessera init` | Yeni arşiv dizini başlat |
| `tessera ingest` | Dataset ingest et |
| `tessera search` | Katalogda ara |
| `tessera inspect` | Dataset detaylarını göster |
| `tessera list` | Tüm datasetleri listele |
| `tessera export` | Dataset dışa aktar |
| `tessera config show` | Konfigürasyonu göster |
| `tessera config validate` | Konfigürasyonu doğrula |
| `tessera plugin list` | Plugin listesi |
| `tessera plugin info` | Plugin detayı |
| `tessera stats` | Arşiv istatistikleri |
| `tessera web` | Web arayüzünü başlat |

## Web Arayüzü

Claude Console tasarım dilinden ilham alan, neurodesign ilkelerine uygun modern arayüz.

### Sayfalar

| Route | Açıklama |
|---|---|
| `/` | Ana sayfa — arşiv istatistikleri |
| `/datasets` | Dataset listesi (grid) → detay görünümü |
| `/ingest` | Yeni dataset arşivleme formu |
| `/settings` | Connector kimlik bilgileri ve konfigürasyon |
| `/api/v1/*` | JSON REST API |

### Datasets Sayfası

İki modlu tasarım:

1. **Grid görünümü** — arama destekli dataset kartları
2. **Detay görünümü** — kart tıklanınca tam sayfa detay açılır

Detay sayfasında:
- Sürüm sayısı, dosya boyutu, **temporal kapsam** (date_start / date_end) stat kartları
- Açıklama ve etiketler
- **Veri Önizleme** — "Önizle" butonuyla ilk 100 satırı tablo olarak görüntüle
- Sürüm geçmişi tablosu (checksum, format, bölge, boyut)
- Yeniden ingest ve silme aksiyonları

### JSON API

```
GET  /api/v1/datasets                       # Dataset listesi
GET  /api/v1/datasets/{id}                  # Dataset + sürümler
GET  /api/v1/datasets/{id}/preview          # İlk 100 satır önizleme
GET  /api/v1/datasets/{id}/lineage          # Veri soy ağacı
POST /api/v1/datasets/{id}/refresh-metadata # Kaynak metadatayı yenile
GET  /api/v1/stats                          # Arşiv istatistikleri
GET  /api/v1/plugins                        # Yüklü plugin listesi
GET  /api/v1/storage                        # Zone disk kullanımı
GET  /api/v1/credentials                    # Kimlik bilgisi durumu
POST /api/v1/ingest                         # Arka planda ingest başlat
GET  /api/v1/ingest/{job_id}                # İngest iş durumu
```

## Pipeline Akışı

```
Connector (download)
    → Duplicate check (checksum)
    → Validators (integrity, schema, quality)
    → Transformers (clean, format → parquet, compress → zstd)
    → Temporal coverage detection (date_start, date_end)
    → Catalog registration (dataset + version + lineage)
    → Profile export (JSON raporu)
    → AI enrichment (AWS Bedrock — opsiyonel)
```

## Proje Yapısı

```
src/tessera/
├── core/
│   ├── pipeline.py          # Orkestrasyon
│   ├── catalog.py           # SQLite katalog
│   ├── storage.py           # Zone yönetimi
│   ├── temporal.py          # Tarih aralığı tespiti
│   ├── ai_enrichment.py     # AWS Bedrock entegrasyonu
│   ├── versioning.py        # Semantic versioning
│   ├── audit.py             # Audit log
│   ├── hashing.py           # SHA-256 checksum
│   ├── models.py            # Pydantic konfigürasyon modelleri
│   └── registry.py          # Plugin registry
├── connectors/              # Kaggle, HuggingFace, GitHub
├── validators/              # integrity, schema, quality
├── transformers/            # clean, format, compress
├── exporters/               # report (JSON profil)
├── hooks/                   # Event hook'ları
└── web/
    ├── app.py               # FastAPI factory
    ├── routes/              # HTML + JSON endpoint'ler
    ├── templates/           # Jinja2 template'ler
    └── static/              # CSS + JS
```

## Production Deploy (AWS)

Proje AWS üzerinde çalışmaktadır:

- **EC2**: t3.small, Amazon Linux 2023, `eu-central-1`
- **Sunucu**: `63.177.161.196`
- **Reverse proxy**: nginx
- **Servis yönetimi**: systemd (`tessera.service`)
- **Depolama**: S3 bucket `tessera-datasets-715557237960`

Deploy için:

```bash
rsync -av -e "ssh -i ~/.ssh/tessera-deploy.pem" src/ ec2-user@63.177.161.196:/home/ec2-user/tessera/src/
ssh -i ~/.ssh/tessera-deploy.pem ec2-user@63.177.161.196 "sudo systemctl restart tessera"
```

## Geliştirme

Testleri çalıştır:

```bash
.venv/bin/python -m pytest -q
```

## Uygulanan Fazlar

| Faz | Kapsam |
|---|---|
| Faz 1 | Foundation — proje iskelet, pyproject, config |
| Faz 2 | Storage, catalog, audit (SQLite) |
| Faz 3 | Plugin interfaces ve registry |
| Faz 4 | Plugin implementasyonları (connectors, validators, transformers, exporters, hooks) |
| Faz 5 | Pipeline orchestrator |
| Faz 6 | CLI (click + rich) |
| Faz 7 | Test genişletmesi |
| Faz 8 | Dokümantasyon ve finalizasyon |
| Faz 9 | Web arayüzü (FastAPI + Jinja2) |
| Faz 10 | Datasets UI yeniden tasarımı (Claude Console ilhamı, grid → detay) |
| Faz 11 | AWS altyapısı (EC2, nginx, systemd, S3, IAM) + Bedrock AI enrichment |
| Faz 12 | Temporal coverage tespiti + Dataset preview (100 satır tablo) |
