# Tessera

Tessera, kurumsal veri arşivleme akışları için tasarlanmış modüler bir dataset ingest ve kataloglama sistemidir. Kaggle, Hugging Face ve GitHub kaynaklarından veri çekmek; doğrulamak; dönüştürmek; versiyonlamak; kataloglamak ve audit kaydı tutmak için katmanlı bir Python mimarisi sunar.

## Özellikler

- Plugin tabanlı connector, validator, transformer, exporter ve hook mimarisi
- SQLite tabanlı katalog, lineage ve audit kayıtları
- Immutable raw zone, processed/archive/quarantine zone ayrımı
- `click` + `rich` tabanlı CLI
- Pydantic ile doğrulanan YAML konfigürasyon yapısı
- Mock tabanlı, network gerektirmeyen test kapsamı

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

Varsayılan konfigürasyonu göster:

```bash
.venv/bin/tessera config show
```

Plugin listesini görüntüle:

```bash
.venv/bin/tessera plugin list
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

Varsayılan konfigürasyon dosyası [config/default.yaml](/run/media/life7vision/DataSSD/projects/Tessera/config/default.yaml) içindedir. İsterseniz `--config` seçeneği veya `TESSERA_CONFIG` ortam değişkeni ile farklı bir dosya kullanabilirsiniz.

Nested env override örnekleri:

```bash
export TESSERA_STORAGE__BASE_PATH=/mnt/data/archive
export TESSERA_PROCESSING__COMPRESSION=gzip
export TESSERA_LOGGING__LEVEL=DEBUG
```

## CLI Komutları

- `tessera init`
- `tessera ingest`
- `tessera search`
- `tessera inspect`
- `tessera list`
- `tessera export`
- `tessera config show`
- `tessera config validate`
- `tessera plugin list`
- `tessera plugin info`
- `tessera stats`
- `tessera web`

## Web Arayüzü

Faz 9 ile birlikte minimal bir web frontend eklendi:

- Ana sayfa: `/`
- Arama sonuçları: `/search`
- Dataset detay: `/dataset/{dataset_id}`
- Pipeline görünümü: `/pipeline`
- JSON API: `/api/v1/*`

Frontend yapısı:

- `src/tessera/web/app.py`: FastAPI app factory
- `src/tessera/web/routes`: HTML ve JSON route'lar
- `src/tessera/web/templates`: Jinja2 template'ler
- `src/tessera/web/static`: CSS ve minimal JS

## Geliştirme

Testleri çalıştır:

```bash
.venv/bin/python -m pytest -q
```

Kod yapısı özetle şu katmanlardan oluşur:

- `src/tessera/core`: config, storage, catalog, audit, pipeline, registry
- `src/tessera/connectors`: kaynak connector pluginleri
- `src/tessera/validators`: kalite ve şema kontrolleri
- `src/tessera/transformers`: temizleme, format ve sıkıştırma akışları
- `src/tessera/exporters`: dışa aktarma ve profiling raporları
- `src/tessera/hooks`: event hook implementasyonları
- `tests`: tüm modüller için pytest kapsamı

## Durum

Uygulanan fazlar:

- Faz 1: Foundation
- Faz 2: Storage, catalog, audit
- Faz 3: Plugin interfaces ve registry
- Faz 4: Plugin implementasyonları
- Faz 5: Pipeline orchestrator
- Faz 6: CLI
- Faz 7: Test genişletmesi
- Faz 8: Dokümantasyon ve finalizasyon
