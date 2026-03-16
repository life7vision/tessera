# Claude Code'a Verilecek Başlangıç Promptu

## Tek Seferde Tamamı

```
TESSERA_PROJECT_SPEC.md dosyasını oku. Bu, "Tessera" adlı kurumsal düzeyde bir veri arşivleme sistemi için tam proje spesifikasyonu.

Kurallar:
1. Faz 1'den başla, sırasıyla Faz 8'e kadar ilerle
2. Her faz tamamlandığında testlerini çalıştır
3. Spesifikasyondaki interface tanımlarını birebir uygula
4. Dosya yapısını değiştirme — spec'teki gibi oluştur
5. Her plugin'in name ve version class attribute'u olmalı
6. SQLite'da WAL mode ve foreign keys aktif olmalı
7. Tüm dosya path'leri pathlib.Path ile
8. Hata mesajları Türkçe, docstring'ler İngilizce
9. CLI çıktıları rich kütüphanesi ile renkli tablo
10. Network olmadan çalışabilecek testler yaz (mock kullan)

Başla: Faz 1 - pyproject.toml ve core foundation modüllerini oluştur.
```

## Faz Faz İlerlemek İstersen

### Faz 1
"TESSERA_PROJECT_SPEC.md oku. Faz 1'i implement et: pyproject.toml, __init__.py, __main__.py, exceptions.py, models.py, default.yaml, config.py, hashing.py, versioning.py. Testlerini de yaz."

### Faz 2
"Spec'teki Faz 2'ye geç: storage.py, catalog.py, audit.py. SQLite tablolarını spesifikasyondaki şemaya göre oluştur. Testlerini yaz ve çalıştır."

### Faz 3
"Faz 3: Tüm base ABC'leri oluştur (BaseConnector, BaseValidator, BaseTransformer, BaseExporter, BaseHook) ve plugin registry'yi implement et."

### Faz 4
"Faz 4: Plugin implementasyonları — Kaggle/HuggingFace/GitHub connector'ları, integrity/schema/quality validator'ları, clean/format/compress transformer'ları, local exporter, profiling report, lineage ve notify hook'ları."

### Faz 5
"Faz 5: Pipeline orchestrator'ı implement et. Tüm pluginleri birleştiren ana pipeline akışını spesifikasyondaki 10 adımlı sıraya göre yaz."

### Faz 6
"Faz 6: CLI komutlarını implement et — click ile. ingest, search, inspect, list, export, config, plugin, stats, init komutları. Rich tablo çıktıları."

### Faz 7
"Faz 7: Tüm testleri yaz — conftest.py, fixture'lar, her modül için ayrı test dosyası. Mock ile network-free testler. pytest çalıştır."

### Faz 8
"Faz 8: README.md, .env.example, .gitignore oluştur. Tüm testleri son kez çalıştır. Kalan hataları düzelt."
