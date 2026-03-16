# TESSERA — Kurumsal Veri Arşivleme Sistemi
## Proje Spesifikasyonu v1.0 — Claude Code İçin Tam Kılavuz

> *Tessera: Latince "mozaik taşı". Her dataset bir parça — kataloğa yerleştikçe büyük resim oluşur.*

---

## 1. PROJE ÖZETI

**Proje Adı:** tessera
**Dil:** Python 3.11+
**Paket Yönetimi:** pyproject.toml (setuptools)
**Veritabanı:** SQLite (katalog + audit)
**Test:** pytest
**CLI:** click
**Amaç:** Kaggle, HuggingFace, GitHub ve özel API'lerden dataset çeken, valide eden, dönüştüren, versiyonlayan ve kataloglayan modüler arşiv sistemi.

**Temel Felsefe:**
- Her özellik bir plugin — ekle/çıkar sıfır downtime
- Her katman izole — bir katmanı değiştirmek diğerini etkilemez
- Metadata-first — veri olmadan bile katalog sorgulanabilir
- Immutable raw — orijinal dosyalara asla dokunulmuyor
- Full audit trail — her operasyon loglanıyor

---

## 2. DİZİN YAPISI

```
tessera/
├── pyproject.toml
├── README.md
├── LICENSE
├── .env.example
├── config/
│   ├── default.yaml
│   └── schemas/
│       └── config_schema.json
│
├── src/
│   └── tessera/
│       ├── __init__.py
│       ├── __main__.py
│       │
│       ├── core/
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── registry.py
│       │   ├── pipeline.py
│       │   ├── catalog.py
│       │   ├── audit.py
│       │   ├── storage.py
│       │   ├── versioning.py
│       │   ├── hashing.py
│       │   ├── exceptions.py
│       │   └── models.py
│       │
│       ├── connectors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── kaggle.py
│       │   ├── huggingface.py
│       │   └── github.py
│       │
│       ├── validators/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── schema.py
│       │   ├── integrity.py
│       │   └── quality.py
│       │
│       ├── transformers/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── format.py
│       │   ├── compress.py
│       │   └── clean.py
│       │
│       ├── exporters/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── local.py
│       │   └── report.py
│       │
│       ├── hooks/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── notify.py
│       │   └── lineage.py
│       │
│       └── cli/
│           ├── __init__.py
│           ├── main.py
│           ├── ingest.py
│           ├── search.py
│           ├── inspect_cmd.py
│           ├── list_cmd.py
│           ├── export.py
│           ├── config_cmd.py
│           └── plugin_cmd.py
│
├── tests/
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_catalog.py
│   ├── test_pipeline.py
│   ├── test_hashing.py
│   ├── test_versioning.py
│   ├── test_connectors/
│   │   ├── test_kaggle.py
│   │   ├── test_huggingface.py
│   │   └── test_github.py
│   ├── test_validators/
│   │   ├── test_schema.py
│   │   ├── test_integrity.py
│   │   └── test_quality.py
│   ├── test_transformers/
│   │   ├── test_format.py
│   │   ├── test_compress.py
│   │   └── test_clean.py
│   ├── test_cli/
│   │   ├── test_ingest.py
│   │   └── test_search.py
│   └── fixtures/
│       ├── sample.csv
│       ├── sample.json
│       └── sample_config.yaml
│
└── data/                          # .gitignore — runtime oluşur
    ├── raw/
    ├── processed/
    ├── archive/
    ├── quarantine/
    ├── catalog.db
    └── audit.db
```

---

## 3. BAĞIMLILIKLAR (pyproject.toml)

```toml
[project]
name = "tessera"
version = "0.1.0"
description = "Enterprise-grade dataset archival system with plugin architecture"
requires-python = ">=3.11"
dependencies = [
    "click>=8.1",
    "pyyaml>=6.0",
    "pydantic>=2.0",
    "requests>=2.31",
    "rich>=13.0",
    "tqdm>=4.65",
    "pyarrow>=14.0",
    "pandas>=2.1",
    "zstandard>=0.22",
    "jsonschema>=4.20",
    "kaggle>=1.6",
    "huggingface-hub>=0.20",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4",
    "pytest-cov>=4.1",
    "pytest-mock>=3.12",
]

[project.scripts]
tessera = "tessera.cli.main:cli"

[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"
```

---

## 4. KONFİGÜRASYON SİSTEMİ

### 4.1 default.yaml

```yaml
project:
  name: "my-data-archive"
  version: "0.1.0"

storage:
  base_path: "./data"
  zones:
    raw: "raw"
    processed: "processed"
    archive: "archive"
    quarantine: "quarantine"
  catalog_db: "catalog.db"
  audit_db: "audit.db"

ingestion:
  default_connector: "kaggle"
  checksum_algorithm: "sha256"
  skip_existing: true
  quarantine_on_fail: true

processing:
  default_format: "parquet"
  compression: "zstd"
  compression_level: 3
  auto_profile: true

versioning:
  strategy: "semantic"
  keep_versions: 5
  archive_older: true

connectors:
  kaggle:
    enabled: true
    credentials_env: "KAGGLE_KEY"
    download_timeout: 300
    max_retries: 3
  huggingface:
    enabled: true
    token_env: "HF_TOKEN"
    download_timeout: 300
  github:
    enabled: true
    token_env: "GITHUB_TOKEN"

validators:
  - integrity
  - schema
  - quality

transformers:
  - clean
  - format
  - compress

hooks:
  pre_ingest: []
  post_ingest: ["lineage", "notify"]
  on_error: ["notify"]

logging:
  level: "INFO"
  format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
  file: null
```

### 4.2 Config Loader (core/config.py)

- YAML yükle (default: `./config/default.yaml`, override: `--config` flag veya `TESSERA_CONFIG` env)
- Env override: `TESSERA_STORAGE__BASE_PATH` → `storage.base_path` (çift underscore = nested key)
- Pydantic model ile validation
- Singleton pattern — bir kere yüklenince cachele

---

## 5. VERİTABANI ŞEMALARI

### 5.1 Katalog (catalog.db)

```sql
CREATE TABLE datasets (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    source          TEXT NOT NULL,
    source_ref      TEXT NOT NULL,
    current_version TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    tags            TEXT DEFAULT '[]',
    description     TEXT DEFAULT '',
    is_archived     INTEGER DEFAULT 0,
    UNIQUE(source, source_ref)
);

CREATE TABLE dataset_versions (
    id              TEXT PRIMARY KEY,
    dataset_id      TEXT NOT NULL REFERENCES datasets(id),
    version         TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    file_count      INTEGER NOT NULL DEFAULT 1,
    raw_path        TEXT NOT NULL,
    processed_path  TEXT,
    archive_path    TEXT,
    zone            TEXT NOT NULL DEFAULT 'raw',
    format          TEXT,
    compression     TEXT,
    row_count       INTEGER,
    column_count    INTEGER,
    profile_path    TEXT,
    metadata_json   TEXT DEFAULT '{}',
    created_at      TEXT NOT NULL,
    UNIQUE(dataset_id, version)
);

CREATE TABLE lineage (
    id              TEXT PRIMARY KEY,
    version_id      TEXT NOT NULL REFERENCES dataset_versions(id),
    operation       TEXT NOT NULL,
    plugin_name     TEXT NOT NULL,
    input_checksum  TEXT,
    output_checksum TEXT,
    parameters_json TEXT DEFAULT '{}',
    status          TEXT NOT NULL,
    error_message   TEXT,
    duration_ms     INTEGER,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_datasets_source ON datasets(source);
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_tags ON datasets(tags);
CREATE INDEX idx_versions_dataset ON dataset_versions(dataset_id);
CREATE INDEX idx_versions_checksum ON dataset_versions(checksum_sha256);
CREATE INDEX idx_versions_zone ON dataset_versions(zone);
CREATE INDEX idx_lineage_version ON lineage(version_id);
CREATE INDEX idx_lineage_operation ON lineage(operation);
```

### 5.2 Audit (audit.db)

```sql
CREATE TABLE audit_log (
    id              TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    actor           TEXT NOT NULL DEFAULT 'system',
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    details_json    TEXT DEFAULT '{}',
    status          TEXT NOT NULL,
    ip_address      TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX idx_audit_action ON audit_log(action);
CREATE INDEX idx_audit_resource ON audit_log(resource_type, resource_id);
```

---

## 6. PLUGIN SİSTEMİ — INTERFACE TANIMLARI

### 6.1 BaseConnector (connectors/base.py)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass
class DatasetInfo:
    source: str
    source_ref: str
    name: str
    description: str
    size_bytes: Optional[int]
    file_count: int
    format_hint: Optional[str]
    tags: list[str]
    license: Optional[str]
    last_updated: Optional[str]
    url: Optional[str]
    extra_metadata: dict

@dataclass
class DownloadResult:
    success: bool
    local_path: Path
    checksum_sha256: str
    size_bytes: int
    file_count: int
    duration_seconds: float
    error_message: Optional[str] = None

class BaseConnector(ABC):
    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def validate_credentials(self) -> bool: ...

    @abstractmethod
    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]: ...

    @abstractmethod
    def fetch_metadata(self, source_ref: str) -> DatasetInfo: ...

    @abstractmethod
    def download(self, source_ref: str, target_dir: Path) -> DownloadResult: ...
```

### 6.2 BaseValidator (validators/base.py)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

class ValidationLevel(Enum):
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"

@dataclass
class ValidationIssue:
    level: ValidationLevel
    code: str
    message: str
    column: str | None = None
    details: dict = field(default_factory=dict)

@dataclass
class ValidationResult:
    validator_name: str
    level: ValidationLevel
    issues: list[ValidationIssue]
    duration_ms: int
    metadata: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.level != ValidationLevel.FAILED

class BaseValidator(ABC):
    name: str = "base"
    version: str = "0.1.0"
    supported_formats: list[str] = []

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def validate(self, file_path: Path, metadata: dict | None = None) -> ValidationResult: ...
```

### 6.3 BaseTransformer (transformers/base.py)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass
class TransformResult:
    transformer_name: str
    success: bool
    input_path: Path
    output_path: Path
    input_checksum: str
    output_checksum: str
    input_size: int
    output_size: int
    duration_ms: int
    details: dict
    error_message: Optional[str] = None

class BaseTransformer(ABC):
    name: str = "base"
    version: str = "0.1.0"
    input_formats: list[str] = []
    output_format: str = ""

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult: ...

    def can_handle(self, file_path: Path) -> bool:
        suffix = file_path.suffix.lstrip('.')
        return not self.input_formats or suffix in self.input_formats
```

### 6.4 BaseExporter (exporters/base.py)

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass
class ExportResult:
    success: bool
    exporter_name: str
    output_path: Path
    size_bytes: int
    duration_ms: int
    error_message: Optional[str] = None

class BaseExporter(ABC):
    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def export(self, version_id: str, target_path: Path, **kwargs) -> ExportResult: ...
```

### 6.5 BaseHook (hooks/base.py)

```python
from abc import ABC, abstractmethod
from typing import Any

class BaseHook(ABC):
    name: str = "base"
    version: str = "0.1.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def execute(self, event: str, context: dict[str, Any]) -> None: ...
```

---

## 7. CORE MODÜL DAVRANIŞLARI

### 7.1 Plugin Registry (core/registry.py)

Plugin'leri otomatik keşfet, kaydet, isme göre döndür.

```python
class PluginRegistry:
    _connectors: dict[str, type[BaseConnector]]
    _validators: dict[str, type[BaseValidator]]
    _transformers: dict[str, type[BaseTransformer]]
    _exporters: dict[str, type[BaseExporter]]
    _hooks: dict[str, type[BaseHook]]

    def discover_plugins(self) -> None: ...
    def get_connector(self, name: str) -> BaseConnector: ...
    def get_validator(self, name: str) -> BaseValidator: ...
    def get_transformer(self, name: str) -> BaseTransformer: ...
    def get_exporter(self, name: str) -> BaseExporter: ...
    def get_hook(self, name: str) -> BaseHook: ...
    def list_plugins(self) -> dict[str, list[str]]: ...
```

- `importlib` ile dinamik import
- `inspect.getmembers()` ile base class'tan türeyen sınıfları bul
- `base.py` dosyalarını atla

### 7.2 Pipeline (core/pipeline.py)

**Akış:**
```
1. PRE_INGEST hooks
2. Connector.download() → raw zone
3. SHA-256 checksum
4. Duplikasyon kontrolü → aynı checksum varsa SKIP
5. Validators sırayla (integrity → schema → quality)
   → FAIL ise quarantine zone'a taşı, DUR
6. Transformers sırayla (clean → format → compress)
   → processed zone'a yaz
7. Version tag
8. Catalog kaydı
9. Profiling raporu (auto_profile=true ise)
10. POST_INGEST hooks
11. Eski versiyonları archive zone'a taşı
```

Her adım lineage + audit kaydı atar. Hata → on_error hooks.

```python
@dataclass
class StageResult:
    stage: str
    plugin_name: str
    status: str
    duration_ms: int
    details: dict

@dataclass
class PipelineResult:
    success: bool
    dataset_id: str
    version: str
    stages: list[StageResult]
    total_duration_ms: int
    error_message: str | None

class Pipeline:
    def __init__(self, config, registry, catalog, audit): ...
    def ingest(self, source, source_ref, tags=None, force=False) -> PipelineResult: ...
    def reingest(self, dataset_id) -> PipelineResult: ...
```

### 7.3 CatalogManager (core/catalog.py)

```python
class CatalogManager:
    def __init__(self, db_path: Path): ...
    def initialize(self) -> None: ...

    def register_dataset(self, info: DatasetInfo) -> str: ...
    def get_dataset(self, dataset_id: str) -> dict | None: ...
    def search_datasets(self, query=None, source=None, tags=None) -> list[dict]: ...
    def update_dataset(self, dataset_id: str, **kwargs) -> None: ...
    def archive_dataset(self, dataset_id: str) -> None: ...

    def register_version(self, dataset_id: str, version_data: dict) -> str: ...
    def get_version(self, version_id: str) -> dict | None: ...
    def get_versions(self, dataset_id: str) -> list[dict]: ...
    def get_latest_version(self, dataset_id: str) -> dict | None: ...
    def update_version_zone(self, version_id, zone, path) -> None: ...

    def record_lineage(self, version_id, operation, plugin_name, **kwargs) -> str: ...
    def get_lineage(self, version_id: str) -> list[dict]: ...
    def check_duplicate(self, checksum: str) -> dict | None: ...
    def get_stats(self) -> dict: ...
```

### 7.4 AuditLogger (core/audit.py)

```python
class AuditLogger:
    def __init__(self, db_path: Path): ...
    def initialize(self) -> None: ...
    def log(self, action, resource_type, resource_id=None, actor="system", details=None, status="success") -> None: ...
    def get_logs(self, action=None, resource_type=None, since=None, limit=100) -> list[dict]: ...
```

### 7.5 StorageManager (core/storage.py)

```python
class StorageManager:
    def __init__(self, config: dict): ...
    def initialize(self) -> None: ...
    def get_zone_path(self, zone: str) -> Path: ...
    def store_raw(self, source_path, dataset_name, version) -> Path: ...
    def store_processed(self, source_path, dataset_name, version) -> Path: ...
    def move_to_archive(self, processed_path, dataset_name, version) -> Path: ...
    def quarantine(self, file_path, dataset_name, reason) -> Path: ...
    def get_zone_size(self, zone: str) -> int: ...
    def cleanup_old_versions(self, dataset_name, keep) -> list[Path]: ...
```

**Dosya Yerleşimi:**
```
data/raw/{dataset_name}/v{version}/{orijinal_dosya}
data/processed/{dataset_name}/v{version}/{dönüştürülmüş_dosya}
data/archive/{dataset_name}/v{version}/{dosya}
data/quarantine/{dataset_name}/{timestamp}_{dosya}
```

### 7.6 VersionManager (core/versioning.py)

```python
class VersionManager:
    def __init__(self, strategy="semantic"): ...
    def next_version(self, current: str | None, change_type="minor") -> str: ...
    def compare(self, v1: str, v2: str) -> int: ...
    def parse(self, version: str) -> tuple[int, int, int]: ...
```

### 7.7 Hashing (core/hashing.py)

```python
def compute_file_checksum(file_path, algorithm="sha256", chunk_size=8192) -> str: ...
def compute_directory_checksum(dir_path, algorithm="sha256") -> str: ...
def verify_checksum(file_path, expected, algorithm="sha256") -> bool: ...
```

### 7.8 Models (core/models.py)

```python
from pydantic import BaseModel, Field
from typing import Optional

class StorageConfig(BaseModel):
    base_path: str = "./data"
    zones: dict[str, str] = {"raw": "raw", "processed": "processed", "archive": "archive", "quarantine": "quarantine"}
    catalog_db: str = "catalog.db"
    audit_db: str = "audit.db"

class IngestionConfig(BaseModel):
    default_connector: str = "kaggle"
    checksum_algorithm: str = "sha256"
    skip_existing: bool = True
    quarantine_on_fail: bool = True

class ProcessingConfig(BaseModel):
    default_format: str = "parquet"
    compression: str = "zstd"
    compression_level: int = Field(default=3, ge=1, le=22)
    auto_profile: bool = True

class VersioningConfig(BaseModel):
    strategy: str = "semantic"
    keep_versions: int = Field(default=5, ge=1)
    archive_older: bool = True

class ConnectorConfig(BaseModel):
    enabled: bool = True
    credentials_env: Optional[str] = None
    token_env: Optional[str] = None
    download_timeout: int = 300
    max_retries: int = 3

class AppConfig(BaseModel):
    project: dict = {"name": "my-data-archive", "version": "0.1.0"}
    storage: StorageConfig = StorageConfig()
    ingestion: IngestionConfig = IngestionConfig()
    processing: ProcessingConfig = ProcessingConfig()
    versioning: VersioningConfig = VersioningConfig()
    connectors: dict[str, ConnectorConfig] = {}
    validators: list[str] = ["integrity", "schema", "quality"]
    transformers: list[str] = ["clean", "format", "compress"]
    hooks: dict[str, list[str]] = {}
    logging: dict = {"level": "INFO"}
```

### 7.9 Exceptions (core/exceptions.py)

```python
class TesseraError(Exception): ...
class ConfigError(TesseraError): ...
class ConnectorError(TesseraError): ...
class ValidationError(TesseraError): ...
class TransformError(TesseraError): ...
class StorageError(TesseraError): ...
class CatalogError(TesseraError): ...
class VersionError(TesseraError): ...
class PipelineError(TesseraError): ...
class PluginNotFoundError(TesseraError): ...
class DuplicateDatasetError(TesseraError): ...
class QuarantineError(TesseraError): ...
```

---

## 8. CONNECTOR İMPLEMENTASYONLARI

### 8.1 Kaggle (connectors/kaggle.py)
- **Lib:** `kaggle` SDK
- **Cred:** `KAGGLE_KEY` env (username:api_key) veya `~/.kaggle/kaggle.json`
- `validate_credentials()`: `kaggle.api.authenticate()`
- `search(query)`: `kaggle.api.dataset_list(search=query)`
- `fetch_metadata(source_ref)`: `kaggle.api.dataset_view(source_ref)`
- `download(source_ref, target_dir)`: `kaggle.api.dataset_download_files(source_ref, path=target_dir, unzip=True)` + tqdm + retry (exponential backoff)
- **source_ref:** `"owner/dataset-name"`

### 8.2 HuggingFace (connectors/huggingface.py)
- **Lib:** `huggingface_hub`
- **Cred:** `HF_TOKEN` env
- `validate_credentials()`: `huggingface_hub.whoami(token)`
- `search(query)`: `huggingface_hub.list_datasets(search=query)`
- `fetch_metadata(source_ref)`: `huggingface_hub.dataset_info(source_ref)`
- `download(source_ref, target_dir)`: `huggingface_hub.snapshot_download(repo_id=source_ref, repo_type="dataset", local_dir=target_dir)`
- **source_ref:** `"username/dataset-name"`

### 8.3 GitHub (connectors/github.py)
- **Lib:** `requests` (REST API)
- **Cred:** `GITHUB_TOKEN` env (opsiyonel)
- `validate_credentials()`: `GET /user`
- `search(query)`: `GET /search/repositories?q={query}+topic:dataset`
- `fetch_metadata(source_ref)`: `GET /repos/{owner}/{repo}` + releases
- `download(source_ref, target_dir)`: Latest release assets veya zipball
- **source_ref:** `"owner/repo"`

---

## 9. VALIDATOR İMPLEMENTASYONLARI

### 9.1 integrity.py
- Dosya boyutu > 0
- Extension + magic bytes eşleşmesi
- Zip corrupt kontrolü (`zipfile.is_zipfile()`)
- Parquet okunabilirlik (`pyarrow.parquet.read_metadata()`)

### 9.2 schema.py
- CSV: başlık var mı, delimiter tutarlı mı, sütun sayısı sabit mi
- Parquet: schema okunabilir mi
- JSON: valid JSON mı, array/object

### 9.3 quality.py
- Null oranı (sütun başına >%50 → warning)
- Tam duplike satır kontrolü
- Boş dosya kontrolü
- Veri tipi tutarlılık kontrolü

---

## 10. TRANSFORMER İMPLEMENTASYONLARI

### 10.1 clean.py
- Encoding → UTF-8, BOM kaldır, whitespace trim, boş satır kaldır
- Sütun adları: lowercase, boşluk → underscore

### 10.2 format.py
- CSV → Parquet (pyarrow), JSON → Parquet (pandas + pyarrow)
- `default_format: "original"` ise atla

### 10.3 compress.py
- zstd (default), gzip, lz4
- Zaten sıkıştırılmış dosyaları atla (.gz, .zst, .zip)

---

## 11. CLI KOMUTLARI

```
tessera — Kurumsal Veri Arşivleme Sistemi

Komutlar:
  init      Yeni arşiv projesi başlat
  ingest    Dataset indir ve arşivle
  search    Katalogda ara
  inspect   Dataset/versiyon detaylarını göster
  list      Datasetleri listele
  export    Dataset'i dışa aktar
  config    Konfigürasyonu göster/düzenle
  plugin    Plugin'leri listele/yönet
  stats     Arşiv istatistiklerini göster
```

```bash
tessera init [--path ./my-archive] [--config config.yaml]

tessera ingest kaggle "zillow/zecon" --tags housing,economics
tessera ingest huggingface "mnist/mnist" --tags vision,digits
tessera ingest github "fivethirtyeight/data" --tags journalism
tessera ingest kaggle "zillow/zecon" --force

tessera search "titanic"
tessera search --source kaggle --tags classification,tabular --format parquet

tessera inspect <dataset_id>
tessera inspect <dataset_id> --version 1.0.0
tessera inspect <dataset_id> --lineage
tessera inspect <dataset_id> --audit

tessera list [--zone raw] [--archived] [--sort size] [--format table|json]

tessera export <dataset_id> ./output/ [--version 1.0.0] [--zone raw]

tessera plugin list
tessera plugin info kaggle

tessera config show
tessera config validate

tessera stats
```

**Çıktı:** rich tablolar (default), `--format json`, `--quiet`, tqdm progress, renkli hata mesajları

---

## 12. TEST STRATEJİSİ

```python
# tests/conftest.py
import pytest

@pytest.fixture
def sample_csv(tmp_path):
    csv = "id,name,value\n1,alpha,100\n2,beta,200\n3,gamma,300\n"
    p = tmp_path / "sample.csv"
    p.write_text(csv)
    return p

@pytest.fixture
def sample_config():
    return {
        "storage": {"base_path": "./test_data"},
        "ingestion": {"checksum_algorithm": "sha256", "skip_existing": True},
        "processing": {"default_format": "parquet", "compression": "zstd"},
    }

@pytest.fixture
def catalog_db(tmp_path):
    from tessera.core.catalog import CatalogManager
    db = CatalogManager(tmp_path / "test.db")
    db.initialize()
    return db

@pytest.fixture
def mock_kaggle_api(mocker):
    return mocker.patch("kaggle.api")
```

**10 Kritik Test Senaryosu:**
1. Pipeline happy path (mock connector → validate → transform → catalog)
2. Duplikasyon kontrolü (aynı checksum → skip)
3. Validation fail → quarantine
4. Versiyonlama (ikinci ingest → v1.1.0)
5. Archive rotation (keep_versions aşılınca)
6. Lineage zinciri (tüm adımlar kayıtlı mı)
7. Config env override
8. Checksum doğruluğu
9. CLI integration (mock ile end-to-end)
10. Hata kurtarma (transform hatası → partial cleanup)

---

## 13. İMPLEMENTASYON SIRASI

### Faz 1: Foundation
pyproject.toml, __init__.py, __main__.py, exceptions.py, models.py, default.yaml, config.py, hashing.py, versioning.py

### Faz 2: Depolama & Katalog
storage.py, catalog.py, audit.py

### Faz 3: Plugin Sistemi
Tüm base ABC'ler + registry.py

### Faz 4: Plugin İmplementasyonları
Connectors (kaggle, huggingface, github), validators (integrity, schema, quality), transformers (clean, format, compress), exporters (local, report), hooks (lineage, notify)

### Faz 5: Pipeline
core/pipeline.py — orchestrator

### Faz 6: CLI
Tüm click komutları

### Faz 7: Testler
conftest + fixtures + tüm test dosyaları

### Faz 8: Finalizasyon
README.md, .env.example, .gitignore, son test

---

## 14. ÖNEMLİ NOTLAR

- **SQLite:** WAL mode + foreign keys ON, UUID v4 ID'ler, UTC ISO 8601 timestamps
- **Dosya ops:** Atomic write (.tmp → rename), chunk read (8KB), pathlib.Path daima
- **Error handling:** Her katman kendi exception'ını fırlatsın, pipeline yakalasın
- **CLI hataları:** rich ile renkli, stack trace sadece --debug ile
- **Concurrency:** Şimdilik yok — sequential pipeline yeterli
- **Plugin discovery:** importlib + inspect, base.py atla, name attribute zorunlu

---

## 15. GENİŞLETİLEBİLİRLİK (ŞIMDI YAPMA)

Mimari hazır olsun ama implement etme:
- Scheduler (APScheduler), REST API (FastAPI), Web Dashboard (Streamlit)
- S3 Backend, Parallel Ingest (asyncio), Lineage Graph (NetworkX)
- Notification: Slack, email, webhook

---

## 16. ÖRNEK KULLANIM

```bash
tessera init --path ./my-ml-archive
tessera ingest kaggle "competitions/titanic" --tags classification,beginner,tabular
tessera list
# ┌──────────┬─────────┬─────────┬────────┬───────────┬─────────────┐
# │ Name     │ Source  │ Version │ Zone   │ Size      │ Updated     │
# ├──────────┼─────────┼─────────┼────────┼───────────┼─────────────┤
# │ titanic  │ kaggle  │ 1.0.0   │ proc.  │ 34.2 KB   │ 2026-03-15  │
# └──────────┴─────────┴─────────┴────────┴───────────┴─────────────┘

tessera inspect titanic-xxxxx --lineage
# Download (kaggle_connector) → SUCCESS → 245ms
# Validate (integrity)        → PASSED  → 12ms
# Validate (schema)           → PASSED  → 8ms
# Validate (quality)          → WARNING → 15ms  (age: 19.8% null)
# Transform (clean)           → SUCCESS → 22ms
# Transform (format)          → SUCCESS → 45ms  (CSV → Parquet)
# Transform (compress)        → SUCCESS → 18ms  (zstd, 34.2KB → 12.1KB)

tessera export titanic-xxxxx ./exports/
```
