"""AWS Bedrock tabanlı dataset description zenginleştirme."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tessera.connectors.base import DatasetInfo


def enrich_description(data_path: Path, info: "DatasetInfo", config: dict | None = None) -> str | None:
    """Bedrock Claude ile dataset açıklaması üret.

    Hata durumunda None döner — çağıran kod bunu güvenle görmezden gelebilir.
    """
    cfg = config or {}
    if not cfg.get("enabled", False):
        return None

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        return None

    model_id = cfg.get("model", "eu.anthropic.claude-haiku-4-5-20251001-v1:0")
    region   = cfg.get("region", os.getenv("AWS_DEFAULT_REGION", "eu-central-1"))
    max_tokens = int(cfg.get("max_tokens", 1024))

    file_summary = _summarize_files(data_path, max_files=cfg.get("max_files", 3))
    if not file_summary:
        return None

    source_line   = f"Kaynak: {info.source} — {info.source_ref}" if info.source_ref else ""
    existing_hint = f"Mevcut kısa açıklama: {info.description}\n" if info.description else ""

    prompt = (
        f"Sen bir veri bilimcisisin. Aşağıdaki dataset bilgilerini analiz ederek "
        f"kapsamlı bir açıklama yaz.\n\n"
        f"Dataset adı: {info.name}\n"
        f"{source_line}\n"
        f"{existing_hint}\n"
        f"{file_summary}\n\n"
        f"Açıklamayı şunları kapsayacak şekilde yaz (düz metin, markdown işareti kullanma):\n"
        f"1. Bu dataset ne hakkında, ne tür verileri içeriyor?\n"
        f"2. Zaman aralığı ve coğrafi/kategorik kapsam (eğer varsa)\n"
        f"3. Her kolonun ne anlama geldiği (kısa)\n"
        f"4. Olası kullanım alanları ve araştırma soruları"
    )

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    })

    try:
        client = boto3.client("bedrock-runtime", region_name=region)
        response = client.invoke_model(
            modelId=model_id,
            body=body,
            contentType="application/json",
            accept="application/json",
        )
        result  = json.loads(response["body"].read())
        content = result.get("content", [])
        if content and content[0].get("type") == "text":
            return content[0]["text"].strip()
        return None
    except (BotoCoreError, ClientError):
        return None
    except Exception:
        return None


def _summarize_files(data_path: Path, max_files: int = 3) -> str | None:
    """CSV/Parquet dosyalarından kolon adları, tipler ve örnek satırlar çıkar."""
    try:
        import pandas as pd
    except ImportError:
        return None

    if data_path.is_file():
        candidates = [data_path]
    else:
        candidates = (
            list(data_path.rglob("*.csv"))
            + list(data_path.rglob("*.parquet"))
            + list(data_path.rglob("*.tsv"))
        )

    summaries: list[str] = []
    for fp in candidates[:max_files]:
        try:
            if fp.suffix == ".csv":
                df = pd.read_csv(fp, nrows=5, low_memory=False)
            elif fp.suffix == ".tsv":
                df = pd.read_csv(fp, sep="\t", nrows=5, low_memory=False)
            elif fp.suffix == ".parquet":
                df = pd.read_parquet(fp).head(5)
            else:
                continue

            cols   = ", ".join(f"{col} ({df[col].dtype})" for col in list(df.columns)[:25])
            sample = df.head(3).to_string(index=False, max_cols=12)
            summaries.append(
                f"Dosya: {fp.name}\n"
                f"Kolonlar ({len(df.columns)} adet): {cols}\n\n"
                f"Örnek veri (ilk 3 satır):\n{sample}"
            )
        except Exception:
            continue

    return "\n\n---\n\n".join(summaries) if summaries else None
