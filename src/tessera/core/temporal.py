"""Dataset temporal coverage detection — date_start, date_end çıkarma."""

from __future__ import annotations

from pathlib import Path


_DATA_SUFFIXES = {".csv", ".parquet", ".tsv", ".jsonl"}


def detect_temporal_coverage(data_path: Path, max_files: int = 3) -> dict[str, str | None]:
    """CSV/Parquet dosyalarından veri başlangıç ve bitiş tarihlerini çıkar.

    Returns:
        {"date_start": "YYYY-MM-DD", "date_end": "YYYY-MM-DD", "date_column": "col_name"}
        Tespit edilemezse değerler None.
    """
    try:
        import pandas as pd
    except ImportError:
        return {"date_start": None, "date_end": None, "date_column": None}

    if data_path.is_file():
        candidates = [data_path] if data_path.suffix.lower() in _DATA_SUFFIXES else []
    else:
        candidates = [
            f for f in (
                list(data_path.rglob("*.csv"))
                + list(data_path.rglob("*.parquet"))
                + list(data_path.rglob("*.tsv"))
            )
            if f.is_file()
        ]

    global_min: pd.Timestamp | None = None
    global_max: pd.Timestamp | None = None
    best_col: str | None = None

    for fp in candidates[:max_files]:
        try:
            df = _read_file(fp, pd)
            if df is None or df.empty:
                continue
            col, mn, mx = _find_date_range(df, pd)
            if col is None:
                continue
            if global_min is None or mn < global_min:
                global_min = mn
                best_col = col
            if global_max is None or mx > global_max:
                global_max = mx
        except Exception:
            continue

    return {
        "date_start":  global_min.strftime("%Y-%m-%d") if global_min is not None else None,
        "date_end":    global_max.strftime("%Y-%m-%d") if global_max is not None else None,
        "date_column": best_col,
    }


# ── İç yardımcılar ───────────────────────────────────────────────────────────

def _read_file(fp: Path, pd):
    if fp.suffix == ".csv":
        return pd.read_csv(fp, low_memory=False)
    if fp.suffix == ".tsv":
        return pd.read_csv(fp, sep="\t", low_memory=False)
    if fp.suffix == ".parquet":
        return pd.read_parquet(fp)
    return None


def _find_date_range(df, pd):
    """En uygun tarih kolonunu bul, min/max döndür."""
    # Yıl sayısı içeren kolonları kontrol et (örn. 2018-2030)
    year_result = _check_year_column(df, pd)
    if year_result[0]:
        return year_result

    # Halihazırda datetime olan kolonlar
    dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    # Yoksa tarih gibi görünen string/int kolonları dene
    if not dt_cols:
        dt_cols = _coerce_date_columns(df, pd)

    if not dt_cols:
        return None, None, None

    best_col = _pick_best_date_col(dt_cols, df)
    series = pd.to_datetime(df[best_col], errors="coerce").dropna()
    if series.empty:
        return None, None, None

    return best_col, series.min(), series.max()


def _check_year_column(df, pd):
    """Yalnızca yıl değeri (1900-2100) içeren integer kolonları tespit et."""
    YEAR_HINTS = {"year", "yil", "yıl", "yr", "anno"}
    for col in df.columns:
        if col.lower() not in YEAR_HINTS and not any(h in col.lower() for h in YEAR_HINTS):
            continue
        try:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if series.empty:
                continue
            mn, mx = int(series.min()), int(series.max())
            if 1900 <= mn <= 2100 and 1900 <= mx <= 2100:
                # Yılı "YYYY-01-01" formatına çevir
                mn_ts = pd.Timestamp(year=mn, month=1, day=1)
                mx_ts = pd.Timestamp(year=mx, month=12, day=31)
                return col, mn_ts, mx_ts
        except Exception:
            continue
    return None, None, None


def _coerce_date_columns(df, pd) -> list[str]:
    """Tarih içerme ihtimali yüksek kolonları dönüştürmeyi dene."""
    DATE_HINTS = {"date", "time", "year", "month", "day", "dt", "period",
                  "tarih", "yil", "ay", "gun", "zaman"}
    candidates = []
    for col in df.columns:
        col_lower = col.lower()
        if not any(hint in col_lower for hint in DATE_HINTS):
            continue
        try:
            series = pd.to_datetime(df[col], errors="coerce")
            valid_ratio = series.notna().mean()
            if valid_ratio >= 0.7:
                df[col] = series
                candidates.append(col)
        except Exception:
            continue
    return candidates


def _pick_best_date_col(cols: list[str], df) -> str:
    """Birden fazla tarih kolonu varsa en kapsamlı olanı seç."""
    DATE_PRIORITY = ["date", "datetime", "timestamp", "time", "tarih", "created_at", "updated_at"]
    for hint in DATE_PRIORITY:
        for col in cols:
            if hint in col.lower():
                return col
    return cols[0]
