#!/usr/bin/env python3
"""Bulk ingest already-downloaded football-data.co.uk CSVs into Tessera catalog.

Walks data/football_data/{main,extra}/ and registers every CSV directly into
the catalog without re-downloading. Use after football_bulk.py has completed.

Usage:
  python scripts/collect/football_ingest.py
  python scripts/collect/football_ingest.py --data-dir /app/data/football_data
  python scripts/collect/football_ingest.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pandas as pd
from tqdm import tqdm

from tessera.connectors.football_data import (
    EXTRA_LEAGUES,
    MAIN_LEAGUES,
    _season_code_to_year,
    season_label,
)
from tessera.core.catalog import CatalogManager
from tessera.core.config import load_config
from tessera.core.exceptions import DuplicateDatasetError
from tessera.core.hashing import compute_file_checksum


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest football-data CSVs into Tessera catalog")
    p.add_argument(
        "--data-dir", type=Path,
        default=Path(__file__).parent.parent.parent / "data" / "football_data",
        help="Root directory of downloaded football data",
    )
    p.add_argument("--dry-run", action="store_true", help="Print plan, no writes")
    return p.parse_args()


def csv_stats(path: Path) -> tuple[int, int, int]:
    """Returns (row_count, column_count, file_size_bytes)."""
    try:
        df = pd.read_csv(path, encoding="latin1", nrows=0)
        col_count = len(df.columns)
        # count rows without loading everything into RAM
        row_count = sum(1 for _ in open(path, encoding="latin1")) - 1
        size = path.stat().st_size
        return max(row_count, 0), col_count, size
    except Exception:
        return 0, 0, path.stat().st_size


def make_dataset_info(source_ref: str, csv_path: Path) -> dict:
    """Build metadata dict for a single CSV."""
    parts = source_ref.split("/")
    kind = parts[0]  # league code or "extra"

    if kind == "extra":
        country_code = parts[1]
        country, league = EXTRA_LEAGUES.get(country_code, ("Unknown", country_code))
        name = f"{country} — {league} (All Seasons)"
        description = (
            f"Historical match results and betting odds for {country} {league}, "
            f"all available seasons. Source: football-data.co.uk"
        )
        tags = ["football", "sports", "odds", "betting", country.lower(), "all-seasons"]
        season_str = "all"
    else:
        league_code, season_code = parts[0], parts[1]
        country, league = MAIN_LEAGUES.get(league_code, ("Unknown", league_code))
        year = _season_code_to_year(season_code)
        season_str = season_label(year)
        name = f"{country} — {league} {season_str}"
        description = (
            f"Match results and betting odds for {country} {league}, season {season_str}. "
            f"Includes fulltime/halftime scores, match statistics (shots, corners, cards) "
            f"and odds from up to 10 major bookmakers. Source: football-data.co.uk"
        )
        tags = [
            "football", "sports", "odds", "betting",
            country.lower(), league_code.lower(), season_str,
        ]

    return {
        "name": name,
        "description": description,
        "tags": tags,
        "season": season_str,
    }


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir

    if not data_dir.exists():
        print(f"ERROR: data dir not found: {data_dir}")
        sys.exit(1)

    # Load config + catalog
    config = load_config()
    catalog_path = Path(config.storage.base_path) / config.storage.catalog_db
    catalog = CatalogManager(catalog_path)
    catalog.initialize()

    # Collect all CSV paths
    tasks: list[tuple[str, Path]] = []

    main_dir = data_dir / "main"
    if main_dir.exists():
        for season_dir in sorted(main_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            sc = season_dir.name
            for csv in sorted(season_dir.glob("*.csv")):
                league_code = csv.stem
                source_ref = f"{league_code}/{sc}"
                tasks.append((source_ref, csv))

    extra_dir = data_dir / "extra"
    if extra_dir.exists():
        for csv in sorted(extra_dir.glob("*.csv")):
            source_ref = f"extra/{csv.stem}"
            tasks.append((source_ref, csv))

    print("=" * 60)
    print("football-data.co.uk → Tessera catalog bulk ingest")
    print("=" * 60)
    print(f"  Catalog  : {catalog_path}")
    print(f"  CSV files: {len(tasks)}")
    print(f"  Dry-run  : {args.dry_run}")
    print("=" * 60)

    ok = skip = error = 0

    for source_ref, csv_path in tqdm(tasks, desc="Ingesting", unit="dataset"):
        meta = make_dataset_info(source_ref, csv_path)

        if args.dry_run:
            tqdm.write(f"  [dry-run] {source_ref} → {meta['name']}")
            ok += 1
            continue

        try:
            row_count, col_count, size_bytes = csv_stats(csv_path)
            checksum = compute_file_checksum(csv_path)

            # Register dataset
            from tessera.connectors.base import DatasetInfo as DI
            info = DI(
                source="football_data",
                source_ref=source_ref,
                name=meta["name"],
                description=meta["description"],
                size_bytes=size_bytes,
                file_count=1,
                format_hint="csv",
                tags=meta["tags"],
                license="Free for personal/research use — football-data.co.uk",
                last_updated=None,
                url="https://www.football-data.co.uk",
                extra_metadata={"season": meta["season"]},
            )
            dataset_id = catalog.register_dataset(info)

            # Register version
            catalog.register_version(dataset_id, {
                "version": "1.0.0",
                "checksum_sha256": checksum,
                "file_size_bytes": size_bytes,
                "file_count": 1,
                "raw_path": str(csv_path),
                "processed_path": str(csv_path),
                "zone": "raw",
                "format": "csv",
                "compression": None,
                "row_count": row_count,
                "column_count": col_count,
                "metadata_json": json.dumps({"season": meta["season"]}),
            })
            ok += 1

        except DuplicateDatasetError:
            skip += 1
        except Exception as exc:
            tqdm.write(f"  ERROR {source_ref}: {exc}")
            error += 1

    print("\n" + "=" * 60)
    if args.dry_run:
        print(f"Dry-run: {ok} datasets would be ingested.")
    else:
        print(f"Done.")
        print(f"  Registered : {ok}")
        print(f"  Skipped    : {skip} (already in catalog)")
        print(f"  Errors     : {error}")
        total = catalog.get_stats().get("dataset_count", "?")
        print(f"  Catalog    : {total} total datasets")
    print("=" * 60)


if __name__ == "__main__":
    main()
