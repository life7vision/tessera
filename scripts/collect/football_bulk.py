#!/usr/bin/env python3
"""Bulk downloader for football-data.co.uk — historical match results + betting odds.

Downloads all configured leagues × seasons and organises CSVs under:
  data/football_data/
    main/
      {season_code}/        e.g. 2324/
        E0.csv              Premier League 23/24
        T1.csv              Süper Lig 23/24
        ...
    extra/
      ARG.csv               Argentina all seasons
      ...

Usage:
  python scripts/collect/football_bulk.py                  # all leagues, all seasons
  python scripts/collect/football_bulk.py --leagues E0 T1  # specific leagues only
  python scripts/collect/football_bulk.py --from-year 2015 # from 2015/16 onward
  python scripts/collect/football_bulk.py --dry-run        # show plan, no download
  python scripts/collect/football_bulk.py --extra-only     # only extra leagues
"""

from __future__ import annotations

import argparse
import io
import sys
import time
import zipfile
from pathlib import Path

# Allow running without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import requests
from tqdm import tqdm

from tessera.connectors.football_data import (
    BASE_URL,
    EXTRA_LEAGUES,
    FIRST_SEASON,
    CURRENT_SEASON,
    MAIN_LEAGUES,
    _HEADERS,
    _fetch,
    season_code,
    season_label,
)

# ── Default output directory ──────────────────────────────────────────
DEFAULT_OUTPUT = Path(__file__).parent.parent.parent / "data" / "football_data"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="football-data.co.uk bulk downloader")
    p.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help=f"Root output directory (default: {DEFAULT_OUTPUT})",
    )
    p.add_argument(
        "--leagues", nargs="+", metavar="CODE",
        help="Specific main league codes (e.g. E0 T1 SP1). Default: all.",
    )
    p.add_argument(
        "--from-year", type=int, default=FIRST_SEASON, metavar="YEAR",
        help=f"Start from season YEAR/YY+1 (default: {FIRST_SEASON})",
    )
    p.add_argument(
        "--to-year", type=int, default=CURRENT_SEASON, metavar="YEAR",
        help=f"End at season YEAR/YY+1 (default: {CURRENT_SEASON})",
    )
    p.add_argument(
        "--extra-only", action="store_true",
        help="Download only extra leagues (Argentina, Brazil, etc.)",
    )
    p.add_argument(
        "--skip-extra", action="store_true",
        help="Skip extra leagues, download main leagues only",
    )
    p.add_argument(
        "--delay", type=float, default=0.5,
        help="Seconds to wait between requests (default: 0.5)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be downloaded without actually downloading",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-download even if file already exists",
    )
    return p.parse_args()


def download_season(
    sc: str,
    leagues: list[str],
    dest: Path,
    dry_run: bool,
    force: bool,
) -> tuple[int, int]:
    """Download all requested leagues for one season zip. Returns (ok, skipped)."""
    url = f"{BASE_URL}/mmz4281/{sc}/data.zip"
    needed = [lg for lg in leagues if (force or not (dest / sc / f"{lg}.csv").exists())]

    if not needed:
        return 0, len(leagues)  # all already present

    if dry_run:
        for lg in needed:
            print(f"  [dry-run] {sc}/{lg}.csv  ← {url}")
        return len(needed), 0

    try:
        data = _fetch(url)
    except requests.HTTPError as exc:
        tqdm.write(f"  SKIP {sc}: {exc}")
        return 0, 0

    (dest / sc).mkdir(parents=True, exist_ok=True)
    ok = 0
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = {Path(n).name: n for n in zf.namelist()}
        for lg in needed:
            target_name = f"{lg}.csv"
            if target_name not in names:
                continue  # league not in this season's zip
            out_path = dest / sc / target_name
            out_path.write_bytes(zf.read(names[target_name]))
            ok += 1

    return ok, len(leagues) - ok


def download_extra(
    country_code: str,
    dest: Path,
    dry_run: bool,
    force: bool,
) -> bool:
    out_path = dest / f"{country_code}.csv"
    if out_path.exists() and not force:
        return False  # already exists

    url = f"{BASE_URL}/new/{country_code}.csv"
    if dry_run:
        print(f"  [dry-run] extra/{country_code}.csv  ← {url}")
        return True

    try:
        data = _fetch(url)
        out_path.write_bytes(data)
        return True
    except requests.HTTPError as exc:
        tqdm.write(f"  SKIP extra/{country_code}: {exc}")
        return False


def main() -> None:
    args = parse_args()

    main_dest  = args.output / "main"
    extra_dest = args.output / "extra"
    main_dest.mkdir(parents=True, exist_ok=True)
    extra_dest.mkdir(parents=True, exist_ok=True)

    leagues = args.leagues or list(MAIN_LEAGUES.keys())
    seasons = list(range(args.from_year, args.to_year + 1))

    # ── Summary ────────────────────────────────────────────────────
    print("=" * 60)
    print("football-data.co.uk bulk downloader")
    print("=" * 60)
    if not args.extra_only:
        print(f"  Leagues  : {', '.join(leagues)}")
        print(f"  Seasons  : {season_label(seasons[0])} → {season_label(seasons[-1])} ({len(seasons)} seasons)")
    if not args.skip_extra:
        print(f"  Extra    : {', '.join(EXTRA_LEAGUES.keys())} ({len(EXTRA_LEAGUES)} leagues)")
    print(f"  Output   : {args.output}")
    print(f"  Dry-run  : {args.dry_run}")
    print("=" * 60)

    total_ok = total_skip = 0

    # ── Main leagues ───────────────────────────────────────────────
    if not args.extra_only:
        print(f"\nDownloading main leagues ({len(seasons)} seasons × {len(leagues)} leagues)...")
        for year in tqdm(seasons, desc="Seasons", unit="season"):
            sc = season_code(year)
            ok, skip = download_season(sc, leagues, main_dest, args.dry_run, args.force)
            total_ok   += ok
            total_skip += skip
            if not args.dry_run:
                time.sleep(args.delay)

    # ── Extra leagues ──────────────────────────────────────────────
    if not args.skip_extra:
        print(f"\nDownloading extra leagues ({len(EXTRA_LEAGUES)})...")
        for code in tqdm(EXTRA_LEAGUES, desc="Extra", unit="league"):
            downloaded = download_extra(code, extra_dest, args.dry_run, args.force)
            total_ok += int(downloaded)
            if not args.dry_run:
                time.sleep(args.delay)

    # ── Final report ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    if args.dry_run:
        print("Dry-run complete. No files written.")
    else:
        # Count actual files
        main_files  = list(main_dest.rglob("*.csv"))
        extra_files = list(extra_dest.glob("*.csv"))
        total_bytes = sum(f.stat().st_size for f in main_files + extra_files)
        print(f"Done.")
        print(f"  Main  : {len(main_files)} CSV files")
        print(f"  Extra : {len(extra_files)} CSV files")
        print(f"  Total : {_human(total_bytes)}")
    print("=" * 60)


def _human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


if __name__ == "__main__":
    main()
