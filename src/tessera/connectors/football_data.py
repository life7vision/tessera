"""football-data.co.uk connector — historical match results + betting odds.

Source ref format:
  "E0/2324"   → England Premier League, season 2023/24
  "T1/2425"   → Turkey Süper Lig, season 2024/25
  "extra/ARG" → Argentina extra league (all seasons in one CSV)
  "all/2324"  → All main leagues for season 2023/24 (returns zip)

Season code formula: f"{year % 100:02d}{(year+1) % 100:02d}"
  1993 → "9394", 1999 → "9900", 2000 → "0001", 2024 → "2425"
"""

from __future__ import annotations

import io
import time
import zipfile
from pathlib import Path

import requests

from tessera.connectors.base import BaseConnector, DatasetInfo, DownloadResult
from tessera.core.hashing import compute_directory_checksum

BASE_URL = "https://www.football-data.co.uk"

# Main leagues — all included in the per-season zip
MAIN_LEAGUES: dict[str, tuple[str, str]] = {
    "E0":  ("England",     "Premier League"),
    "E1":  ("England",     "Championship"),
    "E2":  ("England",     "League 1"),
    "E3":  ("England",     "League 2"),
    "EC":  ("England",     "Conference"),
    "SC0": ("Scotland",    "Premier League"),
    "SC1": ("Scotland",    "Division 1"),
    "SC2": ("Scotland",    "Division 2"),
    "SC3": ("Scotland",    "Division 3"),
    "D1":  ("Germany",     "Bundesliga 1"),
    "D2":  ("Germany",     "Bundesliga 2"),
    "I1":  ("Italy",       "Serie A"),
    "I2":  ("Italy",       "Serie B"),
    "SP1": ("Spain",       "La Liga"),
    "SP2": ("Spain",       "Segunda"),
    "F1":  ("France",      "Ligue 1"),
    "F2":  ("France",      "Ligue 2"),
    "N1":  ("Netherlands", "Eredivisie"),
    "B1":  ("Belgium",     "Pro League"),
    "P1":  ("Portugal",    "Liga 1"),
    "T1":  ("Turkey",      "Süper Lig"),
    "G1":  ("Greece",      "Super League"),
}

# Extra leagues — single CSV per country, all seasons combined
EXTRA_LEAGUES: dict[str, tuple[str, str]] = {
    "ARG": ("Argentina", "Primera Division"),
    "AUT": ("Austria",   "Bundesliga"),
    "BRA": ("Brazil",    "Serie A"),
    "CHN": ("China",     "Super League"),
    "DNK": ("Denmark",   "Superliga"),
    "FIN": ("Finland",   "Veikkausliiga"),
    "IRL": ("Ireland",   "Premier Division"),
    "JPN": ("Japan",     "J-League"),
    "MEX": ("Mexico",    "Liga MX"),
    "NOR": ("Norway",    "Eliteserien"),
    "POL": ("Poland",    "Ekstraklasa"),
    "ROU": ("Romania",   "Liga 1"),
    "RUS": ("Russia",    "Premier League"),
    "SWE": ("Sweden",    "Allsvenskan"),
    "SUI": ("Switzerland", "Super League"),
    "USA": ("USA",       "MLS"),
}

FIRST_SEASON = 1993   # 1993/94
CURRENT_SEASON = 2025  # 2025/26

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.football-data.co.uk/",
}


def season_code(year: int) -> str:
    """Convert season start year to football-data.co.uk code. 2024 → '2425'."""
    return f"{year % 100:02d}{(year + 1) % 100:02d}"


def season_label(year: int) -> str:
    """Human-readable label. 2024 → '2024/25'."""
    return f"{year}/{(year + 1) % 100:02d}"


def all_season_years() -> list[int]:
    return list(range(FIRST_SEASON, CURRENT_SEASON + 1))


class FootballDataConnector(BaseConnector):
    """Connector for football-data.co.uk.

    Provides free historical football match results and betting odds
    for 38 leagues dating back to 1993/94.
    """

    name = "football_data"
    version = "0.1.0"

    # ── Credentials ──────────────────────────────────────────────────

    def validate_credentials(self) -> bool:
        """No credentials required — public CSV downloads."""
        return True

    # ── Search ───────────────────────────────────────────────────────

    def search(self, query: str, max_results: int = 10) -> list[DatasetInfo]:
        q = query.lower()
        results: list[DatasetInfo] = []

        for code, (country, league) in {**MAIN_LEAGUES, **EXTRA_LEAGUES}.items():
            if q in country.lower() or q in league.lower() or q in code.lower():
                results.append(self._make_info(code, country, league))
            if len(results) >= max_results:
                break

        return results

    # ── Metadata ─────────────────────────────────────────────────────

    def fetch_metadata(self, source_ref: str) -> DatasetInfo:
        """source_ref: 'E0/2324' or 'extra/ARG'."""
        league_code, season = _parse_ref(source_ref)

        if league_code == "extra":
            country, league = EXTRA_LEAGUES.get(season, ("Unknown", "Unknown"))
            return self._make_info(
                season, country, league,
                source_ref=source_ref,
                extra={"type": "extra_league", "all_seasons": True},
            )

        country, league = MAIN_LEAGUES.get(league_code, ("Unknown", "Unknown"))
        year = _season_code_to_year(season) if season != "all" else None
        return self._make_info(
            league_code, country, league,
            source_ref=source_ref,
            extra={
                "season_code": season,
                "season_label": season_label(year) if year else "all",
                "type": "main_league",
            },
        )

    # ── Download ─────────────────────────────────────────────────────

    def download(self, source_ref: str, target_dir: Path) -> DownloadResult:
        """Download CSV(s) for the given source_ref into target_dir."""
        target_dir.mkdir(parents=True, exist_ok=True)
        start = time.perf_counter()
        league_code, season = _parse_ref(source_ref)

        try:
            if league_code == "extra":
                self._download_extra(season, target_dir)
            elif season == "all":
                self._download_all_main(target_dir)
            else:
                self._download_season_league(league_code, season, target_dir)
        except requests.HTTPError as exc:
            return DownloadResult(
                success=False,
                local_path=target_dir,
                checksum_sha256="",
                size_bytes=0,
                file_count=0,
                duration_seconds=time.perf_counter() - start,
                error_message=str(exc),
            )

        checksum = compute_directory_checksum(target_dir)
        size_bytes, file_count = _scan(target_dir)
        return DownloadResult(
            success=True,
            local_path=target_dir,
            checksum_sha256=checksum,
            size_bytes=size_bytes,
            file_count=file_count,
            duration_seconds=time.perf_counter() - start,
        )

    # ── Internal download helpers ────────────────────────────────────

    def _download_season_league(self, league_code: str, season: str, dest: Path) -> None:
        """Download a single league/season CSV from the season zip."""
        url = f"{BASE_URL}/mmz4281/{season}/data.zip"
        data = _fetch(url)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            target_name = f"{league_code}.csv"
            matches = [n for n in zf.namelist() if Path(n).name == target_name]
            if not matches:
                raise FileNotFoundError(
                    f"{target_name} not found in {url}. "
                    f"Available: {zf.namelist()}"
                )
            zf.extract(matches[0], dest)
            # flatten — move file to top-level if nested
            extracted = dest / matches[0]
            final = dest / target_name
            if extracted != final:
                extracted.rename(final)
                try:
                    extracted.parent.rmdir()
                except OSError:
                    pass

    def _download_extra(self, country_code: str, dest: Path) -> None:
        """Download extra-league CSV (all seasons combined)."""
        url = f"{BASE_URL}/new/{country_code}.csv"
        data = _fetch(url)
        (dest / f"{country_code}.csv").write_bytes(data)

    def _download_all_main(self, dest: Path) -> None:
        """Download all seasons × all main leagues."""
        for year in all_season_years():
            sc = season_code(year)
            url = f"{BASE_URL}/mmz4281/{sc}/data.zip"
            try:
                data = _fetch(url)
            except requests.HTTPError:
                continue
            season_dir = dest / sc
            season_dir.mkdir(exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                zf.extractall(season_dir)
            time.sleep(0.4)

    # ── Factory helpers ──────────────────────────────────────────────

    def _make_info(
        self,
        code: str,
        country: str,
        league: str,
        source_ref: str | None = None,
        extra: dict | None = None,
    ) -> DatasetInfo:
        ref = source_ref or code
        return DatasetInfo(
            source="football_data",
            source_ref=ref,
            name=f"{country} — {league}",
            description=(
                f"Historical match results and betting odds for {country} {league} "
                f"from football-data.co.uk. Includes fulltime/halftime scores, "
                f"match statistics (shots, corners, cards) and odds from up to "
                f"10 major bookmakers."
            ),
            size_bytes=None,
            file_count=1,
            format_hint="csv",
            tags=["football", "sports", "odds", "betting", country.lower(), code.lower()],
            license="Free for personal/research use — football-data.co.uk",
            last_updated=None,
            url=f"{BASE_URL}/data.php",
            extra_metadata=extra or {},
        )


# ── Helpers ───────────────────────────────────────────────────────────


def _parse_ref(source_ref: str) -> tuple[str, str]:
    """Parse 'E0/2324' → ('E0', '2324'). Raises ValueError on bad format."""
    parts = source_ref.split("/")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid source_ref '{source_ref}'. "
            "Expected format: 'LEAGUE/SEASON' e.g. 'E0/2324' or 'extra/ARG'"
        )
    return parts[0], parts[1]


def _season_code_to_year(code: str) -> int:
    """Convert '2324' → 2023, '9394' → 1993, '0001' → 2000."""
    hi = int(code[:2])
    return (1900 + hi) if hi >= 93 else (2000 + hi)


def _fetch(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 404:
            resp.raise_for_status()
        if resp.ok:
            return resp.content
        if attempt < retries - 1:
            time.sleep(2 ** attempt)
    resp.raise_for_status()
    return b""  # unreachable


def _scan(path: Path) -> tuple[int, int]:
    files = [f for f in path.rglob("*") if f.is_file()]
    return sum(f.stat().st_size for f in files), len(files)
