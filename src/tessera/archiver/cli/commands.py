"""
Tessera Archiver — CLI komutları.

`tessera archiver` alt komut grubu olarak Tessera CLI'a entegre edilir.

Kullanım:
  tessera archiver status
  tessera archiver archive github:torvalds/linux
  tessera archiver scan github:torvalds/linux
  tessera archiver pipeline --file repos.txt
  tessera archiver report --daily
  tessera archiver verify
  tessera archiver policy
  tessera archiver index
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()


# ---------------------------------------------------------------------------
# Runtime yardımcısı (lazy init)
# ---------------------------------------------------------------------------

def _get_archiver_runtime() -> dict:
    """Archiver için gerekli nesneleri lazy olarak oluşturur."""
    import os
    os.chdir(Path(__file__).parents[5])  # Tessera kökü

    from tessera.archiver.config import get_archiver_config
    from tessera.archiver.catalog import ArchiverCatalog
    from tessera.archiver.storage import ArchiverStorage
    from tessera.archiver.metadata.manager import MetadataManager
    from tessera.archiver.metadata.index import MasterIndex

    cfg = get_archiver_config()
    storage = ArchiverStorage(cfg.storage_root)
    catalog = ArchiverCatalog(cfg.database)
    meta_mgr = MetadataManager(storage)
    index = MasterIndex(storage)

    return {
        "cfg": cfg,
        "storage": storage,
        "catalog": catalog,
        "meta_mgr": meta_mgr,
        "index": index,
    }


# ---------------------------------------------------------------------------
# Ana komut grubu
# ---------------------------------------------------------------------------

@click.group("archiver", help="GitHub/GitLab repo arşivleme ve güvenlik tarama modülü")
def archiver_cli() -> None:
    pass


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@archiver_cli.command("status")
def status_cmd() -> None:
    """Archiver durumunu ve istatistiklerini gösterir."""
    rt = _get_archiver_runtime()
    catalog = rt["catalog"]
    storage = rt["storage"]

    stats = catalog.get_stats()

    table = Table(title="Archiver Durumu", show_header=True)
    table.add_column("Alan", style="cyan")
    table.add_column("Değer", style="white")

    table.add_row("Toplam Repo", str(stats.total_repos))
    table.add_row("Toplam Versiyon", str(stats.total_versions))
    table.add_row(
        "Toplam Boyut",
        _human(stats.total_size_bytes),
    )
    table.add_row(
        "Son Arşiv",
        str(stats.last_archived_at) if stats.last_archived_at else "—",
    )

    if stats.repos_by_provider:
        for prov, cnt in stats.repos_by_provider.items():
            table.add_row(f"  {prov}", str(cnt))

    console.print(table)

    if stats.repos_by_risk:
        risk_table = Table(title="Risk Dağılımı")
        risk_table.add_column("Seviye")
        risk_table.add_column("Repo Sayısı")
        for level, cnt in sorted(stats.repos_by_risk.items()):
            color = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "blue", "CLEAN": "green"}.get(level, "white")
            risk_table.add_row(f"[{color}]{level}[/{color}]", str(cnt))
        console.print(risk_table)


# ---------------------------------------------------------------------------
# archive
# ---------------------------------------------------------------------------

@archiver_cli.command("archive")
@click.argument("repos", nargs=-1)
@click.option("--file", "-f", "repos_file", type=click.Path(exists=True), help="Her satırda bir repo referansı")
@click.option("--org", "-o", help="GitHub org veya GitLab group (tüm repoları)")
@click.option("--provider", default="github", type=click.Choice(["github", "gitlab"]))
@click.option("--force", is_flag=True, help="Değişmemiş olsa da yeniden arşivle")
@click.option("--include-heavy", is_flag=True, help="node_modules, .venv gibi ağır dizinleri dahil et")
@click.option("--dry-run", is_flag=True, help="İşlem yapmadan listele")
def archive_cmd(
    repos: tuple[str, ...],
    repos_file: str | None,
    org: str | None,
    provider: str,
    force: bool,
    include_heavy: bool,
    dry_run: bool,
) -> None:
    """Repo(ları) arşivler. Örn: tessera archiver archive github:torvalds/linux"""
    from tessera.archiver.models import RepoRef
    from tessera.archiver.providers import get_provider
    from tessera.archiver.pipeline.archiver import archive_repo

    rt = _get_archiver_runtime()
    cfg = rt["cfg"]

    # Hedef listesi oluştur
    targets: list[RepoRef] = []
    for r in repos:
        try:
            targets.append(RepoRef.parse(r, default_provider=provider))
        except ValueError as exc:
            console.print(f"[red]Geçersiz repo referansı: {r} — {exc}[/red]")

    if repos_file:
        for line in Path(repos_file).read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                try:
                    targets.append(RepoRef.parse(line, default_provider=provider))
                except ValueError:
                    pass

    if org:
        prov_client = get_provider(provider)
        if provider == "github":
            pairs = prov_client.list_org_repos(org)  # type: ignore[attr-defined]
        else:
            pairs = prov_client.list_group_projects(org)  # type: ignore[attr-defined]
        for ns, repo_name in pairs:
            targets.append(RepoRef(provider=provider, namespace=ns, repo=repo_name))

    if not targets:
        console.print("[yellow]Arşivlenecek repo belirtilmedi.[/yellow]")
        raise click.UsageError("En az bir repo, --file veya --org gerekli")

    console.print(f"[cyan]Toplam {len(targets)} repo işlenecek[/cyan]")

    if dry_run:
        for ref in targets:
            console.print(f"  [dim][DRY-RUN][/dim] {ref.key}")
        return

    success_count = failed = 0
    for ref in targets:
        prov_client = get_provider(ref.provider)
        try:
            result = archive_repo(
                ref=ref,
                provider=prov_client,
                storage=rt["storage"],
                catalog=rt["catalog"],
                cfg=cfg,
                force=force,
                include_heavy=include_heavy,
            )
            if result["skipped"]:
                console.print(f"  [yellow]ATLANDI[/yellow] {ref.key}")
            elif result["success"]:
                success_count += 1
                console.print(
                    f"  [green]OK[/green] {ref.key} → {result.get('version')} "
                    f"({_human(result.get('size_bytes', 0))})"
                )
            else:
                failed += 1
                console.print(f"  [red]FAIL[/red] {ref.key}: {result.get('error')}")
        except Exception as exc:
            failed += 1
            console.print(f"  [red]HATA[/red] {ref.key}: {exc}")

    console.print(
        f"\n[bold]SONUÇ:[/bold] {success_count}/{len(targets)} başarılı"
        + (f", {failed} başarısız" if failed else "")
    )
    if failed:
        sys.exit(1)


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------

@archiver_cli.command("scan")
@click.argument("repo", required=False)
@click.option("--all", "scan_all", is_flag=True, help="Tüm taranmamış repoları tara")
@click.option("--force", is_flag=True, help="Zaten taranmışları da yeniden tara")
def scan_cmd(repo: str | None, scan_all: bool, force: bool) -> None:
    """Arşivlenmiş repo(ları) güvenlik açısından tarar."""
    from tessera.archiver.models import RepoRef
    from tessera.archiver.pipeline.scanner import scan_archive, save_scan_report

    rt = _get_archiver_runtime()
    catalog = rt["catalog"]
    storage = rt["storage"]
    cfg = rt["cfg"]

    _RISK_ICONS = {"HIGH": "[red]✘[/red]", "MEDIUM": "[yellow]⚠[/yellow]",
                   "LOW": "[blue]⚠[/blue]", "CLEAN": "[green]✔[/green]"}

    def _scan_one(ref: RepoRef) -> bool:
        versions = catalog.list_versions(ref.key)
        if not versions:
            console.print(f"  [yellow]Arşiv yok:[/yellow] {ref.key}")
            return False

        latest = versions[0]
        if not force:
            existing = catalog.get_latest_scan(ref.key, latest.version)
            if existing:
                icon = _RISK_ICONS.get(existing.risk_level, "?")
                console.print(f"  {icon} {ref.key} [{latest.version}] — önbellekte")
                return True

        version_dir = storage.raw_version_dir(ref, latest.version)
        archives = sorted(version_dir.glob("*.tar.gz"))
        if not archives:
            console.print(f"  [red]Arşiv dosyası bulunamadı:[/red] {ref.key}/{latest.version}")
            return False

        archive_path = archives[0]
        console.print(f"  Taranıyor: {ref.key} [{latest.version}] ({archive_path.name})")

        from pathlib import Path
        yara_dir = Path(cfg.scanner.yara_rules_dir) if cfg.scanner.yara_rules_dir else None
        report = scan_archive(archive_path, yara_rules_dir=yara_dir)

        # repo_key ve version bilgilerini doldur
        report.repo_key = ref.key
        report.version = latest.version
        report.archive_id = latest.archive_id

        scan_id = catalog.save_scan(report)

        # Geriye uyumluluk: scan_report.json dosyasına da yaz
        save_scan_report(report, version_dir / "scan_report.json")

        icon = _RISK_ICONS.get(report.risk_level, "?")
        console.print(
            f"  {icon} {ref.key} — {report.risk_level} "
            f"(HIGH={report.high_count} MEDIUM={report.medium_count} LOW={report.low_count})"
        )
        return True

    if repo:
        try:
            ref = RepoRef.parse(repo)
        except ValueError as exc:
            console.print(f"[red]Geçersiz repo: {exc}[/red]")
            sys.exit(1)
        _scan_one(ref)

    elif scan_all:
        unscanned = catalog.list_unscanned() if not force else [
            (r.key, (catalog.list_versions(r.key) or [{"version": "?"}])[0])
            for r in catalog.list_repos()
        ]
        if not unscanned:
            console.print("[green]Tüm repolar zaten taranmış.[/green]")
            return
        console.print(f"[cyan]{len(unscanned)} repo taranacak[/cyan]")
        for repo_key, version in unscanned:
            ref = RepoRef.parse(repo_key)
            _scan_one(ref)
    else:
        console.print("[yellow]Bir repo belirtin veya --all kullanın.[/yellow]")


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------

@archiver_cli.command("pipeline")
@click.argument("repos", nargs=-1)
@click.option("--file", "-f", "repos_file", type=click.Path(exists=True))
@click.option("--provider", default="github", type=click.Choice(["github", "gitlab"]))
@click.option("--force", is_flag=True)
def pipeline_cmd(
    repos: tuple[str, ...],
    repos_file: str | None,
    provider: str,
    force: bool,
) -> None:
    """Tam pipeline: archive → scan → policy raporu."""
    ctx = click.get_current_context()
    # archive
    ctx.invoke(archive_cmd, repos=repos, repos_file=repos_file,
               provider=provider, force=force, include_heavy=False, dry_run=False, org=None)
    # scan
    ctx.invoke(scan_cmd, repo=None, scan_all=True, force=False)
    # policy
    ctx.invoke(policy_cmd)
    # report
    ctx.invoke(report_cmd, daily=True, monthly=False, anomalies=False, all_reports=False)


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@archiver_cli.command("report")
@click.option("--daily", is_flag=True)
@click.option("--monthly", is_flag=True)
@click.option("--anomalies", is_flag=True)
@click.option("--all", "all_reports", is_flag=True)
def report_cmd(
    daily: bool, monthly: bool, anomalies: bool, all_reports: bool
) -> None:
    """Rapor üretir. --daily, --monthly, --anomalies veya --all."""
    from tessera.archiver.reporting import (
        generate_daily_report,
        generate_monthly_report,
        detect_anomalies,
    )

    rt = _get_archiver_runtime()
    storage = rt["storage"]

    if daily or all_reports:
        r = generate_daily_report(storage)
        console.print(f"[green]Günlük rapor:[/green] {r['summary']['total_repos']} repo, "
                      f"{r['summary']['archived_today']} bugün arşivlendi")

    if monthly or all_reports:
        r = generate_monthly_report(storage)
        console.print(f"[green]Aylık rapor:[/green] {r['summary']['archived_this_month']} arşiv bu ay")

    if anomalies or all_reports:
        r = detect_anomalies(storage)
        total = r.get("total", 0)
        color = "red" if total > 0 else "green"
        console.print(f"[{color}]Anomali raporu:[/{color}] {total} anomali tespit edildi")

    if not any([daily, monthly, anomalies, all_reports]):
        console.print("[yellow]--daily, --monthly, --anomalies veya --all belirtin.[/yellow]")


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

@archiver_cli.command("verify")
@click.option("--limit", type=int, default=0, help="Sadece ilk N arşivi kontrol et (0=tümü)")
@click.option("--strict", is_flag=True, help="Başarısız varsa exit code 1")
def verify_cmd(limit: int, strict: bool) -> None:
    """Arşiv bütünlüğünü checksum doğrulamasıyla kontrol eder."""
    from tessera.archiver.verification.periodic import run_verification

    rt = _get_archiver_runtime()
    report = run_verification(rt["storage"], limit=limit or None)
    s = report["summary"]

    color = "green" if s["fail"] == 0 else "red"
    console.print(
        f"[{color}]Verify:[/{color}] "
        f"toplam={s['total_checked']} "
        f"[green]ok={s['ok']}[/green] "
        f"[red]fail={s['fail']}[/red] "
        f"missing_sha={s['missing_sha']}"
    )

    if strict and s["fail"] > 0:
        sys.exit(1)


# ---------------------------------------------------------------------------
# policy
# ---------------------------------------------------------------------------

@archiver_cli.command("policy")
@click.option("--allow-missing", is_flag=True, help="Eksik scan raporu varsa hata verme")
def policy_cmd(allow_missing: bool) -> None:
    """Güvenlik politikasını tüm scan raporlarına göre değerlendirir."""
    from tessera.archiver.pipeline.policy import evaluate_policy

    rt = _get_archiver_runtime()
    result = evaluate_policy(rt["catalog"], allow_missing=allow_missing)

    color = "green" if result.passed else "red"
    console.print(f"[{color}]{result.summary}[/{color}]")

    if not result.passed:
        for v in result.violations[:20]:
            console.print(f"  [red]✘[/red] {v.repo_key} [{v.version}]: {v.reason}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------

@archiver_cli.command("index")
@click.option("--rebuild", is_flag=True, help="Tüm repo_info.json'lardan index'i yeniden oluştur")
@click.option("--json-out", "json_out", is_flag=True, help="JSON formatında çıktı ver")
def index_cmd(rebuild: bool, json_out: bool) -> None:
    """Master index.json içeriğini gösterir veya yeniden oluşturur."""
    from tessera.archiver.metadata.index import MasterIndex
    from tessera.archiver.metadata.manager import MetadataManager

    rt = _get_archiver_runtime()
    idx = MasterIndex(rt["storage"])

    if rebuild:
        meta_mgr = MetadataManager(rt["storage"])
        all_meta = meta_mgr.all_repo_metadata()
        idx.rebuild_from_metadata(all_meta)
        console.print(f"[green]Index yeniden oluşturuldu: {len(all_meta)} repo[/green]")
        return

    repos = idx.get_all()

    if json_out:
        console.print_json(json.dumps(repos, default=str))
        return

    if not repos:
        console.print("[yellow]Index boş.[/yellow]")
        return

    table = Table(title=f"Master Index ({len(repos)} repo)")
    table.add_column("Key", style="cyan")
    table.add_column("Versiyon")
    table.add_column("Dil")
    table.add_column("Domain")
    table.add_column("Boyut")
    table.add_column("Yıldız")

    for r in repos[:50]:
        table.add_row(
            r.get("key", ""),
            r.get("current_version", ""),
            r.get("language") or "—",
            r.get("app_type") or "—",
            _human(r.get("size_bytes", 0)),
            str(r.get("stars", 0)),
        )

    console.print(table)
    if len(repos) > 50:
        console.print(f"[dim]... ve {len(repos) - 50} repo daha[/dim]")


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

def _human(b: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {u}"
        b //= 1024
    return f"{b:.1f} PB"
