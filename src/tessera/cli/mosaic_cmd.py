"""tessera mosaic — Tessera Mosaic git depo yönetimi CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()

# Kimlik bilgileri dosyası
KIMLIK_DOSYASI = Path.home() / ".tessera" / "kimlik.json"


# ── Kimlik yardımcıları ───────────────────────────────────────────


def _kimlik_oku() -> dict:
    if KIMLIK_DOSYASI.exists():
        return json.loads(KIMLIK_DOSYASI.read_text())
    return {}


def _kimlik_yaz(veri: dict) -> None:
    KIMLIK_DOSYASI.parent.mkdir(parents=True, exist_ok=True)
    KIMLIK_DOSYASI.write_text(json.dumps(veri, indent=2, ensure_ascii=False))


def _sunucu_al() -> str:
    kimlik = _kimlik_oku()
    host = kimlik.get("sunucu") or os.getenv("TESSERA_MOSAIC_HOST", "")
    if not host:
        console.print(
            "[red]Sunucu adresi ayarlı değil.[/] "
            "→ [bold]tessera mosaic remote ayarla <url>[/]"
        )
        sys.exit(1)
    return host.rstrip("/")


def _token_al() -> str:
    kimlik = _kimlik_oku()
    token = kimlik.get("token") or os.getenv("TESSERA_MOSAIC_TOKEN", "")
    if not token:
        console.print(
            "[red]Oturum açılmamış.[/] "
            "→ [bold]tessera mosaic auth giris[/]"
        )
        sys.exit(1)
    return token


def _api(method: str, yol: str, veri: dict | None = None, token: str | None = None) -> dict:
    """API çağrısı yapar, hata durumunda çıkar."""
    import requests as _requests

    host = _sunucu_al()
    tok = token or _token_al()
    url = f"{host}/api/v1/mosaic{yol}"
    headers = {"Authorization": f"Bearer {tok}"}

    yanit = getattr(_requests, method)(url, json=veri, headers=headers, timeout=30)
    if yanit.status_code >= 400:
        try:
            detay = yanit.json().get("detail", yanit.text)
        except Exception:
            detay = yanit.text
        console.print(f"[red]Hata {yanit.status_code}:[/] {detay}")
        sys.exit(1)
    return yanit.json()


def _clone_url(sahip: str, ad: str, kimlik_gerektir: bool = False) -> str:
    """git clone için URL üretir."""
    kimlik = _kimlik_oku()
    host = _sunucu_al()
    # Protokol bölümünü soy (http:// veya https://)
    host_sade = host.removeprefix("https://").removeprefix("http://")
    proto = "https" if host.startswith("https") else "http"

    if kimlik_gerektir:
        kullanici = kimlik.get("kullanici", "tessera")
        token = kimlik.get("token") or os.getenv("TESSERA_MOSAIC_TOKEN", "")
        return f"{proto}://{kullanici}:{token}@{host_sade}/mosaic/git/{sahip}/{ad}.git"

    return f"{host}/mosaic/git/{sahip}/{ad}.git"


# ── Ana komut grubu ───────────────────────────────────────────────


@click.group(name="mosaic", help="Tessera Mosaic — git depo barındırma")
def mosaic_cli() -> None:
    pass


# ── remote ────────────────────────────────────────────────────────


@mosaic_cli.group(name="remote", help="Sunucu adresi yönetimi")
def remote_grubu() -> None:
    pass


@remote_grubu.command(name="ayarla")
@click.argument("url")
def remote_ayarla(url: str) -> None:
    """Tessera Mosaic sunucu adresini ayarla.

    Örnek: tessera mosaic remote ayarla https://mosaic.sunucum.com
    """
    kimlik = _kimlik_oku()
    kimlik["sunucu"] = url.rstrip("/")
    _kimlik_yaz(kimlik)
    console.print(f"[green]✓[/] Sunucu: [bold]{kimlik['sunucu']}[/]")


@remote_grubu.command(name="goster")
def remote_goster() -> None:
    """Mevcut sunucu adresini göster."""
    kimlik = _kimlik_oku()
    host = kimlik.get("sunucu") or os.getenv("TESSERA_MOSAIC_HOST", "—")
    console.print(f"Sunucu: [bold cyan]{host}[/]")


# ── auth ──────────────────────────────────────────────────────────


@mosaic_cli.group(name="auth", help="Kimlik doğrulama")
def auth_grubu() -> None:
    pass


@auth_grubu.command(name="giris")
@click.option("--sunucu", default="", help="Sunucu adresi (opsiyonel, remote ayarla ile de yapılır)")
@click.option("--kullanici", prompt="Kullanıcı adı")
@click.option("--token", prompt="API token", hide_input=True)
def auth_giris(sunucu: str, kullanici: str, token: str) -> None:
    """Tessera Mosaic'e giriş yap."""
    kimlik = _kimlik_oku()
    if sunucu:
        kimlik["sunucu"] = sunucu.rstrip("/")
    kimlik["kullanici"] = kullanici
    kimlik["token"] = token
    _kimlik_yaz(kimlik)
    console.print(f"[green]✓[/] Giriş yapıldı: [bold]{kullanici}[/] → {kimlik.get('sunucu', '?')}")


@auth_grubu.command(name="cikis")
def auth_cikis() -> None:
    """Oturumu kapat."""
    kimlik = _kimlik_oku()
    kimlik.pop("token", None)
    kimlik.pop("kullanici", None)
    _kimlik_yaz(kimlik)
    console.print("[yellow]Oturum kapatıldı.[/]")


@auth_grubu.command(name="durum")
def auth_durum() -> None:
    """Mevcut oturum bilgilerini göster."""
    kimlik = _kimlik_oku()
    host = kimlik.get("sunucu") or os.getenv("TESSERA_MOSAIC_HOST", "—")
    kullanici = kimlik.get("kullanici") or "—"
    token_var = bool(kimlik.get("token") or os.getenv("TESSERA_MOSAIC_TOKEN"))
    console.print(f"  Sunucu    : [bold]{host}[/]")
    console.print(f"  Kullanıcı : [bold]{kullanici}[/]")
    console.print(f"  Token     : {'[green]aktif[/]' if token_var else '[red]yok[/]'}")


@auth_grubu.command(name="token-yarat")
@click.argument("kullanici")
@click.option("--etiket", default="", help="Token etiketi")
@click.option("--master-anahtar", default="", envvar="TESSERA_MOSAIC_MASTER", help="Master anahtar")
def auth_token_yarat(kullanici: str, etiket: str, master_anahtar: str) -> None:
    """Yeni API token üret.

    İlk kullanım için master anahtara ihtiyaç duyulur.
    Örnek: tessera mosaic auth token-yarat life7vision --etiket laptop
    """
    import requests as _requests

    host = _sunucu_al()
    # Mevcut token varsa onu kullan, yoksa master anahtarla dene
    kimlik = _kimlik_oku()
    mevcut_token = kimlik.get("token") or os.getenv("TESSERA_MOSAIC_TOKEN", "")

    headers = {}
    if mevcut_token:
        headers["Authorization"] = f"Bearer {mevcut_token}"

    yanit = _requests.post(
        f"{host}/api/v1/mosaic/auth/token",
        json={"kullanici": kullanici, "etiket": etiket, "master_anahtar": master_anahtar},
        headers=headers,
        timeout=30,
    )
    if yanit.status_code >= 400:
        try:
            detay = yanit.json().get("detail", yanit.text)
        except Exception:
            detay = yanit.text
        console.print(f"[red]Hata {yanit.status_code}:[/] {detay}")
        sys.exit(1)

    veri = yanit.json()
    console.print("[green]✓ Token üretildi:[/]")
    console.print(f"  Kullanıcı : [bold]{veri['kullanici']}[/]")
    console.print(f"  Token     : [bold yellow]{veri['token']}[/]")
    console.print("[dim]Bu tokeni güvenli saklayın — bir daha gösterilmez.[/]")


# ── depo ──────────────────────────────────────────────────────────


@mosaic_cli.group(name="depo", help="Depo yönetimi")
def depo_grubu() -> None:
    pass


@depo_grubu.command(name="olustur")
@click.argument("ad")
@click.option("--sahip", default="", help="Sahip (varsayılan: giriş yapan kullanıcı)")
@click.option("--aciklama", default="", help="Depo açıklaması")
@click.option("--ozel", is_flag=True, default=False, help="Özel depo")
def depo_olustur(ad: str, sahip: str, aciklama: str, ozel: bool) -> None:
    """Yeni depo oluştur.

    Örnek: tessera mosaic depo olustur benim-projem --sahip life7vision
    """
    kimlik = _kimlik_oku()
    s = sahip or kimlik.get("kullanici", "")
    if not s:
        console.print("[red]--sahip belirtilmeli veya giriş yapılmalı.[/]")
        sys.exit(1)
    veri = _api("post", f"/repos/{s}", {"ad": ad, "aciklama": aciklama, "ozel": ozel})
    console.print(f"[green]✓[/] Depo oluşturuldu: [bold]{s}/{ad}[/]")
    host = _sunucu_al()
    console.print(f"  Clone   : [bold cyan]{host}/mosaic/git/{s}/{ad}.git[/]")
    console.print(f"  CLI     : [bold]tessera mosaic klon {s}/{ad}[/]")


@depo_grubu.command(name="listele")
@click.argument("sahip", default="")
def depo_listele(sahip: str) -> None:
    """Depoları listele.

    Örnek: tessera mosaic depo listele life7vision
    """
    kimlik = _kimlik_oku()
    s = sahip or kimlik.get("kullanici", "")
    params = f"?sahip={s}" if s else ""
    veri = _api("get", f"/repos{params}")
    depolar = veri.get("depolar", [])
    if not depolar:
        console.print("[dim]Henüz depo yok.[/]")
        return
    t = Table(title=f"Depolar ({len(depolar)})", show_header=True, show_lines=False)
    t.add_column("Ad", style="bold cyan", no_wrap=True)
    t.add_column("Sahip", style="dim")
    t.add_column("Açıklama")
    t.add_column("Erişim")
    t.add_column("Tarih", style="dim")
    for d in depolar:
        erisim = "[red]özel[/]" if d["ozel"] else "[green]açık[/]"
        t.add_row(d["ad"], d["sahip"], d.get("aciklama", ""), erisim, d["olusturma"][:10])
    console.print(t)


@depo_grubu.command(name="sil")
@click.argument("ref")  # sahip/ad
@click.option("--evet", is_flag=True, help="Onay sorma")
def depo_sil(ref: str, evet: bool) -> None:
    """Depo sil. REF formatı: sahip/ad

    Örnek: tessera mosaic depo sil life7vision/eski-proje
    """
    if "/" not in ref:
        console.print("[red]Format: sahip/ad[/]")
        sys.exit(1)
    sahip, ad = ref.split("/", 1)
    if not evet:
        click.confirm(f"[bold]{ref}[/] kalıcı olarak silinsin mi?", abort=True)
    _api("delete", f"/repos/{sahip}/{ad}")
    console.print(f"[green]✓[/] Silindi: [bold]{ref}[/]")


@depo_grubu.command(name="bilgi")
@click.argument("ref")  # sahip/ad
def depo_bilgi(ref: str) -> None:
    """Depo bilgilerini göster. REF formatı: sahip/ad"""
    if "/" not in ref:
        console.print("[red]Format: sahip/ad[/]")
        sys.exit(1)
    sahip, ad = ref.split("/", 1)
    veri = _api("get", f"/repos/{sahip}/{ad}")
    host = _sunucu_al()
    console.print(f"  Ad        : [bold]{veri['ad']}[/]")
    console.print(f"  Sahip     : {veri['sahip']}")
    console.print(f"  Açıklama  : {veri.get('aciklama') or '—'}")
    console.print(f"  Erişim    : {'özel' if veri['ozel'] else 'herkese açık'}")
    console.print(f"  Oluşturma : {veri['olusturma'][:10]}")
    console.print(f"  Clone URL : [bold cyan]{host}/mosaic/git/{sahip}/{ad}.git[/]")
    console.print(f"  CLI       : [bold]tessera mosaic klon {sahip}/{ad}[/]")


# ── klon ──────────────────────────────────────────────────────────


@mosaic_cli.command(name="klon")
@click.argument("ref")  # sahip/ad
@click.argument("hedef", default="", required=False)
def mosaic_klon(ref: str, hedef: str) -> None:
    """Mosaic deposunu klonla.

    \b
    Örnek:
      tessera mosaic klon archlinuxcn/repo
      tessera mosaic klon life7vision/projem ~/kod/projem
    """
    if "/" not in ref:
        console.print("[red]Format: sahip/ad[/]")
        sys.exit(1)
    sahip, ad = ref.split("/", 1)
    url = _clone_url(sahip, ad, kimlik_gerektir=True)
    hedef_dizin = hedef or ad
    console.print(f"Klonlanıyor: [bold]{ref}[/] → [bold]{hedef_dizin}[/]")
    komut = ["git", "clone", url]
    if hedef:
        komut.append(hedef)
    sonuc = subprocess.run(komut)
    if sonuc.returncode != 0:
        console.print("[red]Klonlama başarısız.[/]")
        sys.exit(sonuc.returncode)
    console.print(f"[green]✓[/] Klonlandı: [bold]{hedef_dizin}[/]")


# ── gonder ────────────────────────────────────────────────────────


@mosaic_cli.command(name="gonder")
@click.argument("ref")  # sahip/ad
@click.option("--dal", default="main", help="Dal adı (varsayılan: main)")
@click.option("--yarat", is_flag=True, help="Depo yoksa otomatik oluştur")
def mosaic_gonder(ref: str, dal: str, yarat: bool) -> None:
    """Mevcut git deposunu Mosaic'e gönder.

    \b
    Örnek:
      tessera mosaic gonder life7vision/projem
      tessera mosaic gonder life7vision/yeni-proje --yarat
    """
    if "/" not in ref:
        console.print("[red]Format: sahip/ad[/]")
        sys.exit(1)
    sahip, ad = ref.split("/", 1)

    if yarat:
        kimlik = _kimlik_oku()
        s = sahip or kimlik.get("kullanici", "")
        try:
            _api("post", f"/repos/{s}", {"ad": ad, "aciklama": "", "ozel": False})
            console.print(f"[green]✓[/] Depo oluşturuldu: [bold]{ref}[/]")
        except SystemExit:
            pass  # Depo zaten var, devam et

    remote_url = _clone_url(sahip, ad, kimlik_gerektir=True)

    # mosaic remote ekle veya güncelle
    mevcut = subprocess.run(
        ["git", "remote", "get-url", "mosaic"],
        capture_output=True,
    )
    if mevcut.returncode == 0:
        subprocess.run(["git", "remote", "set-url", "mosaic", remote_url], check=True)
    else:
        subprocess.run(["git", "remote", "add", "mosaic", remote_url], check=True)

    console.print(f"Gönderiliyor: [bold]{dal}[/] → [bold]{ref}[/]")
    sonuc = subprocess.run(["git", "push", "mosaic", dal])
    if sonuc.returncode != 0:
        console.print("[red]Gönderme başarısız.[/]")
        sys.exit(sonuc.returncode)
    console.print(f"[green]✓[/] Gönderildi: [bold]{ref}[/]")
