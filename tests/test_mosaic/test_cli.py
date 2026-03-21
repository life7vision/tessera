"""Mosaic CLI birim testleri."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tessera.cli.mosaic_cmd import mosaic_cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def kimlik_dizini(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Gerçek ~/.tessera yerine tmp_path kullan."""
    kimlik_dosyasi = tmp_path / ".tessera" / "kimlik.json"
    monkeypatch.setattr(
        "tessera.cli.mosaic_cmd.KIMLIK_DOSYASI", kimlik_dosyasi
    )
    return tmp_path / ".tessera"


# ── remote komutları ──────────────────────────────────────────────


def test_remote_ayarla(runner: CliRunner, kimlik_dizini: Path) -> None:
    result = runner.invoke(mosaic_cli, ["remote", "ayarla", "http://localhost:8000"])
    assert result.exit_code == 0
    assert "localhost:8000" in result.output
    dosya = kimlik_dizini / "kimlik.json"
    assert json.loads(dosya.read_text())["sunucu"] == "http://localhost:8000"


def test_remote_goster(runner: CliRunner, kimlik_dizini: Path) -> None:
    # Önce ayarla
    runner.invoke(mosaic_cli, ["remote", "ayarla", "http://test.server"])
    result = runner.invoke(mosaic_cli, ["remote", "goster"])
    assert result.exit_code == 0
    assert "test.server" in result.output


# ── auth komutları ────────────────────────────────────────────────


def test_auth_giris(runner: CliRunner, kimlik_dizini: Path) -> None:
    result = runner.invoke(
        mosaic_cli,
        ["auth", "giris", "--kullanici", "life7vision", "--token", "tss_abc123"],
    )
    assert result.exit_code == 0
    assert "life7vision" in result.output
    dosya = kimlik_dizini / "kimlik.json"
    veri = json.loads(dosya.read_text())
    assert veri["kullanici"] == "life7vision"
    assert veri["token"] == "tss_abc123"


def test_auth_durum_aktif(runner: CliRunner, kimlik_dizini: Path) -> None:
    runner.invoke(mosaic_cli, ["remote", "ayarla", "http://localhost:8000"])
    runner.invoke(
        mosaic_cli,
        ["auth", "giris", "--kullanici", "user", "--token", "tss_xyz"],
    )
    result = runner.invoke(mosaic_cli, ["auth", "durum"])
    assert result.exit_code == 0
    assert "user" in result.output
    assert "aktif" in result.output


def test_auth_cikis(runner: CliRunner, kimlik_dizini: Path) -> None:
    runner.invoke(
        mosaic_cli,
        ["auth", "giris", "--kullanici", "user", "--token", "tss_xyz"],
    )
    result = runner.invoke(mosaic_cli, ["auth", "cikis"])
    assert result.exit_code == 0
    dosya = kimlik_dizini / "kimlik.json"
    veri = json.loads(dosya.read_text())
    assert "token" not in veri
    assert "kullanici" not in veri


def test_auth_durum_token_yok(runner: CliRunner, kimlik_dizini: Path) -> None:
    runner.invoke(mosaic_cli, ["remote", "ayarla", "http://localhost:8000"])
    result = runner.invoke(mosaic_cli, ["auth", "durum"])
    assert "yok" in result.output


# ── depo komutları (API mock'lu) ──────────────────────────────────


def _hazirla_kimlik(kimlik_dizini: Path, runner: CliRunner) -> None:
    """Test için sunucu + token ayarlar."""
    runner.invoke(mosaic_cli, ["remote", "ayarla", "http://test.local"])
    runner.invoke(
        mosaic_cli,
        ["auth", "giris", "--kullanici", "testci", "--token", "tss_test123"],
    )


def test_depo_olustur_basarili(
    runner: CliRunner, kimlik_dizini: Path
) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    mock_yanit = MagicMock()
    mock_yanit.status_code = 200
    mock_yanit.json.return_value = {
        "id": "abc",
        "sahip": "testci",
        "ad": "projem",
        "aciklama": "",
        "ozel": False,
        "olusturma": "2026-01-01T00:00:00+00:00",
        "guncelleme": "2026-01-01T00:00:00+00:00",
    }
    with patch("requests.post", return_value=mock_yanit):
        result = runner.invoke(mosaic_cli, ["depo", "olustur", "projem"])
    assert result.exit_code == 0
    assert "projem" in result.output


def test_depo_listele_bos(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    mock_yanit = MagicMock()
    mock_yanit.status_code = 200
    mock_yanit.json.return_value = {"depolar": [], "sayi": 0}
    with patch("requests.get", return_value=mock_yanit):
        result = runner.invoke(mosaic_cli, ["depo", "listele"])
    assert result.exit_code == 0
    assert "Henüz depo yok" in result.output


def test_depo_listele_dolu(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    mock_yanit = MagicMock()
    mock_yanit.status_code = 200
    mock_yanit.json.return_value = {
        "depolar": [
            {
                "id": "1",
                "sahip": "testci",
                "ad": "proje-a",
                "aciklama": "Deneme",
                "ozel": False,
                "olusturma": "2026-01-01T00:00:00+00:00",
                "guncelleme": "2026-01-01T00:00:00+00:00",
            }
        ],
        "sayi": 1,
    }
    with patch("requests.get", return_value=mock_yanit):
        result = runner.invoke(mosaic_cli, ["depo", "listele"])
    assert result.exit_code == 0
    assert "proje-a" in result.output


def test_depo_bilgi(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    mock_yanit = MagicMock()
    mock_yanit.status_code = 200
    mock_yanit.json.return_value = {
        "id": "1",
        "sahip": "testci",
        "ad": "projem",
        "aciklama": "Açıklama",
        "ozel": False,
        "olusturma": "2026-01-01T00:00:00+00:00",
        "guncelleme": "2026-01-01T00:00:00+00:00",
    }
    with patch("requests.get", return_value=mock_yanit):
        result = runner.invoke(mosaic_cli, ["depo", "bilgi", "testci/projem"])
    assert result.exit_code == 0
    assert "projem" in result.output
    assert "Clone URL" in result.output


def test_depo_sil_onay_ile(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    mock_yanit = MagicMock()
    mock_yanit.status_code = 200
    mock_yanit.json.return_value = {"tamam": True}
    with patch("requests.delete", return_value=mock_yanit):
        result = runner.invoke(
            mosaic_cli, ["depo", "sil", "testci/eski", "--evet"]
        )
    assert result.exit_code == 0
    assert "Silindi" in result.output


def test_depo_sil_format_hatasi(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    result = runner.invoke(mosaic_cli, ["depo", "sil", "sadece-ad", "--evet"])
    assert result.exit_code != 0
    assert "Format" in result.output


# ── klon / gonder (subprocess mock'lu) ───────────────────────────


def test_klon(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        result = runner.invoke(mosaic_cli, ["klon", "testci/projem"])
    assert result.exit_code == 0
    assert mock_run.called
    # git clone çağrıldı mı?
    args = mock_run.call_args[0][0]
    assert args[0] == "git"
    assert args[1] == "clone"
    assert "projem" in args[2]  # URL'de repo adı var


def test_klon_format_hatasi(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    result = runner.invoke(mosaic_cli, ["klon", "sadece-ad"])
    assert result.exit_code != 0


def test_gonder(runner: CliRunner, kimlik_dizini: Path) -> None:
    _hazirla_kimlik(kimlik_dizini, runner)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        result = runner.invoke(mosaic_cli, ["gonder", "testci/projem"])
    assert result.exit_code == 0
    # git push çağrıldı mı?
    cagrilar = [c[0][0] for c in mock_run.call_args_list]
    assert any("push" in komut for komut in cagrilar)
