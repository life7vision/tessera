"""MosaicStore birim testleri."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tessera.mosaic.store import MosaicStore


@pytest.fixture
def store(tmp_path: Path) -> MosaicStore:
    return MosaicStore(tmp_path / "mosaic_depo")


# ── Depo testleri ─────────────────────────────────────────────────


def test_depo_olustur(store: MosaicStore) -> None:
    depo = store.depo_olustur("life7vision", "test-repo", aciklama="Test deposu")
    assert depo["sahip"] == "life7vision"
    assert depo["ad"] == "test-repo"
    assert depo["aciklama"] == "Test deposu"
    assert depo["ozel"] is False
    assert depo["id"]
    # Disk üzerinde bare repo oluştu mu?
    assert (store.ocak / "life7vision" / "test-repo.git").exists()
    assert (store.ocak / "life7vision" / "test-repo.git" / "HEAD").exists()


def test_depo_olustur_ozel(store: MosaicStore) -> None:
    depo = store.depo_olustur("org", "gizli", ozel=True)
    assert depo["ozel"] is True


def test_depo_tekrar_olustur_hata_verir(store: MosaicStore) -> None:
    store.depo_olustur("life7vision", "proje")
    with pytest.raises(ValueError, match="zaten var"):
        store.depo_olustur("life7vision", "proje")


def test_depo_var_mi(store: MosaicStore) -> None:
    assert store.depo_var_mi("life7vision", "yok") is False
    store.depo_olustur("life7vision", "var")
    assert store.depo_var_mi("life7vision", "var") is True


def test_depo_getir(store: MosaicStore) -> None:
    store.depo_olustur("user", "repo", aciklama="Açıklama")
    d = store.depo_getir("user", "repo")
    assert d is not None
    assert d["aciklama"] == "Açıklama"


def test_depo_getir_yok(store: MosaicStore) -> None:
    assert store.depo_getir("user", "yok") is None


def test_depo_listele_bos(store: MosaicStore) -> None:
    assert store.depo_listele() == []


def test_depo_listele(store: MosaicStore) -> None:
    store.depo_olustur("user", "a")
    store.depo_olustur("user", "b")
    store.depo_olustur("diger", "c")
    assert len(store.depo_listele()) == 3
    assert len(store.depo_listele(sahip="user")) == 2
    assert len(store.depo_listele(sahip="diger")) == 1


def test_depo_sil(store: MosaicStore) -> None:
    store.depo_olustur("user", "silinecek")
    assert store.depo_var_mi("user", "silinecek")
    store.depo_sil("user", "silinecek")
    assert not store.depo_var_mi("user", "silinecek")
    assert store.depo_getir("user", "silinecek") is None


def test_depo_sil_yoksa_hata_vermez(store: MosaicStore) -> None:
    # Kayıt yok, disk yok — yine de hata vermemeli
    store.depo_sil("user", "olmayan")


# ── Bare repo git işlemi ──────────────────────────────────────────


def test_bare_repo_git_commit_push(store: MosaicStore, tmp_path: Path) -> None:
    """Bare repo'ya yerel git push çalışıyor mu?"""
    store.depo_olustur("user", "git-test")
    bare_path = store.depo_yolu("user", "git-test")

    # Geçici çalışma dizini oluştur ve commit yap
    work = tmp_path / "work"
    work.mkdir()
    subprocess.run(["git", "init", str(work)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(work), "config", "user.email", "test@test.com"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(work), "config", "user.name", "Test"], check=True, capture_output=True)
    (work / "dosya.txt").write_text("merhaba")
    subprocess.run(["git", "-C", str(work), "add", "."], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(work), "commit", "-m", "ilk commit"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(work), "remote", "add", "origin", str(bare_path)], check=True, capture_output=True)
    result = subprocess.run(
        ["git", "-C", str(work), "push", "origin", "HEAD:main"],
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr.decode()

    # Bare repo'da commit var mı?
    log = subprocess.run(
        ["git", "-C", str(bare_path), "log", "--oneline"],
        capture_output=True, text=True,
    )
    assert "ilk commit" in log.stdout


# ── Token testleri ────────────────────────────────────────────────


def test_token_olustur_ve_dogrula(store: MosaicStore) -> None:
    ham = store.token_olustur("life7vision", etiket="laptop")
    assert ham.startswith("tss_")
    assert store.token_dogrula(ham) == "life7vision"


def test_token_yanlis_dogrulama(store: MosaicStore) -> None:
    assert store.token_dogrula("tss_yanlis123") is None


def test_token_listele(store: MosaicStore) -> None:
    store.token_olustur("user", "token-1")
    store.token_olustur("user", "token-2")
    store.token_olustur("diger", "token-3")
    liste = store.token_listele("user")
    assert len(liste) == 2
    etiketler = {t["etiket"] for t in liste}
    assert "token-1" in etiketler
    assert "token-2" in etiketler


def test_token_iptal(store: MosaicStore) -> None:
    ham = store.token_olustur("user", "iptal-edilecek")
    token_id = store.token_listele("user")[0]["id"]
    assert store.token_iptal(token_id, "user") is True
    assert store.token_dogrula(ham) is None


def test_token_iptal_baska_kullanici(store: MosaicStore) -> None:
    store.token_olustur("user-a", "token")
    token_id = store.token_listele("user-a")[0]["id"]
    # user-b başkasının tokenını iptal edemez
    assert store.token_iptal(token_id, "user-b") is False
    # Hâlâ geçerli mi?
    ham = store.token_olustur("user-a", "yeni")
    assert store.token_dogrula(ham) == "user-a"
