"""Mosaic REST API entegrasyon testleri."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from tessera.mosaic.store import MosaicStore
from tessera.web.app import create_app


@pytest.fixture
def mosaic_store(tmp_path: Path) -> MosaicStore:
    return MosaicStore(tmp_path / "mosaic_depo")


@pytest.fixture
def client(tmp_path: Path, mosaic_store: MosaicStore) -> TestClient:
    """Mosaic store'u enjekte edilmiş test client'ı döner."""
    app = create_app()
    app.state.mosaic_store = mosaic_store
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def token_ve_header(mosaic_store: MosaicStore) -> tuple[str, dict]:
    ham = mosaic_store.token_olustur("testci", etiket="ci")
    return ham, {"Authorization": f"Bearer {ham}"}


# ── Depo CRUD ─────────────────────────────────────────────────────


def test_depo_listele_bos(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    r = client.get("/api/v1/mosaic/repos", headers=headers)
    assert r.status_code == 200
    assert r.json()["sayi"] == 0


def test_depo_olustur_ve_listele(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    r = client.post(
        "/api/v1/mosaic/repos/testci",
        json={"ad": "proje-a", "aciklama": "İlk proje"},
        headers=headers,
    )
    assert r.status_code == 200
    d = r.json()
    assert d["ad"] == "proje-a"
    assert d["sahip"] == "testci"

    liste = client.get("/api/v1/mosaic/repos?sahip=testci", headers=headers)
    assert liste.json()["sayi"] == 1


def test_depo_olustur_yetki_gerekli(client: TestClient) -> None:
    r = client.post("/api/v1/mosaic/repos/user", json={"ad": "repo"})
    assert r.status_code == 401


def test_depo_tekrar_olustur_409(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    client.post("/api/v1/mosaic/repos/testci", json={"ad": "cift"}, headers=headers)
    r = client.post("/api/v1/mosaic/repos/testci", json={"ad": "cift"}, headers=headers)
    assert r.status_code == 409


def test_depo_getir(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    client.post("/api/v1/mosaic/repos/testci", json={"ad": "bul-beni"}, headers=headers)
    r = client.get("/api/v1/mosaic/repos/testci/bul-beni", headers=headers)
    assert r.status_code == 200
    assert r.json()["ad"] == "bul-beni"


def test_depo_getir_404(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    r = client.get("/api/v1/mosaic/repos/user/yok", headers=headers)
    assert r.status_code == 404


def test_depo_sil(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    client.post("/api/v1/mosaic/repos/testci", json={"ad": "silinecek"}, headers=headers)
    r = client.delete("/api/v1/mosaic/repos/testci/silinecek", headers=headers)
    assert r.status_code == 200
    assert r.json()["tamam"] is True
    assert client.get("/api/v1/mosaic/repos/testci/silinecek", headers=headers).status_code == 404


# ── Auth endpoint'leri ────────────────────────────────────────────


def test_auth_durum(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    r = client.get("/api/v1/mosaic/auth/durum", headers=headers)
    assert r.status_code == 200
    assert r.json()["kullanici"] == "testci"
    assert r.json()["aktif"] is True


def test_auth_durum_yetkisiz(client: TestClient) -> None:
    r = client.get("/api/v1/mosaic/auth/durum")
    assert r.status_code == 401


def test_token_olustur_master_anahtarla(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TESSERA_MOSAIC_MASTER", "gizli123")
    r = client.post(
        "/api/v1/mosaic/auth/token",
        json={"kullanici": "yeni-kullanici", "etiket": "test", "master_anahtar": "gizli123"},
    )
    assert r.status_code == 200
    assert r.json()["token"].startswith("tss_")
    assert r.json()["kullanici"] == "yeni-kullanici"


def test_token_olustur_yanlis_master(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TESSERA_MOSAIC_MASTER", "gizli123")
    r = client.post(
        "/api/v1/mosaic/auth/token",
        json={"kullanici": "hacker", "master_anahtar": "yanlis"},
    )
    assert r.status_code == 403


def test_token_listele_ve_iptal(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    liste = client.get("/api/v1/mosaic/auth/tokenlar", headers=headers)
    assert liste.status_code == 200
    tokenlar = liste.json()["tokenlar"]
    assert len(tokenlar) >= 1

    token_id = tokenlar[0]["id"]
    iptal = client.delete(f"/api/v1/mosaic/auth/tokenlar/{token_id}", headers=headers)
    assert iptal.status_code == 200


def test_token_iptal_404(client: TestClient, token_ve_header: tuple) -> None:
    _, headers = token_ve_header
    r = client.delete("/api/v1/mosaic/auth/tokenlar/olmayan-id", headers=headers)
    assert r.status_code == 404
