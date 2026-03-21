"""Mosaic REST API — depo ve token yönetimi (/api/v1/mosaic/*)."""

from __future__ import annotations

import base64
import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()


# ── Kimlik doğrulama yardımcısı ───────────────────────────────────


def _zorunlu_auth(request: Request) -> str:
    """Bearer veya Basic token doğrular, kullanıcı adı döner. Başarısızsa 401."""
    auth = request.headers.get("authorization", "")
    store = request.app.state.mosaic_store
    kullanici: str | None = None

    if auth.startswith("Bearer "):
        kullanici = store.token_dogrula(auth[7:])
    elif auth.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth[6:]).decode()
            _, token = decoded.split(":", 1)
            kullanici = store.token_dogrula(token)
        except Exception:
            pass

    if not kullanici:
        raise HTTPException(status_code=401, detail="Kimlik doğrulama gerekli.")
    return kullanici


# ── Depo endpoint'leri ────────────────────────────────────────────


class DepoBilgi(BaseModel):
    ad: str
    aciklama: str = ""
    ozel: bool = False


@router.get("/repos")
async def depo_listele(request: Request, sahip: str = ""):
    store = request.app.state.mosaic_store
    depolar = store.depo_listele(sahip or None)
    return {"depolar": depolar, "sayi": len(depolar)}


@router.get("/repos/{sahip}/{ad}")
async def depo_getir(request: Request, sahip: str, ad: str):
    store = request.app.state.mosaic_store
    depo = store.depo_getir(sahip, ad)
    if not depo:
        raise HTTPException(status_code=404, detail="Depo bulunamadı.")
    return depo


@router.post("/repos/{sahip}")
async def depo_olustur(request: Request, sahip: str, bilgi: DepoBilgi):
    _zorunlu_auth(request)
    store = request.app.state.mosaic_store
    try:
        depo = store.depo_olustur(sahip, bilgi.ad, bilgi.aciklama, bilgi.ozel)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return depo


@router.delete("/repos/{sahip}/{ad}")
async def depo_sil(request: Request, sahip: str, ad: str):
    _zorunlu_auth(request)
    store = request.app.state.mosaic_store
    if not store.depo_var_mi(sahip, ad):
        raise HTTPException(status_code=404, detail="Depo bulunamadı.")
    store.depo_sil(sahip, ad)
    return {"tamam": True}


# ── Token endpoint'leri ───────────────────────────────────────────


class TokenIstegi(BaseModel):
    kullanici: str
    etiket: str = ""
    master_anahtar: str = ""  # ilk token üretimi için basit yetki


@router.post("/auth/token")
async def token_olustur(request: Request, istek: TokenIstegi):
    """Yeni API token üretir. Var olan tokenla ya da master anahtarla yetkilendirilir."""
    store = request.app.state.mosaic_store
    master = os.getenv("TESSERA_MOSAIC_MASTER", "")

    # İlk token: master anahtarla; sonraki tokenlar: var olan tokenla
    auth = request.headers.get("authorization", "")
    mevcut_kullanici: str | None = None
    if auth:
        try:
            mevcut_kullanici = _zorunlu_auth(request)
        except HTTPException:
            pass

    if not mevcut_kullanici:
        # Master anahtarla doğrulama dene
        if not master or istek.master_anahtar != master:
            raise HTTPException(
                status_code=403,
                detail="Mevcut token veya geçerli master anahtar gerekli.",
            )

    ham = store.token_olustur(istek.kullanici, istek.etiket)
    return {"token": ham, "kullanici": istek.kullanici}


@router.get("/auth/tokenlar")
async def token_listele(request: Request):
    kullanici = _zorunlu_auth(request)
    store = request.app.state.mosaic_store
    return {"tokenlar": store.token_listele(kullanici)}


@router.delete("/auth/tokenlar/{token_id}")
async def token_iptal(request: Request, token_id: str):
    kullanici = _zorunlu_auth(request)
    store = request.app.state.mosaic_store
    if not store.token_iptal(token_id, kullanici):
        raise HTTPException(status_code=404, detail="Token bulunamadı.")
    return {"tamam": True}


@router.get("/auth/durum")
async def auth_durum(request: Request):
    """Mevcut token'ın geçerli olup olmadığını kontrol eder."""
    kullanici = _zorunlu_auth(request)
    return {"kullanici": kullanici, "aktif": True}
