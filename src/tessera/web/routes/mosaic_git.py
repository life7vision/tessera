"""Mosaic git-http-backend köprüsü — standart git protokolünü FastAPI üzerinden sunar."""

from __future__ import annotations

import asyncio
import base64
import os

from fastapi import APIRouter, Request, Response

router = APIRouter()

# ── Auth yardımcısı ───────────────────────────────────────────────


def _auth_kullanici(authorization: str | None, store) -> str | None:
    """Authorization başlığından kullanıcı adını çözer. Bulamazsa None döner."""
    if not authorization:
        return None
    if authorization.startswith("Basic "):
        try:
            decoded = base64.b64decode(authorization[6:]).decode("utf-8", errors="replace")
            _, token = decoded.split(":", 1)
            return store.token_dogrula(token)
        except Exception:
            return None
    if authorization.startswith("Bearer "):
        return store.token_dogrula(authorization[7:])
    return None


# ── git-http-backend köprüsü ──────────────────────────────────────


async def _git_backend_yanit(
    request: Request,
    path_info: str,
    git_root: str,
    authorization: str | None,
) -> Response:
    """git-http-backend CGI sürecini çalıştırır ve yanıtı döner."""
    env = {
        **os.environ,
        "GIT_PROJECT_ROOT": git_root,
        "GIT_HTTP_EXPORT_ALL": "1",
        "PATH_INFO": path_info,
        "REQUEST_METHOD": request.method,
        "QUERY_STRING": request.url.query or "",
        "CONTENT_TYPE": request.headers.get("content-type", ""),
        "HTTP_CONTENT_ENCODING": request.headers.get("content-encoding", ""),
        "REMOTE_ADDR": request.client.host if request.client else "127.0.0.1",
    }
    if authorization:
        env["HTTP_AUTHORIZATION"] = authorization

    body = await request.body()

    try:
        proc = await asyncio.create_subprocess_exec(
            "git",
            "http-backend",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, _ = await proc.communicate(input=body)
    except FileNotFoundError:
        return Response(status_code=500, content=b"git bulunamadi")

    # CGI yanıtını başlık + gövde olarak ayrıştır
    sep = b"\r\n\r\n"
    split_at = stdout.find(sep)
    if split_at == -1:
        sep = b"\n\n"
        split_at = stdout.find(sep)

    if split_at == -1:
        return Response(content=stdout, status_code=200)

    header_raw = stdout[:split_at].decode("utf-8", errors="replace")
    response_body = stdout[split_at + len(sep):]

    headers: dict[str, str] = {}
    status_code = 200
    for line in header_raw.splitlines():
        if ": " not in line:
            continue
        k, v = line.split(": ", 1)
        if k.lower() == "status":
            status_code = int(v.split()[0])
        else:
            headers[k] = v

    return Response(content=response_body, status_code=status_code, headers=headers)


# ── Route'lar ─────────────────────────────────────────────────────


@router.api_route(
    "/mosaic/git/{sahip}/{depo_adi}/{git_yol:path}",
    methods=["GET", "POST"],
)
async def git_istek(
    request: Request,
    sahip: str,
    depo_adi: str,
    git_yol: str,
) -> Response:
    """Tüm git HTTP smart protocol isteklerini karşılar."""
    store = request.app.state.mosaic_store

    # .git uzantısını normalize et
    ad = depo_adi[:-4] if depo_adi.endswith(".git") else depo_adi

    if not store.depo_var_mi(sahip, ad):
        return Response(status_code=404, content=b"Depo bulunamadi")

    # Yazma işlemleri (push) kimlik doğrulama gerektirir
    authorization = request.headers.get("authorization")
    yazma_istegi = (
        "git-receive-pack" in git_yol
        or (request.method == "POST" and "receive-pack" in git_yol)
    )
    if yazma_istegi:
        kullanici = _auth_kullanici(authorization, store)
        if not kullanici:
            return Response(
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Tessera Mosaic"'},
                content=b"Kimlik dogrulamasi gerekli",
            )

    git_root = str(store.ocak)
    path_info = f"/{sahip}/{ad}.git/{git_yol}"

    return await _git_backend_yanit(request, path_info, git_root, authorization)
