"""MosaicStore — bare git depo yaşam döngüsü ve token yönetimi."""

from __future__ import annotations

import hashlib
import secrets
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class MosaicStore:
    """Disk üzerindeki bare git depolarını ve kimlik doğrulama tokenlarını yönetir."""

    def __init__(self, root: Path):
        self.root = Path(root)
        # bare repolar: <root>/ocak/<owner>/<name>.git
        self.ocak = self.root / "ocak"
        self.db_path = self.root / "kasa.db"
        self._init()

    # ── Başlatma ──────────────────────────────────────────────────

    def _init(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.ocak.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS depolar (
                    id          TEXT PRIMARY KEY,
                    sahip       TEXT NOT NULL,
                    ad          TEXT NOT NULL,
                    aciklama    TEXT DEFAULT '',
                    ozel        INTEGER DEFAULT 0,
                    olusturma   TEXT NOT NULL,
                    guncelleme  TEXT NOT NULL,
                    UNIQUE(sahip, ad)
                );

                CREATE TABLE IF NOT EXISTS tokenlar (
                    id          TEXT PRIMARY KEY,
                    kullanici   TEXT NOT NULL,
                    hash        TEXT NOT NULL UNIQUE,
                    etiket      TEXT DEFAULT '',
                    olusturma   TEXT NOT NULL,
                    son_kullanim TEXT
                );
            """)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ── Depo yolu ─────────────────────────────────────────────────

    def depo_yolu(self, sahip: str, ad: str) -> Path:
        return self.ocak / sahip / f"{ad}.git"

    def depo_var_mi(self, sahip: str, ad: str) -> bool:
        return self.depo_yolu(sahip, ad).exists()

    # ── Depo CRUD ─────────────────────────────────────────────────

    def depo_olustur(
        self,
        sahip: str,
        ad: str,
        aciklama: str = "",
        ozel: bool = False,
    ) -> dict:
        yol = self.depo_yolu(sahip, ad)
        if yol.exists():
            raise ValueError(f"Depo zaten var: {sahip}/{ad}")
        yol.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "init", "--bare", str(yol)],
            check=True,
            capture_output=True,
        )
        now = _utc_now()
        rid = str(uuid4())
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO depolar (id,sahip,ad,aciklama,ozel,olusturma,guncelleme)"
                " VALUES (?,?,?,?,?,?,?)",
                (rid, sahip, ad, aciklama, int(ozel), now, now),
            )
        return self.depo_getir(sahip, ad)  # type: ignore[return-value]

    def depo_getir(self, sahip: str, ad: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id,sahip,ad,aciklama,ozel,olusturma,guncelleme"
                " FROM depolar WHERE sahip=? AND ad=?",
                (sahip, ad),
            ).fetchone()
        if not row:
            return None
        return _depo_dict(row)

    def depo_listele(self, sahip: str | None = None) -> list[dict]:
        with self._conn() as conn:
            if sahip:
                rows = conn.execute(
                    "SELECT id,sahip,ad,aciklama,ozel,olusturma,guncelleme"
                    " FROM depolar WHERE sahip=? ORDER BY guncelleme DESC",
                    (sahip,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id,sahip,ad,aciklama,ozel,olusturma,guncelleme"
                    " FROM depolar ORDER BY guncelleme DESC"
                ).fetchall()
        return [_depo_dict(r) for r in rows]

    def depo_sil(self, sahip: str, ad: str) -> None:
        yol = self.depo_yolu(sahip, ad)
        if yol.exists():
            shutil.rmtree(yol)
        with self._conn() as conn:
            conn.execute("DELETE FROM depolar WHERE sahip=? AND ad=?", (sahip, ad))

    # ── Token yönetimi ────────────────────────────────────────────

    def token_olustur(self, kullanici: str, etiket: str = "") -> str:
        """Yeni ham token üretir ve hash'ini saklar. Ham token'ı döner."""
        ham = "tss_" + secrets.token_hex(32)
        h = hashlib.sha256(ham.encode()).hexdigest()
        now = _utc_now()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO tokenlar (id,kullanici,hash,etiket,olusturma)"
                " VALUES (?,?,?,?,?)",
                (str(uuid4()), kullanici, h, etiket, now),
            )
        return ham

    def token_dogrula(self, ham: str) -> str | None:
        """Token'ı doğrular, geçerliyse kullanıcı adını döner."""
        h = hashlib.sha256(ham.encode()).hexdigest()
        now = _utc_now()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT kullanici FROM tokenlar WHERE hash=?", (h,)
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE tokenlar SET son_kullanim=? WHERE hash=?", (now, h)
                )
                return row[0]
        return None

    def token_listele(self, kullanici: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,etiket,olusturma,son_kullanim FROM tokenlar WHERE kullanici=?",
                (kullanici,),
            ).fetchall()
        return [
            {"id": r[0], "etiket": r[1], "olusturma": r[2], "son_kullanim": r[3]}
            for r in rows
        ]

    def token_iptal(self, token_id: str, kullanici: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM tokenlar WHERE id=? AND kullanici=?", (token_id, kullanici)
            )
            return cur.rowcount > 0


# ── Yardımcılar ───────────────────────────────────────────────────

def _depo_dict(row: tuple) -> dict:
    return {
        "id": row[0],
        "sahip": row[1],
        "ad": row[2],
        "aciklama": row[3],
        "ozel": bool(row[4]),
        "olusturma": row[5],
        "guncelleme": row[6],
    }
