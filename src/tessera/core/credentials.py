"""Credential management — read/write .env file, mask keys."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

try:
    from dotenv import dotenv_values, set_key
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False

# Canonical env variable names per service
SERVICE_KEYS: dict[str, list[str]] = {
    "kaggle":       ["KAGGLE_KEY"],
    "huggingface":  ["HF_TOKEN"],
    "github":       ["GITHUB_TOKEN"],
}

# Human-readable field labels per service
SERVICE_LABELS: dict[str, list[str]] = {
    "kaggle":       ["username:api_key"],
    "huggingface":  ["Token"],
    "github":       ["Token (opsiyonel)"],
}


def _mask(value: str) -> str:
    """Return a masked version: first 4 + •••• + last 4 chars."""
    if not value:
        return ""
    if len(value) <= 10:
        return "•" * len(value)
    return value[:4] + "•" * 8 + value[-4:]


class CredentialManager:
    """Manage service credentials stored in a .env file."""

    def __init__(self, env_path: Path | None = None) -> None:
        self.env_path = Path(env_path) if env_path else Path(".env")

    # ── Read ──────────────────────────────────────────────────────

    def get_raw(self, env_var: str) -> str | None:
        """Return the raw value: env variable takes priority, then .env file."""
        live = os.environ.get(env_var)
        if live:
            return live
        if _DOTENV_AVAILABLE and self.env_path.exists():
            return dotenv_values(self.env_path).get(env_var)
        return None

    def get_masked(self, env_var: str) -> str | None:
        """Return a masked value for display."""
        raw = self.get_raw(env_var)
        return _mask(raw) if raw else None

    def is_set(self, env_var: str) -> bool:
        return bool(self.get_raw(env_var))

    # ── Write ─────────────────────────────────────────────────────

    def set_key(self, env_var: str, value: str) -> None:
        """Write or update a key in the .env file."""
        if not _DOTENV_AVAILABLE:
            raise RuntimeError("python-dotenv yüklü değil.")
        self.env_path.touch(exist_ok=True)
        set_key(str(self.env_path), env_var, value, quote_mode="never")
        # Also update the live process environment
        os.environ[env_var] = value

    def delete_key(self, env_var: str) -> None:
        """Remove a key from the .env file."""
        if not _DOTENV_AVAILABLE or not self.env_path.exists():
            return
        lines = self.env_path.read_text().splitlines()
        new_lines = [l for l in lines if not l.startswith(f"{env_var}=")]
        self.env_path.write_text("\n".join(new_lines) + "\n")
        os.environ.pop(env_var, None)

    # ── Summary ───────────────────────────────────────────────────

    def all_services(self) -> list[dict[str, Any]]:
        """Return credential status for all services."""
        result = []
        for service, env_vars in SERVICE_KEYS.items():
            keys = []
            for i, var in enumerate(env_vars):
                raw = self.get_raw(var)
                keys.append({
                    "env_var": var,
                    "label": SERVICE_LABELS[service][i] if i < len(SERVICE_LABELS[service]) else var,
                    "is_set": bool(raw),
                    "masked": _mask(raw) if raw else None,
                })
            result.append({
                "service": service,
                "fields": keys,
                "connected": all(k["is_set"] for k in keys if service != "github"),
            })
        return result
