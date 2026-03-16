"""Tests for the notify hook."""

from __future__ import annotations

from tessera.hooks.notify import NotifyHook


def test_notify_hook_records_output():
    hook = NotifyHook({})
    hook.execute("post_ingest", {"message": "tamam"})

    assert "tamam" in hook.console.export_text()

