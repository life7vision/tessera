"""Tests for the lineage hook."""

from __future__ import annotations

from tessera.hooks.lineage import LineageHook


def test_lineage_hook_appends_event():
    context = {}
    LineageHook({}).execute("transform", context)

    assert context["lineage_events"][0]["event"] == "transform"
