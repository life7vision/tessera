"""Lineage hook implementation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from tessera.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class LineageHook(BaseHook):
    """Record lineage events to catalog and collect them in context."""

    name = "lineage"
    version = "0.1.0"

    def execute(self, event: str, context: dict[str, Any]) -> None:
        # Always store in context for in-process access
        store = context.setdefault("lineage_events", [])
        store.append({"event": event, "context": {k: v for k, v in context.items() if k != "lineage_events"}})

        # Persist to catalog if version_id and catalog_db are available
        version_id = context.get("version_id")
        catalog_db = context.get("catalog_db") or self.config.get("catalog_db")
        if version_id and catalog_db:
            self._record_to_catalog(
                catalog_db=Path(catalog_db),
                version_id=version_id,
                event=event,
                context=context,
            )

    def _record_to_catalog(
        self, catalog_db: Path, version_id: str, event: str, context: dict[str, Any]
    ) -> None:
        try:
            from tessera.core.catalog import CatalogManager
            catalog = CatalogManager(catalog_db)
            catalog.record_lineage(
                version_id=version_id,
                operation=event,
                plugin_name=self.name,
                parameters={
                    "source": context.get("source"),
                    "source_ref": context.get("source_ref"),
                    "dataset_id": context.get("dataset_id"),
                },
                status="success",
            )
        except Exception as exc:
            logger.warning("Lineage kaydedilemedi: %s", exc)
