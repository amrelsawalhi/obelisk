"""Synchronous layer lookups for code that does not run on the event loop.

The API talks to layers through SQLAlchemy. The dbt runner, the SDK and anything
inside a subprocess need the same answers from plain Python, so they read the table
directly rather than importing the async engine or holding a session open across a
long dbt run.

Everything here degrades to a single landing layer instead of raising: a layer tree
is a convenience, and a missing table must not stop a pipeline from running.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from app.config import settings
from app.core.layers import InvalidLayerPath, resolve_layer

logger = logging.getLogger(__name__)

# Used only when the layer table is missing or empty (fresh install, or a caller
# running before migrations). It is a name, not a tier.
FALLBACK_LAYER = "raw"


def layer_paths() -> list[str]:
    """Every layer path, ordered. Falls back to one landing layer."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=10.0) as conn:
            rows = conn.execute("SELECT path FROM layers ORDER BY path").fetchall()
    except sqlite3.Error as exc:
        logger.warning("Could not read the layer table: %s", exc)
        return [FALLBACK_LAYER]
    return [row[0] for row in rows] or [FALLBACK_LAYER]


def default_layer_path() -> str:
    """The layer marked default, else the first one, else the fallback."""
    try:
        with sqlite3.connect(settings.sqlite_db_path, timeout=10.0) as conn:
            row = conn.execute(
                "SELECT path FROM layers WHERE is_default = 1 LIMIT 1"
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT path FROM layers ORDER BY path LIMIT 1"
                ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("Could not read the default layer: %s", exc)
        return FALLBACK_LAYER
    return row[0] if row else FALLBACK_LAYER


def find_layer_path(value: Optional[str]) -> Optional[str]:
    """Resolves a user-supplied layer reference to a stored layer path.

    Accepts an exact path, or a bare leaf name (``sales`` finds ``marts.sales``) so
    callers do not have to know the whole tree.
    """
    if not value:
        return None
    candidate = value.strip()
    paths = layer_paths()
    if candidate in paths:
        return candidate
    matches = [path for path in paths if path.rsplit(".", 1)[-1] == candidate]
    return matches[0] if len(matches) == 1 else None


def layer_schemas(paths: Optional[list[str]] = None) -> dict[str, str]:
    """Maps layer paths to the warehouse schema each one resolves to."""
    mapping: dict[str, str] = {}
    for path in paths or layer_paths():
        try:
            mapping[path] = resolve_layer(path).warehouse_schema
        except InvalidLayerPath as exc:
            logger.warning("Skipping unrepresentable layer %r: %s", path, exc)
    return mapping
