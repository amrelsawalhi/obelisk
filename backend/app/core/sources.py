"""Source registry — how Obelisk reaches a connection, decided once.

A connection is *data*, not behaviour. Before this module, "is this source
attachable?" was answered by independent ``source_type == "postgres"`` checks
scattered across the query engine, the catalog tree and the ingest dispatcher, so
adding a source type meant editing all three in step and hoping none was missed.

:func:`resolve_source` answers it once, as a value:

* **attach** — DuckDB reads it live through its own scanner: zero copy, always
  fresh, read-only by construction.
* **sdk** — reachable only through a client library (MySQL, Mongo, Oracle,
  SQL Server): rows must be pulled before they can be queried.
* **http** — a REST/ERP endpoint: one request per pull, no predicate pushdown.

The driver is persisted on the row (``connections.driver``) so an operator can pin
it, and inferred from ``source_type`` for rows written before that column existed.
Capabilities are declared here rather than discovered at runtime, because the UI
needs to know what a source can do before it tries.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

ATTACH = "attach"
SDK = "sdk"
HTTP = "http"
DRIVERS: tuple[str, ...] = (ATTACH, SDK, HTTP)

_DRIVER_BY_SOURCE_TYPE: dict[str, str] = {
    # DuckDB has a native postgres scanner, so these never need a copy.
    "postgres": ATTACH,
    "postgresql": ATTACH,
    # Client libraries only — no attach path exists yet.
    "mysql": SDK,
    "mariadb": SDK,
    "mongodb": SDK,
    "mongo": SDK,
    "oracle": SDK,
    "sqlserver": SDK,
    "mssql": SDK,
    # Pulled over HTTP.
    "api": HTTP,
    "rest": HTTP,
    "rest_api": HTTP,
    "erp": HTTP,
}

_CAPABILITIES: dict[str, tuple[str, ...]] = {
    ATTACH: ("sql", "pushdown", "live", "preview", "federated"),
    SDK: ("sql", "pull", "preview"),
    HTTP: ("pull", "preview"),
}

# A live attached source cannot be written through Obelisk at all; a pulled one
# can, but only into the lakehouse, never back into the origin.
_DEFAULT_READ_ONLY: dict[str, bool] = {ATTACH: True, SDK: False, HTTP: False}

_ALIAS_RE = re.compile(r"[^a-zA-Z0-9_]")


class UnsupportedSource(ValueError):
    """Raised when a connection names a source type Obelisk cannot reach."""


def connection_alias(name: str) -> str:
    """Deterministic DuckDB alias / catalog namespace for a connection name.

    Stable across restarts and safe to interpolate, because the same value is used
    as an identifier by the query engine.
    """
    alias = _ALIAS_RE.sub("_", (name or "").lower()).strip("_")
    return alias or "external"


@dataclass(frozen=True)
class Recipe:
    """Everything the rest of the system needs to know about reaching a source."""

    driver: str
    source_type: str
    alias: str
    capabilities: tuple[str, ...]
    read_only: bool

    @property
    def is_attachable(self) -> bool:
        return self.driver == ATTACH

    def has(self, capability: str) -> bool:
        return capability in self.capabilities

    def as_dict(self) -> dict[str, Any]:
        return {
            "driver": self.driver,
            "source_type": self.source_type,
            "alias": self.alias,
            "capabilities": list(self.capabilities),
            "read_only": self.read_only,
        }


def infer_driver(source_type: str) -> str:
    """Maps a ``source_type`` onto a driver, refusing to guess."""
    driver = _DRIVER_BY_SOURCE_TYPE.get((source_type or "").lower().strip())
    if driver is None:
        known = ", ".join(sorted(_DRIVER_BY_SOURCE_TYPE))
        raise UnsupportedSource(
            f"Unknown source type {source_type!r}. Known types: {known}."
        )
    return driver


def capabilities_for(driver: str) -> tuple[str, ...]:
    if driver not in _CAPABILITIES:
        raise UnsupportedSource(f"Unknown driver {driver!r}; expected one of {DRIVERS}.")
    return _CAPABILITIES[driver]


def resolve_source(connection: Any) -> Recipe:
    """Builds the :class:`Recipe` for a ``Connection`` row (or any duck-typed one).

    A pinned ``driver`` wins over inference; anything unrecognised raises instead
    of guessing, so a typo in a connection surfaces at registration rather than as
    a confusing empty catalog node.
    """
    source_type = (getattr(connection, "source_type", "") or "").lower().strip()
    pinned = (getattr(connection, "driver", None) or "").lower().strip()
    driver = pinned or infer_driver(source_type)
    if driver not in DRIVERS:
        raise UnsupportedSource(
            f"Connection driver {driver!r} is not one of {DRIVERS}."
        )

    read_only = getattr(connection, "read_only", None)
    if read_only is None:
        read_only = _DEFAULT_READ_ONLY[driver]

    return Recipe(
        driver=driver,
        source_type=source_type,
        alias=connection_alias(getattr(connection, "name", "")),
        capabilities=capabilities_for(driver),
        read_only=bool(read_only),
    )


def attachable(connections: Iterable[Any]) -> list[Any]:
    """Filters the connections a query engine may attach, skipping unsupported ones."""
    result = []
    for connection in connections:
        try:
            if resolve_source(connection).is_attachable:
                result.append(connection)
        except UnsupportedSource:
            continue
    return result
