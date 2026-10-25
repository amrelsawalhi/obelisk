"""Catalog service — the one place that writes ``catalog_objects``.

Routes and engines ask this module to record what exists; nothing else builds a
:class:`CatalogObject` by hand, so identity (``fqn``) and capabilities cannot drift
between callers.

Two kinds of row live here, and they are written differently on purpose:

* **lakehouse rows** (no ``connection_id``) are a projection. :meth:`refresh` walks
  the warehouse, reads each file's footer and rewrites them, so a row cannot outlive
  its file or describe data nobody wrote.
* **external rows** (a ``connection_id``) are a snapshot of a source's own catalog,
  taken when it was reconciled, because the source may be unreachable when someone
  reads the tree.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.sources import resolve_source
from app.models.catalog_object import CatalogObject


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _strip_schema_prefix(name: str, schema: str) -> str:
    """The introspector prefixes non-``public`` names as ``schema.table``."""
    prefix = f"{schema}."
    return name[len(prefix):] if name.startswith(prefix) else name


class CatalogService:
    """Upserts and deletes catalog rows; never raises for a missing object."""

    async def upsert_object(
        self,
        session: AsyncSession,
        *,
        fqn: str,
        name: str,
        kind: str,
        backend: str,
        location: Optional[str] = None,
        connection_id: Optional[str] = None,
        schema_json: Any = None,
        capabilities_json: Any = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        produced_by: Optional[str] = None,
        docs: Optional[str] = None,
    ) -> CatalogObject:
        existing = (
            await session.execute(select(CatalogObject).where(CatalogObject.fqn == fqn))
        ).scalar_one_or_none()

        if existing is None:
            obj = CatalogObject(
                fqn=fqn,
                name=name,
                kind=kind,
                backend=backend,
                location=location,
                connection_id=connection_id,
                schema_json=schema_json,
                capabilities_json=capabilities_json,
                row_count=row_count,
                size_bytes=size_bytes,
                produced_by=produced_by,
                docs=docs,
                version=1,
            )
            session.add(obj)
            return obj

        existing.name = name
        existing.kind = kind
        existing.backend = backend
        existing.location = location
        existing.connection_id = connection_id
        existing.schema_json = schema_json
        existing.capabilities_json = capabilities_json
        existing.row_count = row_count
        existing.size_bytes = size_bytes
        if produced_by is not None:
            existing.produced_by = produced_by
            existing.last_materialized_at = _utcnow()
        if docs is not None:
            existing.docs = docs
        existing.version += 1
        return existing

    async def reconcile_connection(
        self,
        session: AsyncSession,
        connection: Any,
        tables: Sequence[dict],
    ) -> dict[str, Any]:
        """Mirrors one source's live table list into the registry.

        ``shortcut`` means DuckDB can read it in place; ``external`` means the rows
        have to be pulled first. Objects that vanished from the source are deleted
        in a single statement rather than listed and removed one at a time (M7).
        """
        recipe = resolve_source(connection)
        kind = "shortcut" if recipe.is_attachable else "external"
        seen: set[str] = set()

        for table in tables:
            raw_name = str(table.get("name") or "").strip()
            if not raw_name:
                continue
            schema_name = str(table.get("schema_name") or "public").strip() or "public"
            name = _strip_schema_prefix(raw_name, schema_name)
            if not name:
                continue

            fqn = f"{recipe.alias}.{schema_name}.{name}"
            seen.add(fqn)
            await self.upsert_object(
                session,
                fqn=fqn,
                name=name,
                kind=kind,
                backend=recipe.source_type,
                location=f"{schema_name}.{name}",
                connection_id=connection.id,
                schema_json=table.get("columns"),
                capabilities_json=list(recipe.capabilities),
                size_bytes=table.get("size_bytes"),
            )

        removed = await self.delete_missing(session, connection.id, seen)
        await session.commit()
        return {
            "connection_id": connection.id,
            "alias": recipe.alias,
            "driver": recipe.driver,
            "kind": kind,
            "discovered": len(seen),
            "removed": removed,
        }

    async def delete_missing(
        self, session: AsyncSession, connection_id: str, keep: Sequence[str] | set[str]
    ) -> int:
        """Deletes this connection's rows whose fqn is not in ``keep``."""
        stmt = delete(CatalogObject).where(CatalogObject.connection_id == connection_id)
        if keep:
            stmt = stmt.where(CatalogObject.fqn.notin_(list(keep)))
        result = await session.execute(stmt)
        return result.rowcount or 0

    async def delete_for_connection(self, session: AsyncSession, connection_id: str) -> int:
        """Removes a deleted connection's objects in one statement (M7)."""
        result = await session.execute(
            delete(CatalogObject).where(CatalogObject.connection_id == connection_id)
        )
        return result.rowcount or 0

    async def list_objects(
        self,
        session: AsyncSession,
        *,
        connection_id: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> list[CatalogObject]:
        stmt = select(CatalogObject).order_by(CatalogObject.fqn.asc())
        if connection_id is not None:
            stmt = stmt.where(CatalogObject.connection_id == connection_id)
        if kind is not None:
            stmt = stmt.where(CatalogObject.kind == kind)
        return list((await session.execute(stmt)).scalars().all())

    async def refresh(
        self,
        session: AsyncSession,
        scan_result: Optional[Any] = None,
    ) -> dict[str, Any]:
        """Rewrites the lakehouse half of the registry from the files on disk.

        This is the projection: a row exists because a file exists, its physical facts
        are read from that file, and a row whose file is gone is deleted. Nothing has
        to register a table any more — a writer that saves a Parquet file, a dbt
        ``external`` model, or a file copied in by hand all appear the same way.

        Only rows with no ``connection_id`` are touched: external sources are
        snapshots of a database that may be unreachable right now, and deleting them
        because this walk cannot see their files would be destructive.
        """
        from app.core.catalog import scan as scan_warehouse

        result = scan_result
        if result is None:
            result = await asyncio.to_thread(scan_warehouse)

        existing_rows = list(
            (
                await session.execute(
                    select(CatalogObject).where(CatalogObject.connection_id.is_(None))
                )
            )
            .scalars()
            .all()
        )
        existing = {row.fqn: row for row in existing_rows}

        seen: set[str] = set()
        created = updated = 0

        for table in result.tables:
            seen.add(table.fqn)
            schema_json = {"columns": list(table.columns), "path": table.path}
            row = existing.get(table.fqn)
            if row is None:
                session.add(
                    CatalogObject(
                        fqn=table.fqn,
                        name=table.name,
                        kind="parquet",
                        backend="local",
                        location=table.path,
                        schema_json=schema_json,
                        row_count=table.num_rows,
                        size_bytes=table.size_bytes,
                        version=1,
                        last_materialized_at=table.modified_at,
                    )
                )
                created += 1
                continue

            changed = (
                row.name != table.name
                or row.location != table.path
                or row.row_count != table.num_rows
                or row.size_bytes != table.size_bytes
                or row.schema_json != schema_json
            )
            row.name = table.name
            row.kind = "parquet"
            row.backend = "local"
            row.location = table.path
            row.schema_json = schema_json
            row.row_count = table.num_rows
            row.size_bytes = table.size_bytes
            # The file's own timestamp is the honest materialisation time; the column
            # annotation (produced_by, docs, tags) is left exactly as the writer set it.
            row.last_materialized_at = table.modified_at
            if changed:
                row.version = (row.version or 1) + 1
                updated += 1

        removed = 0
        for fqn, row in existing.items():
            if fqn not in seen:
                await session.delete(row)
                removed += 1

        await session.commit()
        return {
            "tables": len(result.tables),
            "created": created,
            "updated": updated,
            "removed": removed,
            "problems": list(result.problems),
        }


catalog_service = CatalogService()
