"""Connection resolution shared by the query editor and the orchestrator.

Attaching a Postgres source into DuckDB needs four answers: which connections are
attachable, whether the SQL actually references them, their decrypted credentials, and
a host that is reachable from this process. The editor used to own this logic; the
pipeline's ``raw_sql`` step needs the identical answer, so it lives here once. A third
copy is the drift this module exists to prevent.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.sources import UnsupportedSource, attachment_for, resolve_source
from app.core.vault import decrypt
from app.models.connection import Connection


def is_connection_referenced(sql: str, alias: str, db_name: Optional[str] = None) -> bool:
    """Whether a source's alias (or database name) appears in the SQL."""
    clean = re.sub(r"--[^\n]*", "", sql)
    clean = re.sub(r"/\*.*?\*/", "", clean, flags=re.DOTALL)
    tokens = set(re.findall(r"[a-zA-Z0-9_]+", clean.lower()))
    return alias.lower() in tokens or (bool(db_name) and db_name.lower() in tokens)


async def attachable_attachments(
    db: AsyncSession, sql: Optional[str] = None
) -> list[dict[str, Any]]:
    """The connections DuckDB can attach live, filtered to what the SQL references.

    Attachability is a property of the connection's recipe (``app.core.sources``), not a
    hardcoded type, and the shape of each dict comes from
    :func:`app.core.sources.attachment_for` — so a Mongo source yields a URI, a Postgres
    source a DSN, and adding a scanner needs no change here.
    """
    try:
        rows = (await db.execute(select(Connection))).scalars().all()
    except Exception:
        return []

    attachments: list[dict[str, Any]] = []
    for conn in rows:
        try:
            recipe = resolve_source(conn)
        except UnsupportedSource:
            continue
        if not recipe.is_attachable:
            continue
        if sql is not None and not is_connection_referenced(sql, recipe.alias, conn.database):
            continue
        attachments.append(
            attachment_for(recipe, conn, password=decrypt(conn.encrypted_credentials))
        )
    return attachments


# The old name, kept for the orchestrator and any caller that spelled it out.
attachable_postgres_attachments = attachable_attachments
