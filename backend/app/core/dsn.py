"""Safe construction of libpq DSNs and DuckDB SQL literals.

Postgres was attached to DuckDB by interpolating credentials straight into an
f-string, so a password containing a quote, a backslash or whitespace either
broke the connection or injected SQL into a DuckDB session that has unrestricted
filesystem access (audit H4).

Each value is now quoted once for libpq, and the resulting DSN is quoted once as
a SQL literal. Nothing is interpolated raw.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# SQLAlchemy dialects a BI tool (Superset) uses to read each source type. Declared
# once here so the syncer and its tests cannot disagree about what "postgres" means
# as a URI scheme. Source types missing from this map have no BI connector — that is
# a skip, not an error, and callers decide how to report it.
BI_URI_DRIVERS: Dict[str, str] = {
    "postgres": "postgresql+psycopg2",
    "postgresql": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "mariadb": "mysql+pymysql",
    "clickhouse": "clickhousedb+connect",
}

# The pip package that provides the driver half of each scheme above. A URI is only
# usable once its driver is importable, and the Superset image ships none of them
# (apache/superset:4.1.1 has no psycopg2, so every Postgres source failed the connection
# test Superset runs on create with "Connection failed, please check your connection
# settings"). The Superset deploy installs exactly these packages into its container.
BI_DRIVER_PACKAGES: Dict[str, str] = {
    "psycopg2": "psycopg2-binary",
    "pymysql": "PyMySQL",
    "connect": "clickhouse-connect",
}

# The name each package above is *imported* as. The deploy uses these to ask the
# container whether it already has the drivers ("connect" -> "clickhouse_connect" is the
# one that does not match its URI token), instead of reinstalling on every start.
BI_DRIVER_MODULES: Dict[str, str] = {
    "psycopg2": "psycopg2",
    "pymysql": "pymysql",
    "connect": "clickhouse_connect",
}


def bi_container_packages() -> List[str]:
    """The pip packages a BI container needs to serve every :data:`BI_URI_DRIVERS` scheme.

    ``lz4`` is named explicitly: ``clickhouse-connect`` imports it, the Superset image does
    not ship it, and its other dependencies (``urllib3``, ``zstandard``, ``certifi``) *are*
    present — installing them again would shadow the versions Superset itself runs on.
    """
    return sorted(set(BI_DRIVER_PACKAGES.values())) + ["lz4"]


def bi_container_modules() -> List[str]:
    """The import names :func:`bi_container_packages` provides, ``lz4`` included."""
    return [BI_DRIVER_MODULES[name] for name in sorted(BI_DRIVER_MODULES)] + ["lz4"]


def validate_alias(alias: str) -> str:
    """Rejects anything that is not a plain SQL identifier."""
    if not alias or not _ALIAS_RE.fullmatch(alias):
        raise ValueError(
            f"Invalid DuckDB attachment alias {alias!r}: expected [A-Za-z_][A-Za-z0-9_]*"
        )
    return alias


def _libpq_value(value: object) -> str:
    """Quotes one libpq keyword/value pair, escaping backslashes and quotes."""
    text = str(value)
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def build_pg_dsn(**fields: object) -> str:
    """Builds a libpq DSN with every value quoted. ``None`` values are skipped."""
    return " ".join(
        f"{key}={_libpq_value(value)}"
        for key, value in fields.items()
        if value is not None
    )


def build_kv_dsn(**fields: object) -> str:
    """DuckDB's ``key=value`` attach string (the mysql scanner). Same quoting as libpq."""
    return build_pg_dsn(**fields)


def build_mongo_uri(
    host: str,
    port: int,
    *,
    username: object = None,
    password: object = None,
    auth_source: str = "admin",
) -> str:
    """Builds a ``mongodb://`` URI with the credentials percent-encoded.

    A password may contain ``@``, ``:`` or ``/`` — all of which change the meaning of a
    URI — so it is quoted rather than interpolated. ``auth_source`` is where the user was
    created, which is ``admin`` for every deployment this platform creates.
    """
    if username:
        userinfo = quote(str(username), safe="")
        if password is not None:
            userinfo += ":" + quote(str(password), safe="")
        authority = f"{userinfo}@"
    else:
        authority = ""
    host = (host or "127.0.0.1").strip()
    if ":" in host and not host.startswith("["):  # IPv6 literal
        host = f"[{host}]"
    query = f"?authSource={quote(str(auth_source or 'admin'), safe='')}"
    return f"mongodb://{authority}{host}:{int(port)}{query}"


def bi_uri_driver(source_type: str) -> str:
    """The SQLAlchemy dialect a BI tool reads ``source_type`` with.

    Raises :class:`ValueError` rather than defaulting to Postgres: guessing would build a
    URI that connects to the wrong thing, or to nothing, and the failure would only appear
    when somebody ran a query.
    """
    key = (source_type or "").strip().lower()
    try:
        return BI_URI_DRIVERS[key]
    except KeyError:
        raise ValueError(
            f"No BI connector for source type {source_type!r}; "
            f"supported: {', '.join(sorted(BI_URI_DRIVERS))}"
        ) from None


def build_sqlalchemy_uri(
    source_type: str,
    host: str,
    port: int,
    *,
    username: object = None,
    password: object = None,
    database: object = None,
    extras: Optional[Dict[str, Any]] = None,
) -> str:
    """Builds a ``postgresql+psycopg2://``-style URI for a BI consumer (Superset).

    Same rule as :func:`build_mongo_uri`: every component that could change the meaning of
    the URI is percent-encoded instead of interpolated, so a password containing ``@``,
    ``:``, ``/`` or whitespace survives the round-trip. SQLAlchemy decodes userinfo with
    ``unquote`` (not ``unquote_plus``), which is why a space becomes ``%20``.
    """
    scheme = bi_uri_driver(source_type)
    userinfo = ""
    if username:
        userinfo = quote(str(username), safe="")
        if password is not None:
            userinfo += ":" + quote(str(password), safe="")
        userinfo += "@"
    host = (host or "127.0.0.1").strip()
    if ":" in host and not host.startswith("["):  # IPv6 literal
        host = f"[{host}]"
    path = "/" + quote(str(database), safe="") if database else ""
    query = f"?{urlencode(extras)}" if extras else ""
    return f"{scheme}://{userinfo}{host}:{int(port)}{path}{query}"


def sql_string(value: object) -> str:
    """Returns a single-quoted SQL string literal with quotes doubled."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(value: object) -> str:
    """Returns a double-quoted SQL identifier with quotes doubled."""
    return '"' + str(value).replace('"', '""') + '"'
