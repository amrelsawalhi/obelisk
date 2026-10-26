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
from urllib.parse import quote

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def sql_string(value: object) -> str:
    """Returns a single-quoted SQL string literal with quotes doubled."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(value: object) -> str:
    """Returns a double-quoted SQL identifier with quotes doubled."""
    return '"' + str(value).replace('"', '""') + '"'
