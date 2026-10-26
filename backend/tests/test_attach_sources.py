"""Attaching a live source: the URI is safe, the shape is right, writes are refused.

Only PostgreSQL used to reach DuckDB without a copy. MySQL has a core scanner and MongoDB
one in DuckDB's community repository, so a connection of either type is now attached for a
query instead of only being pullable — which makes three things worth pinning down: the
credentials are encoded rather than interpolated, the engine receives the shape its scanner
accepts, and the attach is read-only.

The last test talks to a real MongoDB and is skipped unless ``OBELISK_TEST_MONGO_URI`` is
set (``mongodb://user:pass@host:port/?authSource=admin``).
"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from app.core.dsn import build_mongo_uri
from app.core.sources import attachment_for, resolve_source
from app.engines.duckdb_engine import duckdb_engine


def _connection(source_type: str, **overrides) -> SimpleNamespace:
    values = {
        "name": "Telemetry Source",
        "source_type": source_type,
        "driver": None,
        "read_only": None,
        "host": "127.0.0.1",
        "port": 27018,
        "database": "sourcedb",
        "username": "mongoadmin",
        "options": {},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_mongo_uri_encodes_credentials_instead_of_interpolating_them() -> None:
    uri = build_mongo_uri(
        "127.0.0.1", 27018, username="admin@corp", password="p@ss:w/rd#1"
    )
    assert uri == (
        "mongodb://admin%40corp:p%40ss%3Aw%2Frd%231@127.0.0.1:27018?authSource=admin"
    )
    # A URI without credentials still names the deployment.
    assert build_mongo_uri("db.internal", 27017) == "mongodb://db.internal:27017?authSource=admin"


def test_attachment_shape_matches_the_scanner_that_will_read_it() -> None:
    mongo = attachment_for(
        resolve_source(_connection("mongodb")), _connection("mongodb"), password="secret"
    )
    assert mongo["kind"] == "mongo"
    assert mongo["alias"] == "telemetry_source"
    assert mongo["uri"].startswith("mongodb://mongoadmin:secret@127.0.0.1:27018")
    # A mongo attach must not leak the postgres-only fields.
    assert "password" not in mongo and "sslmode" not in mongo

    mysql = attachment_for(
        resolve_source(_connection("mysql", port=3306)), _connection("mysql", port=3306),
        password="secret",
    )
    assert mysql["kind"] == "mysql"
    assert mysql["port"] == 3306 and mysql["database"] == "sourcedb"
    assert "sslmode" not in mysql

    postgres = attachment_for(
        resolve_source(_connection("postgres", port=5432)),
        _connection("postgres", port=5432),
        password="secret",
    )
    assert postgres["kind"] == "postgres"
    assert postgres["sslmode"] == "disable"


def test_the_dispatcher_refuses_an_unknown_kind() -> None:
    with pytest.raises(ValueError):
        duckdb_engine.attach(object(), {"kind": "cassandra", "alias": "x"})


class _Recorder:
    """A DuckDB connection stand-in that records the statements it is given."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, *args, **kwargs):  # noqa: D401 - duck-typed stand-in
        self.statements.append(sql)
        return None


def test_both_scanners_attach_read_only() -> None:
    """READ_ONLY is what stops a query from writing into a source we do not own."""
    recorder = _Recorder()
    duckdb_engine.attach_mysql(
        recorder,
        alias="shop_src",
        host="127.0.0.1",
        port=3306,
        database="shop",
        username="reader",
        password="secret",
    )
    duckdb_engine.attach_mongo(
        recorder, alias="mongo_src", uri=build_mongo_uri("127.0.0.1", 27018, password="x")
    )

    mysql_attach = next(s for s in recorder.statements if "TYPE MYSQL" in s)
    mongo_attach = next(s for s in recorder.statements if "TYPE MONGO" in s)
    assert "READ_ONLY" in mysql_attach
    assert "READ_ONLY" in mongo_attach
    assert mysql_attach.startswith("ATTACH '") and "shop_src" in mysql_attach
    assert mongo_attach.startswith("ATTACH '") and "mongo_src" in mongo_attach


@pytest.mark.skipif(
    not os.getenv("OBELISK_TEST_MONGO_URI"),
    reason="set OBELISK_TEST_MONGO_URI to exercise a real MongoDB scanner",
)
def test_a_mongo_collection_is_queryable_and_not_writable() -> None:
    uri = os.environ["OBELISK_TEST_MONGO_URI"]
    conn = duckdb_engine.get_connection(with_lakehouse=False)
    try:
        duckdb_engine.attach_mongo(conn, alias="live", uri=uri)
        catalogue = conn.execute(
            "select table_schema, table_name from information_schema.tables "
            "where table_catalog = 'live'"
        ).fetchall()
        assert catalogue, "the attached deployment reported no collections"
        schema, table = catalogue[0]
        count = conn.execute(
            f'select count(*) from live."{schema}"."{table}"'
        ).fetchone()[0]
        assert count >= 0
        with pytest.raises(Exception) as exc:
            conn.execute(f'create table live."{schema}"."zz_probe" as select 1 as x')
        assert "read-only" in str(exc.value).lower()
    finally:
        conn.close()
