"""Phase 1a — Alembic baseline, layer resolution and the layers API.

The properties worth locking down:

* one logical layer path maps to exactly one physical destination, so a path can
  never escape the lakehouse or silently merge with another layer;
* the baseline migration is idempotent, because it has to stamp installations
  that already have data as well as build fresh ones;
* the backfill from ``assets`` preserves identity (fqn) and location;
* the tree never loses its default target, and never lets a rename orphan data.
"""
import sqlite3
import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.core.layers import (
    InvalidLayerPath,
    flatten_layer_path,
    join_layer_path,
    layer_fqn,
    parent_layer_path,
    resolve_layer,
    safe_layer_path,
    split_layer_path,
    validate_layer_path,
)
from app.database import BACKEND_DIR, init_db
from app.main import app

# --------------------------------------------------------------------------- #
# Pure resolution
# --------------------------------------------------------------------------- #


def test_validate_accepts_and_strips() -> None:
    assert validate_layer_path(" raw ") == "raw"
    assert validate_layer_path("marts.sales") == "marts.sales"
    assert validate_layer_path("raw_zone-2") == "raw_zone-2"
    assert split_layer_path("a.b.c") == ("a", "b", "c")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "   ",
        None,
        ".raw",
        "raw.",
        "a..b",
        "a/b",
        "a\\b",
        "../etc",
        "a__b",  # would flatten onto a.b
        "a.b__c",
        "has space",
        "semi;colon",
        "a." * 9,  # deeper than MAX_DEPTH
        "x" * 256,  # longer than MAX_PATH_LENGTH
    ],
)
def test_validate_rejects_ambiguous_or_unsafe_paths(path) -> None:
    with pytest.raises(InvalidLayerPath):
        validate_layer_path(path)


def test_flattening_is_injective_for_valid_paths() -> None:
    """`. -> __` is only reversible because segments cannot contain `__`."""
    paths = ["raw", "marts.sales", "marts.sales.daily", "raw_zone.inbound"]
    flattened = [flatten_layer_path(p) for p in paths]
    assert flattened == ["raw", "marts__sales", "marts__sales__daily", "raw_zone__inbound"]
    assert len(set(flattened)) == len(flattened)


def test_resolve_layer_derives_all_three_shapes(tmp_path: Path) -> None:
    loc = resolve_layer("marts.sales", root=tmp_path)

    assert loc.path == "marts.sales"
    assert loc.depth == 2
    assert loc.filesystem_dir == (tmp_path / "marts.sales").resolve()
    assert loc.warehouse_schema == "marts__sales"
    assert loc.dbt_folder == "models/marts__sales"
    assert loc.table_fqn("orders") == "lakehouse.marts.sales.orders"


def test_resolve_layer_uses_the_configured_lakehouse_root() -> None:
    loc = resolve_layer("raw")
    assert loc.filesystem_dir == (settings.lakehouse_root_path / "raw").resolve()
    assert loc.warehouse_schema == "raw"


def test_resolve_layer_never_escapes_the_root(tmp_path: Path) -> None:
    with pytest.raises(InvalidLayerPath):
        resolve_layer("../../secrets", root=tmp_path)


def test_parent_and_join() -> None:
    assert parent_layer_path("marts") is None
    assert parent_layer_path("marts.sales") == "marts"
    assert join_layer_path("marts", "sales") == "marts.sales"
    assert join_layer_path(None, "raw") == "raw"
    with pytest.raises(InvalidLayerPath):
        join_layer_path("marts", "bad.name")
    with pytest.raises(InvalidLayerPath):
        join_layer_path("marts", "bad__name")


def test_layer_fqn_shape() -> None:
    assert layer_fqn("marts.sales", "orders") == "lakehouse.marts.sales.orders"


# --------------------------------------------------------------------------- #
# Migrations (on a throwaway database, never the operator's)
# --------------------------------------------------------------------------- #


def _alembic_config(db_path: Path):
    from alembic.config import Config

    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path.as_posix()}")
    return cfg


def _head(db_path: Path) -> str:
    """Current head revision, so these tests survive every new migration."""
    from alembic.script import ScriptDirectory

    return ScriptDirectory.from_config(_alembic_config(db_path)).get_current_head()


def test_baseline_then_backfill_preserves_assets(tmp_path: Path) -> None:
    """0001 builds the legacy schema, 0002 moves its rows into the registry."""
    from alembic import command

    db = tmp_path / "brain.db"
    command.upgrade(_alembic_config(db), "0001_baseline")

    with sqlite3.connect(db) as con:
        for name, layer, uri in (
            ("orders", "bronze", "file:///lake/bronze/orders.parquet"),
            ("customers", "silver", "file:///lake/silver/customers.parquet"),
        ):
            con.execute(
                "INSERT INTO assets (id, name, layer, storage_uri, format, record_count,"
                " created_at) VALUES (?, ?, ?, ?, 'parquet', 7, CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), name, layer, uri),
            )
        con.commit()

    command.upgrade(_alembic_config(db), "head")

    with sqlite3.connect(db) as con:
        rows = con.execute(
            "SELECT fqn, name, layer_path, kind, backend, location, row_count"
            " FROM catalog_objects ORDER BY name"
        ).fetchall()
        layers = con.execute("SELECT path, is_default FROM layers").fetchall()
        revision = con.execute("SELECT version_num FROM alembic_version").fetchone()[0]

    assert revision == _head(db)
    # The seeded landing layer is the default …
    assert ("raw", 1) in layers
    # … and the layers the existing assets already live in are discovered too.
    assert ("bronze", 0) in layers
    assert ("silver", 0) in layers
    assert len(rows) == 2
    by_name = {row[1]: row for row in rows}
    assert by_name["orders"] == (
        "lakehouse.bronze.orders",
        "orders",
        "bronze",
        "parquet",
        "local",
        "file:///lake/bronze/orders.parquet",
        7,
    )
    assert by_name["customers"][0] == "lakehouse.silver.customers"


def test_migrations_are_idempotent_and_reversible(tmp_path: Path) -> None:
    from alembic import command

    db = tmp_path / "brain.db"
    cfg = _alembic_config(db)
    head = _head(db)
    command.upgrade(cfg, "head")
    command.upgrade(cfg, "head")  # a second start must be a no-op

    with sqlite3.connect(db) as con:
        assert con.execute("SELECT COUNT(*) FROM layers").fetchone()[0] == 1
        assert con.execute("SELECT version_num FROM alembic_version").fetchone()[0] == head
        assert "driver" in {
            r[1] for r in con.execute("PRAGMA table_info(connections)")
        }

    command.downgrade(cfg, "0001_baseline")
    with sqlite3.connect(db) as con:
        tables = {
            r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        connection_columns = {r[1] for r in con.execute("PRAGMA table_info(connections)")}
    assert "catalog_objects" not in tables and "layers" not in tables
    assert "assets" in tables
    # 0003 rolled back cleanly as well (SQLite needs batch mode for DROP COLUMN).
    assert "driver" not in connection_columns


# --------------------------------------------------------------------------- #
# Live brain: init_db must converge, not duplicate
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_init_db_converges_on_the_live_brain() -> None:
    await init_db()
    await init_db()

    with sqlite3.connect(settings.sqlite_db_path) as con:
        defaults = con.execute("SELECT COUNT(*) FROM layers WHERE is_default = 1").fetchone()[0]
        revisions = con.execute("SELECT COUNT(*) FROM alembic_version").fetchone()[0]

    assert defaults == 1, "exactly one layer must be the default ingest target"
    assert revisions == 1


# --------------------------------------------------------------------------- #
# Layers API
# --------------------------------------------------------------------------- #


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _probe_name() -> str:
    return f"zz{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_layers_roundtrip_and_subtree_rename() -> None:
    root = _probe_name()
    child = f"{root}.staging"
    renamed_child = f"{root}.refined"
    new_root = f"{root}b"

    async with _client() as client:
        created = await client.post("/api/layers", json={"path": root, "description": "probe"})
        assert created.status_code == 201, created.text
        assert created.json()["depth"] == 1
        assert created.json()["parent_path"] is None

        child_created = await client.post("/api/layers", json={"path": child})
        assert child_created.status_code == 201, child_created.text
        assert child_created.json()["parent_path"] == root
        assert child_created.json()["depth"] == 2

        listing = (await client.get("/api/layers")).json()
        assert {root, child} <= {layer["path"] for layer in listing}
        assert len([layer for layer in listing if layer["is_default"]]) == 1

        resolved = (
            await client.get("/api/layers/resolve", params={"path": child})
        ).json()
        assert resolved["warehouse_schema"] == child.replace(".", "__")
        assert resolved["dbt_folder"] == "models/" + child.replace(".", "__")

        # Renaming the leaf is allowed: nothing is stored under it.
        leaf = await client.patch(f"/api/layers/{child}", json={"name": "refined"})
        assert leaf.status_code == 200, leaf.text
        assert leaf.json()["path"] == renamed_child

        # Renaming the parent must carry the whole subtree with it.
        parent = await client.patch(f"/api/layers/{root}", json={"name": new_root})
        assert parent.status_code == 200, parent.text
        listing = (await client.get("/api/layers")).json()
        by_path = {layer["path"]: layer for layer in listing}
        assert f"{new_root}.refined" in by_path
        assert by_path[f"{new_root}.refined"]["parent_path"] == new_root
        assert child not in by_path

        assert (await client.delete(f"/api/layers/{new_root}.refined")).status_code == 200
        assert (await client.delete(f"/api/layers/{new_root}")).status_code == 200
        remaining = {layer["path"] for layer in (await client.get("/api/layers")).json()}
        assert new_root not in remaining


@pytest.mark.asyncio
async def test_create_layer_validates_parent_and_path() -> None:
    root = _probe_name()
    async with _client() as client:
        missing_parent = await client.post("/api/layers", json={"path": f"{root}.orphan"})
        assert missing_parent.status_code == 400
        assert root in missing_parent.json()["detail"]

        for bad in ("../escape", "a__b", ".hidden", f"{root}."):
            response = await client.post("/api/layers", json={"path": bad})
            assert response.status_code == 400, f"{bad!r} should be rejected"

        created = await client.post("/api/layers", json={"path": root})
        assert created.status_code == 201

        duplicate = await client.post("/api/layers", json={"path": root})
        assert duplicate.status_code == 409

        assert (await client.delete(f"/api/layers/{root}")).status_code == 200


@pytest.mark.asyncio
async def test_layer_with_objects_cannot_be_renamed_or_deleted() -> None:
    root = _probe_name()
    fqn = f"lakehouse.{root}.orders"
    async with _client() as client:
        assert (await client.post("/api/layers", json={"path": root})).status_code == 201

        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute(
                "INSERT INTO catalog_objects (id, fqn, name, layer_path, kind, backend,"
                " version, created_at) VALUES (?, ?, 'orders', ?, 'parquet', 'local', 1,"
                " CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), fqn, root),
            )
            con.commit()

        try:
            rename = await client.patch(f"/api/layers/{root}", json={"name": f"{root}x"})
            assert rename.status_code == 409
            assert "catalogued object" in rename.json()["detail"]

            delete = await client.delete(f"/api/layers/{root}")
            assert delete.status_code == 409
        finally:
            with sqlite3.connect(settings.sqlite_db_path) as con:
                con.execute("DELETE FROM catalog_objects WHERE fqn = ?", (fqn,))
                con.commit()

        assert (await client.delete(f"/api/layers/{root}")).status_code == 200


@pytest.mark.asyncio
async def test_default_layer_moves_and_is_never_lost() -> None:
    probe = _probe_name()
    async with _client() as client:
        assert (await client.post("/api/layers", json={"path": probe})).status_code == 201

        promoted = await client.patch(f"/api/layers/{probe}", json={"is_default": True})
        assert promoted.status_code == 200
        assert promoted.json()["is_default"] is True

        listing = (await client.get("/api/layers")).json()
        assert [layer["path"] for layer in listing if layer["is_default"]] == [probe]

        removed = await client.delete(f"/api/layers/{probe}")
        assert removed.status_code == 200
        assert removed.json()["default_layer"] is not None

        listing = (await client.get("/api/layers")).json()
        assert len([layer for layer in listing if layer["is_default"]]) == 1


@pytest.mark.asyncio
async def test_cannot_delete_the_only_layer() -> None:
    async with _client() as client:
        listing = (await client.get("/api/layers")).json()
        if len(listing) != 1:
            pytest.skip("the brain already has user-defined layers")
        response = await client.delete(f"/api/layers/{listing[0]['path']}")
        assert response.status_code == 409


# --------------------------------------------------------------------------- #
# The registry must not drift from the legacy write path
# --------------------------------------------------------------------------- #


def test_safe_layer_path_falls_back_for_legacy_folders() -> None:
    assert safe_layer_path("marts.sales") == "marts.sales"
    assert safe_layer_path("my folder!") == "my_folder"
    assert safe_layer_path("") == "raw"
    assert safe_layer_path("../..") == "raw"


def test_ingest_writes_one_catalog_object_per_fqn() -> None:
    from app.engines.ingest_engine import IngestEngine

    name = f"zz{uuid.uuid4().hex[:8]}"
    layer = f"zzlayer{uuid.uuid4().hex[:6]}"
    fqn = f"lakehouse.{layer}.{name}"
    engine = IngestEngine()

    try:
        engine._record_asset_in_db(
            name=name,
            layer=layer,
            storage_uri="file:///lake/zz.parquet",
            record_count=42,
            schema_metadata={"columns": []},
        )
        with sqlite3.connect(settings.sqlite_db_path) as con:
            first = con.execute(
                "SELECT fqn, layer_path, kind, backend, location, row_count, version"
                " FROM catalog_objects WHERE fqn = ?",
                (fqn,),
            ).fetchall()

        assert first == [
            (fqn, layer, "parquet", "local", "file:///lake/zz.parquet", 42, 1)
        ]

        # A second materialisation updates in place instead of duplicating.
        engine._record_asset_in_db(
            name=name,
            layer=layer,
            storage_uri="file:///lake/zz.parquet",
            record_count=43,
            schema_metadata={"columns": []},
        )
        with sqlite3.connect(settings.sqlite_db_path) as con:
            second = con.execute(
                "SELECT row_count, version FROM catalog_objects WHERE fqn = ?", (fqn,)
            ).fetchall()

        assert second == [(43, 2)]
    finally:
        with sqlite3.connect(settings.sqlite_db_path) as con:
            con.execute("DELETE FROM catalog_objects WHERE fqn = ?", (fqn,))
            con.execute("DELETE FROM assets WHERE name = ? AND layer = ?", (name, layer))
            # Ingest creates the layer it lands in; drop the probe one again.
            con.execute("DELETE FROM layers WHERE path = ?", (layer,))
            con.commit()




