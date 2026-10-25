"""Phase A spike: does DuckLake work on this machine?

Throwaway by design. It imports nothing from ``app`` so a failure here means the
format, not our code. Every check prints PASS/FAIL and why; gating failures exit 1.

Run:  backend\\.venv\\Scripts\\python.exe backend\\scripts\\spike_ducklake.py
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import traceback
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORK = PROJECT_ROOT / "data" / "spike_ducklake"

RESULTS: list[dict] = []


def check(name: str, gating: bool = True):
    """Register one check. A spike reports; it never retries."""

    def wrapper(fn):
        def run(ctx):
            try:
                detail = fn(ctx) or ""
                RESULTS.append({"check": name, "ok": True, "gating": gating, "detail": str(detail)})
                print(f"PASS  {name}: {detail}")
            except Exception as exc:
                RESULTS.append(
                    {
                        "check": name,
                        "ok": False,
                        "gating": gating,
                        "detail": f"{type(exc).__name__}: {exc}",
                        "trace": traceback.format_exc(limit=2),
                    }
                )
                print(f"FAIL  {name}: {type(exc).__name__}: {exc}")

        run.gating = gating
        return run

    return wrapper


@check("extension_loads")
def extension_loads(ctx) -> str:
    ctx["conn"].execute("INSTALL ducklake;")
    ctx["conn"].execute("LOAD ducklake;")
    return f"duckdb {duckdb.__version__}, python {sys.version.split()[0]}"


@check("attach_sqlite_catalog")
def attach_sqlite(ctx) -> str:
    conn = ctx["conn"]
    data_path = (WORK / "files").as_posix()
    try:
        conn.execute(
            f"ATTACH 'ducklake:sqlite:{(WORK / 'catalog.sqlite').as_posix()}' "
            f"AS lake (DATA_PATH '{data_path}');"
        )
        ctx["catalog"] = "sqlite"
        return "sqlite catalog"
    except Exception as exc:
        conn.execute(
            f"ATTACH 'ducklake:{(WORK / 'catalog.ducklake').as_posix()}' "
            f"AS lake (DATA_PATH '{data_path}');"
        )
        ctx["catalog"] = "duckdb-file"
        return f"FALLBACK to duckdb-file catalog ({exc})"


@check("ddl_and_insert")
def ddl_and_insert(ctx) -> str:
    conn = ctx["conn"]
    conn.execute("CREATE SCHEMA lake.raw;")
    conn.execute(
        """
        CREATE TABLE lake.raw.orders (
            order_id   BIGINT,
            dt         DATE,
            amount     DOUBLE,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        INSERT INTO lake.raw.orders VALUES
            (1, DATE '2026-10-01', 10.0, TIMESTAMP '2026-10-01 00:00:00'),
            (2, DATE '2026-10-01', 20.0, TIMESTAMP '2026-10-01 00:00:00'),
            (3, DATE '2026-10-02', 30.0, TIMESTAMP '2026-10-02 00:00:00');
        """
    )
    n = conn.execute("SELECT count(*) FROM lake.raw.orders;").fetchone()[0]
    assert n == 3, f"expected 3 rows, got {n}"
    return f"{n} rows"


@check("merge_upsert")
def merge_upsert(ctx) -> str:
    ctx["conn"].execute(
        """
        MERGE INTO lake.raw.orders AS t
        USING (VALUES (1, 99.0), (4, 40.0)) AS s(order_id, amount)
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET amount = s.amount
        WHEN NOT MATCHED THEN INSERT (order_id, dt, amount, updated_at)
             VALUES (s.order_id, DATE '2026-10-03', s.amount, TIMESTAMP '2026-10-03 00:00:00');
        """
    )
    rows = dict(ctx["conn"].execute("SELECT order_id, amount FROM lake.raw.orders;").fetchall())
    assert rows[1] == 99.0, f"merge did not update row 1: {rows}"
    assert rows[4] == 40.0, f"merge did not insert row 4: {rows}"
    return "update + insert"


@check("update_delete")
def update_delete(ctx) -> str:
    conn = ctx["conn"]
    conn.execute("UPDATE lake.raw.orders SET amount = 55.0 WHERE order_id = 2;")
    conn.execute("DELETE FROM lake.raw.orders WHERE order_id = 3;")
    got = conn.execute("SELECT amount FROM lake.raw.orders WHERE order_id = 2;").fetchone()[0]
    n = conn.execute("SELECT count(*) FROM lake.raw.orders;").fetchone()[0]
    assert got == 55.0 and n == 3, f"update/delete wrong: amount={got} rows={n}"
    return "update + delete"


@check("time_travel")
def time_travel(ctx) -> str:
    conn = ctx["conn"]
    snapshots = conn.execute("FROM ducklake_snapshots('lake');").fetchall()
    if len(snapshots) < 2:
        raise AssertionError(f"expected several snapshots, got {len(snapshots)}")
    # Snapshot 0 is catalog initialisation, where even the schema does not exist yet,
    # so "time travel" has to start at the first snapshot that knows the table.
    usable = []
    for snapshot_id in sorted(row[0] for row in snapshots):
        try:
            rows = conn.execute(
                f"SELECT count(*) FROM lake.raw.orders AT (VERSION => {snapshot_id});"
            ).fetchone()[0]
            usable.append((snapshot_id, rows))
        except Exception:
            continue
    if not usable:
        raise AssertionError("no snapshot could read the table")
    first_id, first_rows = usable[0]
    now = conn.execute("SELECT count(*) FROM lake.raw.orders;").fetchone()[0]
    return f"{len(snapshots)} snapshots; v{first_id} had {first_rows} rows, now {now}"


@check("inlining_small_writes")
def inlining(ctx) -> str:
    conn = ctx["conn"]
    data_dir = WORK / "files"

    def parquet_files() -> int:
        return len(list(data_dir.rglob("*.parquet"))) if data_dir.exists() else 0

    before = parquet_files()
    conn.execute("CALL lake.set_option('data_inlining_row_limit', 100);")
    conn.execute(
        "INSERT INTO lake.raw.orders VALUES "
        "(10, DATE '2026-10-04', 1.0, TIMESTAMP '2026-10-04 00:00:00');"
    )
    inline_delta = parquet_files() - before

    conn.execute("CALL lake.set_option('data_inlining_row_limit', 0);")
    before2 = parquet_files()
    conn.execute(
        "INSERT INTO lake.raw.orders VALUES "
        "(11, DATE '2026-10-04', 2.0, TIMESTAMP '2026-10-04 00:00:00');"
    )
    file_delta = parquet_files() - before2
    assert file_delta > 0, "with inlining off, the insert still wrote no Parquet file"
    return f"inlined insert wrote {inline_delta} file(s); direct insert wrote {file_delta}"


@check("catalog_introspection")
def catalog_introspection(ctx) -> str:
    info = ctx["conn"].execute("FROM ducklake_table_info('lake');").fetchall()
    assert info, "ducklake_table_info returned nothing"
    return f"{len(info)} table row(s) from ducklake_table_info"


@check("add_existing_parquet")
def add_existing_parquet(ctx) -> str:
    conn = ctx["conn"]
    exported = WORK / "files" / "legacy_export.parquet"
    pq.write_table(pa.table({"order_id": [100, 101], "amount": [5.0, 6.0]}), exported.as_posix())
    conn.execute("CREATE TABLE lake.raw.imported (order_id BIGINT, amount DOUBLE);")
    # The table argument is the bare name; the schema is a named parameter.
    conn.execute(
        "CALL ducklake_add_data_files("
        f"'lake', 'imported', '{exported.as_posix()}', schema => 'raw');"
    )
    n = conn.execute("SELECT count(*) FROM lake.raw.imported;").fetchone()[0]
    assert n == 2, f"expected 2 imported rows, got {n}"
    return f"{n} rows registered without copying"


@check("parquet_escape_hatch")
def escape_hatch(ctx) -> str:
    pattern = (WORK / "files" / "**" / "*.parquet").as_posix()
    n = ctx["conn"].execute(f"SELECT count(*) FROM read_parquet('{pattern}');").fetchone()[0]
    assert n > 0, "read_parquet over the data path returned nothing"
    return f"{n} rows readable directly from Parquet"


@check("maintenance_functions", gating=False)
def maintenance(ctx) -> str:
    conn = ctx["conn"]
    merged = conn.execute("FROM ducklake_merge_adjacent_files('lake');").fetchall()
    expired = conn.execute(
        "FROM ducklake_expire_snapshots('lake', older_than => now()::TIMESTAMP);"
    ).fetchall()
    return f"merge_adjacent_files -> {len(merged)} row(s), expire_snapshots -> {len(expired)}"


@check("dbt_duckdb_present", gating=False)
def dbt_duckdb_present(ctx) -> str:
    from importlib.metadata import version

    return f"dbt-duckdb {version('dbt-duckdb')} installed (real dbt run proves attach support)"


@check("single_writer_enforced", gating=False)
def single_writer(ctx) -> str:
    """Informational: two connections on one catalog must not silently both write."""
    second = duckdb.connect()
    try:
        second.execute(
            f"ATTACH 'ducklake:sqlite:{(WORK / 'catalog.sqlite').as_posix()}' AS lake;"
        )
        second.execute(
            "INSERT INTO lake.raw.orders VALUES (99, DATE '2026-10-05', 0.0, now());"
        )
        return "second connection ALSO wrote (format does not enforce a single writer)"
    except Exception as exc:
        return f"second writer refused: {type(exc).__name__}"
    finally:
        second.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="keep the spike directory")
    args = parser.parse_args()

    if WORK.exists():
        shutil.rmtree(WORK)
    (WORK / "files").mkdir(parents=True)

    ctx: dict = {"conn": duckdb.connect()}
    checks = [
        extension_loads,
        attach_sqlite,
        ddl_and_insert,
        merge_upsert,
        update_delete,
        time_travel,
        inlining,
        catalog_introspection,
        add_existing_parquet,
        escape_hatch,
        maintenance,
        dbt_duckdb_present,
        single_writer,
    ]
    print(f"spike dir: {WORK}\n")
    for fn in checks:
        fn(ctx)

    try:
        ctx["conn"].execute("USE memory;")
        ctx["conn"].execute("DETACH lake;")
    except Exception:
        pass
    ctx["conn"].close()

    gating_failures = [r for r in RESULTS if r["gating"] and not r["ok"]]
    report = {
        "duckdb": duckdb.__version__,
        "python": sys.version.split()[0],
        "catalog": ctx.get("catalog"),
        "results": RESULTS,
        "gating_failures": len(gating_failures),
    }
    (WORK / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    passed = len(RESULTS) - len([r for r in RESULTS if not r["ok"]])
    print(f"\n{passed}/{len(RESULTS)} checks passed ({len(gating_failures)} gating failure(s))")
    print(f"report: {WORK / 'report.json'}")
    return 1 if gating_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())


