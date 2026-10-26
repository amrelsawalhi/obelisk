# Obelisk Platform — Autonomous Agent Implementation Plan

## Objective & Target Hardware Profile

Refactor the Obelisk codebase so that a single node (**32 GB RAM, 12 CPU cores, 2–4 TB NVMe storage**) can reliably ingest, transform, query, and export datasets in the **100–500 GB range** while supporting **concurrent small-team usage (2–8 users)** without Out-Of-Memory (OOM) crashes, catalog corruption, or workspace state collisions.

## Agent Execution Rules

1. **Zero Mocks & No Silent Fallbacks**: Do not introduce mock data, silent exception swallowing, or fake statuses. Every failure must surface a clear, actionable error message.
2. **DuckLake is the Single Source of Truth**: All lakehouse tables and schemas are owned by DuckLake (`<lakehouse_root>/.obelisk/catalog.sqlite` attached as `lakehouse`). Never manipulate lakehouse tables or schema folders via raw `os.rename`, `shutil.rmtree`, or `<name>.parquet` file globs, and never auto-mirror SDK saves into PostgreSQL.
3. **Out-of-Core by Default**: Never materialize unbounded query results, exports, or full-table ingestions into Python lists (`to_pylist()`), in-memory `pa.Table` objects (`read_all()`), or `io.BytesIO` buffers. Let DuckDB stream directly to/from disk.
4. **Sequential Workstream Execution**: Execute Workstreams 1 through 5 in order. Run the verification checks at the end of each workstream before proceeding to the next.

## Workstream Overview

| Workstream | Focus Area | Primary Files Modified | Target Outcome |
| ----- | ----- | ----- | ----- |
| **WS-1** | Out-of-Core Query Execution, Interrupts, Streaming Exports & Lakehouse-Only SDK Saves | `config.py`, `resource_advisor.py`, `duckdb_engine.py`, `routes_query.py`, `pipeline_engine.py`, `sdk/obelisk.py`, `backend/obelisk.py`, `git_repo/notebooks/obelisk.py` | Un-limited `SELECT *`, multi-GB exports, and SQL materializations run in constant Python RAM, honour timeouts via `conn.interrupt()`, and write strictly to DuckLake without Postgres dual-writes. |
| **WS-2** | Streaming & Direct Lakehouse Ingestion | `ingest_engine.py` | 100+ GB tables ingest directly or in bounded chunks without staging 100% of rows in a temp table or transposing tuples in Python loops. |
| **WS-3** | DuckLake Catalog Consolidation & Metadata Performance | `core/catalog.py`, `api/routes_catalog.py` | Folder/table renames, deletes, and previews operate strictly through DuckLake DDL/metadata with zero N+1 `DESCRIBE` loops, plus live maintenance/diagnostics endpoints. |
| **WS-4** | Small-Team Concurrency, Process Group Hygiene & Container Networking | `dbt_runner.py`, `pipeline_engine.py`, `models/query.py`, `alembic/versions/0015_*.py`, `main.py`, `docker_manager.py`, `routes_services.py`, `Dockerfile`, `requirements.txt` | Isolated `dbt` target dirs per compile/preview, client-scoped query tabs, process-group kill on cancel, cached telemetry, and working container-to-container probes. |
| **WS-5** | Frontend UX, Settings Overhaul, Navigation Consolidation & Memory Hygiene | `App.tsx`, `Sidebar.tsx`, `HomeView.tsx`, `QueryStudio.tsx`, `SettingsView.tsx`, `SdkDocsView.tsx`, `CatalogView.tsx`, `CatalogTree.tsx`, `DbtStudio.tsx`, `NotebooksView.tsx`, `api.ts`, `types.ts` | Bounded DOM retention, functional Settings tab (live DuckLake compaction/diagnostics + inline secret editing + accurate SDK docs), grouped sidebar navigation, and `<ConfirmDialog />` everywhere. |

## Workstream 1: Out-of-Core Query Execution, Interrupts, Streaming Exports & Lakehouse-Only SDK Saves

### Task 1.1: Add Query Row Ceiling and Concurrency-Aware Memory Budgeting

**Files:**
* `backend/app/config.py`
* `backend/app/core/resource_advisor.py`

**Problem:**
1. `advisor.get_duckdb_memory_limit()` (`backend/app/core/resource_advisor.py:124-129`) assigns 25% of host RAM (8 GB on a 32 GB host) to every `duckdb.connect(":memory:")` call. Concurrent queries + ingestion jobs can allocate >32 GB combined and trigger the OS OOM killer.
2. There is no row cap on interactive JSON query responses (`routes_query.py`), causing `SELECT * FROM large_table` to serialize millions of Python dicts into RAM.

**Exact Changes:**
1. In `backend/app/config.py` (around line 147, after `DUCKDB_TEMP_SIZE_LIMIT`), add:
   ```python
   DUCKDB_MAX_QUERY_ROWS: int = Field(
       default_factory=lambda: int(os.getenv("DUCKDB_MAX_QUERY_ROWS", "10000"))
   )
   DUCKDB_MAX_CONCURRENT_HEAVY: int = Field(
       default_factory=lambda: int(os.getenv("DUCKDB_MAX_CONCURRENT_HEAVY", "4"))
   )
   ```
2. In `backend/app/core/resource_advisor.py:124-129`, update `get_duckdb_memory_limit` to account for active container allocations and concurrent sessions:
   ```python
   def get_duckdb_memory_limit(self, concurrent_slots: int = 4) -> str:
       """Returns safe per-connection memory limit for DuckDB initialization."""
       specs = self.get_host_specs()
       active = self.get_active_container_allocations()
       remaining_safe_gb = max(2.0, specs["max_safe_allocation_gb"] - active["total_allocated_gb"])
       slots = max(1, int(concurrent_slots))
       # Allocate at most 50% of remaining safe host RAM across concurrent DuckDB slots
       per_slot_gb = max(1, min(12, int((remaining_safe_gb * 0.50) / slots)))
       return f"{per_slot_gb}GB"
   ```

### Task 1.2: Cap Interactive Query Fetches, Interrupt Timed-Out Queries, and Stream Exports to Disk

**Files:**
* `backend/app/engines/duckdb_engine.py`
* `backend/app/api/routes_query.py`

**Problem:**
1. `DuckDBEngine.execute_query` (`backend/app/engines/duckdb_engine.py:434-440`) calls `arrow_reader.read_all()` and `arrow_table.to_pylist()`, loading 100% of rows into RAM.
2. When `asyncio.wait_for(..., timeout=...)` times out in `routes_query.py:333-340`, the background thread running DuckDB C++ is never interrupted (`conn.interrupt()`) and continues consuming CPU/RAM.
3. `DuckDBEngine.export_query` (`backend/app/engines/duckdb_engine.py:451-487`) loads the entire result into `arrow_table` and writes it into an in-memory `io.BytesIO()` buffer.

**Exact Changes:**
1. In `backend/app/engines/duckdb_engine.py:398-449`, update `execute_query` to accept `max_rows: Optional[int] = None` and `on_connection: Optional[Any] = None`:
   ```python
   def execute_query(
       self,
       sql: str,
       attachments: Optional[List[Dict[str, Any]]] = None,
       postgres_attachments: Optional[List[Dict[str, Any]]] = None,
       max_rows: Optional[int] = None,
       on_connection: Optional[Any] = None,
   ) -> Dict[str, Any]:
       start_time = time.time()
       storage_path = self._ensure_storage()
       conn = self.get_connection()
       if on_connection is not None:
           on_connection(conn)
       try:
           for attachment in attachments or postgres_attachments or []:
               self.attach(conn, attachment)
           exec_sql = self._prepare_sql_and_views(conn, sql, storage_path)
           rel = conn.sql(exec_sql)
           if rel is None:
               duration_ms = int((time.time() - start_time) * 1000)
               return {
                   "columns": [],
                   "rows": [],
                   "row_count": 0,
                   "truncated": False,
                   "duration_ms": duration_ms,
               }

           limit_cap = max_rows if max_rows is not None else settings.DUCKDB_MAX_QUERY_ROWS
           columns = list(rel.columns)
           # Fetch limit_cap + 1 rows to detect truncation without materialising the full relation
           fetched_tuples = rel.fetchmany(limit_cap + 1) if limit_cap > 0 else rel.fetchall()
           truncated = False
           if limit_cap > 0 and len(fetched_tuples) > limit_cap:
               fetched_tuples = fetched_tuples[:limit_cap]
               truncated = True

           rows = [dict(zip(columns, tup)) for tup in fetched_tuples]
           duration_ms = int((time.time() - start_time) * 1000)
           return {
               "columns": columns,
               "rows": rows,
               "row_count": len(rows),
               "truncated": truncated,
               "duration_ms": duration_ms,
           }
       finally:
           conn.close()
   ```
2. In `backend/app/engines/duckdb_engine.py:451-487`, add `export_query_to_file` that uses DuckDB's native `COPY` to stream results directly to disk with zero Python/Arrow full-table buffering:
   ```python
   def export_query_to_file(
       self,
       sql: str,
       dest_path: Path,
       output_format: str = "parquet",
       attachments: Optional[List[Dict[str, Any]]] = None,
   ) -> Path:
       """Streams query results directly to `dest_path` using DuckDB native COPY."""
       storage_path = self._ensure_storage()
       conn = self.get_connection()
       try:
           for attachment in attachments or []:
               self.attach(conn, attachment)
           exec_sql = self._prepare_sql_and_views(conn, sql, storage_path).strip().rstrip(";")
           dest_posix = dest_path.resolve().as_posix().replace("'", "''")
           fmt = output_format.lower()
           if fmt == "parquet":
               conn.execute(
                   f"COPY ({exec_sql}) TO '{dest_posix}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
               )
           elif fmt == "csv":
               conn.execute(
                   f"COPY ({exec_sql}) TO '{dest_posix}' (FORMAT CSV, HEADER TRUE);"
               )
           else:
               raise ValueError(f"Unsupported export format: {output_format}")
           return dest_path
       finally:
           conn.close()
   ```
   *(Keep `export_query` as a thin wrapper that writes to a temp file, reads bytes, and unlinks if any existing caller requires bytes, while `routes_query.py` uses `export_query_to_file` + `FileResponse`.)*
3. In `backend/app/api/routes_query.py`:
   * Add `truncated: bool = False` to `QueryExecuteResponse` (line 45).
   * In `execute_sql` (lines 332–364), pass `on_connection` to capture the live `duckdb.DuckDBPyConnection`, and call `live_conn.interrupt()` inside `except asyncio.TimeoutError:`:
     ```python
     live_conn_holder: list = []
     try:
         res = await asyncio.wait_for(
             asyncio.to_thread(
                 duckdb_engine.execute_query,
                 sql=req.sql,
                 attachments=attachments,
                 on_connection=live_conn_holder.append,
             ),
             timeout=float(req.timeout_seconds),
         )
     except asyncio.TimeoutError:
         if live_conn_holder:
             try:
                 live_conn_holder[0].interrupt()
             except Exception:
                 pass
         ...
     ```
   * In `export_sql_results` (`backend/app/api/routes_query.py:374-414`), write the export to `settings.duckdb_temp_path / f"export_{uuid.uuid4().hex}.{fmt}"` via `duckdb_engine.export_query_to_file`, and return a `starlette.responses.FileResponse` with a `BackgroundTask(lambda: temp_file.unlink(missing_ok=True))`.

### Task 1.3: Remove `sync_postgres` Entirely & Add Out-of-Core `materialize_sql` to DuckLake

**Files:**
* `backend/app/sdk/obelisk.py`
* `backend/app/engines/pipeline_engine.py`
* `backend/obelisk.py`
* `git_repo/notebooks/obelisk.py`

**Problem:**
1. `sdk.save()` (`backend/app/sdk/obelisk.py:408-485`) and `_sync_table_to_postgres` (`lines 488-525`) copy saved tables into PostgreSQL via `ATTACH ... (TYPE POSTGRES)`. The platform's storage engine is strictly the DuckLake lakehouse (`lakehouse.<schema>.<table>`), so `sync_postgres` and `_sync_table_to_postgres` must be removed altogether.
2. `PipelineEngine._materialize_sql` (`backend/app/engines/pipeline_engine.py:1472-1509`) calls `arrow = sdk.query_arrow(sql)` (materializing the entire table into Python RAM) and then `sdk.save(arrow, rel_path, sync_postgres=False)`.

**Exact Changes:**
1. In `backend/app/sdk/obelisk.py`:
   * Delete `_sync_table_to_postgres` (`lines 488-525`) completely.
   * Update `save` (`lines 408-485`) to remove the `sync_postgres` parameter and all `warehouse_sync` / `_sync_table_to_postgres` logic, saving strictly to DuckLake:
     ```python
     def save(
         df: Any,
         target: str,
     ) -> Dict[str, Any]:
         """Persists a DataFrame / Arrow table into the DuckLake catalog.

         ``target`` is ``"<folder>/<name>"`` (nested paths allowed) or bare
         ``"<name>"`` for a root-level table.
         """
         from app.core.catalog import quote_ident

         folder, stem = split_table_path(target)
         arrow_tbl = _coerce_to_arrow(df)

         schema_name = folder.replace("/", ".") if folder else "main"
         rel_path = f"{folder}/{stem}" if folder else stem
         address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"

         conn = duckdb_engine.get_connection()
         try:
             conn.execute(
                 f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};"
             )
             conn.register("_obelisk_save_src", arrow_tbl)
             try:
                 conn.execute(
                     f"CREATE OR REPLACE TABLE {address} AS SELECT * FROM _obelisk_save_src;"
                 )
             finally:
                 try:
                     conn.unregister("_obelisk_save_src")
                 except Exception:
                     pass
         finally:
             conn.close()

         fqn = fqn_for(f"{rel_path}.parquet")
         sql_name = sql_name_for(f"{rel_path}.parquet")
         num_rows = int(arrow_tbl.num_rows)

         logger.info(
             "obelisk.save: wrote %s rows -> %s (fqn=%s)",
             f"{num_rows:,}",
             address,
             fqn,
         )

         return {
             "status": "success",
             "target": target,
             "path": rel_path,
             "fqn": fqn,
             "sql_name": sql_name,
             "folder": folder,
             "name": stem,
             "num_rows": num_rows,
             "file_path": address,
         }
     ```
   * Add `materialize_sql(sql: str, target: str) -> Dict[str, Any]` right below `save` in `backend/app/sdk/obelisk.py`:
     ```python
     def materialize_sql(
         sql: str,
         target: str,
     ) -> Dict[str, Any]:
         """Materializes a SQL query directly into a DuckLake table without loading rows into Python."""
         from app.core.catalog import quote_ident

         folder, stem = split_table_path(target)
         schema_name = folder.replace("/", ".") if folder else "main"
         rel_path = f"{folder}/{stem}" if folder else stem
         address = f"{SOURCE}.{quote_ident(schema_name)}.{quote_ident(stem)}"

         conn = duckdb_engine.get_connection()
         try:
             storage_path = duckdb_engine._ensure_storage()
             for attachment in _attachments_for_sql(sql):
                 duckdb_engine.attach(conn, attachment)
             exec_sql = duckdb_engine._prepare_sql_and_views(conn, sql, storage_path).strip().rstrip(";")
             conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}.{quote_ident(schema_name)};")
             conn.execute(f"CREATE OR REPLACE TABLE {address} AS {exec_sql};")
             num_rows = int(conn.execute(f"SELECT count(*) FROM {address};").fetchone()[0])
         finally:
             conn.close()

         fqn = fqn_for(f"{rel_path}.parquet")
         sql_name = sql_name_for(f"{rel_path}.parquet")

         logger.info(
             "obelisk.materialize_sql: wrote %s rows -> %s (fqn=%s)",
             f"{num_rows:,}",
             address,
             fqn,
         )

         return {
             "status": "success",
             "target": target,
             "path": rel_path,
             "fqn": fqn,
             "sql_name": sql_name,
             "folder": folder,
             "name": stem,
             "num_rows": num_rows,
             "file_path": address,
         }
     ```
   * Add `"materialize_sql"` to `__all__` in `backend/app/sdk/obelisk.py`, `backend/obelisk.py`, and `git_repo/notebooks/obelisk.py`.
2. In `backend/app/engines/pipeline_engine.py:1493-1496`, replace `_write()` inside `_materialize_sql`:
   ```python
   def _write() -> Dict[str, Any]:
       saved = sdk.materialize_sql(sql, rel_path)
       return {"rows": int(saved["num_rows"]), "saved": saved}
   ```

## Workstream 2: Streaming & Chunked Ingestion Without Full-Table Staging

### Task 2.1: Direct DuckDB-to-DuckLake Transfer for Postgres Snapshot & Append

**File:** `backend/app/engines/ingest_engine.py`

**Problem:**
Currently, `IngestEngine.ingest_entity` (`ingest_engine.py:300-377`) calls `self._stage(...)`, which for PostgreSQL (`_stage_postgres`, lines 393–430) executes:
```sql
CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM pg_src_xxx.schema.table WHERE ...
```
and then `_write_staged` copies `staging` a second time into `lakehouse.<schema>.<table>`. For a 150 GB Postgres table, writing 150 GB into `TEMP TABLE staging` spills 150 GB to `duckdb_tmp` (exceeding `50GB` `DUCKDB_TEMP_SIZE_LIMIT`) before a single byte is written to the lakehouse.

**Exact Changes in `backend/app/engines/ingest_engine.py`:**
1. When `source_type.lower() == "postgres"` and `write_strategy != "upsert"`:
   * Attach the Postgres source (`ATTACH ... AS pg_src_xxx (TYPE POSTGRES)`).
   * Build the `SELECT * FROM {alias}.{sql_ident(schema_name)}.{sql_ident(table_name)} {where}{limit_clause}` query.
   * If `mode == "snapshot"` or `not exists`:
     * First check `SELECT 1 FROM {source_ref} {where} LIMIT 1`. If no row exists and `exists` is True, skip the replace and log the warning (preserving the empty-snapshot safety rule at `ingest_engine.py:354-363`).
     * Execute `CREATE OR REPLACE TABLE {target} AS SELECT * FROM ...` directly in one DuckDB statement (zero intermediate `staging` table).
   * If `mode == "incremental"` and `exists` and `write_strategy == "append"`:
     * Execute `INSERT INTO {target} SELECT * FROM {source_ref} {where}{limit_clause}` directly and compute `extracted = after_count - before_count`.
   * Only use `TEMP TABLE staging` for `postgres` when `write_strategy == "upsert"` (where deduplication and `MERGE INTO` are required).

### Task 2.2: Incremental Batch Commits & Columnar Arrow Construction for Cursor Drivers

**File:** `backend/app/engines/ingest_engine.py`

**Problem:**
1. `_arrow_batch(columns, rows)` (`ingest_engine.py:58-65`) loops over 50,000 rows in Python and appends each cell to a per-column Python list (`for row in rows: for name, value in zip(names, row): data[name].append(value)`).
2. `_stage_batches` (`ingest_engine.py:151-178`) accumulates all batches into `TEMP TABLE staging` before `_write_staged` runs. On a 100M-row MySQL or Oracle table, `staging` grows unboundedly in DuckDB memory/spill.

**Exact Changes in `backend/app/engines/ingest_engine.py`:**
1. Optimize `_arrow_batch` (`ingest_engine.py:58-65`) using `zip(*rows)` C-level transposition instead of nested Python loops:
   ```python
   def _arrow_batch(columns: Iterable[str], rows: Sequence[tuple]) -> pa.Table:
       names = list(columns)
       if not rows:
           return pa.Table.from_pydict({name: [] for name in names})
       transposed = list(zip(*rows))
       return pa.Table.from_pydict(dict(zip(names, transposed)))
   ```
2. In `_stage_mysql` (`ingest_engine.py:438-446`), use `pymysql.cursors.SSCursor` (unbuffered server-side cursor) instead of `DictCursor`. `pymysql.cursors.DictCursor` buffers the **entire query result set in client RAM inside `cur.execute()`** before `cur.fetchmany(BATCH_ROWS)` is ever called. Switching to `pymysql.cursors.SSCursor` streams rows from the MySQL socket in true `BATCH_ROWS` chunks and yields tuples directly.
3. For `write_strategy == "append"` (both `snapshot` and `incremental` modes), stream batches directly into `{target}` (or into a temporary table inside the `lakehouse` schema that is atomically swapped on completion) every `N` batches so `TEMP TABLE staging` never exceeds `DUCKDB_TEMP_SIZE_LIMIT`.

## Workstream 3: DuckLake Catalog Consolidation, Metadata Performance & Maintenance API

### Task 3.1: Eliminate N+1 `DESCRIBE` Queries in `app/core/catalog.py`

**File:** `backend/app/core/catalog.py`

**Problem:**
In `scan()` (`backend/app/core/catalog.py:455-461`), `lakehouse_columns(conn, schema, name)` executes a separate `DESCRIBE lakehouse.<schema>.<table>` query for every table in the lakehouse.

**Exact Changes in `backend/app/core/catalog.py`:**
1. Add a bulk column fetcher `lakehouse_all_columns(conn) -> dict[int, tuple[dict, ...]]` that queries DuckLake's internal metadata table `__ducklake_metadata_lakehouse.ducklake_column` in a single SQL query, falling back to `lakehouse_columns(conn, schema, name)` only if the metadata query fails:
   ```python
   def lakehouse_all_columns(conn) -> dict[int, tuple[dict, ...]]:
       """Reads all active table columns from DuckLake metadata in one query, keyed by table_id."""
       try:
           rows = conn.execute(
               f"""
               SELECT c.table_id,
                      c.column_name,
                      c.column_type,
                      c.nulls_allowed
               FROM __ducklake_metadata_{SOURCE}.ducklake_column AS c
               WHERE c.end_snapshot IS NULL
                 AND c.parent_column IS NULL
               ORDER BY c.table_id, c.column_order
               """
           ).fetchall()
       except Exception:
           return {}
       by_table: dict[int, list[dict]] = {}
       for table_id, col_name, col_type, nulls_allowed in rows:
           by_table.setdefault(int(table_id), []).append(
               {
                   "name": str(col_name),
                   "data_type": str(col_type),
                   "is_nullable": bool(nulls_allowed) if nulls_allowed is not None else True,
                   "is_primary_key": False,
               }
           )
       return {tid: tuple(cols) for tid, cols in by_table.items()}
   ```
2. In `scan()` (`backend/app/core/catalog.py:452-461`), call `cols_by_id = lakehouse_all_columns(conn)` once before the loop and look up `columns = cols_by_id.get(row["table_id"]) or lakehouse_columns(conn, schema, name)`.

### Task 3.2: Fix Directory Rename, Directory Delete, Table Preview, and Add Lakehouse Maintenance API in `routes_catalog.py`

**File:** `backend/app/api/routes_catalog.py`

**Problem:**
1. `rename_lakehouse_directory` (`routes_catalog.py:638-670`) renames the OS folder via `target_dir.rename(dest_dir)` without updating DuckLake schemas, breaking DuckLake's file pointers.
2. `delete_lakehouse_directory` (`routes_catalog.py:671-703`) checks `_parquet_files_under(target_dir)` and runs `shutil.rmtree(target_dir)` without dropping the tables/schema from DuckLake.
3. `get_table_preview` (`routes_catalog.py:1000-1080`) executes dead filesystem searches on lines 1042–1063 and then on line 1071 matches `next((t for t in catalogue.tables if t.name == table_name), None)` **without checking `t.folder`**, returning the wrong table when two schemas have a table with the same name (e.g., `raw/orders` vs `marts/orders`).
4. `backend/scripts/lakehouse_maintenance.py` implements DuckLake compaction (`ducklake_merge_adjacent_files`), snapshot expiration (`ducklake_expire_snapshots`), and orphan cleanup (`ducklake_cleanup_old_files`), plus live catalog/storage sizing, but none of this is exposed via FastAPI for the Settings UI.

**Exact Changes in `backend/app/api/routes_catalog.py`:**
1. Add synchronous DuckLake schema helpers in `routes_catalog.py` (next to `_move_table_sync` at line 90):
   ```python
   def _rename_directory_sync(old_rel_path: str, new_rel_path: str) -> None:
       conn = duckdb_engine.get_connection()
       try:
           rows = conn.execute(
               "SELECT s.schema_name, t.table_name "
               "FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
               "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s ON s.schema_id = t.schema_id "
               "WHERE t.end_snapshot IS NULL"
           ).fetchall()
           old_schema_prefix = _lakehouse_schema(old_rel_path)
           new_schema_prefix = _lakehouse_schema(new_rel_path)
           for schema_name, table_name in rows:
               if schema_name == old_schema_prefix or schema_name.startswith(old_schema_prefix + "."):
                   suffix = schema_name[len(old_schema_prefix):]
                   dst_schema = new_schema_prefix + suffix
                   _move_table_sync(schema_name, dst_schema, table_name)
       finally:
           conn.close()

   def _delete_directory_sync(rel_path: str, cascade: bool) -> int:
       conn = duckdb_engine.get_connection()
       try:
           schema_prefix = _lakehouse_schema(rel_path)
           rows = conn.execute(
               "SELECT s.schema_name, t.table_name "
               "FROM __ducklake_metadata_lakehouse.ducklake_table AS t "
               "JOIN __ducklake_metadata_lakehouse.ducklake_schema AS s ON s.schema_id = t.schema_id "
               "WHERE t.end_snapshot IS NULL"
           ).fetchall()
           matching = [
               (s, t) for s, t in rows
               if s == schema_prefix or s.startswith(schema_prefix + ".")
           ]
           if matching and not cascade:
               return -len(matching)
           for s, t in matching:
               conn.execute(f"DROP TABLE IF EXISTS lakehouse.{quote_ident(s)}.{quote_ident(t)};")
           for s in sorted({s for s, _ in matching}, key=len, reverse=True):
               try:
                   conn.execute(f"DROP SCHEMA IF EXISTS lakehouse.{quote_ident(s)};")
               except Exception:
                   pass
           return len(matching)
       finally:
           conn.close()
   ```
2. Update `rename_lakehouse_directory` (`routes_catalog.py:638-670`) to call `await asyncio.to_thread(_rename_directory_sync, rel_path, dest_rel_path)` before renaming/cleaning the directory on disk.
3. Update `delete_lakehouse_directory` (`routes_catalog.py:671-703`) to check both `_delete_directory_sync(rel_path, cascade)` and `_parquet_files_under(target_dir)` so DuckLake tables are properly dropped via SQL before `shutil.rmtree(target_dir, True)` removes empty folders.
4. In `get_table_preview` (`routes_catalog.py:1022-1080`), remove the dead `parquet_file` filesystem lookups (lines 1042–1063) and match the exact folder first before falling back to a unique bare name:
   ```python
   catalogue = await asyncio.to_thread(scan_lakehouse)
   wanted_folder = "" if is_root else clean_tier
   match = next(
       (t for t in catalogue.tables if t.name == table_name and (t.folder or "") == wanted_folder),
       None,
   )
   if match is None and is_root:
       candidates = [t for t in catalogue.tables if t.name == table_name]
       if len(candidates) == 1:
           match = candidates[0]
   if match is None:
       raise HTTPException(
           status_code=status.HTTP_404_NOT_FOUND,
           detail=f"Table '{table_name}' does not exist in folder '{wanted_folder or 'root'}'.",
       )
   ```
5. Add two endpoints in `backend/app/api/routes_catalog.py` to power the Settings Storage & Diagnostics tabs:
   * `GET /api/catalog/lakehouse/diagnostics`: Runs in `asyncio.to_thread(...)` and returns live metrics:
     * `storage_uri`: `settings.STORAGE_URI`
     * `lakehouse_root`: `str(settings.local_storage_path)`
     * `catalog_sqlite_bytes`: size of `<lakehouse_root>/.obelisk/catalog.sqlite`
     * `obelisk_db_bytes`: size of `obelisk.db`
     * `duckdb_tmp_bytes` and `duckdb_tmp_limit`: current size of `settings.duckdb_temp_path` and `settings.DUCKDB_TEMP_SIZE_LIMIT`
     * `duckdb_memory_limit_per_slot`: `advisor.get_duckdb_memory_limit(settings.DUCKDB_MAX_CONCURRENT_HEAVY)`
     * `snapshot_count`, `active_table_count`, `active_data_files`, `total_data_bytes`: queried from `__ducklake_metadata_lakehouse.ducklake_snapshot`, `ducklake_table`, and `ducklake_data_file`.
   * `POST /api/catalog/lakehouse/maintain`: Runs DuckLake maintenance in `asyncio.to_thread(...)` (`CALL ducklake_merge_adjacent_files('lakehouse')`, `CALL ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '7 days')`, `CALL ducklake_cleanup_old_files('lakehouse', cleanup_all => true)`) and returns before/after snapshot and file counts.

## Workstream 4: Small-Team Concurrency, Process Isolation & Docker Networking Fixes

### Task 4.1: Isolate Concurrent `dbt compile` / `dbt preview` Target Directories

**File:** `backend/app/engines/dbt_runner.py`

**Problem:**
`compile_model` (`dbt_runner.py:1200-1237`) calls `run_scoped_dbt(command="compile", ...)`, which writes to the shared `git_repo/dbt_project/target/` directory. Concurrent users compiling/previewing models in `dbt Studio` overwrite `target/compiled/` and `target/manifest.json`.

**Exact Changes in `backend/app/engines/dbt_runner.py`:**
1. Add an optional `target_path: Optional[Path] = None` parameter to `run_scoped_dbt` (`dbt_runner.py:1088-1198`). When `target_path` is provided, pass `["--target-path", str(target_path)]` to the `dbt` CLI command.
2. In `compile_model` (`dbt_runner.py:1200-1237`), create a temporary isolated target directory under `settings.duckdb_temp_path / f"dbt_compile_{uuid.uuid4().hex[:8]}"`, pass `target_path=scratch_target` to `run_scoped_dbt`, read the compiled SQL from `scratch_target / "compiled"`, and remove `scratch_target` in a `finally:` block.

### Task 4.2: Scope `OpenQueryTab` by Client ID (`X-Obelisk-Client-Id`)

**Files:**
* `backend/app/models/query.py`
* `backend/alembic/versions/0015_client_scoped_query_tabs.py` (new migration)
* `backend/app/api/routes_query.py`

**Problem:**
`open_query_tabs` (`backend/app/models/query.py:21-36`) has a global table with `id` (`"tab-1"`) as primary key and no client/user scope. Multiple browser windows or team members overwrite each other's SQL tabs.

**Exact Changes:**
1. In `backend/app/models/query.py:21-36`, add `client_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default", server_default="default", index=True)` to `OpenQueryTab`.
2. Create `backend/alembic/versions/0015_client_scoped_query_tabs.py` with `revision = "0015_client_scoped_query_tabs"` and `down_revision = "0014_attachable_drivers"`:
   ```python
   from alembic import op
   import sqlalchemy as sa

   revision = "0015_client_scoped_query_tabs"
   down_revision = "0014_attachable_drivers"
   branch_labels = None
   depends_on = None

   def upgrade() -> None:
       op.add_column(
           "open_query_tabs",
           sa.Column("client_id", sa.String(length=64), nullable=False, server_default="default"),
       )
       op.create_index("ix_open_query_tabs_client_id", "open_query_tabs", ["client_id"])

   def downgrade() -> None:
       op.drop_index("ix_open_query_tabs_client_id", table_name="open_query_tabs")
       op.drop_column("open_query_tabs", "client_id")
   ```
3. In `backend/app/api/routes_query.py:456-521`, accept `x_obelisk_client_id: Optional[str] = Header(default="default", alias="X-Obelisk-Client-Id")` in `list_open_tabs`, `save_open_tab`, and `delete_open_tab`. Namespace stored tab primary keys as `f"{client_id}:{req.id}"` (when not already prefixed) and filter `list_open_tabs` by `OpenQueryTab.client_id == client_id`, stripping the `f"{client_id}:"` prefix when returning `id` to the frontend so existing API contracts remain 100% compatible.

### Task 4.3: Kill Full Process Groups on Pipeline Step Timeout/Cancel

**File:** `backend/app/engines/pipeline_engine.py`

**Problem:**
In `_run_child` (`pipeline_engine.py:1531-1588`) and `cancel_run` (`pipeline_engine.py:747-752`), `proc.kill()` only kills the top-level child PID, leaving grandchild processes (such as `dbt` or spawned workers) running in the background.

**Exact Changes in `backend/app/engines/pipeline_engine.py`:**
1. Create a helper `_kill_process_tree(proc: subprocess.Popen) -> None` in `pipeline_engine.py`:
   ```python
   def _kill_process_tree(proc: subprocess.Popen) -> None:
       try:
           import psutil
           parent = psutil.Process(proc.pid)
           for child in parent.children(recursive=True):
               try:
                   child.kill()
               except (psutil.NoSuchProcess, psutil.AccessDenied):
                   pass
           parent.kill()
       except Exception:
           try:
               proc.kill()
           except OSError:
               pass
   ```
2. Replace `proc.kill()` in `stop()` (`line 363`), `cancel_run()` (`line 750`), and `_run_child()` (`lines 1575, 1584`) with `_kill_process_tree(proc)`.

### Task 4.4: Cache Telemetry Broadcasts & Fix Container Readiness Probes

**Files:**
* `backend/app/main.py`
* `backend/app/core/docker_manager.py`
* `backend/app/core/host_resolver.py`

**Problem:**
1. In `backend/app/main.py:183-233`, every connected `/ws/telemetry` WebSocket client independently calls `advisor.get_host_specs()`, `advisor.get_active_container_allocations()`, and `docker_mgr.list_managed_containers()` every 2 seconds. With 5 team members open, the backend hammers `/var/run/docker.sock` 10 times per second.
2. When `obelisk-core` runs inside Docker (`docker-compose.yml`), `docker_mgr.probe_readiness` (`docker_manager.py:387, 392`) and `host_resolver.py:18-36` only probe `127.0.0.1:{port}`, which fails inside the `obelisk-core` container when probing sibling containers (`obelisk_postgres`, `obelisk_superset`, `obelisk_gitea`, `obelisk_clickhouse`, `obelisk_redpanda`) on `obelisk_net`.

**Exact Changes:**
1. In `backend/app/main.py:183-233`, cache the computed telemetry dictionary at module level for `1.5` seconds (`_telemetry_cache: tuple[float, dict] | None = None`) inside a shared async helper `await _get_cached_telemetry()`, and use it in both `/ws/telemetry` and `/api/services/telemetry`.
2. In `backend/app/core/docker_manager.py:373-396` (`probe_readiness`), for `http` and `tcp` probes, try `127.0.0.1:{port}` first, and if that fails and `container` is running on `obelisk_net`, also probe `f"obelisk_{service_name.lower()}:{container_internal_port}"` so readiness probes succeed both when `obelisk-core` runs on the host and when it runs inside Docker.
3. In `backend/app/core/host_resolver.py:12-37` (`resolve_connection_host`), when `is_local` is True and `_is_tcp_port_open("127.0.0.1", port)` is False:
   * Check `host.docker.internal:{port}` and, for known service ports (`5432 -> obelisk_postgres`, `8123 -> obelisk_clickhouse`), check if the sibling container hostname on `obelisk_net` is reachable.
4. In `backend/Dockerfile:8-20` and `backend/requirements.txt:52-54`:
   * Remove `default-jre-headless` from `backend/Dockerfile` (`JAVA_HOME` is only needed for local JVM Spark, whereas `obelisk-core` is an embedded DuckDB/Polars appliance and supports remote Spark via `pyspark[connect]`).

## Workstream 5: Frontend UX, Settings Tab Overhaul, Navigation Consolidation & Memory Hygiene

### Task 5.1: Cap Keep-Alive Mounted Views & Send Client-Scoped Tab Header

**Files:**
* `frontend/src/lib/api.ts`
* `frontend/src/App.tsx`

**Exact Changes:**
1. In `frontend/src/lib/api.ts:47-53`, generate a persistent browser client ID in `localStorage` (`obelisk_client_id`) and attach it as an `X-Obelisk-Client-Id` header on `apiClient`:
   ```ts
   const getClientId = (): string => {
     try {
       let cid = localStorage.getItem('obelisk_client_id');
       if (!cid) {
         cid = typeof crypto !== 'undefined' && 'randomUUID' in crypto
           ? crypto.randomUUID()
           : `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
         localStorage.setItem('obelisk_client_id', cid);
       }
       return cid;
     } catch {
       return 'default';
     }
   };

   export const apiClient = axios.create({
     baseURL: '/api',
     headers: {
       'Content-Type': 'application/json',
       'X-Obelisk-Client-Id': getClientId(),
     },
     withCredentials: true,
   });
   ```
2. In `frontend/src/App.tsx:79-83`, cap `mountedTabs` to keep at most the **active tab plus the 3 most recently visited stateful workspaces** (`query`, `dbt`, `pipelines`) mounted simultaneously, unmounting idle `<iframe>` tabs (`dashboards`, `notebooks`) or older views when the cap is exceeded so browser RAM stays bounded.

### Task 5.2: Overhaul `SettingsView.tsx` & `SdkDocsView.tsx` into a Live Operational Control Center

**Files:**
* `frontend/src/components/settings/SettingsView.tsx`
* `frontend/src/components/settings/SdkDocsView.tsx`
* `frontend/src/lib/api.ts`

**Problem:**
1. **Variables & Secrets (`SettingsView.tsx:316-456`)**: Rows only have Copy and Delete buttons (`lines 427-447`); there is no **Edit** button on existing variables, forcing users to click `+ Add Variable` and re-type the exact variable key name to update a secret or config value.
2. **Lakehouse Storage (`SettingsView.tsx:651-699`)**: 100% static HTML with hardcoded `file:///data/lakehouse` (`line 669`), hardcoded `Snappy` (`line 679`), a false claim that setting `STORAGE_URI=s3://my-lakehouse` works (`lines 693-695`), and zero buttons to trigger DuckLake file compaction or snapshot cleanup.
3. **Diagnostics (`SettingsView.tsx:701-763`)**: 4 static cards (`Internal Brain (SQLite)`, `Cryptography Vault`, `Docker Daemon`, `Federation Engine`) that display zero live metrics.
4. **SDK & Platform Guide (`SdkDocsView.tsx`)**: References removed features (`sync_postgres=True` on line 159, `Dagster daemon` & `git_repo/dags/` on lines 396–403, and legacy `lakehouse.bronze.venues` / `gold.aggregated_events` schemas on lines 108, 142, 153, 199, 205).

**Exact Changes:**
1. In `frontend/src/lib/api.ts` (`catalogApi`), add:
   * `getLakehouseDiagnostics: async () => (await apiClient.get('/catalog/lakehouse/diagnostics')).data`
   * `maintainLakehouse: async () => (await apiClient.post('/catalog/lakehouse/maintain')).data`
2. In `frontend/src/components/settings/SettingsView.tsx`:
   * **Variables & Secrets tab**: Add an `Edit` (`Edit2` icon) action button on each variable row (next to Copy and Delete at `line 427`) that opens the modal pre-populated with `variable.key`, `variable.description`, `variable.is_secret`, and fetches/prefills the current value so users can update existing variables in one click.
   * **Lakehouse Storage tab**: Fetch live metrics from `catalogApi.getLakehouseDiagnostics()` when the tab opens. Display:
     * Live **Storage Root URI** (`diagnostics.storage_uri`) and **Catalog Engine** (`DuckLake SQLite (.obelisk/catalog.sqlite)` with actual file size in MB).
     * Live **Active Tables**, **Snapshots Count**, **Parquet Data Files**, and **Total Lakehouse Disk Usage**.
     * An interactive **"Run Maintenance (Compact & Expire Snapshots)"** button that calls `catalogApi.maintainLakehouse()` and displays the compacted file/snapshot delta via `toast.success(...)`.
     * Replace the false `STORAGE_URI=s3://...` callout with accurate guidance on DuckLake snapshot retention and local NVMe backup via `lakehouse_maintenance.py backup`.
   * **Diagnostics tab**: Bind the 4 diagnostic cards to real data from `catalogApi.getLakehouseDiagnostics()` and `/health`:
     * **Control Plane DB (`obelisk.db`)**: Live file size + SQLite WAL mode status.
     * **DuckDB Execution Engine**: Live per-slot memory cap (`diagnostics.duckdb_memory_limit_per_slot`), max query row cap (`10,000`), and live `duckdb_tmp` spill usage vs `DUCKDB_TEMP_SIZE_LIMIT`.
     * **DuckLake Metadata Catalog**: `.obelisk/catalog.sqlite` size + active snapshot ID.
     * **Docker Engine & cryptography vault**: Live connection count and active container RAM allocation.
3. In `frontend/src/components/settings/SdkDocsView.tsx`:
   * Replace all `lakehouse.bronze.*` / `gold.*` examples with `lakehouse.raw.orders` and `lakehouse.marts.daily_revenue`.
   * Update the `obelisk.save(df, target)` signature and examples (`lines 159-176`) to remove `sync_postgres=True`, document that `save()` writes directly to DuckLake (`lakehouse.<folder>.<name>`), and document the new out-of-core `obelisk.materialize_sql(sql, target)` helper.
   * Update Step `05` (`lines 382-406`, "Orchestration & Jobs") to document Obelisk's built-in **DAG Pipeline Engine** (`Ingest`, `dbt`, `SQL`, `Script`, and `Check` nodes with cron scheduling and watermarks) and remove all mentions of `Dagster` and `git_repo/dags/`.

### Task 5.3: Streamline Sidebar Navigation, Query Studio Defaults & Modal Standardization

**Files:**
* `frontend/src/components/layout/Sidebar.tsx`
* `frontend/src/components/home/HomeView.tsx`
* `frontend/src/components/query/QueryStudio.tsx`
* `frontend/src/components/catalog/CatalogView.tsx`
* `frontend/src/components/catalog/CatalogTree.tsx`
* `frontend/src/components/dbt/DbtStudio.tsx`
* `frontend/src/components/notebooks/NotebooksView.tsx`
* `frontend/src/components/settings/SettingsView.tsx`

**Exact Changes:**
1. **Group Sidebar Navigation & Demote Redundant Raw Tools (`Sidebar.tsx`)**:
   * Group the sidebar items into 3 scannable sections with subtle section headers when expanded:
     * **Explore & Analyze**: `Home`, `Data Catalog`, `Query Studio`, `Notebooks`, `Dashboards`
     * **Build & Orchestrate**: `dbt Studio`, `Pipelines`
     * **Platform**: `Service Catalog`, `Settings`
   * Remove `Files` (`FileManagerView`) and `Terminal` (`TerminalView`) from the primary sidebar by default (or gate them behind an `Enable Developer Raw Tools (Files & Terminal)` toggle in `SettingsView` stored in `localStorage`), and allow editing `git_repo/scripts/*.py` directly from `dbt Studio` / `Pipelines` so users don't have a redundant file tree that conflicts with `dbt Studio` and `Marimo`.
2. **Fix Home & Query Studio Defaults**:
   * In `frontend/src/components/home/HomeView.tsx:281`, change the **dbt Models** metric card `onClick` handler from `onClick={() => onNavigate('pipelines')}` to `onClick={() => onNavigate('dbt')}`.
   * In `frontend/src/components/query/QueryStudio.tsx:318-325`, remove the hardcoded `'bronze'` directory preference; pick the first directory with tables (`const firstDirWithTables = lakehouseNode?.directories?.find((d) => d.tables && d.tables.length > 0);`) and generate `SELECT * FROM lakehouse.${firstDirWithTables.path.replace(/\//g, '.')}.${firstTable.name} LIMIT 50;`.
   * In the Results telemetry bar (`QueryStudio.tsx:1550-1563`), if `activeTab.result.truncated` is `true`, render an amber badge: `Showing first {activeTab.result.row_count.toLocaleString()} rows (result truncated — use Export for full dataset)`.
3. **Replace Native `window.confirm` / `alert` with `<ConfirmDialog />` and `toast`**:
   * Replace all `alert(...)` calls in `CatalogTree.tsx` (`lines 187, 209, 235, 261, 286`) and `DbtStudio.tsx` (`lines 266, 446`) with `toast.error(...)` from `frontend/src/lib/toast.ts`.
   * Replace `window.confirm(...)` in `CatalogView.tsx:112`, `DbtStudio.tsx:219, 440`, `NotebooksView.tsx:204`, and `SettingsView.tsx:155, 212` with `<ConfirmDialog />` (`frontend/src/components/common/ConfirmDialog.tsx`) so all destructive actions use a consistent, non-blocking modal across the platform.

## Verification & Acceptance Gates

After completing each workstream, run the following verification commands from the repository root:

1. **Backend Automated Test Suite:**
   ```bash
   cd backend && pytest -q
   ```
   * Verify that all existing tests pass and update any test assertions in `backend/tests/test_sdk.py` that passed `sync_postgres` or expected `warehouse_sync` in `obelisk.save()`.
2. **Async Route Non-Blocking Verification:**
   ```bash
   python3 backend/scripts/find_blocking_calls.py
   ```
   * Must exit `0` with zero blocking I/O calls inside `async def` FastAPI routes.
3. **DuckLake Spike & Maintenance Verification:**
   ```bash
   python3 backend/scripts/spike_ducklake.py
   python3 backend/scripts/lakehouse_maintenance.py maintain --dry-run
   ```
   * Must pass all gating checks and report snapshot status cleanly.
4. **Frontend TypeScript & Production Bundle Build:**
   ```bash
   cd frontend && npm run build
   ```
   * `tsc && vite build` must exit `0` with zero TypeScript or Rollup errors.