# Obelisk Platform — Settings Tab, SDK Docs & UX Implementation Plan

> **Scope note:** This is a *separate* plan from `obelisk_agent_implementation_plan.md`.
> It covers only the Settings surface (`SettingsView.tsx`, `SdkDocsView.tsx`), the
> navigation/home/command-palette UX, and connection-management consolidation. It does
> not restate the out-of-core query, ingestion, DuckLake catalog, or concurrency
> workstreams from the other plan; where it depends on them, the dependency is called out
> explicitly in **Dependencies on the Other Plan**.

## Objective & Success Criteria

Make every Settings sub-tab **tell the truth and do something**:

1. A user can **edit an existing variable or secret in place** (no re-typing the key).
2. Lakehouse Storage shows **live** storage, catalog, table, snapshot and spill metrics,
   and can run **one-click DuckLake maintenance** (merge files, expire snapshots) that
   already exists as a script but is not reachable from the UI.
3. Diagnostics shows **live numbers** (SQLite size + journal mode, DuckLake catalog size +
   snapshot count, DuckDB per-slot memory + spill usage, Docker state) instead of four
   static paragraphs.
4. The SDK & Platform Guide documents **what the code actually does today** — DuckLake
   folder schemas, DuckLake-owned files, no `sync_postgres`, no Dagster.
5. The app is easier to navigate: a grouped sidebar, a first-run quickstart on Home, one
   place to manage connections, and a command palette that can jump to **pipelines, saved
   queries, dbt models, notebooks and connections** (not just tabs).

## Verified Baseline (what the code does today)

| Area | Current reality | Evidence |
| --- | --- | --- |
| Variables table | Copy + Delete only, no Edit | `SettingsView.tsx:427-447` |
| Variable API | `POST /api/variables` already **upserts by key**; `UpdateVariableRequest` is defined but **unused**; no `PUT` | `routes_variables.py:13-107` |
| Connection Vault | Table with test/edit/delete; **shares** `AddConnectionModal` with Catalog | `SettingsView.tsx:30, 458-649`; `CatalogView.tsx:11, 219-228` |
| Warehouse selector | Lives only in Settings; `GET/PUT /api/dbt/warehouse` exists | `SettingsView.tsx:485-512`; `routes_dbt.py:180-213` |
| Lakehouse Storage tab | 100 % static; hardcodes `file:///data/lakehouse`, `Snappy`, and an S3 promise | `SettingsView.tsx:651-699` |
| S3 promise | `s3://` is **rejected at startup** (`RuntimeError`) | `config.py:194-213` |
| Diagnostics tab | 4 static cards, zero live values | `SettingsView.tsx:701-763` |
| Maintenance script | `maintain` / `backup` CLI exists with verified CALL signatures | `backend/scripts/lakehouse_maintenance.py:29-97` |
| SDK docs | `sync_postgres=True`, bronze/gold names, `data/lakehouse/...parquet` paths, Dagster | `SdkDocsView.tsx:108, 142, 153, 159-180, 199-205, 396-403` |
| Sidebar | 11 flat items incl. Files + Terminal | `Sidebar.tsx:36-48` |
| Home | dbt Models card navigates to **pipelines**; no first-run state | `HomeView.tsx:280-298` |
| Command palette | **Already exists** and is wired to Cmd/Ctrl+K; indexes tables, connections, notebooks, dbt models | `App.tsx:5, 73, 96-109, 192`; `CommandMenu.tsx:95-230` |
| Vault algorithm | Fernet — AES-128-CBC + HMAC-SHA256 (**not** AES-256-GCM) | `backend/app/core/vault.py:1-36` |
| Toast + confirm primitives | `toast` singleton + `ToastHost` (mounted); `ConfirmDialog` component (no global hook) | `lib/toast.ts`; `components/common/ToastHost.tsx`; `components/common/ConfirmDialog.tsx` |
| Telemetry hook | `useTelemetry()` already does WS + 2 s polling fallback | `lib/useTelemetry.ts` |

## Agent Execution Rules

1. **No fake data in UI.** Every number rendered must come from an endpoint; if an endpoint
   fails, the tab shows the error and the retry control — never a placeholder value.
2. **No silent fallbacks in the backend.** A failed DuckDB/SQLite/Docker probe must raise
   or return an explicit `error` field; it must not be swallowed into a zero.
3. **Async routes never block.** All filesystem/DuckDB/Docker work goes through
   `asyncio.to_thread`, matching `routes_catalog.py` and the
   `backend/tests/test_async_routes_do_not_block.py` gate.
4. **Reuse verified primitives.** `lakehouse_maintenance.py`'s CALL signatures,
   `ConfirmDialog`, `toast`, `useTelemetry`, `AddConnectionModal`, and the existing
   `api` client. Do not introduce a new UI library or a second settings store.
5. **Workstreams run in order.** WS-1 provides the contracts WS-2…WS-4 consume. WS-5 is
   gated on the SDK change (see §10). WS-6…WS-8 are independent of WS-2…WS-5 but must
   build after WS-1 so the final verification pass sees one consistent tree.

## Workstream Overview

| WS | Focus | Primary files | Outcome |
| --- | --- | --- | --- |
| **WS-1** | Settings backend contracts | `routes_catalog.py`, `routes_variables.py` | `GET /api/catalog/lakehouse/diagnostics`, `POST /api/catalog/lakehouse/maintain`, `PUT /api/variables/{key}` |
| **WS-2** | Variables & Secrets inline edit | `SettingsView.tsx`, `api.ts`, `types.ts` | Edit button → prefilled modal → `PUT`; `ConfirmDialog` for delete |
| **WS-3** | Lakehouse Storage: live + maintenance | `SettingsView.tsx` | Live URI/root/catalog/tables/snapshots/files/bytes/spill + maintenance panel |
| **WS-4** | Diagnostics: live cards | `SettingsView.tsx` | 4 cards bound to real metrics + `useTelemetry()` |
| **WS-5** | SDK & Platform Guide truth pass | `SdkDocsView.tsx`, `docs/sdk.md` | DuckLake-correct snippets; no Dagster; no `sync_postgres` |
| **WS-6** | Navigation, Preferences, First-run | `Sidebar.tsx`, `HomeView.tsx`, `SettingsView.tsx`, new `lib/preferences.ts` | 3 grouped sections (+ optional Developer), quickstart banner, card fix |
| **WS-7** | One connection model | shared hook + `DbtStudio.tsx` + `SettingsView.tsx` | Shared state/actions; warehouse selector moved next to dbt targets |
| **WS-8** | Command palette extension | `CommandMenu.tsx`, `App.tsx`, `CatalogView.tsx`, `DbtStudio.tsx` | Pipelines + saved queries + deep links |

---

## Workstream 1: Backend Contracts for Settings

### Task 1.1: `GET /api/catalog/lakehouse/diagnostics`

**File:** `backend/app/api/routes_catalog.py`

**Problem:** The Settings tabs have nothing to read. The maintenance script already knows
how to size the lakehouse, and `app/core/catalog.py` already has the metadata queries, but
neither is exposed over HTTP.

**Verified building blocks (do not invent new metadata tables):**

- `settings.STORAGE_URI`, `settings.storage_scheme`, `settings.lakehouse_root_path`,
  `settings.lakehouse_catalog_path`, `settings.duckdb_temp_path`,
  `settings.DUCKDB_TEMP_SIZE_LIMIT`, `settings.DUCKDB_THREADS`, `settings.sqlite_db_path`.
- `app.core.catalog.lakehouse_tables(conn)` → `schema`, `table`, `num_rows`, `size_bytes`,
  `table_id` (`catalog.py:188-223`).
- `app.core.catalog.last_snapshot_time(conn)` (`catalog.py:245-260`).
- `ducklake_snapshots('lakehouse')` table function — proven by
  `lakehouse_maintenance.py:62` and already used inside `last_snapshot_time`.
- `advisor.get_duckdb_memory_limit()` (`resource_advisor.py:124-129`).
- `docker_mgr.engine_status()` / `docker_mgr.list_managed_containers()`
  (`docker_manager.py:399-405`).

Do **not** query `ducklake_column` / `ducklake_data_file` here; their presence is not
proven in this codebase (only `ducklake_table`, `ducklake_schema`, `ducklake_table_stats`
and `ducklake_snapshots(...)` are). Parquet-file counting is done on the filesystem and is
capped.

**Exact changes — add near `_catalog_tree_cache` (`routes_catalog.py:114-120`):**

```python
_diagnostics_cache: Optional[tuple[float, Dict[str, Any]]] = None
_DIAGNOSTICS_TTL_SECONDS = 2.0
_DIAGNOSTICS_MAX_FILES = 200_000


def _dir_bytes(path: Path) -> int:
    """Sum of file sizes under one directory; 0 when absent."""
    if not path.is_dir():
        return 0
    total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_size
        except OSError:
            continue
    return total


def _sqlite_journal_mode(db_path: Path) -> str:
    """The journal mode SQLite reports for the control-plane database."""
    if not db_path.is_file():
        return "absent"
    import sqlite3
    try:
        with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as conn:
            row = conn.execute("PRAGMA journal_mode;").fetchone()
            return str(row[0]).upper() if row else "unknown"
    except Exception as exc:
        return f"error: {exc}"


def _count_parquet_files(root: Path) -> tuple[int, bool]:
    """Counts Parquet files, stopping at a ceiling so a huge lake is not walked twice.

    Returns ``(count, capped)``; ``capped`` tells the UI the number is a lower bound.
    """
    count = 0
    for candidate in root.rglob("*.parquet"):
        if ".obelisk" in candidate.parts:
            continue
        count += 1
        if count >= _DIAGNOSTICS_MAX_FILES:
            return count, True
    return count, False
```

```python
def _lakehouse_diagnostics_sync() -> Dict[str, Any]:
    """Live storage, catalog, engine and container facts for the Settings UI."""
    from app.core.catalog import (
        SOURCE,
        last_snapshot_time,
        lakehouse_connection,
        lakehouse_tables,
    )
    from app.core.resource_advisor import advisor

    lakehouse_root = settings.lakehouse_root_path
    catalog_path = settings.lakehouse_catalog_path
    db_path = Path(settings.sqlite_db_path)
    wal_path = db_path.parent / f"{db_path.name}-wal"
    tmp_path = settings.duckdb_temp_path

    conn = lakehouse_connection()
    try:
        tables = lakehouse_tables(conn)
        snapshots = conn.execute(f"FROM ducklake_snapshots('{SOURCE}');").fetchall()
        last_snapshot_at = last_snapshot_time(conn).isoformat()
    finally:
        conn.close()

    parquet_files, files_capped = _count_parquet_files(lakehouse_root)

    engine = docker_mgr.engine_status()
    allocations = advisor.get_active_container_allocations()
    containers = docker_mgr.list_managed_containers()

    return {
        "storage_uri": settings.STORAGE_URI,
        "storage_scheme": settings.storage_scheme,
        "lakehouse_root": lakehouse_root.as_posix(),
        "catalog_sqlite_bytes": catalog_path.stat().st_size if catalog_path.is_file() else 0,
        "obelisk_db_bytes": db_path.stat().st_size if db_path.is_file() else 0,
        "obelisk_db_wal_bytes": wal_path.stat().st_size if wal_path.is_file() else 0,
        "obelisk_db_journal_mode": _sqlite_journal_mode(db_path),
        "duckdb_tmp_bytes": _dir_bytes(tmp_path),
        "duckdb_tmp_limit": settings.DUCKDB_TEMP_SIZE_LIMIT,
        "duckdb_memory_limit_per_slot": advisor.get_duckdb_memory_limit(),
        "duckdb_threads": settings.DUCKDB_THREADS,
        "active_tables": len(tables),
        "total_rows": sum(int(t["num_rows"] or 0) for t in tables),
        "total_data_bytes": sum(int(t["size_bytes"] or 0) for t in tables),
        "parquet_files": parquet_files,
        "parquet_files_capped": files_capped,
        "snapshot_count": len(snapshots),
        "last_snapshot_at": last_snapshot_at,
        "docker_available": bool(engine.get("available")),
        "docker_error": engine.get("error"),
        "managed_containers": len(containers),
        "container_allocated_gb": allocations.get("total_allocated_gb", 0.0),
    }


@router.get("/lakehouse/diagnostics")
async def lakehouse_diagnostics(refresh: bool = False):
    """Live lakehouse/catalog/engine metrics for the Settings tabs.

    Cached for a couple of seconds so opening a tab and clicking Sync cannot hammer
    Docker or walk a large lake repeatedly. ``refresh=true`` bypasses the cache.
    """
    global _diagnostics_cache
    now = time.time()
    if (
        not refresh
        and _diagnostics_cache is not None
        and now - _diagnostics_cache[0] < _DIAGNOSTICS_TTL_SECONDS
    ):
        return _diagnostics_cache[1]

    payload = await asyncio.to_thread(_lakehouse_diagnostics_sync)
    _diagnostics_cache = (now, payload)
    return payload
```

> If the memory-budget workstream from the other plan has landed, pass
> `settings.DUCKDB_MAX_CONCURRENT_HEAVY` to `advisor.get_duckdb_memory_limit(...)` in the
> payload; the call above is the compatible form for today's signature.
> `routes_catalog.py` does **not** currently import `docker_mgr` (`routes_catalog.py:1-32`),
> so add `from app.core.docker_manager import docker_mgr` at the top.

### Task 1.2: `POST /api/catalog/lakehouse/maintain`

**File:** `backend/app/api/routes_catalog.py`

**Problem:** `backend/scripts/lakehouse_maintenance.py` already implements compaction,
snapshot expiry and orphan cleanup, but it is CLI-only. The Settings tab advertises
storage housekeeping and offers no button.

**Exact changes — mirror the script's verified CALL signatures
(`lakehouse_maintenance.py:36-56`) exactly:**

```python
class LakehouseMaintenanceRequest(BaseModel):
    keep_days: int = Field(7, ge=1, le=3650)
    max_compacted_files: int = Field(50, ge=1, le=1000)
    cleanup_files: bool = Field(False)
    dry_run: bool = Field(False)


def _maintain_lakehouse_sync(req: LakehouseMaintenanceRequest) -> Dict[str, Any]:
    """Runs DuckLake compaction/expiry/cleanup and reports before/after counts.

    Error semantics are the script's: a DuckDB failure propagates with its message,
    so the UI can show what the extension refused and why.
    """
    from datetime import datetime, timedelta, timezone
    from app.core.catalog import SOURCE, lakehouse_connection

    conn = lakehouse_connection()
    try:
        before = conn.execute(f"FROM ducklake_snapshots('{SOURCE}');").fetchall()

        merged = conn.execute(
            f"CALL ducklake_merge_adjacent_files('{SOURCE}', "
            f"max_compacted_files => {int(req.max_compacted_files)});"
        ).fetchall()

        older_than = datetime.now(timezone.utc) - timedelta(days=int(req.keep_days))
        dry_run_sql = "true" if req.dry_run else "false"
        expired = conn.execute(
            f"CALL ducklake_expire_snapshots('{SOURCE}', "
            f"older_than => TIMESTAMPTZ '{older_than.isoformat()}', "
            f"dry_run => {dry_run_sql});"
        ).fetchall()

        cleaned: list = []
        if req.cleanup_files:
            cleaned = conn.execute(
                f"CALL ducklake_cleanup_old_files('{SOURCE}', "
                f"older_than => TIMESTAMPTZ '{older_than.isoformat()}', "
                f"dry_run => {dry_run_sql});"
            ).fetchall()

        after = conn.execute(f"FROM ducklake_snapshots('{SOURCE}');").fetchall()
    finally:
        conn.close()

    return {
        "status": "dry-run" if req.dry_run else "ok",
        "merged_tables": len(merged),
        "expired_snapshots": len(expired),
        "cleaned_files": len(cleaned),
        "snapshots_before": len(before),
        "snapshots_after": len(after),
        "keep_days": req.keep_days,
        "max_compacted_files": req.max_compacted_files,
        "cleanup_files": req.cleanup_files,
        "dry_run": req.dry_run,
    }


@router.post("/lakehouse/maintain")
async def maintain_lakehouse(req: LakehouseMaintenanceRequest):
    """One-click DuckLake housekeeping for the Settings → Lakehouse Storage tab."""
    global _diagnostics_cache
    result = await asyncio.to_thread(_maintain_lakehouse_sync, req)
    _diagnostics_cache = None  # counts just changed
    return result
```

**Safety defaults:** `cleanup_files=False` (deleting bytes is irreversible) and
`dry_run=False` only for merge + expire. The UI must show a `ConfirmDialog` whenever
`cleanup_files` is true, and label the result as a dry run when it is one.

### Task 1.3: `PUT /api/variables/{key}`

**File:** `backend/app/api/routes_variables.py`

**Problem:** `UpdateVariableRequest` already exists (`routes_variables.py:28-31`) but no
route consumes it. Today an edit must go through `POST`, which overwrites `description`
with `req.description or ""` and cannot distinguish "leave the secret alone" from "set it
to empty".

**Exact changes — add after `set_variable`:**

```python
def _masked(v: Variable) -> dict:
    return {
        "id": v.id,
        "key": v.key,
        "value": "••••••••••••" if v.is_secret else v.value,
        "is_secret": v.is_secret,
        "description": v.description,
        "created_at": v.created_at.isoformat() if v.created_at else None,
        "updated_at": v.updated_at.isoformat() if v.updated_at else None,
    }


@router.put("/{key}")
async def update_variable(key: str, req: UpdateVariableRequest):
    """Partially updates an existing variable.

    Omitted fields are left untouched (unlike POST, which is an upsert). Turning a
    variable into a secret — or back — requires the plaintext value, because the
    stored bytes cannot be reinterpreted.
    """
    clean_key = key.strip().upper()
    async with AsyncSessionLocal() as session:
        stmt = select(Variable).where(Variable.key == clean_key)
        v = (await session.execute(stmt)).scalars().first()
        if not v:
            raise HTTPException(
                status_code=404, detail=f"Variable '{clean_key}' not found"
            )

        if req.is_secret is not None and req.is_secret != v.is_secret:
            if req.value is None:
                raise HTTPException(
                    status_code=400,
                    detail="Changing is_secret requires a new value: the stored "
                    "representation cannot be converted in place.",
                )
            v.is_secret = req.is_secret

        if req.value is not None:
            v.value = vault.encrypt(req.value) if v.is_secret else req.value
        if req.description is not None:
            v.description = req.description

        await session.commit()
        await session.refresh(v)
        return _masked(v)
```

**Acceptance:** editing only the description leaves the ciphertext unchanged; changing
`is_secret` without a value returns 400; unknown key returns 404.

### Task 1.4: New regression tests

**File (new):** `backend/tests/test_settings_diagnostics.py`

Follow the existing `httpx.AsyncClient` fixture pattern (see
`backend/tests/test_warehouse_binding.py:99-118`). Cover:

1. `GET /api/catalog/lakehouse/diagnostics` → 200 and every documented key is present;
   `catalog_sqlite_bytes >= 0`, `obelisk_db_bytes >= 0`, `snapshot_count >= 1`.
2. `POST /api/catalog/lakehouse/maintain` with `{"dry_run": true}` → 200, `status ==
   "dry-run"`, and `snapshots_before == snapshots_after` (a dry run must not expire).
3. `PUT /api/variables/{key}` description-only → value/ciphertext unchanged (reveal
   before and after and compare), description changed.
4. `PUT /api/variables/{key}` toggling `is_secret` without `value` → 400.
5. `PUT /api/variables/UNKNOWN` → 404.

---

## Workstream 2: Variables & Secrets — Inline Edit

### Task 2.1: Client types and API methods

**Files:** `frontend/src/lib/types.ts`, `frontend/src/lib/api.ts`

Add to `types.ts` (next to `CreateVariableRequest`):

```ts
export interface UpdateVariableRequest {
  value?: string | null;
  is_secret?: boolean | null;
  description?: string | null;
}
```

Add to `api.ts` after `createVariable` (`api.ts:411-414`):

```ts
updateVariable: async (
  key: string,
  payload: import('./types').UpdateVariableRequest,
): Promise<VariableItem> => {
  const res = await apiClient.put<VariableItem>(
    `/variables/${encodeURIComponent(key)}`,
    payload,
  );
  return res.data;
},
```

### Task 2.2: Edit action on every variable row

**File:** `frontend/src/components/settings/SettingsView.tsx`

1. `Edit2` is **already imported** (`SettingsView.tsx:18`) — no new icon.
2. New state near the other variable state (`SettingsView.tsx:41-51`):

```ts
const [editingVariable, setEditingVariable] = useState<VariableItem | null>(null);
const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);
```

Imports to add:

```ts
import { toast } from '../../lib/toast';
import { ConfirmDialog, type ConfirmRequest } from '../common/ConfirmDialog';
```

3. Open helpers:

```ts
const openCreateVariable = () => {
  setEditingVariable(null);
  setNewVarKey('');
  setNewVarValue('');
  setNewVarIsSecret(false);
  setNewVarDesc('');
  setIsAddVarOpen(true);
};

const openEditVariable = (item: VariableItem) => {
  setEditingVariable(item);
  setNewVarKey(item.key);
  // A stored secret cannot be pre-filled: the API only ever returns the mask.
  setNewVarValue(item.is_secret ? '' : item.value);
  setNewVarIsSecret(item.is_secret);
  setNewVarDesc(item.description || '');
  setIsAddVarOpen(true);
};
```

4. Render the Edit button **before** Copy/Delete in the actions cell
   (`SettingsView.tsx:427-447`):

```tsx
<button
  onClick={() => openEditVariable(v)}
  className="p-1 rounded-md border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 hover:text-slate-900 shadow-2xs transition-colors"
  title={`Edit ${v.key}`}
>
  <Edit2 className="w-3.5 h-3.5" />
</button>
```

5. Point the header "+ Add Variable or Secret" button (`SettingsView.tsx:332-338`) at
   `openCreateVariable` instead of `setIsAddVarOpen(true)`, so it always opens in create
   mode after an edit.

### Task 2.3: Make the existing modal serve both modes

**File:** `frontend/src/components/settings/SettingsView.tsx`

`handleSaveVariable` (`SettingsView.tsx:167-192`) currently early-returns when the value
is blank, which would make "edit description only" impossible. Replace its guard and
branch:

```ts
const handleSaveVariable = async (e: React.FormEvent) => {
  e.preventDefault();
  if (!newVarKey.trim()) return;
  const editing = editingVariable !== null;
  if (!editing && !newVarValue.trim()) return;

  setSavingVar(true);
  setError(null);
  try {
    if (editing) {
      await api.updateVariable(editingVariable.key, {
        // Blank means "keep the current value" — required for secrets.
        value: newVarValue.trim() ? newVarValue : undefined,
        is_secret: newVarIsSecret,
        description: newVarDesc.trim(),
      });
      toast.success(`Updated ${editingVariable.key}`);
    } else {
      await api.createVariable({
        key: newVarKey.trim().toUpperCase(),
        value: newVarValue,
        is_secret: newVarIsSecret,
        description: newVarDesc.trim(),
      });
      toast.success(`Created ${newVarKey.trim().toUpperCase()}`);
    }
    setIsAddVarOpen(false);
    setEditingVariable(null);
    setNewVarKey('');
    setNewVarValue('');
    setNewVarIsSecret(false);
    setNewVarDesc('');
    await loadData(true);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Failed to save variable';
    setError(msg);
    toast.error(msg);
  } finally {
    setSavingVar(false);
  }
};
```

Modal changes (`SettingsView.tsx:768-887`):

- Title: `editingVariable ? 'Edit Variable or Secret' : 'Create Variable or Secret'`.
- Key input: `disabled={editingVariable !== null}` with a helper line
  "The key is the variable's identity; create a new one to rename it."
- Value input: `required={editingVariable === null}` and, when editing a secret,
  placeholder `"Leave blank to keep the current secret"`; when editing a plaintext
  variable, prefill its value (done by `openEditVariable`).
- Submit button label: `editingVariable ? 'Save Changes' : 'Create Variable'`.
- Also clear `editingVariable` in the close (✕) handler.

### Task 2.4: Replace `window.confirm` on delete

`handleDeleteVariable` (`SettingsView.tsx:154-165`) must use the shared dialog:

```ts
const handleDeleteVariable = (item: VariableItem) => {
  setConfirmRequest({
    title: `Delete ${item.key}?`,
    body:
      'The variable is removed from the vault immediately. Notebooks and pipelines ' +
      'that call obelisk.var() with this key will fail until it is recreated.',
    confirmLabel: 'Delete',
    destructive: true,
    onConfirm: async () => {
      try {
        await api.deleteVariable(item.id);
        toast.success(`Deleted ${item.key}`);
        await loadData(true);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : 'Failed to delete variable';
        setError(msg);
        toast.error(msg);
      }
    },
  });
};
```

Render once at the end of the component, beside the existing modal:

```tsx
<ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
```

**Acceptance:** editing a secret without typing a value keeps it decryptable and
unchanged; editing a description persists; deleting asks through `ConfirmDialog` and
raises a toast; no `window.confirm` remains in this component.

---

## Workstream 3: Lakehouse Storage — Live Metrics & Maintenance

### Task 3.1: Client types and API methods

**Files:** `frontend/src/lib/types.ts`, `frontend/src/lib/api.ts`

Add to `types.ts`:

```ts
export interface LakehouseDiagnostics {
  storage_uri: string;
  storage_scheme: string;
  lakehouse_root: string;
  catalog_sqlite_bytes: number;
  obelisk_db_bytes: number;
  obelisk_db_wal_bytes: number;
  obelisk_db_journal_mode: string;
  duckdb_tmp_bytes: number;
  duckdb_tmp_limit: string;
  duckdb_memory_limit_per_slot: string;
  duckdb_threads: number | null;
  active_tables: number;
  total_rows: number;
  total_data_bytes: number;
  parquet_files: number;
  parquet_files_capped: boolean;
  snapshot_count: number;
  last_snapshot_at: string;
  docker_available: boolean;
  docker_error: string | null;
  managed_containers: number;
  container_allocated_gb: number;
}

export interface LakehouseMaintenanceRequest {
  keep_days?: number;
  max_compacted_files?: number;
  cleanup_files?: boolean;
  dry_run?: boolean;
}

export interface LakehouseMaintenanceResult {
  status: 'ok' | 'dry-run';
  merged_tables: number;
  expired_snapshots: number;
  cleaned_files: number;
  snapshots_before: number;
  snapshots_after: number;
  keep_days: number;
  max_compacted_files: number;
  cleanup_files: boolean;
  dry_run: boolean;
}
```

Add to `api.ts` next to the other catalog calls (near `moveLakehouseTable`,
`api.ts:141-148`):

```ts
getLakehouseDiagnostics: async (refresh = false): Promise<LakehouseDiagnostics> => {
  const res = await apiClient.get<LakehouseDiagnostics>(
    '/catalog/lakehouse/diagnostics',
    { params: { refresh } },
  );
  return res.data;
},
maintainLakehouse: async (
  payload: LakehouseMaintenanceRequest = {},
): Promise<LakehouseMaintenanceResult> => {
  const res = await apiClient.post<LakehouseMaintenanceResult>(
    '/catalog/lakehouse/maintain',
    payload,
  );
  return res.data;
},
```

Import the three new types in `api.ts`'s type import block (`api.ts:2-45`).

### Task 3.2: Load diagnostics when the tab opens

**File:** `frontend/src/components/settings/SettingsView.tsx`

New state:

```ts
const [diagnostics, setDiagnostics] = useState<LakehouseDiagnostics | null>(null);
const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
const [diagnosticsError, setDiagnosticsError] = useState<string | null>(null);
const [maintenance, setMaintenance] = useState<LakehouseMaintenanceResult | null>(null);
const [maintenanceRunning, setMaintenanceRunning] = useState(false);
const [maintenanceOptions, setMaintenanceOptions] = useState({
  keepDays: 7,
  cleanupFiles: false,
  dryRun: false,
});
```

Loader (fires only when a metrics tab is visible, so Variables/Vault cost nothing):

```ts
const loadDiagnostics = async (refresh = false) => {
  setDiagnosticsLoading(true);
  setDiagnosticsError(null);
  try {
    setDiagnostics(await api.getLakehouseDiagnostics(refresh));
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Failed to read lakehouse diagnostics';
    setDiagnosticsError(msg);
  } finally {
    setDiagnosticsLoading(false);
  }
};

useEffect(() => {
  if (activeSubTab === 'storage' || activeSubTab === 'diagnostics') {
    void loadDiagnostics();
  }
}, [activeSubTab]);
```

The existing **Sync** button (`SettingsView.tsx:298-305`) should also call
`loadDiagnostics(true)` when a metrics tab is active.

**Formatting:** render bytes with `formatBytes` from `../../lib/format` (it returns
`string | null`, so render `—` for a null result) and never compute sizes in the UI from
strings.

### Task 3.3: Replace the static Storage tab (`SettingsView.tsx:651-699`)

Delete the hardcoded cards and the S3 banner. Build the tab from real values:

**Card 1 — Storage root (live):**

- `diagnostics.storage_uri` (no literal `file:///data/lakehouse`).
- `diagnostics.lakehouse_root` ("resolved local path").
- `diagnostics.storage_scheme` badge.
- If `storage_scheme !== 'file'`, render an error callout quoting the startup rule
  (`config.py:203-208`): remote object stores are not implemented and are rejected.

**Card 2 — DuckLake catalog (live):**

- `Active Tables` = `diagnostics.active_tables`
- `Snapshots` = `diagnostics.snapshot_count`
- `Last snapshot` = `new Date(diagnostics.last_snapshot_at).toLocaleString()`
- `Catalog metadata` = `formatBytes(diagnostics.catalog_sqlite_bytes)`
- `Data files` = `diagnostics.parquet_files` (+ `≥` and a "count capped" note when
  `parquet_files_capped` is true)
- `Logical data size` = `formatBytes(diagnostics.total_data_bytes)`
- `Rows` = `diagnostics.total_rows.toLocaleString()`

**Card 3 — Engine budget (live):**

- `Per-slot memory limit` = `diagnostics.duckdb_memory_limit_per_slot`
- `Spill usage` = `formatBytes(diagnostics.duckdb_tmp_bytes)` of
  `diagnostics.duckdb_tmp_limit`
- A progress bar with `Math.min(100, tmpBytes / limitBytes * 100)`; the limit string
  (`"50GB"`) must be parsed by a small `parseSizeToBytes()` helper placed next to
  `formatBytes` in `lib/format.ts` (unit-tested by inspection; no magic in JSX).

**Card 4 — Maintenance panel (interactive):**

Controls: `keep_days` number (default 7), `Dry run` checkbox, `Also delete expired
files` checkbox (default off), and a primary button:

```tsx
<button
  disabled={maintenanceRunning}
  onClick={() => setConfirmRequest({
    title: 'Run lakehouse maintenance?',
    body: maintenanceOptions.cleanupFiles
      ? 'Merges small files, expires snapshots older than the retention window, and ' +
        'deletes the Parquet files those snapshots left behind. The deletion cannot be undone.'
      : 'Merges small files and expires snapshots older than the retention window. ' +
        'No data files are deleted.',
    confirmLabel: maintenanceOptions.dryRun ? 'Run dry run' : 'Run maintenance',
    destructive: maintenanceOptions.cleanupFiles && !maintenanceOptions.dryRun,
    onConfirm: runMaintenance,
  })}
>
  {maintenanceRunning ? 'Running…' : 'Run Maintenance'}
</button>
```

Handler:

```ts
const runMaintenance = async () => {
  setMaintenanceRunning(true);
  setMaintenance(null);
  try {
    const result = await api.maintainLakehouse({
      keep_days: maintenanceOptions.keepDays,
      cleanup_files: maintenanceOptions.cleanupFiles,
      dry_run: maintenanceOptions.dryRun,
    });
    setMaintenance(result);
    toast.success(
      result.dry_run
        ? `Dry run: ${result.merged_tables} table(s) would compact, ` +
          `${result.expired_snapshots} snapshot(s) would expire`
        : `Compacted ${result.merged_tables} table(s); expired ` +
          `${result.expired_snapshots} snapshot(s)` +
          (result.cleanup_files ? `; removed ${result.cleaned_files} file(s)` : ''),
    );
    await loadDiagnostics(true);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Maintenance failed';
    toast.error(msg);
    setDiagnosticsError(msg);
  } finally {
    setMaintenanceRunning(false);
  }
};
```

Render the last result under the button (`snapshots_before → snapshots_after`,
`merged_tables`, `expired_snapshots`, `cleaned_files`) so the operator sees exactly what
happened.

**Copy replacement (delete `SettingsView.tsx:687-696`):**

> **Storage scope (today):** Obelisk writes DuckLake-managed Parquet files to the local
> filesystem only. `STORAGE_URI` accepts a `file://` URI or an absolute path; a remote
> scheme (`s3://`, `gs://`, `abfs://`) is rejected at startup rather than silently written
> to local disk.
>
> **Retention:** snapshots older than the window you choose are expired here. To keep a
> restorable copy, run
> `lakehouse_maintenance.py backup --destination <dir>` — it copies the lakehouse folder
> (data **and** `catalog.sqlite`) and takes a consistent SQLite snapshot of `obelisk.db`.

Do **not** claim the compression codec is selectable; DuckLake writes the files it
writes.

---

## Workstream 4: Diagnostics — Bind the Four Cards to Real Metrics

### Task 4.1: Reuse the existing telemetry stream

**File:** `frontend/src/components/settings/SettingsView.tsx`

`useTelemetry()` (used by Home) already maintains a WebSocket with a 2 s HTTP fallback.
Reuse it rather than adding a second poller:

```ts
import { useTelemetry } from '../../lib/useTelemetry';

// Always called (hooks cannot be conditional), but the tab renders its values only
// while `diagnostics` is visible.
const { telemetry, isLive, error: telemetryError } = useTelemetry();
```

To avoid an idle WebSocket when the user never opens Diagnostics, extract the four cards
into a child component `DiagnosticsPanel` defined in the same file and render it only for
`activeSubTab === 'diagnostics'`; the hook then mounts with that subtree.

### Task 4.2: Card contents (replace `SettingsView.tsx:701-763`)

**Card 1 — Control Plane DB (`obelisk.db`):**

- Size: `formatBytes(diagnostics.obelisk_db_bytes)` + WAL:
  `formatBytes(diagnostics.obelisk_db_wal_bytes)`
- Journal: `diagnostics.obelisk_db_journal_mode` — render the "WAL Mode Active" badge
  **only** when the value is `WAL`; otherwise render the value as a warning badge. This
  replaces the hardcoded `WAL Mode Active` at `SettingsView.tsx:716-718`.
- Copy: "Assets, pipelines, runs and variables. WAL files are counted separately."

**Card 2 — DuckDB Execution Engine:**

- `Per-slot memory limit`: `diagnostics.duckdb_memory_limit_per_slot`
- `Threads`: `diagnostics.duckdb_threads ?? 'auto'`
- `Spill directory`: `formatBytes(diagnostics.duckdb_tmp_bytes)` of
  `diagnostics.duckdb_tmp_limit`
- `Host RAM now`: `telemetry.ram_used_gb / telemetry.total_ram_gb GB`
  (`telemetry` may be null → render `—` and the `telemetryError` text)
- Live indicator: `isLive ? 'WebSocket' : 'Polling (2 s)'`.

**Card 3 — DuckLake Metadata Catalog (`.obelisk/catalog.sqlite`):**

- `Catalog size`: `formatBytes(diagnostics.catalog_sqlite_bytes)`
- `Snapshots`: `diagnostics.snapshot_count`
- `Last snapshot`: localized `last_snapshot_at`
- `Active tables`: `diagnostics.active_tables`
- `Root`: `diagnostics.lakehouse_root`
- Copy: "The catalog travels with the data folder; moving one without the other is
  refused by DuckLake."

**Card 4 — Docker Engine & Vault:**

- `Engine`: `diagnostics.docker_available ? 'Reachable' : 'Unavailable'`, with
  `diagnostics.docker_error` rendered verbatim when unavailable (never "Socket path:
  /var/run/docker.sock" as a static claim).
- `Managed containers`: `diagnostics.managed_containers` running,
  `diagnostics.container_allocated_gb` GB allocated (live from Docker).
- `Host CPU / RAM`: from `telemetry` (`cpu_percent`, `ram_percent_used`, `cpu_count`).
- `Vault`: "Fernet (AES-128-CBC + HMAC-SHA256), key from `OBELISK_MASTER_KEY`" — accurate
  per `backend/app/core/vault.py`. Delete the current text saying AES-128 is the whole
  story without naming Fernet, and never call it AES-256-GCM.

**Shared states:** when `diagnosticsLoading` show the existing spinner block; when
`diagnosticsError` show the error banner with a "Retry" button calling
`loadDiagnostics(true)`; when `diagnostics` is null and not loading, render nothing rather
than zeros.

---

## Workstream 5: SDK & Platform Guide — Truth Pass

**File:** `frontend/src/components/settings/SdkDocsView.tsx` (plus `docs/sdk.md`)

> **Gate:** Task 5.2 assumes the `sync_postgres` removal from
> `obelisk_agent_implementation_plan.md` Task 1.3 has landed. If it has **not** landed, do
> Task 5.1 and 5.3 first, and write Task 5.2's signature as
> `obelisk.save(df, target)` while leaving the existing optional mirror behaviour
> undocumented (it is scheduled for removal, not a feature to advertise). Never document
> a parameter the SDK does not have, and never document one it is about to lose as if it
> were permanent.

### Task 5.1: Replace legacy schema names

| Line | Current | Replace with |
| --- | --- | --- |
| `SdkDocsView.tsx:108` | `obelisk.table("lakehouse.bronze.venues")` | `obelisk.table("lakehouse.raw.orders")` |
| `SdkDocsView.tsx:142` | `FROM lakehouse.bronze.orders o` | `FROM lakehouse.raw.orders o` |
| `SdkDocsView.tsx:153` | `lakehouse.bronze.fact_news_articles` | `lakehouse.marts.daily_revenue` |
| `SdkDocsView.tsx:199` | `spark.read.parquet("data/lakehouse/bronze/events.parquet")` | `spark.read.parquet("data/lakehouse/raw/orders.parquet")` — see note below |
| `SdkDocsView.tsx:205` | `obelisk.save(df_aggregated, "gold.aggregated_events")` | `obelisk.save(df_aggregated, "marts.aggregated_events")` |

Also replace the "3-tier FQN" wording at `SdkDocsView.tsx:107` with the real model: a
folder is a schema (`raw/orders` → `lakehouse.raw.orders`; root tables are
`lakehouse.<name>`), because the repository no longer has fixed bronze/silver/gold tiers.

> **Parquet-path caveat:** DuckLake owns the physical files, so a hand-written
> `read_parquet("data/lakehouse/raw/orders.parquet")` is not guaranteed to exist as a
> single file after compaction. Prefer `obelisk.table(...)` / `obelisk.query(...)` in the
> Spark example, or demonstrate `SPARK_REMOTE_URL` + `obelisk.table` instead of a raw
> path. If a path example is kept, label it "illustrative — prefer the catalog".

### Task 5.2: Rewrite the `save` section (`SdkDocsView.tsx:157-183`)

- Title: `obelisk.save(df, target)`.
- Description: "Writes a Polars, Pandas, Arrow or Spark DataFrame as a DuckLake table.
  `target` is `<folder>/<name>` (nested folders allowed) or a bare `<name>` for the root;
  the address is `lakehouse.<folder>.<name>`. One statement writes it, so a failed save
  leaves the previous table intact."
- Remove every claim about `data/lakehouse/...parquet` file paths and about
  `data/obelisk.db` metadata sync (`SdkDocsView.tsx:163, 175-180`) — `save()` returns the
  address plus `fqn`, `sql_name` and `num_rows`, and DuckLake records the files.
- Snippet:

```python
import obelisk
import polars as pl

# 1. Transform or aggregate your analytical dataset:
df_summary = obelisk.table("lakehouse.raw.orders").to_polars().group_by("region").agg([
    pl.col("amount").sum().alias("total_revenue"),
    pl.col("order_id").count().alias("order_count"),
])

# 2. Persist into a lakehouse folder (folder == schema):
res = obelisk.save(df_summary, "marts.daily_revenue")
print(res["sql_name"])   # lakehouse.marts.daily_revenue
print(res["num_rows"])

# 3. A bare name lands in the catalog's default schema:
res_flat = obelisk.save(df_summary, "quick_export")
```

### Task 5.3: Rewrite Step 05 (`SdkDocsView.tsx:390-404`)

Delete the Dagster daemon line and the `git_repo/dags/` instruction. Document the engine
that exists (`pipeline_engine.py:61-71`):

- Step types: `dbt_run`, `dbt_test`, `script`, `notebook`, `webhook`, `raw_sql`,
  `pipeline`.
- Scheduling: five-part cron expressions (`_create_scheduled_runs`), overlap policies
  (`skip`, `queue`, `kill`), dependency edges with conditions (`success`, `failure`,
  `completion`, `skipped`), watermarks on ingest steps, and live streaming logs.
- Python steps run `git_repo/scripts/*.py`; notebooks are Marimo files; the pipeline
  engine owns scheduling (no external daemon).

Suggested copy:

> **Build & Orchestrate:** compose DAGs from `dbt run/test`, SQL, Python script,
> Marimo notebook, webhook and nested-pipeline steps. Schedule them with standard
> cron, chain them on success/failure/completion/skipped edges, and watch live logs in
> the Pipelines tab. Python work lives in `git_repo/scripts/`; there is no external
> orchestrator daemon.

### Task 5.4: Document `materialize_sql` (conditional)

Only if `obelisk.materialize_sql(sql, target)` exists (other plan, Task 1.3). Add a card
after `save`:

```python
import obelisk

# Materialize a query straight into DuckLake — rows never enter Python.
obelisk.materialize_sql(
    "SELECT region, sum(amount) AS revenue FROM lakehouse.raw.orders GROUP BY 1",
    "marts.revenue_by_region",
)
```

If the function is not in `app/sdk/obelisk.py` yet, **skip this task**; the docs must not
teach a missing API.

### Task 5.5: Keep `docs/sdk.md` in sync

`docs/sdk.md:57-68` still documents `save(df, target, sync_postgres=None)` and the mirror
behaviour. Update it in the same commit as Task 5.2 so the in-app guide and the repository
doc cannot disagree. Also grep the repository for stale examples after the change:

```powershell
Get-ChildItem -Path . -Recurse -Include *.md,*.tsx,*.py |
  Select-String -Pattern 'sync_postgres|lakehouse\.bronze|gold\.aggregated_events|git_repo/dags|Dagster'
```

Every remaining hit must be a legitimate historical document (`docs/architecture.md`,
`docs/pipeline-orchestrator-plan.md`) or removed.

**Acceptance:** no `sync_postgres`, `Dagster`, `git_repo/dags`, `lakehouse.bronze` or
`gold.` example in `SdkDocsView.tsx`; every snippet's API exists in
`backend/app/sdk/obelisk.py`; `docs/sdk.md` matches.

---

---

## Workstream 6: Navigation, Preferences & First-Run Workflow

### Task 6.1: A single preference store for developer tools

**File (new):** `frontend/src/lib/preferences.ts`

```ts
import { useEffect, useState } from 'react';

export interface Preferences {
  /** Show Files and Terminal in the sidebar and command palette. */
  developerTools: boolean;
}

const STORAGE_KEY = 'obelisk_preferences';
const DEFAULTS: Preferences = { developerTools: false };
const listeners = new Set<(prefs: Preferences) => void>();

export function getPreferences(): Preferences {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? { ...DEFAULTS, ...(JSON.parse(raw) as Partial<Preferences>) } : { ...DEFAULTS };
  } catch {
    // Private-mode / disabled storage: the defaults are the documented behaviour.
    return { ...DEFAULTS };
  }
}

export function setPreferences(patch: Partial<Preferences>): Preferences {
  const next = { ...getPreferences(), ...patch };
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    /* storage unavailable: the in-memory listeners still update this session */
  }
  listeners.forEach((listener) => listener(next));
  return next;
}

export function subscribePreferences(listener: (prefs: Preferences) => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function usePreferences(): Preferences {
  const [prefs, setPrefs] = useState<Preferences>(getPreferences);
  useEffect(() => subscribePreferences(setPrefs), []);
  return prefs;
}
```

### Task 6.2: Group the sidebar into three sections

**File:** `frontend/src/components/layout/Sidebar.tsx`

Replace the flat `navItems` (`Sidebar.tsx:36-48`) with:

```ts
const navSections = [
  {
    title: 'Explore & Analyze',
    items: [
      { id: 'home', label: 'Home', icon: Home },
      { id: 'catalog', label: 'Data Catalog', icon: Database },
      { id: 'query', label: 'Query Studio', icon: Code2 },
      { id: 'notebooks', label: 'Notebooks', icon: BookOpen },
      { id: 'dashboards', label: 'Dashboards', icon: BarChart3 },
    ],
  },
  {
    title: 'Build & Orchestrate',
    items: [
      { id: 'dbt', label: 'dbt Studio', icon: Boxes },
      { id: 'pipelines', label: 'Pipelines', icon: GitFork },
    ],
  },
  {
    title: 'Platform',
    items: [
      { id: 'services', label: 'Service Catalog', icon: Server },
      { id: 'settings', label: 'Settings', icon: Settings },
    ],
  },
] as const;

const developerItems = [
  { id: 'files', label: 'Files', icon: FolderCode },
  { id: 'terminal', label: 'Terminal', icon: SquareTerminal },
] as const;
```

Render:

- Each section gets a small uppercase header (`text-[10px] font-semibold uppercase
  tracking-wider text-slate-400 px-3 pt-3 pb-1`), only when the sidebar is expanded (it is
  fixed-width today, so always).
- A fourth "Developer" section renders only when `usePreferences().developerTools` is true.
- Keep the existing button classes and the active style unchanged.

**Behaviour decision:** the `files`/`terminal` routes stay registered in `App.tsx`
(`App.tsx:139-142`) and remain reachable by URL; only their *navigation entries* are
hidden. This avoids a hard 404 for anyone with a bookmark and keeps the change reversible.

### Task 6.3: Preferences tab in Settings

**File:** `frontend/src/components/settings/SettingsView.tsx`

1. Extend the union: `type SettingsSubTab = 'variables' | 'vault' | 'storage' |
   'diagnostics' | 'sdk' | 'preferences';`
2. Add a sixth pill to the sub-tab row (`SettingsView.tsx:240-296`) with a `Sliders`
   (or `Settings2`) icon labelled **Preferences**.
3. Content:

```tsx
{activeSubTab === 'preferences' && (
  <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
    <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
      Interface
    </h3>
    <label className="flex items-start gap-3 p-3 rounded-lg border border-slate-200 bg-slate-50 cursor-pointer">
      <input
        type="checkbox"
        checked={prefs.developerTools}
        onChange={(e) => setPreferences({ developerTools: e.target.checked })}
        className="mt-0.5"
      />
      <span>
        <span className="block text-xs font-semibold text-slate-800">
          Enable Developer Raw Tools (Files &amp; Terminal)
        </span>
        <span className="block text-[11px] text-slate-500 mt-0.5">
          Adds a Developer section to the sidebar. Editing <code>git_repo/scripts/</code>
          {' '}and dbt models remains available through dbt Studio and Pipelines either way.
        </span>
      </span>
    </label>
    <button
      onClick={() => {
        try { localStorage.removeItem('obelisk_quickstart_dismissed'); } catch { /* ignore */ }
        toast.success('The Home quickstart will show again on your next visit.');
      }}
      className="text-[11px] font-semibold text-sky-600 hover:text-sky-700"
    >
      Restore the Home quickstart
    </button>
  </section>
)}
```

4. Import `usePreferences`/`setPreferences` from `../../lib/preferences`.

### Task 6.4: First-run quickstart on Home

**File:** `frontend/src/components/home/HomeView.tsx`

1. Fix the obvious navigation bug: the dbt Models card (`HomeView.tsx:280-298`) must call
   `onNavigate('dbt')`, not `onNavigate('pipelines')` (`HomeView.tsx:281`).
2. Add the quickstart above the grid (`HomeView.tsx:114-118`):

```tsx
const [quickstartDismissed, setQuickstartDismissed] = useState<boolean>(() => {
  try { return localStorage.getItem('obelisk_quickstart_dismissed') === '1'; }
  catch { return false; }
}); // plus an effect that re-reads it on mount, since Settings can clear the key

const isFreshInstall =
  externalConnsCount === 0 &&
  totalPipelineCount === 0 &&
  totalDbtModels === 0 &&
  notebooksCount === 0;

const dismissQuickstart = () => {
  setQuickstartDismissed(true);
  try { localStorage.setItem('obelisk_quickstart_dismissed', '1'); } catch { /* ignore */ }
};
```

Render (only when `isFreshInstall && !quickstartDismissed`):

```tsx
<div className="rounded-2xl border border-sky-200 bg-gradient-to-r from-sky-50 to-indigo-50/60 p-5">
  <div className="flex items-start justify-between gap-4">
    <div>
      <h3 className="text-sm font-bold text-slate-900">Welcome to Obelisk — start here</h3>
      <p className="text-xs text-slate-600 mt-1">
        Three steps take this server from empty to a scheduled, transformed dataset.
      </p>
    </div>
    <button onClick={dismissQuickstart} className="text-[11px] text-slate-500 hover:text-slate-800">
      Dismiss
    </button>
  </div>
  <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
    <button onClick={() => onNavigate('catalog')} className="…card…">
      <span className="font-mono text-[10px] text-sky-700">01</span>
      <span className="block text-xs font-semibold text-slate-800 mt-1">
        Connect a database or inspect the lakehouse
      </span>
      <span className="block text-[11px] text-slate-500 mt-0.5">
        Add PostgreSQL, MySQL, Oracle, MongoDB or a REST source in Data Catalog.
      </span>
    </button>
    <button onClick={() => onNavigate('dbt')} className="…card…">
      <span className="font-mono text-[10px] text-violet-700">02</span>
      <span className="block text-xs font-semibold text-slate-800 mt-1">
        Build a model in dbt Studio
      </span>
      <span className="block text-[11px] text-slate-500 mt-0.5">
        Create a staging model, compile it, and preview its rows.
      </span>
    </button>
    <button onClick={() => onNavigate('pipelines')} className="…card…">
      <span className="font-mono text-[10px] text-emerald-700">03</span>
      <span className="block text-xs font-semibold text-slate-800 mt-1">
        Schedule a pipeline
      </span>
      <span className="block text-[11px] text-slate-500 mt-0.5">
        Chain ingest → dbt → checks on a cron schedule with live logs.
      </span>
    </button>
  </div>
</div>
```

3. **Optional sample data CTA.** A "Load sample data" button in step 01 can call the
   existing backend endpoint `POST /api/dbt/sample-data` (`routes_dbt.py:94`), but the
   frontend client has no wrapper today — add
   `loadDbtSampleData: async () => (await apiClient.post('/dbt/sample-data')).data` and
   confirm before writing. It is opt-in only; never seed data silently.

**Acceptance:** a fresh install (no sources, models, pipelines, notebooks) shows the
banner; dismissing it persists; after any one of those exists the banner never renders;
the dbt card opens dbt Studio.

---

## Workstream 7: One Connection Model

### Task 7.1: Shared connection state and actions

**Files (new):** `frontend/src/lib/useConnections.ts`;
**modified:** `SettingsView.tsx`, `CatalogView.tsx`

Both screens already share the **form** (`AddConnectionModal`) but duplicate the list,
test and delete logic (`SettingsView.tsx:65-87, 195-223`; `CatalogView.tsx:100-115`).
Extract the state/actions so the two surfaces cannot drift:

```ts
export function useConnections() {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      setConnections(await api.listConnections());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load connections');
    } finally {
      setLoading(false);
    }
  }, []);

  const remove = useCallback(async (id: string) => {
    await api.deleteConnection(id);
    await reload();
  }, [reload]);

  const test = useCallback((id: string) => api.testConnection(id), []);

  useEffect(() => { void reload(); }, [reload]);
  return { connections, loading, error, reload, remove, test };
}
```

- `SettingsView`: delete its local connection state (`SettingsView.tsx:54-59`) and use the
  hook; keep the table UI and the `AddConnectionModal` wiring.
- `CatalogView`: use the same hook for `conns`, `handleDeleteConnection` and the test
  action; keep the tree.

### Task 7.2: Move "Publish marts into" next to the dbt target selector

**Files:** `frontend/src/components/dbt/DbtStudio.tsx`, `SettingsView.tsx`

The selection is consumed by `dbt run` and `obelisk.save()` publishing
(`routes_dbt.py:180-213`; `warehouse_resolver.py:310-334`), and dbt Studio is where a
target is already chosen (`DbtStudio.tsx:70-82, 139-150`). Put the control where the
decision is made:

1. In dbt Studio's target bar, add a compact select bound to
   `api.getDbtWarehouse()` / `api.setDbtWarehouse(id)`:
   - Label: "Default warehouse".
   - Options: Postgres connections from `api.getDbtTargets()` (or `listConnections()`
     filtered to `postgres`/`postgresql`), disabled when `read_only`.
   - On change: `await api.setDbtWarehouse(id)`; toast success/error; show
     `warehouse.message` when present.
   - This is the **default** for builds that do not bind a connection explicitly; the
     per-run target selector keeps working as it does today.
2. In Settings → Connection Vault, replace the interactive block
   (`SettingsView.tsx:484-512`) with a read-only summary:

```tsx
<div className="flex flex-wrap items-center gap-3 p-3 bg-slate-50 border border-slate-200 rounded-lg">
  <Database className="w-3.5 h-3.5 text-slate-400" />
  <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-600">
    Default mart warehouse
  </span>
  <span className="text-[11px] font-mono text-slate-800">
    {warehouse?.name ?? 'Not selected'}
  </span>
  <button onClick={() => navigate('/dbt')} className="text-[11px] font-semibold text-sky-600 hover:text-sky-700">
    Change in dbt Studio
  </button>
</div>
```

   Keep loading `getDbtWarehouse()` here for the summary; remove `chooseWarehouse` and the
   `warehouseSaving` state. `useNavigate` is already used elsewhere in the app
   (`CatalogView.tsx:1-2`), so add it to `SettingsView`.

### Task 7.3: Cross-link instead of duplicating

- Settings → Connection Vault header gets a secondary link
  **"Open in Data Catalog"** → `navigate('/catalog', { state: { openConnections: true } })`.
- `CatalogView` reads that state once and opens the existing add-connection modal or
  focuses the connections node (implementation detail: `useLocation()` + a one-shot
  effect that clears the state with `navigate(location.pathname, { replace: true,
  state: null })`).
- Add a one-line note in Settings: "Data Catalog is the primary place to browse sources;
  this table is the credential vault view of the same records." This removes the
  "two managers" confusion without deleting either surface.

**Acceptance:** changing the default warehouse is possible in dbt Studio and reflected in
Settings without reloading the page; Settings cannot silently write a warehouse; both
screens list the same connection records from one hook.

---

## Workstream 8: Command Palette — Extend, Don't Rebuild

> The audit's "add a Cmd+K palette" item is **already implemented** (`App.tsx:73,
> 96-109, 192`; `CommandMenu.tsx`). The real gaps are coverage and deep links.

### Task 8.1: Index pipelines and saved queries

**File:** `frontend/src/components/layout/CommandMenu.tsx`

1. Extend the data model:

```ts
type CommandItemKind =
  | 'action' | 'table' | 'connection' | 'notebook' | 'model' | 'pipeline' | 'query';

interface PaletteData {
  nodes: CatalogNode[];
  connections: Connection[];
  notebooks: Notebook[];
  dbtFiles: DbtTreeNode[];
  pipelines: PipelineSummary[];
  queries: QueryHistoryItem[];
}
```

2. Fetch both in the existing one-shot `Promise.allSettled`
   (`CommandMenu.tsx:216-230`) with `api.listPipelines()` and `api.getQueryHistory()`
   (both exist: `api.ts:209`, `api.ts:261`).
3. Build items:

```ts
for (const p of data.pipelines) {
  items.push({
    id: `pipeline:${p.id}`,
    kind: 'pipeline',
    label: p.name,
    subtitle: `${p.is_enabled ? 'enabled' : 'disabled'} • ${p.step_count ?? 0} steps`,
    keywords: `pipeline schedule ${p.name}`,
    icon: GitFork,
    run: () => navigate('/pipelines'),
  });
}

for (const q of data.queries) {
  items.push({
    id: `query:${q.id}`,
    kind: 'query',
    label: q.title || 'Untitled query',
    subtitle: `${q.status} • ${q.row_count ?? 0} rows`,
    keywords: `query history sql ${q.sql_text?.slice(0, 120) ?? ''}`,
    icon: Terminal,
    run: () => navigate('/query', { state: { sql: q.sql_text } }),
  });
}
```

   `PipelineSummary` already declares `step_count`, `is_enabled`, `last_status` and
   `recent_statuses` (`types.ts:656-679`); use those fields, do not invent one.
4. Add headers (`KIND_HEADERS`) and ranking weights for `pipeline` and `query`, and reset
   the palette cache when a pipeline is created/deleted only if it is already cheap to do
   so — otherwise the next open picks it up because `data` is fetched once per mount
   (`CommandMenu.tsx:213-230`). Simplest correct option: keep the one-shot cache and note
   that a newly created pipeline appears after the palette component remounts; do not add
   a polling loop.

### Task 8.2: Deep links instead of tab-only navigation

**Files:** `CommandMenu.tsx`, `App.tsx`, `CatalogView.tsx`, `DbtStudio.tsx`,
`NotebooksView.tsx`

Today the palette's non-table items only switch tabs (`CommandMenu.tsx:132, 144, 156`).
Make them land on the object:

1. `App.tsx` `RouteState` (`App.tsx:49-52`) gains the new optional fields:

```ts
interface RouteState {
  sql?: string;
  target?: NotebookTarget;
  connectionId?: string;
  dbtPath?: string;
  notebook?: string;
  openConnections?: boolean;
}
```

2. Pass them through `renderView`:

```tsx
case 'catalog':
  return <CatalogView focusConnectionId={routeState.connectionId}
                      openConnections={routeState.openConnections} />;
case 'dbt':
  return <DbtStudio initialPath={routeState.dbtPath} />;
case 'notebooks':
  return (
    <NotebooksView
      target={routeState.target ?? null}
      initialNotebook={routeState.notebook ?? null}
      onClearTarget={() => navigate('/notebooks', { replace: true })}
    />
  );
```

3. `CatalogView`: a one-shot effect selects the connection (expand its tree node and
   highlight it) from `focusConnectionId`, or opens the add-connection modal when
   `openConnections` is true. Clear the state after applying it. Declare
   `interface CatalogViewProps { focusConnectionId?: string; openConnections?: boolean }`
   — the component is prop-less today, and both fields are optional so existing call sites
   keep compiling.
4. `DbtStudio`: an effect that calls the same open-file handler the explorer uses for a
   tree click (the one that updates `openTabs` / `activeTabPath`) with `initialPath`.
   Guard against re-opening on every render by tracking the last applied path; declare
   `interface DbtStudioProps { initialPath?: string }` with an optional prop.
5. `NotebooksView`: new optional prop `initialNotebook?: string | null`; a
   `useEffect(() => { if (initialNotebook) setActiveNotebook(initialNotebook); },
   [initialNotebook])`. This is the only new capability — the view currently only opens a
   notebook from its own card click (`NotebooksView.tsx:325`).
6. `CommandMenu` items use the new state:

```ts
run: () => navigate('/catalog', { state: { connectionId: conn.id } })
run: () => navigate('/dbt', { state: { dbtPath: file.path } })
run: () => navigate('/notebooks', { state: { notebook: nb.name } })
```

### Task 8.3: Respect the developer-tools preference

In `CommandMenu`, build `NAV_ACTIONS` filtered by `usePreferences().developerTools` so the
palette and the sidebar agree: Files and Terminal appear only when the Developer section
is enabled. Keep every other action.

**Acceptance:** typing a pipeline name finds it; typing a saved query's text finds the
query and opens Query Studio with that SQL; a connection opens Catalog on that connection;
a dbt model opens dbt Studio with the file loaded; a notebook opens that notebook; Files
and Terminal are absent from the palette until enabled in Settings → Preferences.

---

## Verification & Acceptance Gates

Run after each workstream, and again at the end:

1. **Backend tests** (Windows venv path — the repo's own scripts use it):

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest -q
```

Focus: `tests/test_async_routes_do_not_block.py`, `tests/test_variables.py`,
`tests/test_api_services.py`, `tests/test_catalog_scan.py`,
`tests/test_catalog_projection.py`, `tests/test_dbt_studio.py`, plus the new
`tests/test_settings_diagnostics.py`.

2. **Async route non-blocking gate:**

```powershell
cd backend
.\.venv\Scripts\python.exe scripts\find_blocking_calls.py
```

Must exit 0 — the new endpoints use `asyncio.to_thread` only.

3. **Frontend type-check and bundle:**

```powershell
cd frontend
npm run build
```

`tsc && vite build` (package.json:8) must exit 0. This is the gate that catches missing
type fields (`UpdateVariableRequest`, `LakehouseDiagnostics`, `RouteState`,
`initialNotebook`).

4. **Manual acceptance checklist:**

- Variables: edit a plaintext value; edit a secret's description only; toggle a secret
  with a new value; delete through the dialog. Reveal before/after to confirm ciphertext
  survived.
- Storage: sizes match `du`/Explorer; Sync refreshes; dry run changes no snapshot count;
  a real run reports deltas; `cleanup_files` requires the destructive dialog.
- Diagnostics: stop Docker and confirm the card shows the real error; the WAL badge
  matches `PRAGMA journal_mode`.
- Sidebar/Preferences: toggling developer tools updates the sidebar and palette without a
  reload; direct `/files` still works.
- Home: fresh install shows the banner; one source/model/pipeline hides it; dismissal
  persists; the dbt card opens dbt Studio.
- Palette: jump to pipeline, saved query, model, notebook, connection.
- SDK guide: every snippet runs against the current SDK (copy into a scratch notebook).

---

## Risks, Edge Cases & Open Questions

1. **Maintenance on a live catalog.** `ducklake_expire_snapshots` /
   `ducklake_cleanup_old_files` are normally run offline by the script. Through the API a
   long-running query could be reading a snapshot that expiry wants to remove. Mitigations
   in this plan: `cleanup_files` defaults to **off**, the destructive path is confirmed,
   and a DuckDB error is surfaced verbatim. If the team wants a hard guarantee, add a
   "no active heavy jobs" guard before enabling the endpoint — there is no job registry
   today, so do not pretend there is one.
2. **Extension signature drift.** The CALLs mirror
   `backend/scripts/lakehouse_maintenance.py` exactly; if the installed DuckLake build
   rejects a named parameter, the error text will say so. Do not "fix" it by swallowing
   the error or falling back to a no-op.
3. **Diagnostics cost on a huge lake.** The Parquet count walks the filesystem and stops at
   200 000 files (`parquet_files_capped`). `_dir_bytes` on `duckdb_tmp` is bounded by the
   spill cap. The 2 s cache protects the tab; it does **not** protect a 500 GB lake from a
   deliberate refresh storm. If that becomes real, replace the walk with a metadata query
   once a proven one exists.
4. **`sqlite_journal_mode` under WAL.** Opening the database read-only is safe while the
   app writes; if the OS refuses the read-only connection, the field returns
   `error: ...` and the UI shows it rather than claiming WAL.
5. **Hiding Files/Terminal** is a discoverability change. The route stays reachable and
   the toggle is one click; document it in the Preferences helper text and in the release
   notes.
6. **Quickstart visibility** depends on four counts that all start at zero. If the shipped
   demo pipelines are seeded on first boot, the banner will never show — acceptable, but
   confirm the seed behaviour in this build (`backend/scripts/create_demo_pipelines.py`
   exists; whether it runs automatically is a deploy choice).
7. **Warehouse selector move.** Two writers to one setting would be worse than the current
   duplication, so Settings becomes read-only in the same change. `test_dbt_studio.py` and
   `test_warehouse_binding.py` exercise `PUT /api/dbt/warehouse` directly and are
   unaffected.
8. **Palette data freshness.** `CommandMenu` fetches its index once per mount; a pipeline
   created later appears after remount (tab switch/reload). Do not add polling; if
   freshness matters, invalidate the cached `data` when the palette opens *after* an
   action that changes the inventory, as a deliberate follow-up.
9. **`formatBytes` / `parseSizeToBytes`.** `formatBytes` exists in `lib/format.ts`. Add
   `parseSizeToBytes("50GB")` there with tests-by-inspection for `KB/MB/GB/TB` so the
   spill bar cannot divide by `NaN`.
10. **No frontend test runner exists** (package.json has no test script). The acceptance
    gates are `tsc` + the manual checklist; do not invent a test framework in this plan.

## Dependencies on the Other Plan

| Dependency | Needed by | If the other plan has not landed |
| --- | --- | --- |
| `obelisk.materialize_sql` | WS-5 Task 5.4 | Skip the docs card; do not document a missing API |
| `save()` without `sync_postgres` | WS-5 Task 5.2 | Still document `save(df, target)`; do not advertise the doomed parameter |
| `DUCKDB_MAX_CONCURRENT_HEAVY` / `DUCKDB_MAX_QUERY_ROWS` | WS-1 payload, WS-4 card | Omit those rows; `advisor.get_duckdb_memory_limit()` works today |
| DuckLake diagnostics/maintain endpoints (other plan Task 3.2.5) | WS-1 | Implement WS-1 here (it is self-contained); if both land, keep **one** copy — prefer the other plan's endpoints and delete the duplicate route + tests here |
| Telemetry caching (`main.py`) | WS-4 | Not required — `useTelemetry()` already polls/streams |

**Ordering recommendation:** land the other plan's Task 1.3 (SDK) and Task 3.2.5
(endpoints) first if both plans are being executed. Then this plan is purely additive.

## Corrections to the Source Audit (apply before coding)

These points in the supplied critique are stale or inaccurate; the plan above already
reflects the corrected reality:

1. **The command palette already exists.** `App.tsx` wires Cmd/Ctrl+K to
   `<CommandMenu>` (`App.tsx:5, 73, 96-109, 192`), and `CommandMenu.tsx:95-230` already
   indexes tables, connections, notebooks and dbt models. The work is **coverage + deep
   links** (pipelines, saved queries, opening the target object), not "add a palette."
2. **Connection CRUD is not duplicated wholesale.** Both screens already import the same
   `AddConnectionModal` (`SettingsView.tsx:30`; `CatalogView.tsx:11`). What is duplicated
   is list/test/delete state and actions — hence the shared `useConnections()` hook and
   the cross-link, not a second form.
3. **`POST /api/variables` already upserts by key**, and `UpdateVariableRequest` already
   exists but is unused (`routes_variables.py:13-107`). The edit feature therefore needs a
   proper `PUT` (partial semantics) rather than "re-type the key and rely on upsert."
4. **The vault is Fernet**, i.e. AES-128-CBC + HMAC-SHA256 (`backend/app/core/vault.py`),
   not AES-256-GCM. The UI copy must say Fernet/AES-128; do not repeat the critique's
   algorithm claim.

Two additional accuracy notes for the SDK-guide task:

5. `obelisk.save()` no longer writes `data/lakehouse/<folder>/<name>.parquet` directly —
   DuckLake owns the files — and it does not sync metadata into `data/obelisk.db`.
   `SdkDocsView.tsx:163, 175-180` are wrong on both counts.
6. `sync_postgres` is still present in the SDK **today**; the critique calls it "removed."
   It is scheduled for removal by the other plan. Until that lands, treat it as internal
   detail, not documented API.

## Definition of Done

- Every Settings tab either shows live data or is removed; zero hardcoded paths, sizes,
  codecs, algorithms, or feature promises remain in `SettingsView.tsx`.
- The storage tab can run maintenance and reports exactly what changed.
- Variables can be edited in place; secrets are never displayed or overwritten by accident.
- `SdkDocsView.tsx` contains no Dagster, no `sync_postgres`, no `lakehouse.bronze`/`gold.`
  examples, and no non-existent API.
- The sidebar has three workflow sections (plus an opt-in Developer section), Home teaches
  the first three steps, and the palette can reach pipelines, queries, models, notebooks
  and connections.
- `pytest -q`, `find_blocking_calls.py`, and `npm run build` all pass.
