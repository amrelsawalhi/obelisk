# The `obelisk` Python SDK

Every notebook, pipeline script and Python step talks to the platform through one module.
No connection strings, no credentials in your code.

```python
import obelisk
```

Inside a Marimo notebook the module resolves through the shim at
`git_repo/notebooks/obelisk.py`; pipeline steps get the same shim on `PYTHONPATH`.

## Addressing a table

A table lives at a path in the lakehouse, and the path is the address:

| Form | Example | Notes |
| :--- | :--- | :--- |
| Canonical | `lakehouse.raw.orders` | always works; what the catalog shows |
| Folder shorthand | `raw.orders` | what pipeline SQL and the docs usually write |
| Bare name | `orders` | only while that name is unique in the warehouse |
| External source | `pg_enterprise_warehouse.public.customers` | a registered connection alias |

## `table(identifier) -> TableWrapper`

A handle, not a copy: nothing is read until you ask for a representation.

```python
tbl = obelisk.table("lakehouse.raw.orders")

tbl.exists()            # is there a file behind this address?
lazy = tbl.to_lazy_polars()   # Polars LazyFrame: nothing is read until .collect()
tbl.to_polars()         # Polars DataFrame   (eager)
tbl.to_arrow()          # PyArrow Table      (eager)
tbl.to_df()             # pandas DataFrame   (eager)
tbl.to_duckdb()         # DuckDB relation (optionally pass a connection)
tbl.to_spark()          # Spark DataFrame via Arrow, when a JVM is available
```

The eager three load the whole table into memory, so they are guarded: a warning at 2 GB of
Parquet, and a refusal above `max(8 GB, 25% of RAM)`. Both thresholds are settings
(`OBELISK_SDK_EAGER_WARN_BYTES`, `OBELISK_SDK_EAGER_REFUSE_BYTES`) and the refusal names
`to_lazy_polars()` as the way out. Use the lazy form for anything that might not fit:

```python
top = (
    obelisk.table("lakehouse.marts.events")
    .to_lazy_polars()
    .filter(pl.col("region") == "emea")
    .group_by("day")
    .agg(pl.len().alias("events"))
    .collect()          # only this much is ever in memory
)
```

## `query(sql) -> polars.DataFrame` / `lazy_query(sql) -> polars.LazyFrame` / `query_arrow(sql)`

Runs SQL on the in-process DuckDB. Table names resolve the same way they do in the
editor, and **Postgres sources referenced in the SQL are attached automatically** --
that is what makes a federated join a single statement:

```python
df = obelisk.query("""
    SELECT c.region, count(*) AS events
    FROM pg_enterprise_warehouse.public.customers c
    JOIN raw.clickstream e ON e.customer_id = c.customer_id
    GROUP BY 1
""")
```

Use `query_arrow` when the next hop is Arrow-native (Spark, Polars, another engine) and
you would rather not round-trip through a DataFrame.

## `save(df, target) -> dict`

Writes the DataFrame into a DuckLake table at `lakehouse.<folder>.<name>`. DuckLake owns
the files: the table is created through its catalog, and nothing is mirrored into
PostgreSQL.

```python
result = obelisk.save(df, "marts.monthly_revenue")
# {'num_rows': 120, 'file_path': 'lakehouse.marts.monthly_revenue', ...}
```

Accepts Polars, PyArrow, pandas or Spark DataFrames.

## `materialize_sql(sql, target) -> dict`

Materialises a query straight into a DuckLake table. DuckDB reads the sources the SQL
names and writes the result in one statement, so a multi-GB result never becomes a Python
or Arrow object:

```python
obelisk.materialize_sql(
    "SELECT region, count(*) AS events FROM raw.clickstream GROUP BY 1",
    "marts.events_by_region",
)
```

## `var(key, default=None)`

Reads a variable from the vault, decrypting in memory at call time:

```python
api_key = obelisk.var("OPENAI_API_KEY")
env = obelisk.var("ENV", default="production")
```

## `spark(...)` / `is_java_compatible()`

`obelisk.spark()` returns a local Spark session sized for the host, and
`is_java_compatible()` reports whether a JVM 17+ is usable. Data moves through Arrow, so
no Hadoop `winutils.exe` is involved on Windows.

## From a pipeline step

A script or notebook step receives its pipeline parameters as environment variables --
`OBELISK_PARAM_ROWS`, `OBELISK_PARAM_TABLE` -- and can write declared outputs to the file
named by `OBELISK_OUTPUT_FILE`. `raw_sql` steps instead use `{{param}}` placeholders and
`{{steps.<key>.<output>}}` bindings; the engine handles both forms before execution.