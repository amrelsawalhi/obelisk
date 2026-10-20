import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import obelisk

    return mo, obelisk


@app.cell
def _(mo):
    mo.md(r"""
    # 🚀 Obelisk Lakehouse & SDK Quickstart
    Interactive exploratory workspace querying Obelisk Parquet Lakehouse and external databases.
    """)
    return


@app.cell
def _(obelisk):
    # Query DuckDB in-process engine
    df = obelisk.query("SELECT 'Obelisk Control Plane' AS platform, 1 AS live, CURRENT_DATE AS run_date")
    df
    return (df,)


@app.cell
def _(df, mo):
    mo.md(f"""
    Status check: Loaded **{len(df)}** rows with Obelisk SDK.
    """)
    return


if __name__ == "__main__":
    app.run()
