from app.engines.pipeline_engine import render_sql_params


def test_render_sql_params_basic():
    # Direct name with whitespace variations
    assert render_sql_params("SELECT * FROM t LIMIT {{LIMIT}};", {"LIMIT": 10}) == "SELECT * FROM t LIMIT 10;"
    assert render_sql_params("SELECT * FROM t LIMIT {{ LIMIT }};", {"LIMIT": 10}) == "SELECT * FROM t LIMIT 10;"

    # params. prefix with whitespace variations
    assert render_sql_params("SELECT * FROM t LIMIT {{params.LIMIT}};", {"LIMIT": 10}) == "SELECT * FROM t LIMIT 10;"
    assert render_sql_params("SELECT * FROM t LIMIT {{ params.LIMIT }};", {"LIMIT": 10}) == "SELECT * FROM t LIMIT 10;"


def test_render_sql_params_types():
    sql = "SELECT * FROM t WHERE status = {{ params.status }} AND active = {{ is_active }} AND price > {{ price }} AND note IS {{ note }};"
    params = {"status": "PAID", "is_active": True, "price": 19.99, "note": None}
    rendered = render_sql_params(sql, params)
    assert rendered == "SELECT * FROM t WHERE status = 'PAID' AND active = TRUE AND price > 19.99 AND note IS NULL;"


def test_render_sql_params_case_insensitive():
    assert render_sql_params("SELECT * FROM t LIMIT {{ params.limit }};", {"LIMIT": 50}) == "SELECT * FROM t LIMIT 50;"
    assert render_sql_params("SELECT * FROM t LIMIT {{ LIMIT }};", {"limit": 50}) == "SELECT * FROM t LIMIT 50;"

