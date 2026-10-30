{% macro configure_duckdb_temp() %}
  {# dbt-duckdb re-applies profile settings on every cursor, and DuckDB rejects a
     second SET temp_directory after the first one has been used. Set it once here,
     before any model runs, using the per-run directory supplied by Obelisk. #}
  {% if target.type == 'duckdb' %}
    {% set temp_dir = env_var('DBT_DUCKDB_TEMP_DIR', 'dbt_spill') %}
    {{ return("SET temp_directory = '" ~ (temp_dir | replace("'", "''")) ~ "'") }}
  {% endif %}
  {{ return('SELECT 1') }}
{% endmacro %}
