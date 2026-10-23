{#
  Turns a layer path into the warehouse schema it resolves to.

  Schemas cannot nest, so ``marts.sales`` has to become ``marts__sales``. Obelisk
  resolves every layer and passes the mapping in as ``layer_schemas``; anything not
  in the map is used as written. That keeps ``dbt run`` working on its own, and makes
  a mistyped layer visible in the schema list instead of silently landing elsewhere.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- set mapping = var('layer_schemas', {}) -%}
        {%- set mapped = mapping.get(custom_schema_name) if mapping is mapping else none -%}
        {{ mapped if mapped else custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

