{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Use the custom schema name (staging / marts / seed) verbatim instead
        of dbt's default `<target_schema>_<custom_schema>` concatenation, so
        each layer gets its own clearly-named BigQuery dataset.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
