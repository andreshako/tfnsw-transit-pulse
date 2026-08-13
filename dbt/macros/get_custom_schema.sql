{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Use the custom schema name (staging / marts / seed) verbatim instead
        of dbt's default `<target_schema>_<custom_schema>` concatenation, so
        each layer gets its own clearly-named BigQuery dataset in dev/prod.

        In CI, collapse every layer into the single `ci` dataset from the
        `ci` target instead: it's a throwaway fixture run, and one dataset
        is simpler to provision/teardown than replicating the dev/prod
        multi-dataset layout for a dataset that only ever holds test rows.
    -#}
    {%- if target.name == 'ci' or custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
