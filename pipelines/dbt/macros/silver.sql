{% macro storage_root() -%}
{{- env_var('DBT_STORAGE_ROOT', 's3://osint-processed') -}}
{%- endmacro %}

{% macro reference_month() -%}
{%- set rm = var('reference_month', 'auto') -%}
{%- if rm == 'auto' -%}
(
    SELECT max(reference_month)
    FROM read_parquet('{{ storage_root() }}/cnpj/silver/socios/reference_month=*/socios_*.parquet', hive_partitioning=true)
)
{%- else -%}
'{{ rm }}'
{%- endif -%}
{%- endmacro %}

{% macro gold_location(table) -%}
{{- storage_root() ~ '/cnpj/gold/' ~ table ~ '/' ~ table ~ '.parquet' -}}
{%- endmacro %}

{#- The Receita occasionally publishes a key twice (e.g. a real company plus an empty
    placeholder row); keep the most complete row per key. Goes after the WHERE clause. -#}
{% macro keep_most_complete(key, columns) -%}
qualify row_number() over (
    partition by {{ key }}
    order by {% for column in columns %}({{ column }} is not null)::int{% if not loop.last %} + {% endif %}{% endfor %} desc
) = 1
{%- endmacro %}
