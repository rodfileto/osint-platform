{{ config(
    materialized='external',
    location=gold_location('natureza_juridica'),
    options={'compression': 'zstd'}
) }}

select
    codigo,
    descricao
from {{ source('cnpj_silver', 'naturezas') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('codigo', ['codigo', 'descricao']) }}
