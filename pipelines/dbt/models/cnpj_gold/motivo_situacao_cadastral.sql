{{ config(
    materialized='external',
    location=gold_location('motivo_situacao_cadastral'),
    options={'compression': 'zstd'}
) }}

select
    codigo,
    descricao
from {{ source('cnpj_silver', 'motivos') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('codigo', ['codigo', 'descricao']) }}
