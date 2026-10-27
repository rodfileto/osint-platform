{{ config(
    materialized='external',
    location=gold_location('pais'),
    options={'compression': 'zstd'}
) }}

select
    codigo,
    nome
from {{ source('cnpj_silver', 'paises') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('codigo', ['codigo', 'nome']) }}
