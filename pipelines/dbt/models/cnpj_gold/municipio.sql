{{ config(
    materialized='external',
    location=gold_location('municipio'),
    options={'compression': 'zstd'}
) }}

select
    codigo,
    nome
from {{ source('cnpj_silver', 'municipios') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('codigo', ['codigo', 'nome']) }}
