{{ config(
    materialized='external',
    location=gold_location('cnae'),
    options={'compression': 'zstd'}
) }}

select
    cnae_fiscal,
    descricao
from {{ source('cnpj_silver', 'cnaes') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('cnae_fiscal', ['cnae_fiscal', 'descricao']) }}
