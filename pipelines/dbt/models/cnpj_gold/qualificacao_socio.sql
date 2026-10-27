{{ config(
    materialized='external',
    location=gold_location('qualificacao_socio'),
    options={'compression': 'zstd'}
) }}

select
    codigo,
    descricao
from {{ source('cnpj_silver', 'qualificacoes') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('codigo', ['codigo', 'descricao']) }}
