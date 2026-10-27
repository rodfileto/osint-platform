{{ config(
    materialized='external',
    location=gold_location('empresa'),
    options={'compression': 'zstd'}
) }}

{% set columns = [
    'cnpj_basico',
    'razao_social',
    'natureza_juridica',
    'qualificacao_responsavel',
    'capital_social',
    'porte_empresa',
    'ente_federativo_responsavel'
] %}

select
    {{ columns | join(',\n    ') }},
    reference_month
from {{ source('cnpj_silver', 'empresas') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('cnpj_basico', columns) }}
