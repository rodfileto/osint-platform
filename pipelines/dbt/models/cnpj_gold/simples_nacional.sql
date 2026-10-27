{{ config(
    materialized='external',
    location=gold_location('simples_nacional'),
    options={'compression': 'zstd'}
) }}

{% set columns = [
    'cnpj_basico',
    'optante_simples_nacional',
    'data_optante_simples_nacional',
    'data_exclusao_simples_nacional',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei'
] %}

select
    {{ columns | join(',\n    ') }},
    reference_month
from {{ source('cnpj_silver', 'simples') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('cnpj_basico', columns) }}
