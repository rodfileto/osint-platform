{{ config(
    materialized='external',
    location=gold_location('socio'),
    options={'compression': 'zstd'}
) }}

{% set columns = [
    'cnpj_basico',
    'identificador_socio',
    'nome_socio_razao_social',
    'cpf_cnpj_socio',
    'qualificacao_socio',
    'data_entrada_sociedade',
    'pais',
    'representante_legal',
    'nome_do_representante',
    'qualificacao_representante_legal',
    'faixa_etaria'
] %}

select
    {{ columns | join(',\n    ') }},
    reference_month
from {{ source('cnpj_silver', 'socios') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('cnpj_basico, identificador_socio, cpf_cnpj_socio, nome_socio_razao_social, qualificacao_socio', columns) }}
