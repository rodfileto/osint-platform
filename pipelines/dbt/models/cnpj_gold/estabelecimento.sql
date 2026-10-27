{{ config(
    materialized='external',
    location=gold_location('estabelecimento'),
    options={'compression': 'zstd'}
) }}

{% set columns = [
    'cnpj_basico',
    'cnpj_ordem',
    'cnpj_dv',
    'identificador_matriz_filial',
    'nome_fantasia',
    'situacao_cadastral',
    'data_situacao_cadastral',
    'motivo_situacao_cadastral',
    'nome_cidade_exterior',
    'codigo_pais',
    'data_inicio_atividade',
    'cnae_fiscal_principal',
    'cnae_fiscal_secundaria',
    'tipo_logradouro',
    'logradouro',
    'numero',
    'complemento',
    'bairro',
    'cep',
    'uf',
    'codigo_municipio',
    'ddd_1',
    'telefone_1',
    'ddd_2',
    'telefone_2',
    'ddd_fax',
    'fax',
    'correio_eletronico',
    'situacao_especial',
    'data_situacao_especial'
] %}

select
    {{ columns | join(',\n    ') }},
    reference_month
from {{ source('cnpj_silver', 'estabelecimentos') }}
where reference_month = {{ reference_month() }}
{{ keep_most_complete('cnpj_basico, cnpj_ordem, cnpj_dv', columns) }}
