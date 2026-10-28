{{ config(
    materialized='external',
    location=gold_location('company_search'),
    options={'compression': 'zstd'}
) }}

-- Replaces mv_company_search and mv_company_search_inactive with one table:
-- every matrix establishment (active or not) plus the lookup labels the API
-- joins today, with an is_active flag.

select
    emp.cnpj_basico,
    emp.razao_social,
    est.nome_fantasia,
    est.cnpj_ordem,
    est.cnpj_dv,
    emp.cnpj_basico || est.cnpj_ordem || est.cnpj_dv as cnpj_14,
    est.situacao_cadastral,
    est.codigo_municipio,
    m.nome as municipio_nome,
    est.uf,
    est.cnae_fiscal_principal,
    cnae.descricao as cnae_descricao,
    emp.porte_empresa,
    emp.natureza_juridica,
    nj.descricao as natureza_juridica_descricao,
    emp.capital_social,
    est.data_inicio_atividade,
    est.correio_eletronico,
    emp.reference_month,
    (est.situacao_cadastral is not distinct from '02') as is_active
from {{ ref('empresa') }} emp
join {{ ref('estabelecimento') }} est
    on est.cnpj_basico = emp.cnpj_basico
    and est.identificador_matriz_filial = 1
left join {{ ref('municipio') }} m
    on m.codigo = est.codigo_municipio
left join {{ ref('cnae') }} cnae
    on cnae.cnae_fiscal = est.cnae_fiscal_principal
left join {{ ref('natureza_juridica') }} nj
    on nj.codigo = emp.natureza_juridica
-- Physical order for the serving table (the publish copies rows in this order): each
-- is_active half stays together, as in the two views this replaced, and rows sit in name
-- order, so the API's name-order index walks and name searches read few heap pages.
order by is_active desc, razao_social, cnpj_14
