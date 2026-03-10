-- MATERIALIZED VIEW FOR SEARCH (INACTIVE COMPANIES)
-- =====================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cnpj.mv_company_search_inactive AS
SELECT
    emp.cnpj_basico,
    emp.razao_social,
    est.nome_fantasia,
    est.cnpj_ordem,
    est.cnpj_dv,
    (emp.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj_14,
    est.situacao_cadastral,
    est.codigo_municipio,
    m.nome                      AS municipio_nome,
    est.uf,
    est.cnae_fiscal_principal,
    c.descricao                 AS cnae_descricao,
    emp.porte_empresa,
    emp.natureza_juridica,
    nj.descricao                AS natureza_juridica_descricao,
    emp.capital_social,
    est.data_inicio_atividade,
    est.correio_eletronico,
    emp.reference_month
FROM cnpj.empresa emp
JOIN cnpj.estabelecimento est
    ON emp.cnpj_basico = est.cnpj_basico
    AND est.identificador_matriz_filial = 1          -- only HQ (matriz)
LEFT JOIN cnpj.municipio m  ON est.codigo_municipio = m.codigo
LEFT JOIN cnpj.cnae c       ON est.cnae_fiscal_principal = c.cnae_fiscal
LEFT JOIN cnpj.natureza_juridica nj ON emp.natureza_juridica = nj.codigo
WHERE est.situacao_cadastral != '02'                 -- inactive companies
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW cnpj.mv_company_search_inactive IS
    'Denormalised search view for inactive companies. '
    'First refresh: REFRESH MATERIALIZED VIEW cnpj.mv_company_search_inactive; '
    'Subsequent: REFRESH MATERIALIZED VIEW CONCURRENTLY cnpj.mv_company_search_inactive;';
