-- Indexes for cnpj.mv_company_search_inactive
-- =====================================================
-- The inactive MV was created in V2026.03.08.001 WITHOUT any indexes.
-- REFRESH MATERIALIZED VIEW CONCURRENTLY requires at least one unique index.
-- These indexes mirror the pattern used for cnpj.mv_company_search (V2).

-- Unique index — required for REFRESH ... CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_mvinact_cnpj14
    ON cnpj.mv_company_search_inactive(cnpj_14);

-- Trigram indexes for fuzzy/prefix search
CREATE INDEX IF NOT EXISTS idx_mvinact_razao_social_trgm
    ON cnpj.mv_company_search_inactive
    USING gin (razao_social gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_mvinact_nome_fantasia_trgm
    ON cnpj.mv_company_search_inactive
    USING gin (nome_fantasia gin_trgm_ops);

-- Filter / lookup indexes
CREATE INDEX IF NOT EXISTS idx_mvinact_uf_municipio
    ON cnpj.mv_company_search_inactive(uf, codigo_municipio);

CREATE INDEX IF NOT EXISTS idx_mvinact_situacao
    ON cnpj.mv_company_search_inactive(situacao_cadastral);
