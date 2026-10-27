-- ============================================================
-- Step 4: the API now reads cnpj.company_search (one table with an
-- is_active flag) and the ownership network no longer needs socio.id.
-- ============================================================

-- The search materialized views are replaced by cnpj.company_search.
DROP MATERIALIZED VIEW IF EXISTS cnpj.mv_company_search;
DROP MATERIALIZED VIEW IF EXISTS cnpj.mv_company_search_inactive;

-- The network service dedups by the natural partnership key instead of the
-- surrogate id, which is no longer published.
ALTER TABLE cnpj.socio DROP COLUMN IF EXISTS id;
DROP SEQUENCE IF EXISTS cnpj.socio_id_seq;
