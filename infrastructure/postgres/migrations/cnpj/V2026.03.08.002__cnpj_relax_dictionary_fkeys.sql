-- ====================================================================
-- DROPPING DICTIONARY FOREIGN KEYS TO PREVENT BULK LOAD FAILURES
-- ====================================================================
-- Due to dirty data from Receita Federal, some valid existing companies
-- reference dictionary codes (like natureza_juridica '4014' or deleted
-- CNAEs/municipios) that do not exist in the official dictionary CSVs.
-- To prevent these valid companies from being blocked during Airflow
-- bulk load, we drop the restrictive foreign keys. We still keep the
-- FKs among the main entities (e.g. socio -> empresa).
-- ====================================================================

-- 1. EMPRESA FKs
ALTER TABLE cnpj.empresa DROP CONSTRAINT IF EXISTS empresa_natureza_juridica_fkey;
ALTER TABLE cnpj.empresa DROP CONSTRAINT IF EXISTS empresa_qualificacao_responsavel_fkey;

-- 2. ESTABELECIMENTO FKs
ALTER TABLE cnpj.estabelecimento DROP CONSTRAINT IF EXISTS estabelecimento_motivo_situacao_cadastral_fkey;
ALTER TABLE cnpj.estabelecimento DROP CONSTRAINT IF EXISTS estabelecimento_codigo_pais_fkey;
ALTER TABLE cnpj.estabelecimento DROP CONSTRAINT IF EXISTS estabelecimento_cnae_fiscal_principal_fkey;
ALTER TABLE cnpj.estabelecimento DROP CONSTRAINT IF EXISTS estabelecimento_codigo_municipio_fkey;

-- 3. SOCIO FKs
ALTER TABLE cnpj.socio DROP CONSTRAINT IF EXISTS socio_qualificacao_socio_fkey;
ALTER TABLE cnpj.socio DROP CONSTRAINT IF EXISTS socio_pais_fkey;
ALTER TABLE cnpj.socio DROP CONSTRAINT IF EXISTS socio_qualificacao_representante_legal_fkey;
