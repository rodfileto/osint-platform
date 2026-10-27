-- ============================================================
-- Gold serving tables: cnpj.company_search and cnpj.socio_history
-- ============================================================
-- Step 3 of MEDALLION_MIGRATION_PLAN.md. Both tables are built from scratch
-- on every publish (build-and-swap, see pipelines/scripts/cnpj/gold_publish.py),
-- so they carry only column definitions here; indexes are created by the
-- publish step with canonical names.

-- company_search replaces mv_company_search and mv_company_search_inactive
-- (dropped in Step 4, when the API no longer reads them).
CREATE TABLE IF NOT EXISTS cnpj.company_search (
    cnpj_basico                VARCHAR(8) NOT NULL,
    razao_social               TEXT,
    nome_fantasia              TEXT,
    cnpj_ordem                 VARCHAR(4),
    cnpj_dv                    VARCHAR(2),
    cnpj_14                    VARCHAR(14) NOT NULL,
    situacao_cadastral         VARCHAR(2),
    codigo_municipio           VARCHAR(4),
    municipio_nome             TEXT,
    uf                         VARCHAR(2),
    cnae_fiscal_principal      VARCHAR(7),
    cnae_descricao             TEXT,
    porte_empresa              VARCHAR(2),
    natureza_juridica          INTEGER,
    natureza_juridica_descricao TEXT,
    capital_social             DECIMAL(15, 2),
    data_inicio_atividade      DATE,
    correio_eletronico         TEXT,
    reference_month            VARCHAR(7) NOT NULL,
    is_active                  BOOLEAN NOT NULL,
    created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE cnpj.company_search IS
    'Denormalised company search table (one row per matrix establishment, active or not). '
    'Rebuilt from scratch by cnpj gold publish; replaces mv_company_search and mv_company_search_inactive.';
COMMENT ON COLUMN cnpj.company_search.is_active IS 'TRUE when situacao_cadastral = ''02'' (active registry status)';

-- socio_history: one row per continuous period of a partnership (gaps-and-islands
-- over all silver months). Built by the dbt model cnpj_gold.socio_history.
CREATE TABLE IF NOT EXISTS cnpj.socio_history (
    cnpj_basico                     VARCHAR(8) NOT NULL,
    identificador_socio             INTEGER,
    cpf_cnpj_socio                  VARCHAR(14),
    nome_socio_razao_social         TEXT,
    qualificacao_socio              VARCHAR(2),
    valid_from                      DATE NOT NULL,
    valid_to                        DATE NOT NULL,
    is_current                      BOOLEAN NOT NULL,
    data_entrada_sociedade          DATE,
    faixa_etaria                    INTEGER,
    pais                            VARCHAR(3),
    representante_legal             VARCHAR(14),
    nome_do_representante           TEXT,
    qualificacao_representante_legal VARCHAR(2),
    created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE cnpj.socio_history IS
    'Partnership periods over all CNPJ silver months: one row per continuous period of a partnership key, '
    'with valid_from / valid_to (month starts) and is_current. A qualification change or a gap opens a new period.';
COMMENT ON COLUMN cnpj.socio_history.is_current IS 'TRUE when the period extends to the latest silver month';

-- ============================================================
-- Soft references
-- ============================================================
-- cnpj.municipio is rebuilt on every publish (build-and-swap), so the hard FK
-- from the geo mapping table is replaced by an indexed soft reference. The same
-- treatment was already applied to the cnpj-internal FKs.
ALTER TABLE geo.cnpj_br_municipality_map
    DROP CONSTRAINT IF EXISTS fk_cnpj_br_municipality_map_cnpj;

CREATE INDEX IF NOT EXISTS idx_geo_cnpj_br_map_cnpj_code
    ON geo.cnpj_br_municipality_map (cnpj_municipality_code);

-- ============================================================
-- Dead triggers
-- ============================================================
-- The updated_at triggers on the swapped tables would stay bound to the
-- pre-swap tables; the gold publish is insert-only, so they are dropped.
-- The download_manifest trigger is kept (that table is not swapped).
DROP TRIGGER IF EXISTS update_empresa_updated_at ON cnpj.empresa;
DROP TRIGGER IF EXISTS update_estabelecimento_updated_at ON cnpj.estabelecimento;
DROP TRIGGER IF EXISTS update_simples_nacional_updated_at ON cnpj.simples_nacional;
DROP TRIGGER IF EXISTS update_socio_updated_at ON cnpj.socio;

-- socio.id is filled by the gold publish (row_number over the gold rows) and its
-- sequence is synced with setval after each publish. The sequence must not be
-- owned by a column: the swap keeps the previous live table as <t>_old, and
-- dropping that table would drop an owned sequence with it.
ALTER SEQUENCE IF EXISTS cnpj.socio_id_seq OWNED BY NONE;

-- ============================================================
-- Grants
-- ============================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cnpj TO osint_admin;
