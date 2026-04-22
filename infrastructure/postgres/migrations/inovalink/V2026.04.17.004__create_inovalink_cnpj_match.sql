-- ============================================================
-- INOVALINK Soft CNPJ Resolution
-- Optional linkage between Inovalink organizations and CNPJ records.
-- No hard FK is enforced against raw organization_snapshot.cnpj because
-- the CNPJ ingestion pipeline intentionally tolerates orphan conditions.
-- ============================================================

CREATE TABLE IF NOT EXISTS inovalink.organization_cnpj_match (
    id                          BIGSERIAL PRIMARY KEY,
    organization_snapshot_id    BIGINT          NOT NULL REFERENCES inovalink.organization_snapshot(id) ON DELETE CASCADE,
    match_status                VARCHAR(20)     NOT NULL DEFAULT 'pending',
    match_method                VARCHAR(50),
    match_confidence            NUMERIC(10, 6),
    matched_at                  TIMESTAMPTZ,

    -- Raw candidate captured from Inovalink
    source_cnpj                VARCHAR(14),

    -- Resolved CNPJ components when a confident match exists
    cnpj_basico                VARCHAR(8),
    cnpj_ordem                 VARCHAR(4),
    cnpj_dv                    VARCHAR(2),
    cnpj_14                    VARCHAR(14),

    -- Optional references to physical CNPJ records when available
    matched_empresa_cnpj_basico VARCHAR(8),
    matched_estab_cnpj_basico   VARCHAR(8),
    matched_estab_cnpj_ordem    VARCHAR(4),
    matched_estab_cnpj_dv       VARCHAR(2),

    resolution_notes           TEXT,
    evidence                   JSONB,
    created_at                 TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                 TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_inovalink_org_cnpj_match UNIQUE (organization_snapshot_id),
    CONSTRAINT chk_inovalink_org_cnpj_match_status
        CHECK (match_status IN ('pending', 'matched', 'ambiguous', 'unmatched', 'invalid')),
    CONSTRAINT fk_inovalink_cnpj_match_empresa
        FOREIGN KEY (matched_empresa_cnpj_basico) REFERENCES cnpj.empresa(cnpj_basico) ON DELETE SET NULL,
    CONSTRAINT fk_inovalink_cnpj_match_estab
        FOREIGN KEY (matched_estab_cnpj_basico, matched_estab_cnpj_ordem, matched_estab_cnpj_dv)
        REFERENCES cnpj.estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv)
        ON DELETE SET NULL
);

COMMENT ON TABLE inovalink.organization_cnpj_match IS
    'Soft-resolution table linking Inovalink organizations to CNPJ records without enforcing a hard FK from raw source CNPJ values.';
COMMENT ON COLUMN inovalink.organization_cnpj_match.match_status IS
    'pending | matched | ambiguous | unmatched | invalid';
COMMENT ON COLUMN inovalink.organization_cnpj_match.match_method IS
    'How the linkage was resolved, e.g. exact_cnpj, normalized_cnpj, name_city_similarity, manual_review.';
COMMENT ON COLUMN inovalink.organization_cnpj_match.match_confidence IS
    'Optional confidence score between 0 and 1 for non-exact matches.';
COMMENT ON COLUMN inovalink.organization_cnpj_match.source_cnpj IS
    'Raw 14-digit CNPJ as published by Inovalink.';
COMMENT ON COLUMN inovalink.organization_cnpj_match.evidence IS
    'JSON evidence used by the matcher, such as compared names, municipality, or candidate list.';

CREATE OR REPLACE FUNCTION inovalink.update_cnpj_match_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_inovalink_organization_cnpj_match_updated_at ON inovalink.organization_cnpj_match;
CREATE TRIGGER update_inovalink_organization_cnpj_match_updated_at
    BEFORE UPDATE ON inovalink.organization_cnpj_match
    FOR EACH ROW
    EXECUTE FUNCTION inovalink.update_cnpj_match_updated_at_column();

CREATE INDEX IF NOT EXISTS idx_inovalink_org_cnpj_match_status
    ON inovalink.organization_cnpj_match (match_status);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_cnpj_match_source_cnpj
    ON inovalink.organization_cnpj_match (source_cnpj)
    WHERE source_cnpj IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_cnpj_match_cnpj14
    ON inovalink.organization_cnpj_match (cnpj_14)
    WHERE cnpj_14 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_cnpj_match_empresa
    ON inovalink.organization_cnpj_match (matched_empresa_cnpj_basico)
    WHERE matched_empresa_cnpj_basico IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_cnpj_match_estab
    ON inovalink.organization_cnpj_match (matched_estab_cnpj_basico, matched_estab_cnpj_ordem, matched_estab_cnpj_dv)
    WHERE matched_estab_cnpj_basico IS NOT NULL;

CREATE OR REPLACE VIEW inovalink.organization_cnpj_match_resolved AS
SELECT
    match.id,
    match.organization_snapshot_id,
    org.manifest_id,
    org.source_collection,
    org.source_record_id,
    org.entity_kind,
    org.name,
    org.corporate_name,
    org.city_ibge_code,
    org.city_name,
    org.state_abbrev,
    match.match_status,
    match.match_method,
    match.match_confidence,
    match.source_cnpj,
    match.cnpj_14,
    match.matched_empresa_cnpj_basico,
    match.matched_estab_cnpj_basico,
    match.matched_estab_cnpj_ordem,
    match.matched_estab_cnpj_dv,
    emp.razao_social                         AS matched_empresa_razao_social,
    est.nome_fantasia                        AS matched_estabelecimento_nome_fantasia,
    est.uf                                   AS matched_estabelecimento_uf,
    est.codigo_municipio                     AS matched_estabelecimento_codigo_municipio,
    match.resolution_notes,
    match.evidence,
    match.matched_at,
    match.created_at,
    match.updated_at
FROM inovalink.organization_cnpj_match AS match
JOIN inovalink.organization_snapshot AS org
    ON org.id = match.organization_snapshot_id
LEFT JOIN cnpj.empresa AS emp
    ON emp.cnpj_basico = match.matched_empresa_cnpj_basico
LEFT JOIN cnpj.estabelecimento AS est
    ON est.cnpj_basico = match.matched_estab_cnpj_basico
   AND est.cnpj_ordem = match.matched_estab_cnpj_ordem
   AND est.cnpj_dv = match.matched_estab_cnpj_dv;

COMMENT ON VIEW inovalink.organization_cnpj_match_resolved IS
    'Resolver-friendly view joining soft Inovalink-to-CNPJ matches with the underlying CNPJ company and establishment records.';

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA inovalink TO osint_admin;