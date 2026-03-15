-- ============================================================
-- GEO Schema Initialization
-- Canonical Brazilian geography and source-specific mapping objects.
-- ============================================================

CREATE TABLE IF NOT EXISTS geo.br_state (
    state_ibge_code     CHAR(2) PRIMARY KEY,
    state_abbrev        CHAR(2) NOT NULL UNIQUE,
    state_name          TEXT NOT NULL,
    country_code        CHAR(2) NOT NULL DEFAULT 'BR',
    geobr_year          SMALLINT NOT NULL,
    is_simplified       BOOLEAN NOT NULL DEFAULT TRUE,
    source_name         TEXT NOT NULL DEFAULT 'geobr',
    source_version      TEXT,
    geometry_srid       INTEGER NOT NULL DEFAULT 4674,
    area_km2            NUMERIC(18, 4),
    centroid            geometry(POINT, 4674),
    geom                geometry(MULTIPOLYGON, 4674) NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_br_state_country_code CHECK (country_code = 'BR')
);

CREATE TABLE IF NOT EXISTS geo.br_municipality (
    municipality_ibge_code CHAR(7) PRIMARY KEY,
    state_ibge_code        CHAR(2) NOT NULL,
    state_abbrev           CHAR(2) NOT NULL,
    municipality_name      TEXT NOT NULL,
    country_code           CHAR(2) NOT NULL DEFAULT 'BR',
    geobr_year             SMALLINT NOT NULL,
    is_simplified          BOOLEAN NOT NULL DEFAULT TRUE,
    source_name            TEXT NOT NULL DEFAULT 'geobr',
    source_version         TEXT,
    geometry_srid          INTEGER NOT NULL DEFAULT 4674,
    area_km2               NUMERIC(18, 4),
    centroid               geometry(POINT, 4674),
    geom                   geometry(MULTIPOLYGON, 4674) NOT NULL,
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_br_municipality_state
        FOREIGN KEY (state_ibge_code) REFERENCES geo.br_state(state_ibge_code),
    CONSTRAINT chk_br_municipality_country_code CHECK (country_code = 'BR')
);

CREATE TABLE IF NOT EXISTS geo.cnpj_br_municipality_map (
    cnpj_municipality_code VARCHAR(4) PRIMARY KEY,
    municipality_ibge_code CHAR(7) NOT NULL,
    state_abbrev           CHAR(2),
    cnpj_municipality_name TEXT,
    br_municipality_name   TEXT,
    match_method           TEXT NOT NULL,
    match_score            NUMERIC(10, 6),
    mapping_source         TEXT NOT NULL DEFAULT 'dev-geobr-loader',
    mapped_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes                  TEXT,
    CONSTRAINT fk_cnpj_br_municipality_map_cnpj
        FOREIGN KEY (cnpj_municipality_code) REFERENCES cnpj.municipio(codigo),
    CONSTRAINT fk_cnpj_br_municipality_map_br
        FOREIGN KEY (municipality_ibge_code) REFERENCES geo.br_municipality(municipality_ibge_code)
);

CREATE TABLE IF NOT EXISTS geo.cnpj_br_municipality_map_issue (
    id                     BIGSERIAL PRIMARY KEY,
    cnpj_municipality_code VARCHAR(4),
    cnpj_municipality_name TEXT,
    inferred_state_abbrev  CHAR(2),
    candidate_count        INTEGER,
    issue_type             TEXT NOT NULL,
    issue_details          TEXT,
    observed_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolution_notes       TEXT
);

CREATE OR REPLACE FUNCTION geo.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_br_state_updated_at ON geo.br_state;
CREATE TRIGGER update_br_state_updated_at
    BEFORE UPDATE ON geo.br_state
    FOR EACH ROW
    EXECUTE FUNCTION geo.update_updated_at_column();

DROP TRIGGER IF EXISTS update_br_municipality_updated_at ON geo.br_municipality;
CREATE TRIGGER update_br_municipality_updated_at
    BEFORE UPDATE ON geo.br_municipality
    FOR EACH ROW
    EXECUTE FUNCTION geo.update_updated_at_column();

CREATE INDEX IF NOT EXISTS idx_geo_br_state_geom
    ON geo.br_state USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_geo_br_state_abbrev
    ON geo.br_state(state_abbrev);

CREATE INDEX IF NOT EXISTS idx_geo_br_state_name
    ON geo.br_state(state_name);

CREATE INDEX IF NOT EXISTS idx_geo_br_municipality_geom
    ON geo.br_municipality USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_geo_br_municipality_state_code
    ON geo.br_municipality(state_ibge_code);

CREATE INDEX IF NOT EXISTS idx_geo_br_municipality_state_abbrev
    ON geo.br_municipality(state_abbrev);

CREATE INDEX IF NOT EXISTS idx_geo_br_municipality_name
    ON geo.br_municipality(municipality_name);

CREATE INDEX IF NOT EXISTS idx_geo_cnpj_br_map_ibge
    ON geo.cnpj_br_municipality_map(municipality_ibge_code);

CREATE INDEX IF NOT EXISTS idx_geo_cnpj_br_map_state_abbrev
    ON geo.cnpj_br_municipality_map(state_abbrev);

CREATE INDEX IF NOT EXISTS idx_geo_cnpj_br_map_issue_type
    ON geo.cnpj_br_municipality_map_issue(issue_type);

CREATE INDEX IF NOT EXISTS idx_geo_cnpj_br_map_issue_code
    ON geo.cnpj_br_municipality_map_issue(cnpj_municipality_code);

CREATE OR REPLACE VIEW geo.v_cnpj_br_municipality AS
SELECT
    cnpj_municipality.codigo                        AS cnpj_municipality_code,
    cnpj_municipality.nome                          AS cnpj_municipality_name,
    mapping.municipality_ibge_code                 AS municipality_ibge_code,
    municipality.municipality_name                 AS municipality_name,
    municipality.state_abbrev                      AS state_abbrev,
    municipality.country_code                      AS country_code,
    municipality.geobr_year                        AS geobr_year,
    municipality.is_simplified                     AS is_simplified,
    municipality.area_km2                          AS area_km2,
    municipality.centroid                          AS centroid,
    municipality.geom                              AS geom,
    mapping.match_method                           AS match_method,
    mapping.match_score                            AS match_score,
    mapping.mapping_source                         AS mapping_source,
    mapping.mapped_at                              AS mapped_at
FROM cnpj.municipio AS cnpj_municipality
LEFT JOIN geo.cnpj_br_municipality_map AS mapping
    ON mapping.cnpj_municipality_code = cnpj_municipality.codigo
LEFT JOIN geo.br_municipality AS municipality
    ON municipality.municipality_ibge_code = mapping.municipality_ibge_code;

COMMENT ON TABLE geo.br_state IS
    'Canonical Brazilian states loaded from geobr.';
COMMENT ON TABLE geo.br_municipality IS
    'Canonical Brazilian municipalities loaded from geobr.';
COMMENT ON TABLE geo.cnpj_br_municipality_map IS
    'Crosswalk from CNPJ municipality codes to canonical Brazilian municipalities.';
COMMENT ON TABLE geo.cnpj_br_municipality_map_issue IS
    'Audit table for unresolved or ambiguous CNPJ municipality mappings.';
COMMENT ON VIEW geo.v_cnpj_br_municipality IS
    'Backend-friendly join between CNPJ municipality codes and canonical Brazilian municipalities.';

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA geo TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA geo TO osint_admin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA geo TO osint_admin;
