-- ============================================================
-- INOVALINK Schema Initialization
-- Livewire-backed public ecosystem map snapshots and normalized actors.
-- ============================================================

CREATE TABLE IF NOT EXISTS inovalink.sync_manifest (
    id                      SERIAL PRIMARY KEY,
    source_name             VARCHAR(50)     NOT NULL DEFAULT 'inovalink',
    entrypoint_url          TEXT            NOT NULL,
    request_url             TEXT            NOT NULL,
    endpoint_path           VARCHAR(255)    NOT NULL DEFAULT '/livewire/update',
    request_method          VARCHAR(10)     NOT NULL DEFAULT 'POST',
    component_name          VARCHAR(100)    NOT NULL DEFAULT 'site.map',
    component_id            VARCHAR(100),
    collected_at            TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    snapshot_date           DATE,
    response_status_code    INTEGER,
    response_content_type   TEXT,
    response_content_encoding TEXT,
    response_size_bytes     BIGINT,
    response_checksum_sha256 VARCHAR(64),
    http_etag               TEXT,
    http_last_modified      TIMESTAMPTZ,
    collection_counts       JSONB,
    records_total           INTEGER,
    raw_request_path        TEXT,
    raw_response_path       TEXT,
    processing_status       VARCHAR(20)     NOT NULL DEFAULT 'collected',
    error_message           TEXT,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_inovalink_sync_manifest_method
        CHECK (request_method IN ('GET', 'POST')),
    CONSTRAINT chk_inovalink_sync_manifest_status
        CHECK (processing_status IN ('collected', 'normalized', 'loaded', 'failed'))
);

COMMENT ON TABLE inovalink.sync_manifest IS
    'One collection execution against the public Inovalink Livewire endpoint.';
COMMENT ON COLUMN inovalink.sync_manifest.snapshot_date IS
    'Declared source snapshot date when available. The current source does not expose it explicitly, so this may remain NULL.';
COMMENT ON COLUMN inovalink.sync_manifest.collection_counts IS
    'Per-collection counts returned by the Livewire payload, e.g. residentCompanies=4084.';
COMMENT ON COLUMN inovalink.sync_manifest.raw_request_path IS
    'Optional artifact path for the serialized POST payload used during collection.';
COMMENT ON COLUMN inovalink.sync_manifest.raw_response_path IS
    'Optional artifact path for the raw JSON response captured during collection.';

CREATE TABLE IF NOT EXISTS inovalink.component_payload (
    id                      BIGSERIAL PRIMARY KEY,
    manifest_id             INTEGER         NOT NULL REFERENCES inovalink.sync_manifest(id) ON DELETE CASCADE,
    component_name          VARCHAR(100)    NOT NULL,
    component_id            VARCHAR(100),
    component_snapshot      JSONB,
    component_effects       JSONB,
    component_html          TEXT,
    payload_row_count       INTEGER,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (manifest_id, component_name, component_id)
);

COMMENT ON TABLE inovalink.component_payload IS
    'Raw Livewire component payload stored per collection execution for auditing and replay support.';
COMMENT ON COLUMN inovalink.component_payload.component_snapshot IS
    'Decoded Livewire snapshot JSON for the component after the request completes.';
COMMENT ON COLUMN inovalink.component_payload.component_effects IS
    'Decoded Livewire effects JSON, including xjs payloads used by the frontend.';

CREATE TABLE IF NOT EXISTS inovalink.organization_snapshot (
    id                      BIGSERIAL PRIMARY KEY,
    manifest_id             INTEGER         NOT NULL REFERENCES inovalink.sync_manifest(id) ON DELETE CASCADE,
    source_collection       VARCHAR(50)     NOT NULL,
    source_record_id        BIGINT          NOT NULL,
    entity_kind             VARCHAR(20)     NOT NULL,
    category_id             INTEGER,
    category_label          TEXT,
    source_linkable_type    TEXT,
    source_linkable_id      BIGINT,
    name                    TEXT            NOT NULL,
    acronym                 TEXT,
    corporate_name          TEXT,
    cnpj                    VARCHAR(14),
    telephone               TEXT,
    email                   TEXT,
    site                    TEXT,
    instagram               TEXT,
    linkedin                TEXT,
    logo_path               TEXT,
    institution             TEXT,
    active                  BOOLEAN,
    company_status_id       INTEGER,
    cnae_id                 INTEGER,
    graduated               BOOLEAN,
    founding_year           SMALLINT,
    founding_date           DATE,
    termination_year        SMALLINT,
    pre_acceleration_year   SMALLINT,
    acceleration_year       SMALLINT,
    pre_incubation_year     SMALLINT,
    start_incubation_year   SMALLINT,
    end_incubation_year     SMALLINT,
    spin_off                BOOLEAN,
    expertise_area_codes    TEXT[],
    affiliated_societies_cities JSONB,
    motivations_for_opening JSONB,
    main_participation_aspects JSONB,
    meet_way_id             INTEGER,
    postcode                VARCHAR(8),
    street                  TEXT,
    street_number           TEXT,
    complement              TEXT,
    neighborhood            TEXT,
    city_ibge_code          CHAR(7),
    city_name               TEXT,
    state_ibge_code         CHAR(2),
    state_name              TEXT,
    state_abbrev            CHAR(2),
    latitude                NUMERIC(10, 7),
    longitude               NUMERIC(10, 7),
    source_created_at       TIMESTAMPTZ,
    source_updated_at       TIMESTAMPTZ,
    raw_payload             JSONB           NOT NULL,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_inovalink_org_snapshot UNIQUE (manifest_id, source_collection, source_record_id),
    CONSTRAINT chk_inovalink_entity_kind
        CHECK (entity_kind IN ('incubator', 'accelerator', 'park', 'company')),
    CONSTRAINT fk_inovalink_org_city
        FOREIGN KEY (city_ibge_code) REFERENCES geo.br_municipality(municipality_ibge_code) ON DELETE SET NULL,
    CONSTRAINT fk_inovalink_org_state
        FOREIGN KEY (state_ibge_code) REFERENCES geo.br_state(state_ibge_code) ON DELETE SET NULL
);

COMMENT ON TABLE inovalink.organization_snapshot IS
    'Normalized snapshot rows for incubators, accelerators, parks, and companies returned by Inovalink.';
COMMENT ON COLUMN inovalink.organization_snapshot.source_collection IS
    'Original Livewire collection name, e.g. incubators, operationParks, residentCompanies.';
COMMENT ON COLUMN inovalink.organization_snapshot.source_record_id IS
    'Primary identifier used by the upstream Inovalink payload for the record within its collection.';
COMMENT ON COLUMN inovalink.organization_snapshot.city_ibge_code IS
    'IBGE municipality code inferred directly from address.city.id in the source payload.';
COMMENT ON COLUMN inovalink.organization_snapshot.raw_payload IS
    'Complete original JSON object for the actor as returned by the Livewire xjs payload.';

CREATE OR REPLACE FUNCTION inovalink.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_inovalink_sync_manifest_updated_at ON inovalink.sync_manifest;
CREATE TRIGGER update_inovalink_sync_manifest_updated_at
    BEFORE UPDATE ON inovalink.sync_manifest
    FOR EACH ROW
    EXECUTE FUNCTION inovalink.update_updated_at_column();

DROP TRIGGER IF EXISTS update_inovalink_organization_snapshot_updated_at ON inovalink.organization_snapshot;
CREATE TRIGGER update_inovalink_organization_snapshot_updated_at
    BEFORE UPDATE ON inovalink.organization_snapshot
    FOR EACH ROW
    EXECUTE FUNCTION inovalink.update_updated_at_column();

CREATE INDEX IF NOT EXISTS idx_inovalink_sync_manifest_collected_at
    ON inovalink.sync_manifest (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_inovalink_sync_manifest_status
    ON inovalink.sync_manifest (processing_status);

CREATE INDEX IF NOT EXISTS idx_inovalink_sync_manifest_checksum
    ON inovalink.sync_manifest (response_checksum_sha256)
    WHERE response_checksum_sha256 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_component_payload_manifest
    ON inovalink.component_payload (manifest_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_collection_record
    ON inovalink.organization_snapshot (source_collection, source_record_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_manifest
    ON inovalink.organization_snapshot (manifest_id);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_kind
    ON inovalink.organization_snapshot (entity_kind);

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_cnpj
    ON inovalink.organization_snapshot (cnpj)
    WHERE cnpj IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_city
    ON inovalink.organization_snapshot (city_ibge_code)
    WHERE city_ibge_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_state
    ON inovalink.organization_snapshot (state_ibge_code)
    WHERE state_ibge_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inovalink_org_snapshot_name_tsv
    ON inovalink.organization_snapshot
    USING GIN (to_tsvector('portuguese', COALESCE(name, '') || ' ' || COALESCE(corporate_name, '')));

CREATE OR REPLACE VIEW inovalink.latest_organization_snapshot AS
SELECT DISTINCT ON (source_collection, source_record_id)
    organization_snapshot.id,
    organization_snapshot.manifest_id,
    organization_snapshot.source_collection,
    organization_snapshot.source_record_id,
    organization_snapshot.entity_kind,
    organization_snapshot.category_id,
    organization_snapshot.category_label,
    organization_snapshot.source_linkable_type,
    organization_snapshot.source_linkable_id,
    organization_snapshot.name,
    organization_snapshot.acronym,
    organization_snapshot.corporate_name,
    organization_snapshot.cnpj,
    organization_snapshot.telephone,
    organization_snapshot.email,
    organization_snapshot.site,
    organization_snapshot.instagram,
    organization_snapshot.linkedin,
    organization_snapshot.logo_path,
    organization_snapshot.institution,
    organization_snapshot.active,
    organization_snapshot.city_ibge_code,
    organization_snapshot.city_name,
    organization_snapshot.state_ibge_code,
    organization_snapshot.state_name,
    organization_snapshot.state_abbrev,
    organization_snapshot.latitude,
    organization_snapshot.longitude,
    organization_snapshot.source_created_at,
    organization_snapshot.source_updated_at,
    sync_manifest.collected_at,
    sync_manifest.snapshot_date,
    sync_manifest.processing_status AS manifest_processing_status,
    organization_snapshot.raw_payload
FROM inovalink.organization_snapshot
JOIN inovalink.sync_manifest
    ON sync_manifest.id = organization_snapshot.manifest_id
ORDER BY
    organization_snapshot.source_collection,
    organization_snapshot.source_record_id,
    sync_manifest.collected_at DESC,
    organization_snapshot.id DESC;

COMMENT ON VIEW inovalink.latest_organization_snapshot IS
    'Latest known snapshot per upstream Inovalink record across all collection runs.';

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA inovalink TO osint_admin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA inovalink TO osint_admin;
GRANT USAGE ON SCHEMA inovalink TO osint_admin;