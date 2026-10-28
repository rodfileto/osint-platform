-- ============================================================
-- OpenAlex lake: operational state only (docs/repo/OPENALEX_INTEGRATION_PLAN.md)
-- ============================================================
-- Bronze lives in MinIO and silver on the NVMe; these tables record which
-- bronze files are synced and which silver files were built from which bronze
-- version (the upstream ETag), so the DAG can resume and rebuild only changes.

CREATE SCHEMA IF NOT EXISTS openalex;

CREATE TABLE IF NOT EXISTS openalex.release (
    release_date      DATE PRIMARY KEY,
    synced_at         TIMESTAMPTZ,
    verified_at       TIMESTAMPTZ,
    silver_built_at   TIMESTAMPTZ,
    catalog_built_at  TIMESTAMPTZ
);

COMMENT ON TABLE openalex.release IS
    'One row per OpenAlex release seen; each step of openalex_pipeline sets its column when it completes.';

CREATE TABLE IF NOT EXISTS openalex.bronze_file (
    key           TEXT PRIMARY KEY,           -- path below data/parquet/, e.g. works/updated_date=.../part_000.parquet
    entity        TEXT NOT NULL,
    size_bytes    BIGINT NOT NULL,
    record_count  BIGINT NOT NULL,
    source_etag   TEXT NOT NULL,              -- ETag in the OpenAlex bucket when it was copied
    release_date  DATE NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_openalex_bronze_file_entity ON openalex.bronze_file (entity);

COMMENT ON TABLE openalex.bronze_file IS
    'Bronze files in s3://osint-raw/openalex/parquet/ (key relative to that prefix), latest release only.';

CREATE TABLE IF NOT EXISTS openalex.silver_file (
    silver_table  TEXT NOT NULL,
    source_key    TEXT NOT NULL,              -- openalex.bronze_file.key it was built from
    source_etag   TEXT NOT NULL,
    rows          BIGINT NOT NULL,
    built_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (silver_table, source_key)
);

COMMENT ON TABLE openalex.silver_file IS
    'Silver outputs on the NVMe; rebuilt when the bronze file''s source ETag changes.';

GRANT USAGE ON SCHEMA openalex TO osint_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA openalex TO osint_admin;
