-- Neo4j is no longer loaded, and these columns were never written. The two views that read them
-- are recreated without them (CREATE OR REPLACE VIEW cannot drop a column).
DROP VIEW IF EXISTS cnpj.download_progress;
DROP VIEW IF EXISTS cnpj.files_ready_to_ingest;

ALTER TABLE cnpj.download_manifest
    DROP COLUMN IF EXISTS loaded_neo4j_at,
    DROP COLUMN IF EXISTS rows_loaded_neo4j;

CREATE VIEW cnpj.download_progress AS
SELECT
    reference_month,
    file_type,
    COUNT(*) as total_files,
    SUM(CASE WHEN processing_status = 'loaded' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN processing_status = 'pending' THEN 1 ELSE 0 END) as pending,
    SUM(CASE WHEN extracted_at IS NOT NULL THEN 1 ELSE 0 END) as extracted,
    SUM(CASE WHEN transformed_at IS NOT NULL THEN 1 ELSE 0 END) as transformed,
    SUM(CASE WHEN loaded_postgres_at IS NOT NULL THEN 1 ELSE 0 END) as loaded_postgres,
    SUM(file_size_bytes) / 1024.0 / 1024.0 / 1024.0 as total_size_gb,
    SUM(rows_transformed) as total_rows_transformed,
    SUM(rows_loaded_postgres) as total_rows_loaded_postgres,
    AVG(processing_duration_seconds) as avg_processing_seconds,
    MAX(download_date) as last_download,
    MAX(updated_at) as last_update
FROM cnpj.download_manifest
GROUP BY reference_month, file_type
ORDER BY reference_month DESC, file_type;

CREATE VIEW cnpj.files_ready_to_ingest AS
SELECT
    id,
    reference_month,
    file_name,
    file_type,
    file_size_bytes / 1024.0 / 1024.0 as file_size_mb,
    download_date,
    CASE
        WHEN extracted_at IS NULL THEN 'needs_extraction'
        WHEN transformed_at IS NULL THEN 'needs_transformation'
        WHEN loaded_postgres_at IS NULL THEN 'needs_postgres_load'
        ELSE 'completed'
    END as next_stage
FROM cnpj.download_manifest
WHERE download_date IS NOT NULL
    AND processing_status NOT IN ('failed', 'loaded')
    AND (
        extracted_at IS NULL
        OR transformed_at IS NULL
        OR loaded_postgres_at IS NULL
    )
ORDER BY reference_month DESC, file_type, file_name;
