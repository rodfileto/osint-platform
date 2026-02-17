#!/bin/bash
# Monitor CNPJ Ingestion Progress

MONTH="${1:-2024-02}"

echo "=== CNPJ Ingestion Progress for $MONTH ==="
echo ""

docker-compose exec -T postgres psql -U osint_admin -d osint_metadata <<EOF
-- Overall progress
SELECT 
    file_type,
    COUNT(*) as total_files,
    COUNT(CASE WHEN extracted_at IS NOT NULL THEN 1 END) as extracted,
    COUNT(CASE WHEN transformed_at IS NOT NULL THEN 1 END) as transformed,
    COUNT(CASE WHEN loaded_postgres_at IS NOT NULL THEN 1 END) as loaded_pg,
    SUM(rows_transformed) as total_rows,
    ROUND(AVG(processing_duration_seconds), 2) as avg_seconds
FROM cnpj.download_manifest 
WHERE reference_month = '$MONTH' 
    AND file_type IN ('empresas', 'estabelecimentos')
GROUP BY file_type
ORDER BY file_type;

-- Currently processing
SELECT 
    file_name,
    file_type,
    CASE 
        WHEN extracted_at IS NULL THEN 'extracting...'
        WHEN transformed_at IS NULL THEN 'transforming...'
        WHEN loaded_postgres_at IS NULL THEN 'loading to postgres...'
        ELSE 'completed'
    END as status
FROM cnpj.download_manifest
WHERE reference_month = '$MONTH'
    AND file_type IN ('empresas', 'estabelecimentos')
    AND loaded_postgres_at IS NULL
ORDER BY file_type, file_name
LIMIT 5;
EOF
