-- PostgreSQL CNPJ Schema Initialization
-- Run this after the main init-db.sh to set up CNPJ-specific tables
-- Usage: psql -U osint_admin -d osint_metadata -f init-cnpj-schema.sql

-- =====================================================
-- CNPJ Schema Tables
-- =====================================================

-- Drop existing tables to ensure clean creation with constraints
DROP TABLE IF EXISTS cnpj.socio CASCADE;
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;

-- Download Manifest Table (tracks downloaded files and processing status)
CREATE TABLE IF NOT EXISTS cnpj.download_manifest (
    id SERIAL PRIMARY KEY,
    reference_month VARCHAR(7) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    file_size_bytes BIGINT,
    file_checksum VARCHAR(64),
    source_url TEXT,
    download_date TIMESTAMP,
    last_modified_remote TIMESTAMP,
    processing_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    
    -- Ingestion pipeline tracking
    extracted_at TIMESTAMP,
    transformed_at TIMESTAMP,
    loaded_postgres_at TIMESTAMP,
    loaded_neo4j_at TIMESTAMP,
    
    -- Processing metrics
    rows_extracted INTEGER,
    rows_transformed INTEGER,
    rows_loaded_postgres INTEGER,
    rows_loaded_neo4j INTEGER,
    processing_duration_seconds DECIMAL(10,2),
    
    -- Output paths
    csv_path TEXT,
    parquet_path TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(reference_month, file_name)
);

COMMENT ON TABLE cnpj.download_manifest IS 'Tracks downloaded CNPJ files and their complete processing pipeline status';
COMMENT ON COLUMN cnpj.download_manifest.reference_month IS 'YYYY-MM format, e.g., 2024-02';
COMMENT ON COLUMN cnpj.download_manifest.file_type IS 'empresas, estabelecimentos, socios, or reference';
COMMENT ON COLUMN cnpj.download_manifest.processing_status IS 'pending, downloaded, extracted, transformed, loaded, or failed';
COMMENT ON COLUMN cnpj.download_manifest.extracted_at IS 'Timestamp when ZIP was extracted to CSV';
COMMENT ON COLUMN cnpj.download_manifest.transformed_at IS 'Timestamp when CSV was transformed to Parquet';
COMMENT ON COLUMN cnpj.download_manifest.loaded_postgres_at IS 'Timestamp when data was loaded to PostgreSQL';
COMMENT ON COLUMN cnpj.download_manifest.loaded_neo4j_at IS 'Timestamp when data was loaded to Neo4j';

-- Empresa table (company base data) - singular form used by current pipeline
CREATE TABLE IF NOT EXISTS cnpj.empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DECIMAL(15,2) DEFAULT 0.0,
    porte_empresa VARCHAR(2) DEFAULT '00',
    ente_federativo_responsavel TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_month VARCHAR(7) NOT NULL
);

-- Estabelecimento table (establishment locations) - singular form used by current pipeline
CREATE TABLE IF NOT EXISTS cnpj.estabelecimento (
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    identificador_matriz_filial INTEGER,
    nome_fantasia TEXT,
    situacao_cadastral INTEGER,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    codigo_pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal INTEGER,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep VARCHAR(8),
    uf VARCHAR(2),
    codigo_municipio INTEGER,
    municipio TEXT,
    ddd_telefone_1 TEXT,
    ddd_telefone_2 TEXT,
    ddd_fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_month VARCHAR(7) NOT NULL,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
    FOREIGN KEY (cnpj_basico) REFERENCES cnpj.empresa(cnpj_basico) ON DELETE CASCADE
);

-- Socio table (partner/shareholder data)
CREATE TABLE IF NOT EXISTS cnpj.socio (
    id                              BIGSERIAL PRIMARY KEY,
    cnpj_basico                     VARCHAR(8)   NOT NULL,
    identificador_socio             INTEGER,
    nome_socio_razao_social         TEXT,
    cpf_cnpj_socio                  VARCHAR(14),
    qualificacao_socio              INTEGER,
    data_entrada_sociedade          DATE,
    pais                            INTEGER,
    representante_legal             VARCHAR(14),
    nome_do_representante           TEXT,
    qualificacao_representante_legal INTEGER,
    faixa_etaria                    INTEGER,
    reference_month                 VARCHAR(7)   NOT NULL,
    created_at                      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cnpj_basico) REFERENCES cnpj.empresa(cnpj_basico) ON DELETE CASCADE
);

COMMENT ON TABLE cnpj.socio IS 'Quadro Societário — snapshot por reference_month (sem chave natural única)';
COMMENT ON COLUMN cnpj.socio.identificador_socio IS '1=PJ, 2=PF, 3=Estrangeiro';
COMMENT ON COLUMN cnpj.socio.cpf_cnpj_socio IS 'CPF mascarado pela RF (***XXXXXX**) ou CNPJ completo para PJ';
COMMENT ON COLUMN cnpj.socio.pais IS 'NULL = Brasil; código da tabela cnpj.pais';
COMMENT ON COLUMN cnpj.socio.faixa_etaria IS '0=não informado, 1=<=20, 2=21-30, ..., 9=>80';
COMMENT ON COLUMN cnpj.socio.reference_month IS 'Snapshot de origem YYYY-MM; carga via DELETE+INSERT por mês';

-- Indexes for performance

-- Download manifest indexes
CREATE INDEX IF NOT EXISTS idx_download_manifest_ref_month ON cnpj.download_manifest(reference_month);
CREATE INDEX IF NOT EXISTS idx_download_manifest_status ON cnpj.download_manifest(processing_status);
CREATE INDEX IF NOT EXISTS idx_download_manifest_file_type ON cnpj.download_manifest(file_type);
CREATE INDEX IF NOT EXISTS idx_download_manifest_download_date ON cnpj.download_manifest(download_date DESC);
CREATE INDEX IF NOT EXISTS idx_download_manifest_transformed ON cnpj.download_manifest(reference_month, file_type) WHERE transformed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_download_manifest_loaded ON cnpj.download_manifest(reference_month, file_type) WHERE loaded_postgres_at IS NOT NULL;

-- Empresa indexes
CREATE INDEX IF NOT EXISTS idx_empresa_razao_social ON cnpj.empresa USING GIN (to_tsvector('portuguese', razao_social));
CREATE INDEX IF NOT EXISTS idx_empresa_porte ON cnpj.empresa(porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresa_natureza ON cnpj.empresa(natureza_juridica);
CREATE INDEX IF NOT EXISTS idx_empresa_ref_month ON cnpj.empresa(reference_month);

-- Socio indexes
CREATE INDEX IF NOT EXISTS idx_socio_cnpj_basico     ON cnpj.socio(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socio_cpf_cnpj        ON cnpj.socio(cpf_cnpj_socio);
CREATE INDEX IF NOT EXISTS idx_socio_nome            ON cnpj.socio USING GIN (to_tsvector('portuguese', nome_socio_razao_social)) WHERE nome_socio_razao_social IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_socio_ref_month       ON cnpj.socio(reference_month);
CREATE INDEX IF NOT EXISTS idx_socio_qualificacao    ON cnpj.socio(qualificacao_socio);

-- Estabelecimento indexes
CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON cnpj.estabelecimento(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON cnpj.estabelecimento(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON cnpj.estabelecimento(uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnae ON cnpj.estabelecimento(cnae_fiscal_principal);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_ref_month ON cnpj.estabelecimento(reference_month);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_nome_fantasia ON cnpj.estabelecimento USING GIN (to_tsvector('portuguese', nome_fantasia));

-- View for complete CNPJ (14 digits) with empresa data
CREATE OR REPLACE VIEW cnpj.estabelecimento_completo AS
SELECT 
    e.*,
    (e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv) AS cnpj_completo,
    emp.razao_social,
    emp.capital_social,
    emp.porte_empresa
FROM cnpj.estabelecimento e
LEFT JOIN cnpj.empresa emp ON e.cnpj_basico = emp.cnpj_basico;

-- View for download progress by month
CREATE OR REPLACE VIEW cnpj.download_progress AS
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
    SUM(CASE WHEN loaded_neo4j_at IS NOT NULL THEN 1 ELSE 0 END) as loaded_neo4j,
    SUM(file_size_bytes) / 1024.0 / 1024.0 / 1024.0 as total_size_gb,
    SUM(rows_transformed) as total_rows_transformed,
    SUM(rows_loaded_postgres) as total_rows_loaded_postgres,
    AVG(processing_duration_seconds) as avg_processing_seconds,
    MAX(download_date) as last_download,
    MAX(updated_at) as last_update
FROM cnpj.download_manifest
GROUP BY reference_month, file_type
ORDER BY reference_month DESC, file_type;

-- View for incomplete months (need reprocessing)
CREATE OR REPLACE VIEW cnpj.incomplete_months AS
SELECT 
    reference_month,
    COUNT(*) as files_found,
    37 - COUNT(*) as files_missing,
    SUM(CASE WHEN processing_status != 'loaded' THEN 1 ELSE 0 END) as files_pending,
    SUM(CASE WHEN transformed_at IS NULL THEN 1 ELSE 0 END) as files_not_transformed,
    SUM(CASE WHEN loaded_postgres_at IS NULL THEN 1 ELSE 0 END) as files_not_loaded_postgres,
    STRING_AGG(
        CASE WHEN processing_status = 'failed' THEN file_name ELSE NULL END, 
        ', '
    ) as failed_files,
    STRING_AGG(
        CASE WHEN download_date IS NOT NULL AND transformed_at IS NULL THEN file_name ELSE NULL END,
        ', '
    ) as pending_transform_files
FROM cnpj.download_manifest
GROUP BY reference_month
HAVING COUNT(*) < 37 
    OR SUM(CASE WHEN processing_status != 'loaded' THEN 1 ELSE 0 END) > 0
ORDER BY reference_month DESC;

-- View for files ready to process (downloaded but not yet ingested)
CREATE OR REPLACE VIEW cnpj.files_ready_to_ingest AS
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
        WHEN loaded_neo4j_at IS NULL THEN 'needs_neo4j_load'
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

-- Grant table permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cnpj TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA cnpj TO osint_admin;

\echo '✓ CNPJ tables and indexes created successfully'
