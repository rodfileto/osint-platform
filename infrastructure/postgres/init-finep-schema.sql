-- ============================================================
-- FINEP Schema Initialization
-- Financiadora de Estudos e Projetos — dados de contratações
-- e liberações financeiras publicados em xlsx aberto.
--
-- Usage:
--   psql -U osint_admin -d osint_metadata -f init-finep-schema.sql
-- ============================================================

-- =====================================================
-- Criar schema dedicado
-- =====================================================
CREATE SCHEMA IF NOT EXISTS finep;

-- =====================================================
-- Tabela de manifesto de downloads
-- =====================================================
-- Cada linha representa UMA execução de download para
-- um arquivo específico (Contratacao.xlsx / Liberacao.xlsx).
-- Mantemos histórico completo (sem sobrescrever linhas),
-- o que permite rastrear quando o arquivo mudou ao longo do tempo.

CREATE TABLE IF NOT EXISTS finep.download_manifest (
    id                      SERIAL PRIMARY KEY,

    -- Identificação do arquivo
    file_name               VARCHAR(255)    NOT NULL,   -- ex: 'Contratacao.xlsx'
    source_url              TEXT            NOT NULL,   -- URL completa de origem
    dataset_type            VARCHAR(50)     NOT NULL,   -- 'contratacao' | 'liberacao'

    -- Resultado do download
    download_date           TIMESTAMP       NOT NULL,   -- momento em que a coleta ocorreu
    file_size_bytes         BIGINT,
    file_checksum_sha256    VARCHAR(64),                -- hash SHA-256 do arquivo baixado
    local_path              TEXT,                       -- caminho absoluto no host Airflow

    -- Detecção de mudança via cabeçalhos HTTP
    http_last_modified      TIMESTAMP,                  -- valor de Last-Modified da resposta
    http_etag               TEXT,                       -- valor de ETag da resposta
    http_content_length     BIGINT,                     -- Content-Length declarado pelo servidor

    -- Resultado da validação básica
    is_valid                BOOLEAN         DEFAULT FALSE,
    rows_count              INTEGER,                    -- linhas encontradas no xlsx (excl. cabeçalho)
    sheet_names             TEXT[],                     -- lista de abas encontradas no arquivo

    -- Status geral
    processing_status       VARCHAR(20)     NOT NULL DEFAULT 'downloaded',
    --   downloaded  → arquivo salvo com sucesso
    --   validated   → estrutura do xlsx verificada
    --   failed      → erro no download ou validação
    error_message           TEXT,

    -- Controle
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  finep.download_manifest IS
    'Histórico de downloads dos arquivos XLSX publicados pela FINEP (Contratacao / Liberacao).';
COMMENT ON COLUMN finep.download_manifest.dataset_type IS
    'contratacao | liberacao';
COMMENT ON COLUMN finep.download_manifest.http_etag IS
    'Usado para detectar mudanças sem baixar o arquivo inteiro (HEAD request).';
COMMENT ON COLUMN finep.download_manifest.file_checksum_sha256 IS
    'SHA-256 calculado após o download; permite confirmar integridade.';
COMMENT ON COLUMN finep.download_manifest.processing_status IS
    'downloaded | validated | failed';

-- =====================================================
-- Índices
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_finep_manifest_dataset_type
    ON finep.download_manifest(dataset_type);

CREATE INDEX IF NOT EXISTS idx_finep_manifest_download_date
    ON finep.download_manifest(download_date DESC);

CREATE INDEX IF NOT EXISTS idx_finep_manifest_status
    ON finep.download_manifest(processing_status);

CREATE INDEX IF NOT EXISTS idx_finep_manifest_checksum
    ON finep.download_manifest(file_checksum_sha256)
    WHERE file_checksum_sha256 IS NOT NULL;

-- =====================================================
-- View: última versão de cada arquivo
-- =====================================================
CREATE OR REPLACE VIEW finep.latest_downloads AS
SELECT DISTINCT ON (dataset_type)
    id,
    dataset_type,
    file_name,
    source_url,
    download_date,
    file_size_bytes,
    file_checksum_sha256,
    local_path,
    http_last_modified,
    http_etag,
    rows_count,
    sheet_names,
    processing_status,
    error_message
FROM finep.download_manifest
ORDER BY dataset_type, download_date DESC;

COMMENT ON VIEW finep.latest_downloads IS
    'Última coleta bem-sucedida para cada tipo de dataset FINEP.';

-- =====================================================
-- View: histórico de alterações (quando o arquivo mudou)
-- =====================================================
CREATE OR REPLACE VIEW finep.change_history AS
SELECT
    dataset_type,
    download_date,
    file_size_bytes,
    rows_count,
    file_checksum_sha256,
    http_last_modified,
    processing_status,
    LAG(file_checksum_sha256) OVER (
        PARTITION BY dataset_type ORDER BY download_date
    ) AS previous_checksum,
    CASE
        WHEN file_checksum_sha256 IS DISTINCT FROM
             LAG(file_checksum_sha256) OVER (
                 PARTITION BY dataset_type ORDER BY download_date
             )
        THEN TRUE
        ELSE FALSE
    END AS content_changed
FROM finep.download_manifest
WHERE processing_status IN ('downloaded', 'validated')
ORDER BY dataset_type, download_date DESC;

COMMENT ON VIEW finep.change_history IS
    'Rastreamento de quando o conteúdo dos arquivos FINEP efetivamente mudou.';

-- =====================================================
-- Permissões
-- =====================================================
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;
GRANT USAGE ON SCHEMA finep TO osint_admin;

\echo '✓ Schema finep criado com sucesso'
