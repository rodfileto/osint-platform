-- ============================================================
-- INPI Schema — Patentes
-- Instituto Nacional da Propriedade Industrial
-- Dados abertos publicados em CSV via dadosabertos.inpi.gov.br
-- ============================================================

CREATE SCHEMA IF NOT EXISTS inpi;
GRANT ALL PRIVILEGES ON SCHEMA inpi TO osint_admin;

-- =====================================================
-- Tabela de manifesto de downloads
-- =====================================================
-- Cada linha representa UMA execução de download para
-- um arquivo CSV específico do portal de dados abertos do INPI.
-- Mantemos histórico completo (sem sobrescrever linhas), o que
-- permite rastrear quando um arquivo mudou ao longo do tempo
-- e identificar o snapshot correspondente no MinIO.

CREATE TABLE IF NOT EXISTS inpi.download_manifest (
    id                      SERIAL PRIMARY KEY,

    -- Identificação do arquivo
    file_name               VARCHAR(255)    NOT NULL,
    -- ex: 'PATENTES_DADOS_BIBLIOGRAFICOS.csv'
    source_url              TEXT            NOT NULL,
    -- URL completa de origem
    dataset_name            VARCHAR(100)    NOT NULL,
    -- ex: 'patentes_dados_bibliograficos'

    -- Snapshot MinIO onde o arquivo foi armazenado
    minio_key               TEXT            NOT NULL,
    -- ex: 'inpi/patentes/20260328/PATENTES_DADOS_BIBLIOGRAFICOS.csv'
    minio_bucket            VARCHAR(100)    NOT NULL DEFAULT 'osint-raw',
    snapshot_date           DATE            NOT NULL,
    -- data derivada do header Last-Modified

    -- Resultado do download
    download_date           TIMESTAMPTZ     NOT NULL,
    file_size_bytes         BIGINT,
    file_checksum_sha256    VARCHAR(64),
    -- hash SHA-256 calculado após o download

    -- Detecção de mudança via cabeçalhos HTTP
    http_last_modified      TIMESTAMPTZ,
    http_etag               TEXT,
    http_content_length     BIGINT,

    -- Validação básica do CSV
    is_valid                BOOLEAN         DEFAULT FALSE,
    row_count               BIGINT,
    -- linhas contadas no CSV (excluindo cabeçalho)
    column_names            TEXT[],
    -- nomes das colunas encontradas no cabeçalho

    -- Status geral
    processing_status       VARCHAR(20)     NOT NULL DEFAULT 'downloaded',
    --   downloaded  → arquivo salvo no MinIO com sucesso
    --   failed      → erro no download ou upload
    error_message           TEXT,

    -- Controle
    created_at              TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,

    -- Unicidade: um snapshot por dataset por data
    CONSTRAINT uq_inpi_manifest_minio_key UNIQUE (minio_key)
);

COMMENT ON TABLE inpi.download_manifest IS
    'Histórico de downloads dos CSVs de patentes publicados pelo INPI (dadosabertos.inpi.gov.br). '
    'Cada linha corresponde a um arquivo de um snapshot específico armazenado no MinIO.';
COMMENT ON COLUMN inpi.download_manifest.dataset_name IS
    'Nome lógico do dataset: patentes_dados_bibliograficos | patentes_conteudo | '
    'patentes_inventores | patentes_depositantes | patentes_classificacao_ipc | '
    'patentes_despachos | patentes_procuradores | patentes_prioridades | '
    'patentes_vinculos | patentes_renumeracoes';
COMMENT ON COLUMN inpi.download_manifest.minio_key IS
    'Caminho completo do objeto no MinIO: inpi/patentes/<YYYYMMDD>/<FILENAME>.csv';
COMMENT ON COLUMN inpi.download_manifest.snapshot_date IS
    'Data do snapshot derivada do header HTTP Last-Modified do arquivo remoto.';
COMMENT ON COLUMN inpi.download_manifest.http_etag IS
    'Usado para detectar mudanças sem baixar o arquivo inteiro (HEAD request).';
COMMENT ON COLUMN inpi.download_manifest.file_checksum_sha256 IS
    'SHA-256 calculado após o download; permite confirmar integridade do objeto no MinIO.';
COMMENT ON COLUMN inpi.download_manifest.processing_status IS
    'downloaded | failed';

-- =====================================================
-- Índices
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_inpi_manifest_dataset_name
    ON inpi.download_manifest (dataset_name);

CREATE INDEX IF NOT EXISTS idx_inpi_manifest_snapshot_date
    ON inpi.download_manifest (snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_inpi_manifest_download_date
    ON inpi.download_manifest (download_date DESC);

CREATE INDEX IF NOT EXISTS idx_inpi_manifest_status
    ON inpi.download_manifest (processing_status);

CREATE INDEX IF NOT EXISTS idx_inpi_manifest_checksum
    ON inpi.download_manifest (file_checksum_sha256)
    WHERE file_checksum_sha256 IS NOT NULL;

-- =====================================================
-- View: último snapshot de cada dataset
-- =====================================================
CREATE OR REPLACE VIEW inpi.v_latest_download AS
SELECT DISTINCT ON (dataset_name)
    id,
    dataset_name,
    file_name,
    minio_key,
    snapshot_date,
    download_date,
    file_size_bytes,
    file_checksum_sha256,
    row_count,
    http_etag,
    http_last_modified,
    processing_status,
    error_message
FROM inpi.download_manifest
WHERE processing_status = 'downloaded'
ORDER BY dataset_name, snapshot_date DESC, download_date DESC;

COMMENT ON VIEW inpi.v_latest_download IS
    'Última versão baixada com sucesso de cada dataset INPI.';
