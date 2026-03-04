-- ============================================================
-- FINEP Liberações Schema
-- Abas de Liberacao.xlsx: eventos de pagamento (parcelas)
--
-- Modelo normalizado: apenas os campos delta de cada liberação.
-- Contexto do contrato (proponente, título, status etc.) via JOIN
-- com as tabelas de contratação usando (contrato, ref).
--
-- Tabelas:
--   finep.liberacoes_operacao_direta        (~33.800 linhas)
--   finep.liberacoes_credito_descentralizado (~4.700 linhas)
--   finep.liberacoes_ancine                  (~300 linhas)
--
-- Usage:
--   docker exec -i osint_postgres psql -U osint_admin -d osint_metadata \
--     < infrastructure/postgres/init-finep-liberacoes-schema.sql
-- ============================================================


-- ── Operação Direta ────────────────────────────────────────────────────────
-- Parcelas de contratos da aba Projetos_Operação_Direta.
-- JOIN com finep.projetos_operacao_direta via (contrato, ref).

CREATE TABLE IF NOT EXISTS finep.liberacoes_operacao_direta (
    id                  SERIAL          PRIMARY KEY,

    -- Chaves de JOIN com projetos_operacao_direta
    contrato            VARCHAR(20)     NOT NULL,   -- ex: '01.08.0408.01'
    ref                 VARCHAR(20),                -- ex: '0371/08'

    -- Evento de liberação
    num_parcela         SMALLINT,                   -- Nº Parcela
    num_liberacao       SMALLINT,                   -- Nº Liberação
    data_liberacao      DATE,
    valor_liberado      NUMERIC(15,2),

    -- Rastreabilidade
    manifest_id         INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.liberacoes_operacao_direta IS
    'Parcelas/liberações de projetos de Operação Direta da FINEP. '
    'Cada linha representa um desembolso individual. '
    'JOIN com finep.projetos_operacao_direta via (contrato, ref). '
    'Fonte: aba Projetos_Operação_Direta do Liberacao.xlsx.';

CREATE INDEX IF NOT EXISTS idx_finep_lod_contrato
    ON finep.liberacoes_operacao_direta(contrato);

CREATE INDEX IF NOT EXISTS idx_finep_lod_contrato_ref
    ON finep.liberacoes_operacao_direta(contrato, ref);

CREATE INDEX IF NOT EXISTS idx_finep_lod_data
    ON finep.liberacoes_operacao_direta(data_liberacao DESC);

CREATE INDEX IF NOT EXISTS idx_finep_lod_manifest
    ON finep.liberacoes_operacao_direta(manifest_id);


-- ── Crédito Descentralizado ────────────────────────────────────────────────
-- Parcelas de contratos de crédito intermediados por agentes financeiros.
-- JOIN com finep.projetos_credito_descentralizado via (contrato).
--
-- Mantém cnpj_proponente_norm para JOIN direto com cnpj.estabelecimento
-- sem precisar passar pela tabela de contratação.

CREATE TABLE IF NOT EXISTS finep.liberacoes_credito_descentralizado (
    id                      SERIAL          PRIMARY KEY,

    -- Chave de JOIN com projetos_credito_descentralizado
    contrato                VARCHAR(100)    NOT NULL,   -- pode conter formatos compostos ex: '5002035-2023.020469-6 / 5002044-2023.027147-3'

    -- Identificação do beneficiário (evita JOIN duplo para consultas CNPJ)
    cnpj_proponente         VARCHAR(18),
    cnpj_proponente_norm    VARCHAR(14)
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_proponente, '[^0-9]', '', 'g')
        ) STORED,

    -- Evento de liberação
    num_parcela             SMALLINT,
    data_liberacao          DATE,
    valor_liberado          NUMERIC(15,2),

    -- Rastreabilidade
    manifest_id             INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.liberacoes_credito_descentralizado IS
    'Parcelas de operações de Crédito Descentralizado FINEP/agentes financeiros. '
    'cnpj_proponente_norm permite JOIN direto com cnpj.estabelecimento. '
    'JOIN com finep.projetos_credito_descentralizado via (contrato). '
    'Fonte: aba Proj__Crédito_Descentralizado do Liberacao.xlsx.';

CREATE INDEX IF NOT EXISTS idx_finep_lcd_contrato
    ON finep.liberacoes_credito_descentralizado(contrato);

CREATE INDEX IF NOT EXISTS idx_finep_lcd_cnpj
    ON finep.liberacoes_credito_descentralizado(cnpj_proponente_norm)
    WHERE cnpj_proponente_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_lcd_data
    ON finep.liberacoes_credito_descentralizado(data_liberacao DESC);

CREATE INDEX IF NOT EXISTS idx_finep_lcd_manifest
    ON finep.liberacoes_credito_descentralizado(manifest_id);


-- ── Ancine ─────────────────────────────────────────────────────────────────
-- Parcelas de projetos audiovisuais do FSA.
-- JOIN com finep.projetos_ancine via (contrato, ref).

CREATE TABLE IF NOT EXISTS finep.liberacoes_ancine (
    id                  SERIAL          PRIMARY KEY,

    -- Chaves de JOIN com projetos_ancine
    contrato            VARCHAR(20)     NOT NULL,
    ref                 VARCHAR(20),

    -- Evento de liberação
    num_parcela         SMALLINT,
    num_liberacao       SMALLINT,
    data_liberacao      DATE,
    valor_liberado      NUMERIC(15,2),

    -- Rastreabilidade
    manifest_id         INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.liberacoes_ancine IS
    'Parcelas de projetos audiovisuais FSA (Ancine/FINEP). '
    'JOIN com finep.projetos_ancine via (contrato, ref). '
    'Fonte: aba Projetos_Ancine do Liberacao.xlsx.';

CREATE INDEX IF NOT EXISTS idx_finep_la_contrato
    ON finep.liberacoes_ancine(contrato);

CREATE INDEX IF NOT EXISTS idx_finep_la_contrato_ref
    ON finep.liberacoes_ancine(contrato, ref);

CREATE INDEX IF NOT EXISTS idx_finep_la_data
    ON finep.liberacoes_ancine(data_liberacao DESC);

CREATE INDEX IF NOT EXISTS idx_finep_la_manifest
    ON finep.liberacoes_ancine(manifest_id);


-- ── Permissões ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;

\echo '✓ finep.liberacoes_operacao_direta criada'
\echo '✓ finep.liberacoes_credito_descentralizado criada'
\echo '✓ finep.liberacoes_ancine criada'
