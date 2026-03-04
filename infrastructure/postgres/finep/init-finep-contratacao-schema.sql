-- ============================================================
-- FINEP Contratacao Schema
-- Tabelas para as abas da planilha Contratacao.xlsx
--
-- Abas mapeadas:
--   1. Projetos_Operação_Direta     → finep.projetos_operacao_direta
--   2. Proj__Crédito_Descentralizado → finep.projetos_credito_descentralizado (TODO)
--   3. Projetos_Investimento         → finep.projetos_investimento            (TODO)
--   4. Projetos_Ancine               → finep.projetos_ancine                  (TODO)
--
-- Usage:
--   docker exec -i osint_postgres psql -U osint_admin -d osint_metadata \
--     < infrastructure/postgres/init-finep-contratacao-schema.sql
-- ============================================================

-- ============================================================
-- 1. Projetos_Operação_Direta
--    Header: linha 6 | Dados: linhas 7..25749 (~25.743 registros)
--
-- Instrumentos observados: Crédito Direto, Não Reembolsável a ICTs,
--   Subvenção Econômica, Não Reembolsável a Empresas,
--   Crédito Descentralizado (agente), Subvenção Descentralizada (parceiro)
-- ============================================================

CREATE TABLE IF NOT EXISTS finep.projetos_operacao_direta (
    id                          SERIAL          PRIMARY KEY,

    -- Identificação do projeto
    instrumento                 VARCHAR(100)    NOT NULL,   -- ex: 'Crédito Direto', 'Subvenção Econômica'
    demanda                     TEXT,                       -- nome/edital da demanda (pode ter espaços extras)
    ref                         VARCHAR(20),                -- referência interna ex: '0816/11'
    contrato                    VARCHAR(20),                -- número do contrato ex: '02.12.0492.00'

    -- Datas
    data_assinatura             DATE,
    prazo_execucao_original     DATE,
    prazo_execucao              DATE,

    -- Projeto
    titulo                      TEXT,
    status                      VARCHAR(50),                -- ex: 'ENCERRADO', 'EM EXECUÇÃO', 'CANCELADO'

    -- Proponente
    proponente                  TEXT,
    cnpj_proponente             VARCHAR(18),                -- formatado: XX.XXX.XXX/XXXX-XX
    cnpj_proponente_norm        VARCHAR(14)                 -- apenas dígitos (gerado na carga)
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_proponente, '[^0-9]', '', 'g')
        ) STORED,

    -- Executor (pode ser diferente do proponente)
    executor                    TEXT,
    cnpj_executor               VARCHAR(18),
    cnpj_executor_norm          VARCHAR(14)
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_executor, '[^0-9]', '', 'g')
        ) STORED,

    -- Localização
    municipio                   VARCHAR(100),
    uf                          CHAR(2),

    -- Valores financeiros (R$)
    valor_finep                 NUMERIC(15,2),
    contrapartida_financeira    NUMERIC(15,2),
    contrapartida_nao_financeira NUMERIC(15,2),
    valor_pago                  NUMERIC(15,2),

    -- Intervenientes
    intervenientes              INTEGER         DEFAULT 0,  -- quantidade de intervenientes
    aporte_financeiro_interv    NUMERIC(15,2),
    aporte_nao_financeiro_interv NUMERIC(15,2),

    -- Texto livre
    resumo_publicavel           TEXT,

    -- Rastreabilidade: qual download originou este registro
    manifest_id                 INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.projetos_operacao_direta IS
    'Projetos apoiados por operação direta da FINEP: Crédito, Não Reembolsável, Subvenção. '
    'Aba Projetos_Operação_Direta da planilha Contratacao.xlsx. Header na linha 6.';

COMMENT ON COLUMN finep.projetos_operacao_direta.contrato IS
    'Número do contrato FINEP (ex: 02.12.0492.00). Não é garantidamente único — '
    'um mesmo contrato pode ter aditivos representados em linhas distintas.';
COMMENT ON COLUMN finep.projetos_operacao_direta.cnpj_proponente_norm IS
    'CNPJ do proponente apenas com dígitos. Permite JOIN com cnpj.estabelecimento.';
COMMENT ON COLUMN finep.projetos_operacao_direta.cnpj_executor_norm IS
    'CNPJ do executor apenas com dígitos. Permite JOIN com cnpj.estabelecimento.';
COMMENT ON COLUMN finep.projetos_operacao_direta.intervenientes IS
    'Quantidade de intervenientes vinculados ao projeto.';
COMMENT ON COLUMN finep.projetos_operacao_direta.manifest_id IS
    'FK para finep.download_manifest — identifica o arquivo de origem dos dados.';

-- ── Índices ────────────────────────────────────────────────────────────────

-- Busca por contrato (não unique pois pode haver linhas duplicadas por aditivo)
CREATE INDEX IF NOT EXISTS idx_finep_pod_contrato
    ON finep.projetos_operacao_direta(contrato)
    WHERE contrato IS NOT NULL;

-- Filtros operacionais frequentes
CREATE INDEX IF NOT EXISTS idx_finep_pod_instrumento
    ON finep.projetos_operacao_direta(instrumento);

CREATE INDEX IF NOT EXISTS idx_finep_pod_status
    ON finep.projetos_operacao_direta(status);

CREATE INDEX IF NOT EXISTS idx_finep_pod_uf
    ON finep.projetos_operacao_direta(uf);

CREATE INDEX IF NOT EXISTS idx_finep_pod_data_assinatura
    ON finep.projetos_operacao_direta(data_assinatura DESC);

-- JOIN com CNPJ (coluna normalizada gerada automaticamente)
CREATE INDEX IF NOT EXISTS idx_finep_pod_cnpj_proponente
    ON finep.projetos_operacao_direta(cnpj_proponente_norm)
    WHERE cnpj_proponente_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pod_cnpj_executor
    ON finep.projetos_operacao_direta(cnpj_executor_norm)
    WHERE cnpj_executor_norm IS NOT NULL;

-- Busca textual em título e proponente
CREATE INDEX IF NOT EXISTS idx_finep_pod_titulo_fts
    ON finep.projetos_operacao_direta
    USING GIN (to_tsvector('portuguese', coalesce(titulo, '')));

CREATE INDEX IF NOT EXISTS idx_finep_pod_proponente_fts
    ON finep.projetos_operacao_direta
    USING GIN (to_tsvector('portuguese', coalesce(proponente, '')));

-- Rastreabilidade
CREATE INDEX IF NOT EXISTS idx_finep_pod_manifest_id
    ON finep.projetos_operacao_direta(manifest_id);

-- ── Permissões ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;

\echo '✓ finep.projetos_operacao_direta criada com sucesso'
