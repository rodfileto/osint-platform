-- ============================================================
-- FINEP Projetos Ancine Schema
-- Aba Projetos_Ancine do Contratacao.xlsx
-- Header: linha 6 | Dados: linhas 7..181 (~175 projetos)
--
-- Projetos apoiados com recursos do Fundo Setorial do Audiovisual
-- (FSA), operado pela Finep entre 2009 e 2013. Estrutura semelhante
-- à Projetos_Operação_Direta, com algumas diferenças:
--   - Sem: Prazo Execução Original, CNPJ Executor, Resumo Publicável
--   - Instrumento fixo: 'Audiovisual'
--   - Período histórico: 2009–2013
--
-- Usage:
--   docker exec -i osint_postgres psql -U osint_admin -d osint_metadata \
--     < infrastructure/postgres/init-finep-ancine-schema.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS finep.projetos_ancine (
    id                          SERIAL          PRIMARY KEY,

    -- Identificação do projeto
    instrumento                 VARCHAR(100),               -- fixo: 'Audiovisual' (FSA)
    demanda                     TEXT,                       -- edital / chamada pública
    ref                         VARCHAR(20),                -- referência interna ex: '0088/09'
    contrato                    VARCHAR(20),                -- número do contrato ex: '02.10.0056.00'

    -- Datas
    data_assinatura             DATE,
    prazo_execucao              DATE,                       -- (sem prazo original nesta aba)

    -- Projeto audiovisual
    titulo                      TEXT,                       -- nome da obra (filme, série etc.)
    status                      VARCHAR(50),

    -- Proponente
    proponente                  TEXT,
    cnpj_proponente             VARCHAR(18),
    cnpj_proponente_norm        VARCHAR(14)
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_proponente, '[^0-9]', '', 'g')
        ) STORED,

    -- Executor (sem CNPJ nesta aba)
    executor                    TEXT,

    -- Localização
    municipio                   VARCHAR(100),
    uf                          CHAR(2),

    -- Valores financeiros (R$)
    valor_finep                 NUMERIC(15,2),
    contrapartida_financeira    NUMERIC(15,2),
    contrapartida_nao_financeira NUMERIC(15,2),
    valor_pago                  NUMERIC(15,2),

    -- Intervenientes
    intervenientes              INTEGER         DEFAULT 0,
    aporte_financeiro_interv    NUMERIC(15,2),
    aporte_nao_financeiro_interv NUMERIC(15,2),

    -- Rastreabilidade
    manifest_id                 INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.projetos_ancine IS
    'Projetos audiovisuais apoiados com recursos do FSA (Fundo Setorial do Audiovisual), '
    'operado pela Finep entre 2009 e 2013. Fonte: aba Projetos_Ancine do Contratacao.xlsx.';

COMMENT ON COLUMN finep.projetos_ancine.instrumento IS
    'Fixo: Audiovisual (FSA). Mantido para consistência com projetos_operacao_direta.';
COMMENT ON COLUMN finep.projetos_ancine.titulo IS
    'Nome da obra audiovisual (filme, série, documentário etc.).';
COMMENT ON COLUMN finep.projetos_ancine.cnpj_proponente_norm IS
    'CNPJ apenas com dígitos. Permite JOIN direto com cnpj.estabelecimento.';

-- ── Índices ────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_finep_pa_contrato
    ON finep.projetos_ancine(contrato)
    WHERE contrato IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pa_cnpj_proponente
    ON finep.projetos_ancine(cnpj_proponente_norm)
    WHERE cnpj_proponente_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pa_status
    ON finep.projetos_ancine(status);

CREATE INDEX IF NOT EXISTS idx_finep_pa_uf
    ON finep.projetos_ancine(uf);

CREATE INDEX IF NOT EXISTS idx_finep_pa_data_assinatura
    ON finep.projetos_ancine(data_assinatura DESC);

-- Busca textual: título da obra e proponente
CREATE INDEX IF NOT EXISTS idx_finep_pa_titulo_fts
    ON finep.projetos_ancine
    USING GIN (to_tsvector('portuguese', coalesce(titulo, '')));

CREATE INDEX IF NOT EXISTS idx_finep_pa_proponente_fts
    ON finep.projetos_ancine
    USING GIN (to_tsvector('portuguese', coalesce(proponente, '')));

CREATE INDEX IF NOT EXISTS idx_finep_pa_manifest_id
    ON finep.projetos_ancine(manifest_id);

-- ── Permissões ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;

\echo '✓ finep.projetos_ancine criada com sucesso'
