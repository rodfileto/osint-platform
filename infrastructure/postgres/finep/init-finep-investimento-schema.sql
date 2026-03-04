-- ============================================================
-- FINEP Projetos Investimento Schema
-- Aba Projetos_Investimento do Contratacao.xlsx
-- Header: linha 6 | Dados: linhas 7..39 (~32 projetos)
--
-- Instrumento de capital da FINEP: investimento direto em startups/
-- empresas inovadoras via opção de compra (equity). A Finep aporta
-- capital e tem direito de exercer opção de compra ou desinvestir.
-- Métricas de retorno (TIR, MOI) tornam esta aba única.
--
-- Usage:
--   docker exec -i osint_postgres psql -U osint_admin -d osint_metadata \
--     < infrastructure/postgres/init-finep-investimento-schema.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS finep.projetos_investimento (
    id                          SERIAL          PRIMARY KEY,

    -- Identificação
    ref                         VARCHAR(20),                -- ex: '0060/19'
    numero_contrato             VARCHAR(20),                -- ex: '90.20.0020.00'

    -- Empresa investida
    cnpj_proponente             VARCHAR(18),                -- formatado: XX.XXX.XXX/XXXX-XX
    cnpj_proponente_norm        VARCHAR(14)                 -- apenas dígitos para JOIN
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_proponente, '[^0-9]', '', 'g')
        ) STORED,
    proponente                  TEXT,

    -- Datas
    data_assinatura             DATE,
    data_follow_on              DATE,                       -- data do aporte follow-on (pode ser NULL)

    -- Valores (R$)
    valor_follow_on             NUMERIC(15,2),              -- aporte follow-on (NULL se não houve)
    valor_total_contratado      NUMERIC(15,2),
    valor_total_liberado        NUMERIC(15,2),

    -- Opção de compra
    opcao_compra_exercida       BOOLEAN,                    -- 'Sim'/'Não' → TRUE/FALSE
    opcao_compra_prorrogada     BOOLEAN,
    data_exercicio_opcao        DATE,
    valuation_opcao             NUMERIC(15,2),              -- valuation aplicado no exercício (R$)
    participacao_finep          NUMERIC(7,4),               -- % de participação da Finep

    -- Desinvestimento
    desinvestimento_realizado   BOOLEAN,
    data_desinvestimento        DATE,
    valor_desinvestimento       NUMERIC(15,2),

    -- Métricas de retorno
    tir                         NUMERIC(10,6),              -- Taxa Interna de Retorno (decimal, ex: 0.15 = 15%)
    moi                         NUMERIC(10,4),              -- Multiple on Investment (ex: 2.5x)

    -- Rastreabilidade
    manifest_id                 INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.projetos_investimento IS
    'Projetos apoiados pelo instrumento Investimento da FINEP (equity em startups/inovação). '
    'Inclui métricas de retorno financeiro (TIR, MOI) e dados de desinvestimento. '
    'Aba Projetos_Investimento do Contratacao.xlsx. Header na linha 6.';

COMMENT ON COLUMN finep.projetos_investimento.opcao_compra_exercida IS
    'Convertido de Sim/Não para BOOLEAN na carga.';
COMMENT ON COLUMN finep.projetos_investimento.participacao_finep IS
    'Percentual de participação da FINEP no capital da empresa (ex: 15.5000 = 15,5%).';
COMMENT ON COLUMN finep.projetos_investimento.tir IS
    'Taxa Interna de Retorno do investimento. Armazenada como decimal (0.15 = 15%).';
COMMENT ON COLUMN finep.projetos_investimento.moi IS
    'Multiple on Investment — quantas vezes o valor foi multiplicado (ex: 2.5 = 2,5x).';
COMMENT ON COLUMN finep.projetos_investimento.cnpj_proponente_norm IS
    'CNPJ apenas com dígitos. Permite JOIN direto com cnpj.estabelecimento.';

-- ── Índices ────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_finep_pi_contrato
    ON finep.projetos_investimento(numero_contrato)
    WHERE numero_contrato IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pi_cnpj
    ON finep.projetos_investimento(cnpj_proponente_norm)
    WHERE cnpj_proponente_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pi_data_assinatura
    ON finep.projetos_investimento(data_assinatura DESC);

CREATE INDEX IF NOT EXISTS idx_finep_pi_desinvestimento
    ON finep.projetos_investimento(desinvestimento_realizado, data_desinvestimento DESC)
    WHERE desinvestimento_realizado IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finep_pi_proponente_fts
    ON finep.projetos_investimento
    USING GIN (to_tsvector('portuguese', coalesce(proponente, '')));

CREATE INDEX IF NOT EXISTS idx_finep_pi_manifest_id
    ON finep.projetos_investimento(manifest_id);

-- ── Permissões ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;

\echo '✓ finep.projetos_investimento criada com sucesso'
