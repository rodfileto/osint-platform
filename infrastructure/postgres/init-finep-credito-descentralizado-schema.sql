-- ============================================================
-- FINEP Crédito Descentralizado Schema
-- Aba Proj__Crédito_Descentralizado do Contratacao.xlsx
-- Header: linha 6 | Dados: linhas 7..3041 (~3.035 operações)
--
-- Estrutura: cada linha é uma operação de subcrédito individual
-- a uma empresa (proponente) feita através de um agente financeiro
-- credenciado (banco). O campo 'contrato' identifica o convênio-quadro
-- FINEP ↔ Agente Financeiro — não identifica a operação individual.
--
-- Usage:
--   docker exec -i osint_postgres psql -U osint_admin -d osint_metadata \
--     < infrastructure/postgres/init-finep-credito-descentralizado-schema.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS finep.projetos_credito_descentralizado (
    id                      SERIAL          PRIMARY KEY,

    -- Identificação da operação
    data_assinatura         DATE,                       -- data do contrato com o proponente
    contrato                VARCHAR(100),               -- convênio-quadro FINEP ↔ Agente Financeiro (pode ter formato composto)
                                                        -- ex: '07.13.0001.00' → BRDE com 709 suboperações

    -- Proponente (empresa que recebe o crédito)
    proponente              TEXT,
    cnpj_proponente         VARCHAR(18),                -- formatado: XX.XXX.XXX/XXXX-XX
    cnpj_proponente_norm    VARCHAR(14)                 -- apenas dígitos para JOIN com cnpj.estabelecimento
        GENERATED ALWAYS AS (
            regexp_replace(cnpj_proponente, '[^0-9]', '', 'g')
        ) STORED,
    uf                      CHAR(2),

    -- Valores financeiros (R$)
    valor_financiado        NUMERIC(15,2),              -- valor aprovado/contratado
    valor_liberado          NUMERIC(15,2),              -- valor efetivamente desembolsado (pode ser NULL)
    contrapartida           NUMERIC(15,2),
    outros_recursos         NUMERIC(15,2),

    -- Agente financeiro intermediário
    agente_financeiro       TEXT,                       -- nome do banco/cooperativa credenciado

    -- Rastreabilidade
    manifest_id             INTEGER         REFERENCES finep.download_manifest(id) ON DELETE SET NULL,

    -- Controle
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE finep.projetos_credito_descentralizado IS
    'Operações de subcrédito do Crédito Descentralizado FINEP. '
    'Cada linha = 1 empresa financiada via agente financeiro credenciado. '
    'Aba Proj__Crédito_Descentralizado do Contratacao.xlsx. Header na linha 6.';

COMMENT ON COLUMN finep.projetos_credito_descentralizado.contrato IS
    'Convênio-quadro FINEP ↔ Agente Financeiro (ex: 07.13.0001.00 = BRDE). '
    'NÃO identifica a operação individual — um único contrato pode ter centenas '
    'de suboperações para empresas diferentes.';
COMMENT ON COLUMN finep.projetos_credito_descentralizado.valor_liberado IS
    'NULL = crédito aprovado mas ainda não desembolsado.';
COMMENT ON COLUMN finep.projetos_credito_descentralizado.cnpj_proponente_norm IS
    'CNPJ apenas com dígitos. Permite JOIN direto com cnpj.estabelecimento.';

-- ── Índices ────────────────────────────────────────────────────────────────

-- Busca por convênio (agrupamento de operações de um banco)
CREATE INDEX IF NOT EXISTS idx_finep_pcd_contrato
    ON finep.projetos_credito_descentralizado(contrato)
    WHERE contrato IS NOT NULL;

-- JOIN com CNPJ
CREATE INDEX IF NOT EXISTS idx_finep_pcd_cnpj_proponente
    ON finep.projetos_credito_descentralizado(cnpj_proponente_norm)
    WHERE cnpj_proponente_norm IS NOT NULL;

-- Filtros geográficos e temporais
CREATE INDEX IF NOT EXISTS idx_finep_pcd_uf
    ON finep.projetos_credito_descentralizado(uf);

CREATE INDEX IF NOT EXISTS idx_finep_pcd_data_assinatura
    ON finep.projetos_credito_descentralizado(data_assinatura DESC);

-- Busca por banco intermediário
CREATE INDEX IF NOT EXISTS idx_finep_pcd_agente
    ON finep.projetos_credito_descentralizado(agente_financeiro);

-- Busca textual no proponente
CREATE INDEX IF NOT EXISTS idx_finep_pcd_proponente_fts
    ON finep.projetos_credito_descentralizado
    USING GIN (to_tsvector('portuguese', coalesce(proponente, '')));

-- Combinação frequente: banco + período
CREATE INDEX IF NOT EXISTS idx_finep_pcd_contrato_data
    ON finep.projetos_credito_descentralizado(contrato, data_assinatura DESC)
    WHERE contrato IS NOT NULL;

-- Rastreabilidade
CREATE INDEX IF NOT EXISTS idx_finep_pcd_manifest_id
    ON finep.projetos_credito_descentralizado(manifest_id);

-- ── Permissões ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA finep TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA finep TO osint_admin;

\echo '✓ finep.projetos_credito_descentralizado criada com sucesso'
