-- ============================================================
-- INPI Patents Tables
-- Instituto Nacional da Propriedade Industrial
-- Source: dadosabertos.inpi.gov.br/index/patentes/
--
-- Load strategy: TRUNCATE + INSERT per full snapshot.
-- All tables carry snapshot_date (derived from HTTP Last-Modified)
-- so a full history of which snapshot produced the current load
-- is available via inpi.download_manifest.
--
-- Cross-domain linkage:
--   cnpj_basico_resolved on patentes_depositantes and
--   patentes_procuradores is populated at load time when
--   tipopessoa = 'pessoa jurídica' AND cgccpf ~ '^\d{14}$'.
--   Used for JOIN against cnpj.empresa and for Neo4j relationships.
--   No FK constraint — see architecture notes in ARCHITECTURE_PLAN.md.
-- ============================================================

-- Required extension for trigram search (may already exist from cnpj schema)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =====================================================
-- 1. patentes_dados_bibliograficos
--    Central table — one row per patent.
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_dados_bibliograficos (
    codigo_interno              INTEGER         PRIMARY KEY,
    numero_inpi                 VARCHAR(20)     NOT NULL,
    -- tipo_patente derived from numero_inpi prefix at load time:
    -- PI = Patente de Invenção, MU = Modelo de Utilidade,
    -- PP = Proteção de Planta, MI/DI = legacy designations
    tipo_patente                VARCHAR(5),
    data_deposito               DATE,
    data_protocolo              DATE,
    data_publicacao             DATE,
    numero_pct                  VARCHAR(30),
    numero_wo                   VARCHAR(30),
    data_publicacao_wo          DATE,
    data_entrada_fase_nacional  DATE,
    sigilo                      BOOLEAN         DEFAULT FALSE,
    snapshot_date               DATE            NOT NULL,
    loaded_at                   TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE inpi.patentes_dados_bibliograficos IS
    'Dados bibliográficos de patentes — uma linha por pedido. '
    'Carga via TRUNCATE + INSERT por snapshot completo.';
COMMENT ON COLUMN inpi.patentes_dados_bibliograficos.tipo_patente IS
    'Prefixo do numero_inpi: PI | MU | PP | MI | DI';
COMMENT ON COLUMN inpi.patentes_dados_bibliograficos.sigilo IS
    'Convertido de float 1.0/NULL na fonte para BOOLEAN no load.';

-- =====================================================
-- 2. patentes_conteudo
--    Título e resumo — separados para não penalizar scans
--    de dados_bibliograficos com TEXT longo.
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_conteudo (
    codigo_interno  INTEGER     PRIMARY KEY
                                REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                ON DELETE CASCADE,
    numero_inpi     VARCHAR(20),
    titulo          TEXT,
    resumo          TEXT,           -- pode conter tags HTML residuais da fonte
    snapshot_date   DATE            NOT NULL,
    loaded_at       TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE inpi.patentes_conteudo IS
    'Título e resumo de cada patente. Resumo pode conter HTML residual da fonte.';

-- =====================================================
-- 3. patentes_inventores
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_inventores (
    codigo_interno  INTEGER         NOT NULL
                                    REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                    ON DELETE CASCADE,
    ordem           SMALLINT        NOT NULL,
    autor           VARCHAR(500),
    pais            VARCHAR(5),
    estado          VARCHAR(5),
    snapshot_date   DATE            NOT NULL,
    loaded_at       TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno, ordem)
);

COMMENT ON TABLE inpi.patentes_inventores IS
    'Inventores de cada patente. Chave: (codigo_interno, ordem).';

-- =====================================================
-- 4. patentes_depositantes
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_depositantes (
    codigo_interno          INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    ordem                   SMALLINT        NOT NULL,
    pais                    VARCHAR(5),
    estado                  VARCHAR(5),
    depositante             VARCHAR(500),
    cgccpfdepositante       VARCHAR(30),
    -- Valor bruto preservado: pode ser CNPJ (14 dígitos), CPF mascarado,
    -- 'N/D', 'estrangeiro', etc.
    tipopessoadepositante   VARCHAR(30),
    data_inicio             DATE,
    data_fim                DATE,
    -- Resolução cross-domain: preenchido no load quando tipo = 'pessoa jurídica'
    -- e cgccpfdepositante é um CNPJ numérico de 14 dígitos.
    cnpj_basico_resolved    VARCHAR(8),
    snapshot_date           DATE            NOT NULL,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno, ordem)
);

COMMENT ON TABLE inpi.patentes_depositantes IS
    'Depositantes de cada patente. Chave: (codigo_interno, ordem).';
COMMENT ON COLUMN inpi.patentes_depositantes.cgccpfdepositante IS
    'Valor bruto da fonte: CNPJ, CPF mascarado, N/D, estrangeiro, etc.';
COMMENT ON COLUMN inpi.patentes_depositantes.cnpj_basico_resolved IS
    'LEFT(cgccpfdepositante, 8) quando tipo=pessoa jurídica e valor é CNPJ numérico. '
    'Usado para JOIN com cnpj.empresa sem FK hard. '
    'NULL para pessoas físicas, estrangeiros e valores não numéricos.';

-- =====================================================
-- 5. patentes_classificacao_ipc
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_classificacao_ipc (
    codigo_interno  INTEGER         NOT NULL
                                    REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                    ON DELETE CASCADE,
    ordem           SMALLINT        NOT NULL,
    simbolo         VARCHAR(20),    -- ex: B42F 11/02, F41A 9/00
    versao          VARCHAR(10),    -- ex: 1968.09, 2009.01
    snapshot_date   DATE            NOT NULL,
    loaded_at       TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno, ordem)
);

COMMENT ON TABLE inpi.patentes_classificacao_ipc IS
    'Classificação IPC de cada patente. '
    'Prefixo do símbolo identifica área: F41/F42=defesa, B64=aeroespacial, etc.';

-- =====================================================
-- 6. patentes_despachos
--    Muitos despachos por patente — SERIAL como PK.
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_despachos (
    id                      BIGSERIAL       PRIMARY KEY,
    codigo_interno          INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    numero_rpi              INTEGER,        -- número da Revista da Propriedade Industrial
    data_rpi                DATE,
    codigo_despacho         VARCHAR(20),    -- ex: 11.1, 3.1, 6.6
    complemento_despacho    TEXT,
    snapshot_date           DATE            NOT NULL,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE inpi.patentes_despachos IS
    'Histórico de despachos (atos administrativos) de cada patente. '
    'Pode haver centenas de despachos por patente ao longo do tempo.';
COMMENT ON COLUMN inpi.patentes_despachos.numero_rpi IS
    'Número da edição da Revista da Propriedade Industrial onde o despacho foi publicado.';

-- =====================================================
-- 7. patentes_procuradores
--    BIGSERIAL como PK — data_inicio pode ser nula.
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_procuradores (
    id                      BIGSERIAL       PRIMARY KEY,
    codigo_interno          INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    procurador              VARCHAR(500),
    cgccpfprocurador        VARCHAR(30),
    tipopessoaprocurador    VARCHAR(30),
    data_inicio             DATE,
    -- Resolução cross-domain: mesmo critério de patentes_depositantes
    cnpj_basico_resolved    VARCHAR(8),
    snapshot_date           DATE            NOT NULL,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE inpi.patentes_procuradores IS
    'Procuradores (representantes legais) de cada patente.';
COMMENT ON COLUMN inpi.patentes_procuradores.cnpj_basico_resolved IS
    'LEFT(cgccpfprocurador, 8) quando tipo=Pessoa Jurídica e valor é CNPJ numérico. '
    'NULL caso contrário.';

-- =====================================================
-- 8. patentes_prioridades
--    Prioridades unionistas (País de Origem).
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_prioridades (
    codigo_interno      INTEGER         NOT NULL
                                        REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                        ON DELETE CASCADE,
    pais_prioridade     VARCHAR(5)      NOT NULL,
    numero_prioridade   VARCHAR(50)     NOT NULL,
    data_prioridade     DATE,
    snapshot_date       DATE            NOT NULL,
    loaded_at           TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno, pais_prioridade, numero_prioridade)
);

COMMENT ON TABLE inpi.patentes_prioridades IS
    'Prioridades unionistas de cada patente (Convenção de Paris / PCT).';

-- =====================================================
-- 9. patentes_vinculos
--    Relações entre pedidos (derivação, incorporação).
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_vinculos (
    codigo_interno_derivado INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    codigo_interno_origem   INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    data_vinculo            DATE,
    tipo_vinculo            VARCHAR(5),     -- A = Alteração, I = Incorporação
    snapshot_date           DATE            NOT NULL,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno_derivado, codigo_interno_origem, tipo_vinculo)
);

COMMENT ON TABLE inpi.patentes_vinculos IS
    'Vínculos entre pedidos de patente. '
    'tipo_vinculo: A=Alteração, I=Incorporação.';

-- =====================================================
-- 10. patentes_renumeracoes
--     Números INPI antigos que foram renumerados.
-- =====================================================
CREATE TABLE IF NOT EXISTS inpi.patentes_renumeracoes (
    codigo_interno          INTEGER         NOT NULL
                                            REFERENCES inpi.patentes_dados_bibliograficos(codigo_interno)
                                            ON DELETE CASCADE,
    numero_inpi_original    VARCHAR(20)     NOT NULL,
    snapshot_date           DATE            NOT NULL,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (codigo_interno, numero_inpi_original)
);

COMMENT ON TABLE inpi.patentes_renumeracoes IS
    'Mapeamento de números INPI antigos para o codigo_interno atual.';

-- =====================================================
-- Indexes
-- =====================================================

-- dados_bibliograficos
CREATE INDEX IF NOT EXISTS idx_inpi_bib_numero_inpi
    ON inpi.patentes_dados_bibliograficos (numero_inpi);
CREATE INDEX IF NOT EXISTS idx_inpi_bib_tipo_patente
    ON inpi.patentes_dados_bibliograficos (tipo_patente);
CREATE INDEX IF NOT EXISTS idx_inpi_bib_data_deposito
    ON inpi.patentes_dados_bibliograficos (data_deposito);
CREATE INDEX IF NOT EXISTS idx_inpi_bib_data_publicacao
    ON inpi.patentes_dados_bibliograficos (data_publicacao);
CREATE INDEX IF NOT EXISTS idx_inpi_bib_snapshot
    ON inpi.patentes_dados_bibliograficos (snapshot_date);

-- conteudo — full-text search em título e resumo
CREATE INDEX IF NOT EXISTS idx_inpi_conteudo_titulo_trgm
    ON inpi.patentes_conteudo USING gin (titulo gin_trgm_ops)
    WHERE titulo IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_conteudo_fts
    ON inpi.patentes_conteudo USING gin (
        to_tsvector('portuguese', coalesce(titulo, '') || ' ' || coalesce(resumo, ''))
    );

-- inventores
CREATE INDEX IF NOT EXISTS idx_inpi_inventores_autor_trgm
    ON inpi.patentes_inventores USING gin (autor gin_trgm_ops)
    WHERE autor IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_inventores_pais
    ON inpi.patentes_inventores (pais);

-- depositantes — busca por nome e resolução CNPJ
CREATE INDEX IF NOT EXISTS idx_inpi_depositantes_depositante_trgm
    ON inpi.patentes_depositantes USING gin (depositante gin_trgm_ops)
    WHERE depositante IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_depositantes_cnpj_resolved
    ON inpi.patentes_depositantes (cnpj_basico_resolved)
    WHERE cnpj_basico_resolved IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_depositantes_cgccpf
    ON inpi.patentes_depositantes (cgccpfdepositante)
    WHERE cgccpfdepositante IS NOT NULL;

-- classificacao_ipc — busca por símbolo e prefixo de área técnica
CREATE INDEX IF NOT EXISTS idx_inpi_ipc_simbolo
    ON inpi.patentes_classificacao_ipc (simbolo);
CREATE INDEX IF NOT EXISTS idx_inpi_ipc_simbolo_prefix
    ON inpi.patentes_classificacao_ipc (left(simbolo, 4));

-- despachos — busca por código de despacho e data
CREATE INDEX IF NOT EXISTS idx_inpi_despachos_codigo_interno
    ON inpi.patentes_despachos (codigo_interno);
CREATE INDEX IF NOT EXISTS idx_inpi_despachos_codigo_despacho
    ON inpi.patentes_despachos (codigo_despacho);
CREATE INDEX IF NOT EXISTS idx_inpi_despachos_data_rpi
    ON inpi.patentes_despachos (data_rpi);

-- procuradores — resolução CNPJ
CREATE INDEX IF NOT EXISTS idx_inpi_procuradores_codigo_interno
    ON inpi.patentes_procuradores (codigo_interno);
CREATE INDEX IF NOT EXISTS idx_inpi_procuradores_cnpj_resolved
    ON inpi.patentes_procuradores (cnpj_basico_resolved)
    WHERE cnpj_basico_resolved IS NOT NULL;

-- vinculos
CREATE INDEX IF NOT EXISTS idx_inpi_vinculos_origem
    ON inpi.patentes_vinculos (codigo_interno_origem);

-- =====================================================
-- Materialized view — busca de patentes
--    Desnormalizada: bib + conteudo + depositante principal
--    + classificação IPC primária.
--    Estrutura criada vazia; populada pelo DAG inpi_load_postgres.
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS inpi.mv_patent_search AS
SELECT
    b.codigo_interno,
    b.numero_inpi,
    b.tipo_patente,
    b.data_deposito,
    b.data_publicacao,
    b.sigilo,
    b.snapshot_date,
    c.titulo,
    -- Primeiro inventor
    inv.autor                       AS inventor_principal,
    inv.pais                        AS inventor_pais,
    -- Primeiro depositante
    dep.depositante                 AS depositante_principal,
    dep.tipopessoadepositante       AS depositante_tipo,
    dep.cnpj_basico_resolved        AS depositante_cnpj_basico,
    -- Classificação IPC primária
    ipc.simbolo                     AS ipc_principal,
    left(ipc.simbolo, 4)            AS ipc_secao_classe        -- ex: F41A
FROM inpi.patentes_dados_bibliograficos b
LEFT JOIN inpi.patentes_conteudo c
    ON b.codigo_interno = c.codigo_interno
LEFT JOIN inpi.patentes_inventores inv
    ON b.codigo_interno = inv.codigo_interno AND inv.ordem = 1
LEFT JOIN inpi.patentes_depositantes dep
    ON b.codigo_interno = dep.codigo_interno AND dep.ordem = 1
LEFT JOIN inpi.patentes_classificacao_ipc ipc
    ON b.codigo_interno = ipc.codigo_interno AND ipc.ordem = 1
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW inpi.mv_patent_search IS
    'Visão desnormalizada para busca de patentes. '
    'Populada/atualizada pelo DAG inpi_load_postgres. '
    'Primeira carga: REFRESH MATERIALIZED VIEW inpi.mv_patent_search; '
    'Subsequentes: REFRESH MATERIALIZED VIEW CONCURRENTLY inpi.mv_patent_search;';

-- Indexes on matview
CREATE UNIQUE INDEX IF NOT EXISTS idx_inpi_mv_codigo_interno
    ON inpi.mv_patent_search (codigo_interno);
CREATE INDEX IF NOT EXISTS idx_inpi_mv_numero_inpi
    ON inpi.mv_patent_search (numero_inpi);
CREATE INDEX IF NOT EXISTS idx_inpi_mv_tipo_patente
    ON inpi.mv_patent_search (tipo_patente);
CREATE INDEX IF NOT EXISTS idx_inpi_mv_titulo_trgm
    ON inpi.mv_patent_search USING gin (titulo gin_trgm_ops)
    WHERE titulo IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_mv_inventor_trgm
    ON inpi.mv_patent_search USING gin (inventor_principal gin_trgm_ops)
    WHERE inventor_principal IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_mv_depositante_trgm
    ON inpi.mv_patent_search USING gin (depositante_principal gin_trgm_ops)
    WHERE depositante_principal IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_mv_depositante_cnpj
    ON inpi.mv_patent_search (depositante_cnpj_basico)
    WHERE depositante_cnpj_basico IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inpi_mv_ipc_secao
    ON inpi.mv_patent_search (ipc_secao_classe);
CREATE INDEX IF NOT EXISTS idx_inpi_mv_data_deposito
    ON inpi.mv_patent_search (data_deposito);

-- =====================================================
-- Grants
-- =====================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inpi TO osint_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA inpi TO osint_admin;
