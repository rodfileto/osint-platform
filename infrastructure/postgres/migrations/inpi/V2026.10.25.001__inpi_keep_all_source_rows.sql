-- INPI reuses (codigo_interno, ordem) for distinct rows: successive holders of the same
-- applicant position (holder history, with data_inicio / data_fim), distinct inventors and
-- distinct IPC symbols. The natural keys made the loader discard them, so key these tables
-- by a surrogate id and keep every distinct row; the loader drops only exact repeats.

ALTER TABLE inpi.patentes_inventores
    DROP CONSTRAINT patentes_inventores_pkey,
    ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE INDEX IF NOT EXISTS idx_inpi_inventores_codigo_ordem
    ON inpi.patentes_inventores (codigo_interno, ordem);

ALTER TABLE inpi.patentes_depositantes
    DROP CONSTRAINT patentes_depositantes_pkey,
    ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE INDEX IF NOT EXISTS idx_inpi_depositantes_codigo_ordem
    ON inpi.patentes_depositantes (codigo_interno, ordem);

ALTER TABLE inpi.patentes_classificacao_ipc
    DROP CONSTRAINT patentes_classificacao_ipc_pkey,
    ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE INDEX IF NOT EXISTS idx_inpi_classificacao_codigo_ordem
    ON inpi.patentes_classificacao_ipc (codigo_interno, ordem);

-- A few priorities have no country; they were dropped because the country was part of the key.
ALTER TABLE inpi.patentes_prioridades
    DROP CONSTRAINT patentes_prioridades_pkey,
    ALTER COLUMN pais_prioridade DROP NOT NULL,
    ALTER COLUMN numero_prioridade DROP NOT NULL,
    ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE INDEX IF NOT EXISTS idx_inpi_prioridades_codigo
    ON inpi.patentes_prioridades (codigo_interno);

-- The search view joined ordem = 1, which now can match several rows. Pick one row per patent:
-- the current holder (no data_fim, latest data_inicio), and the first inventor and IPC symbol.
DROP MATERIALIZED VIEW inpi.mv_patent_search;

CREATE MATERIALIZED VIEW inpi.mv_patent_search AS
SELECT
    b.codigo_interno,
    b.numero_inpi,
    b.tipo_patente,
    b.data_deposito,
    b.data_publicacao,
    b.sigilo,
    b.snapshot_date,
    c.titulo,
    inv.autor                   AS inventor_principal,
    inv.pais                    AS inventor_pais,
    dep.depositante             AS depositante_principal,
    dep.tipopessoadepositante   AS depositante_tipo,
    dep.cnpj_basico_resolved    AS depositante_cnpj_basico,
    ipc.simbolo                 AS ipc_principal,
    left(ipc.simbolo, 4)        AS ipc_secao_classe
FROM inpi.patentes_dados_bibliograficos b
LEFT JOIN inpi.patentes_conteudo c
    ON c.codigo_interno = b.codigo_interno
LEFT JOIN LATERAL (
    SELECT autor, pais FROM inpi.patentes_inventores i
    WHERE i.codigo_interno = b.codigo_interno
    ORDER BY i.ordem, i.id
    LIMIT 1
) inv ON true
LEFT JOIN LATERAL (
    SELECT depositante, tipopessoadepositante, cnpj_basico_resolved FROM inpi.patentes_depositantes d
    WHERE d.codigo_interno = b.codigo_interno
    ORDER BY d.ordem, d.data_fim IS NOT NULL, d.data_inicio DESC NULLS LAST, d.id
    LIMIT 1
) dep ON true
LEFT JOIN LATERAL (
    SELECT simbolo FROM inpi.patentes_classificacao_ipc p
    WHERE p.codigo_interno = b.codigo_interno
    ORDER BY p.ordem, p.id
    LIMIT 1
) ipc ON true
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW inpi.mv_patent_search IS
    'Patent search view: one row per patent with its first inventor, current first applicant '
    'and first IPC symbol. Refreshed by the inpi_pipeline DAG.';

CREATE UNIQUE INDEX idx_inpi_mv_codigo_interno ON inpi.mv_patent_search (codigo_interno);
CREATE INDEX idx_inpi_mv_numero_inpi ON inpi.mv_patent_search (numero_inpi);
CREATE INDEX idx_inpi_mv_tipo_patente ON inpi.mv_patent_search (tipo_patente);
CREATE INDEX idx_inpi_mv_data_deposito ON inpi.mv_patent_search (data_deposito);
CREATE INDEX idx_inpi_mv_ipc_secao ON inpi.mv_patent_search (ipc_secao_classe);
CREATE INDEX idx_inpi_mv_titulo_trgm ON inpi.mv_patent_search
    USING gin (titulo gin_trgm_ops) WHERE titulo IS NOT NULL;
CREATE INDEX idx_inpi_mv_inventor_trgm ON inpi.mv_patent_search
    USING gin (inventor_principal gin_trgm_ops) WHERE inventor_principal IS NOT NULL;
CREATE INDEX idx_inpi_mv_depositante_trgm ON inpi.mv_patent_search
    USING gin (depositante_principal gin_trgm_ops) WHERE depositante_principal IS NOT NULL;
CREATE INDEX idx_inpi_mv_depositante_cnpj ON inpi.mv_patent_search (depositante_cnpj_basico)
    WHERE depositante_cnpj_basico IS NOT NULL;
