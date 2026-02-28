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


CREATE INDEX IF NOT EXISTS idx_socio_cnpj_basico  ON cnpj.socio(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socio_cpf_cnpj     ON cnpj.socio(cpf_cnpj_socio);
CREATE INDEX IF NOT EXISTS idx_socio_ref_month    ON cnpj.socio(reference_month);
CREATE INDEX IF NOT EXISTS idx_socio_qualificacao ON cnpj.socio(qualificacao_socio);
CREATE INDEX IF NOT EXISTS idx_socio_nome         ON cnpj.socio USING GIN (to_tsvector(CAST('portuguese' AS regconfig), nome_socio_razao_social)) WHERE nome_socio_razao_social IS NOT NULL;
GRANT ALL PRIVILEGES ON cnpj.socio TO osint_admin;
GRANT USAGE, SELECT ON SEQUENCE cnpj.socio_id_seq TO osint_admin;
