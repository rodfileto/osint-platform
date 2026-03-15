SET client_min_messages TO WARNING;

TRUNCATE TABLE
    finep.liberacoes_ancine,
    finep.liberacoes_credito_descentralizado,
    finep.liberacoes_operacao_direta,
    finep.projetos_ancine,
    finep.projetos_investimento,
    finep.projetos_credito_descentralizado,
    finep.projetos_operacao_direta,
    cnpj.download_manifest,
    cnpj.simples_nacional,
    cnpj.socio,
    cnpj.estabelecimento,
    cnpj.empresa,
    cnpj.qualificacao_socio,
    cnpj.pais,
    cnpj.natureza_juridica,
    cnpj.municipio,
    cnpj.motivo_situacao_cadastral,
    cnpj.cnae
RESTART IDENTITY CASCADE;

INSERT INTO cnpj.cnae (cnae_fiscal, descricao) VALUES
    ('6201501', 'DESENVOLVIMENTO DE PROGRAMAS DE COMPUTADOR SOB ENCOMENDA'),
    ('3511501', 'GERACAO DE ENERGIA ELETRICA'),
    ('5911102', 'PRODUCAO DE FILMES PARA PUBLICIDADE'),
    ('7210000', 'PESQUISA E DESENVOLVIMENTO EXPERIMENTAL EM CIENCIAS FISICAS E NATURAIS');

INSERT INTO cnpj.motivo_situacao_cadastral (codigo, descricao) VALUES
    ('01', 'EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA'),
    ('00', 'SEM MOTIVO INFORMADO');

INSERT INTO cnpj.municipio (codigo, nome) VALUES
    ('7107', 'SAO PAULO'),
    ('6001', 'RIO DE JANEIRO'),
    ('6291', 'CAMPINAS');

INSERT INTO cnpj.natureza_juridica (codigo, descricao) VALUES
    (2054, 'SOCIEDADE ANONIMA FECHADA'),
    (2062, 'SOCIEDADE EMPRESARIA LIMITADA');

INSERT INTO cnpj.pais (codigo, nome) VALUES
    ('105', 'BRASIL');

INSERT INTO cnpj.qualificacao_socio (codigo, descricao) VALUES
    ('17', 'PRESIDENTE'),
    ('22', 'SOCIO'),
    ('49', 'SOCIO-ADMINISTRADOR');

INSERT INTO cnpj.empresa (
    cnpj_basico,
    razao_social,
    natureza_juridica,
    qualificacao_responsavel,
    capital_social,
    porte_empresa,
    ente_federativo_responsavel,
    reference_month
) VALUES
    ('12345678', 'ACME INOVACAO S.A.', 2054, '17', 5000000.00, '05', NULL, '2026-02'),
    ('87654321', 'HOLDING EXEMPLO LTDA', 2062, '49', 1250000.00, '03', NULL, '2026-02'),
    ('11222333', 'NOVA ENERGIA PESQUISA LTDA', 2062, '49', 900000.00, '03', NULL, '2026-02'),
    ('99887766', 'ESTUDIO AUDIOVISUAL BRASIL LTDA', 2062, '49', 750000.00, '01', NULL, '2026-02'),
    ('44556677', 'EMPRESA DESATIVADA SERVICOS LTDA', 2062, '49', 150000.00, '01', NULL, '2026-02');

INSERT INTO cnpj.estabelecimento (
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    identificador_matriz_filial,
    nome_fantasia,
    situacao_cadastral,
    data_situacao_cadastral,
    motivo_situacao_cadastral,
    codigo_pais,
    data_inicio_atividade,
    cnae_fiscal_principal,
    cnae_fiscal_secundaria,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    uf,
    codigo_municipio,
    ddd_1,
    telefone_1,
    ddd_2,
    telefone_2,
    ddd_fax,
    fax,
    correio_eletronico,
    situacao_especial,
    data_situacao_especial,
    reference_month
) VALUES
    ('12345678', '0001', '95', 1, 'ACME LABS', '02', '2024-01-10', '00', '105', '2020-05-01', '6201501', '7210000', 'AVENIDA', 'PAULISTA', '1000', 'CJ 101', 'BELA VISTA', '01310000', 'SP', '7107', '11', 34567890, NULL, NULL, NULL, NULL, 'contato@acme.dev', NULL, NULL, '2026-02'),
    ('12345678', '0002', '76', 2, 'ACME PESQUISA CAMPINAS', '02', '2024-01-10', '00', '105', '2021-02-15', '7210000', '6201501', 'RUA', 'INOVACAO', '55', NULL, 'TAQUARAL', '13076000', 'SP', '6291', '19', 33221144, NULL, NULL, NULL, NULL, 'campinas@acme.dev', NULL, NULL, '2026-02'),
    ('87654321', '0001', '10', 1, 'HOLDING EXEMPLO', '02', '2023-03-02', '00', '105', '2018-08-10', '6201501', NULL, 'RUA', 'DO MERCADO', '88', 'SALA 4', 'CENTRO', '20010000', 'RJ', '6001', '21', 22334455, NULL, NULL, NULL, NULL, 'relacoes@holding.dev', NULL, NULL, '2026-02'),
    ('11222333', '0001', '81', 1, 'NOVA ENERGIA', '02', '2023-08-19', '00', '105', '2019-04-22', '3511501', '7210000', 'RUA', 'DAS PESQUISAS', '240', NULL, 'PARQUE TECNOLOGICO', '13083000', 'SP', '6291', '19', 30112233, NULL, NULL, NULL, NULL, 'contato@novaenergia.dev', NULL, NULL, '2026-02'),
    ('99887766', '0001', '40', 1, 'ESTUDIO BRASIL', '02', '2022-11-01', '00', '105', '2017-06-01', '5911102', NULL, 'AVENIDA', 'ATLANTICA', '2000', NULL, 'COPACABANA', '22021001', 'RJ', '6001', '21', 40050060, NULL, NULL, NULL, NULL, 'projetos@estudiobrasil.dev', NULL, NULL, '2026-02'),
    ('44556677', '0001', '55', 1, 'SERVICOS LEGADOS', '08', '2025-12-31', '01', '105', '2012-09-17', '6201501', NULL, 'RUA', 'ANTIGA', '12', NULL, 'MOOCA', '03104000', 'SP', '7107', '11', 26667788, NULL, NULL, NULL, NULL, 'contato@legado.dev', NULL, NULL, '2026-02');

INSERT INTO cnpj.simples_nacional (
    cnpj_basico,
    optante_simples_nacional,
    data_optante_simples_nacional,
    data_exclusao_simples_nacional,
    opcao_mei,
    data_opcao_mei,
    data_exclusao_mei,
    reference_month
) VALUES
    ('87654321', true, '2019-01-01', NULL, false, NULL, NULL, '2026-02'),
    ('11222333', true, '2020-01-01', NULL, false, NULL, NULL, '2026-02'),
    ('99887766', true, '2018-01-01', NULL, false, NULL, NULL, '2026-02'),
    ('44556677', false, NULL, '2025-12-31', false, NULL, NULL, '2026-02');

INSERT INTO cnpj.socio (
    cnpj_basico,
    identificador_socio,
    nome_socio_razao_social,
    cpf_cnpj_socio,
    qualificacao_socio,
    data_entrada_sociedade,
    pais,
    representante_legal,
    nome_do_representante,
    qualificacao_representante_legal,
    faixa_etaria,
    reference_month
) VALUES
    ('12345678', 2, 'ANA PAULA TESTE', '11122233344', '49', '2020-05-01', '105', NULL, NULL, NULL, 4, '2026-02'),
    ('12345678', 1, 'HOLDING EXEMPLO LTDA', '87654321000110', '22', '2021-01-01', '105', NULL, NULL, NULL, NULL, '2026-02'),
    ('12345678', 2, 'ROBERTO MENDES', '22233344455', '17', '2020-05-01', '105', NULL, NULL, NULL, 5, '2026-02'),
    ('87654321', 2, 'CARLOS LIMA', '55566677788', '49', '2018-08-10', '105', NULL, NULL, NULL, 6, '2026-02'),
    ('11222333', 2, 'ANA PAULA TESTE', '11122233344', '49', '2019-04-22', '105', NULL, NULL, NULL, 4, '2026-02'),
    ('99887766', 2, 'JULIA COSTA', '99988877766', '49', '2017-06-01', '105', NULL, NULL, NULL, 3, '2026-02'),
    ('44556677', 2, 'MARIO SILVA', '44455566677', '49', '2012-09-17', '105', NULL, NULL, NULL, 7, '2026-02');

INSERT INTO cnpj.download_manifest (
    reference_month,
    file_name,
    file_type,
    file_size_bytes,
    source_url,
    download_date,
    processing_status,
    transformed_at,
    loaded_postgres_at,
    loaded_neo4j_at,
    rows_transformed,
    rows_loaded_postgres,
    rows_loaded_neo4j,
    parquet_path
) VALUES
    ('2026-02', 'dev_empresas_sample.parquet', 'empresas', 4096, 'dev://sample', CURRENT_TIMESTAMP, 'loaded', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 5, 5, 5, '/opt/airflow/data/dev/cnpj/processed/dev_empresas_sample.parquet'),
    ('2026-02', 'dev_estabelecimentos_sample.parquet', 'estabelecimentos', 8192, 'dev://sample', CURRENT_TIMESTAMP, 'loaded', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 6, 6, 6, '/opt/airflow/data/dev/cnpj/processed/dev_estabelecimentos_sample.parquet');

REFRESH MATERIALIZED VIEW cnpj.mv_company_search;
REFRESH MATERIALIZED VIEW cnpj.mv_company_search_inactive;
ANALYZE cnpj.mv_company_search;
ANALYZE cnpj.mv_company_search_inactive;
