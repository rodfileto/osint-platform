-- Recreate CNPJ tables with PRIMARY KEYs
DROP TABLE IF EXISTS cnpj.empresa_staging CASCADE;
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;

-- Empresa table with PRIMARY KEY
CREATE TABLE cnpj.empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DECIMAL(15,2) DEFAULT 0.0,
    porte_empresa VARCHAR(2) DEFAULT '00',
    ente_federativo_responsavel TEXT,
    reference_month VARCHAR(7) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Estabelecimento table with PRIMARY KEY
CREATE TABLE cnpj.estabelecimento (
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    identificador_matriz_filial INTEGER,
    nome_fantasia TEXT,
    situacao_cadastral INTEGER,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    codigo_pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal INTEGER,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep VARCHAR(8),
    uf VARCHAR(2),
    codigo_municipio INTEGER,
    municipio TEXT,
    ddd_telefone_1 TEXT,
    ddd_telefone_2 TEXT,
    ddd_fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    reference_month VARCHAR(7) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
    FOREIGN KEY (cnpj_basico) REFERENCES cnpj.empresa(cnpj_basico) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_empresa_razao_social ON cnpj.empresa USING GIN (to_tsvector('portuguese', razao_social));
CREATE INDEX IF NOT EXISTS idx_empresa_porte ON cnpj.empresa(porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresa_natureza ON cnpj.empresa(natureza_juridica);
CREATE INDEX IF NOT EXISTS idx_empresa_ref_month ON cnpj.empresa(reference_month);

CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON cnpj.estabelecimento(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON cnpj.estabelecimento(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON cnpj.estabelecimento(uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnae ON cnpj.estabelecimento(cnae_fiscal_principal);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_ref_month ON cnpj.estabelecimento(reference_month);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_nome_fantasia ON cnpj.estabelecimento USING GIN (to_tsvector('portuguese', nome_fantasia));

SELECT 'Tables created successfully!' as status;
