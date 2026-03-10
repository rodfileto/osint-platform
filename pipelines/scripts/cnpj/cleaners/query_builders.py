"""
Query Builder Templates

Complete SQL templates for cleaning CNPJ entity tables.
"""


def empresas_cleaning_template() -> str:
    """
    Complete SQL template for cleaning Empresas table.
    
    Returns:
        SQL SELECT with all Empresas columns cleaned
        
    Usage:
        query = f'''
            CREATE TABLE empresas_clean AS
            {empresas_cleaning_template()}
            FROM read_csv('empresas.csv', ...)
            WHERE LENGTH(TRIM(column0)) = 8
        '''
    """
    return """SELECT 
        -- CNPJ básico (8 digits, left-padded)
        LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', '')), 8, '0') as cnpj_basico,
        
        -- Razão social (uppercase, trimmed)
        UPPER(TRIM(REPLACE(CAST(column1 AS VARCHAR), '"', ''))) as razao_social,
        
        -- Natureza jurídica (nullable integer FK)
        NULLIF(
            TRY_CAST(TRIM(REPLACE(CAST(column2 AS VARCHAR), '"', '')) AS INTEGER),
            0
        ) as natureza_juridica,
        
        -- Qualificação do responsável (zero-padded 2-char code, e.g. '00', '05', '50')
        NULLIF(
            LPAD(TRIM(REPLACE(CAST(column3 AS VARCHAR), '"', '')), 2, '0'),
            '00'
        ) as qualificacao_responsavel,
        
        -- Capital social (decimal, Brazilian format)
        COALESCE(
            TRY_CAST(
                REPLACE(
                    REPLACE(TRIM(REPLACE(CAST(column4 AS VARCHAR), '"', '')), '.', ''),
                    ',', '.'
                ) AS DECIMAL(15,2)
            ),
            0.0
        ) as capital_social,
        
        -- Porte empresa (validated enum)
        CASE 
            WHEN LPAD(TRIM(REPLACE(CAST(column5 AS VARCHAR), '"', '')), 2, '0') IN ('00', '01', '03', '05')
            THEN LPAD(TRIM(REPLACE(CAST(column5 AS VARCHAR), '"', '')), 2, '0')
            ELSE '00' 
        END as porte_empresa,
        
        -- Ente federativo (string or NULL)
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column6 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column6 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as ente_federativo_responsavel"""


def estabelecimentos_cleaning_template() -> str:
    """
    Complete SQL template for cleaning Estabelecimentos table.
    
    Returns:
        SQL SELECT with all Estabelecimentos columns cleaned
        
    Usage:
        query = f'''
            CREATE TABLE estabelecimentos_clean AS
            {estabelecimentos_cleaning_template()}
            FROM read_csv('estabelecimentos.csv', ...)
        '''
    """
    return """SELECT 
        -- CNPJ components
        LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', '')), 8, '0') as cnpj_basico,
        LPAD(TRIM(REPLACE(CAST(column1 AS VARCHAR), '"', '')), 4, '0') as cnpj_ordem,
        LPAD(TRIM(REPLACE(CAST(column2 AS VARCHAR), '"', '')), 2, '0') as cnpj_dv,
        
        -- Identification matrix flag
        TRY_CAST(TRIM(REPLACE(CAST(column3 AS VARCHAR), '"', '')) AS INTEGER) as identificador_matriz_filial,
        
        -- Nome fantasia
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column4 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column4 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as nome_fantasia,
        
        -- Situação cadastral (zero-padded 2-char: '01','02','03','08')
        NULLIF(LPAD(TRIM(REPLACE(CAST(column5 AS VARCHAR), '"', '')), 2, '0'), '00') as situacao_cadastral,
        
        -- Data situação cadastral (YYYYMMDD -> DATE)
        CASE 
            WHEN CAST(column6 AS VARCHAR) = '0' OR TRIM(CAST(column6 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column6 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column6 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL 
        END as data_situacao_cadastral,
        
        -- Motivo situação cadastral (zero-padded 2-char: '01'..'63')
        NULLIF(LPAD(TRIM(REPLACE(CAST(column7 AS VARCHAR), '"', '')), 2, '0'), '00') as motivo_situacao_cadastral,
        
        -- Cidade exterior
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as nome_cidade_exterior,
        
        -- País code (zero-padded 3-char: '000','013','158')
        NULLIF(LPAD(TRIM(REPLACE(CAST(column9 AS VARCHAR), '"', '')), 3, '0'), '000') as codigo_pais,
        
        -- Data início atividade
        CASE 
            WHEN CAST(column10 AS VARCHAR) = '0' OR TRIM(CAST(column10 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column10 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column10 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL 
        END as data_inicio_atividade,
        
        -- CNAE principal (zero-padded 7-char: '0111301','8888888')
        NULLIF(LPAD(TRIM(REPLACE(CAST(column11 AS VARCHAR), '"', '')), 7, '0'), '0000000') as cnae_fiscal_principal,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column12 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column12 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as cnae_fiscal_secundaria,
        
        -- Address fields
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column13 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column13 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as tipo_logradouro,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column14 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column14 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as logradouro,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column15 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column15 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as numero,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column16 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column16 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as complemento,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column17 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column17 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as bairro,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column18 AS VARCHAR), '"', ''))) > 0 
            THEN LPAD(TRIM(REPLACE(CAST(column18 AS VARCHAR), '"', '')), 8, '0')
            ELSE NULL 
        END as cep,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column19 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column19 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as uf,
        -- codigo_municipio: zero-padded 4-char code, e.g. '0001', '9997'
        NULLIF(LPAD(TRIM(REPLACE(CAST(column20 AS VARCHAR), '"', '')), 4, '0'), '0000') as codigo_municipio,

        -- Contact (RF layout: ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, correio)
        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column21 AS VARCHAR), '"', ''))) > 0
            THEN TRIM(REPLACE(CAST(column21 AS VARCHAR), '"', ''))
            ELSE NULL
        END as ddd_1,
        NULLIF(TRY_CAST(TRIM(REPLACE(CAST(column22 AS VARCHAR), '"', '')) AS INTEGER), 0) as telefone_1,
        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column23 AS VARCHAR), '"', ''))) > 0
            THEN TRIM(REPLACE(CAST(column23 AS VARCHAR), '"', ''))
            ELSE NULL
        END as ddd_2,
        NULLIF(TRY_CAST(TRIM(REPLACE(CAST(column24 AS VARCHAR), '"', '')) AS INTEGER), 0) as telefone_2,
        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column25 AS VARCHAR), '"', ''))) > 0
            THEN TRIM(REPLACE(CAST(column25 AS VARCHAR), '"', ''))
            ELSE NULL
        END as ddd_fax,
        NULLIF(TRY_CAST(TRIM(REPLACE(CAST(column26 AS VARCHAR), '"', '')) AS INTEGER), 0) as fax,
        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column27 AS VARCHAR), '"', ''))) > 0
            THEN LOWER(TRIM(REPLACE(CAST(column27 AS VARCHAR), '"', '')))
            ELSE NULL
        END as correio_eletronico,

        -- Situação especial
        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column28 AS VARCHAR), '"', ''))) > 0
            THEN UPPER(TRIM(REPLACE(CAST(column28 AS VARCHAR), '"', '')))
            ELSE NULL
        END as situacao_especial,
        CASE
            WHEN CAST(column29 AS VARCHAR) = '0' OR TRIM(CAST(column29 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column29 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column29 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL
        END as data_situacao_especial"""


def build_empresas_query(csv_path: str, output_path: str) -> str:
    """
    Build complete DuckDB query for Empresas CSV processing.
    
    Args:
        csv_path: Path to input CSV file
        output_path: Path to output Parquet file
        
    Returns:
        Complete SQL query string
    """
    # Generate column names explicitly to avoid DuckDB's two-digit naming for 10+ columns
    column_names = ','.join([f'column{i}' for i in range(7)])  # Empresas has 7 columns (0-6)
    
    return f"""
    COPY (
        {empresas_cleaning_template()}
        FROM read_csv('{csv_path}', 
            delim=';',
            header=false,
            names=[{column_names}],
            all_varchar=true,
            ignore_errors=true
        )
        WHERE LENGTH(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', ''))) = 8
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


def build_estabelecimentos_query(csv_path: str, output_path: str) -> str:
    """
    Build complete DuckDB query for Estabelecimentos CSV processing.
    
    Args:
        csv_path: Path to input CSV file
        output_path: Path to output Parquet file
        
    Returns:
        Complete SQL query string
    """
    # Generate column names explicitly to avoid DuckDB's two-digit naming for 10+ columns
    column_names = ','.join([f'column{i}' for i in range(30)])  # Estabelecimentos has 30 columns (0-29)
    
    return f"""
    COPY (
        {estabelecimentos_cleaning_template()}
        FROM read_csv('{csv_path}', 
            delim=';',
            header=false,
            names=[{column_names}],
            all_varchar=true,
            ignore_errors=true
        )
        WHERE LENGTH(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', ''))) = 8
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


# ============================================================================
# SOCIOS
# ============================================================================

def socios_cleaning_template() -> str:
    """
    SQL template for cleaning Socios CSV.

    Schema (11 columns, semicolon-delimited, no header):
        0  cnpj_basico              VARCHAR(8)
        1  identificador_socio      INTEGER  (1=PJ, 2=PF, 3=Estrangeiro)
        2  nome_socio_razao_social  TEXT
        3  cpf_cnpj_socio           VARCHAR(14) — mascarado pela RF
        4  qualificacao_socio       VARCHAR(2)  (zero-padded, e.g. '05', '22', '50')
        5  data_entrada_sociedade   DATE (YYYYMMDD)
        6  pais                     VARCHAR(3) (código, zero-padded)
        7  representante_legal      VARCHAR(14) — CPF mascarado
        8  nome_do_representante    TEXT
        9  qualificacao_representante_legal  VARCHAR(2)  (NULL if '00'/empty)
       10  faixa_etaria             INTEGER
    """
    return """SELECT
        LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', '')), 8, '0') AS cnpj_basico,

        TRY_CAST(TRIM(REPLACE(CAST(column1 AS VARCHAR), '"', '')) AS INTEGER) AS identificador_socio,

        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column2 AS VARCHAR), '"', ''))) > 0
            THEN UPPER(TRIM(REPLACE(CAST(column2 AS VARCHAR), '"', '')))
            ELSE NULL
        END AS nome_socio_razao_social,

        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column3 AS VARCHAR), '"', ''))) > 0
            THEN TRIM(REPLACE(CAST(column3 AS VARCHAR), '"', ''))
            ELSE NULL
        END AS cpf_cnpj_socio,

        NULLIF(LPAD(TRIM(REPLACE(CAST(column4 AS VARCHAR), '"', '')), 2, '0'), '00') AS qualificacao_socio,

        CASE
            WHEN TRIM(CAST(column5 AS VARCHAR)) IN ('', '0', '00000000') THEN NULL
            WHEN LENGTH(TRIM(CAST(column5 AS VARCHAR))) = 8
            THEN TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column5 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL
        END AS data_entrada_sociedade,

        NULLIF(LPAD(TRIM(REPLACE(CAST(column6 AS VARCHAR), '"', '')), 3, '0'), '000') AS pais,

        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column7 AS VARCHAR), '"', ''))) > 0
            THEN TRIM(REPLACE(CAST(column7 AS VARCHAR), '"', ''))
            ELSE NULL
        END AS representante_legal,

        CASE
            WHEN LENGTH(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', ''))) > 0
            THEN UPPER(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', '')))
            ELSE NULL
        END AS nome_do_representante,

        NULLIF(LPAD(TRIM(REPLACE(CAST(column9 AS VARCHAR), '"', '')), 2, '0'), '00') AS qualificacao_representante_legal,

        TRY_CAST(TRIM(REPLACE(CAST(column10 AS VARCHAR), '"', '')) AS INTEGER) AS faixa_etaria"""


def build_socios_query(csv_path: str, output_path: str) -> str:
    """
    Build complete DuckDB query for Socios CSV processing.

    Args:
        csv_path: Path to input CSV file
        output_path: Path to output Parquet file

    Returns:
        Complete SQL query string
    """
    column_names = ','.join([f'column{i}' for i in range(11)])

    return f"""
    COPY (
        {socios_cleaning_template()}
        FROM read_csv('{csv_path}',
            delim=';',
            header=false,
            names=[{column_names}],
            all_varchar=true,
            ignore_errors=true
        )
        WHERE LENGTH(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', ''))) = 8
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


# ============================================================================
# SIMPLES NACIONAL
# ============================================================================

def simples_cleaning_template() -> str:
    """
    SQL template for cleaning Simples Nacional CSV.

    Schema (7 columns, semicolon-delimited, no header):
        0  cnpj_basico          VARCHAR(8)
        1  optante_simples_nacional  BOOLEAN ('S'/'N' -> true/false)
        2  data_optante_simples_nacional DATE (YYYYMMDD; '00000000' = NULL)
        3  data_exclusao_simples_nacional DATE
        4  opcao_mei            VARCHAR(1)  'S'/'N'
        5  data_opcao_mei       DATE
        6  data_exclusao_mei    DATE
    """
    def _date_expr(col: str) -> str:
        return f"""CASE
            WHEN TRIM(CAST({col} AS VARCHAR)) IN ('', '0', '00000000') THEN NULL
            WHEN LENGTH(TRIM(CAST({col} AS VARCHAR))) = 8
            THEN TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST({col} AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL
        END"""

    return f"""SELECT
        LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', '')), 8, '0') AS cnpj_basico,

        CASE UPPER(TRIM(REPLACE(CAST(column1 AS VARCHAR), '"', '')))
            WHEN 'S' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END AS optante_simples_nacional,
        {_date_expr('column2')} AS data_optante_simples_nacional,
        {_date_expr('column3')} AS data_exclusao_simples_nacional,

        CASE UPPER(TRIM(REPLACE(CAST(column4 AS VARCHAR), '"', '')))
            WHEN 'S' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END AS opcao_mei,
        {_date_expr('column5')} AS data_opcao_mei,
        {_date_expr('column6')} AS data_exclusao_mei"""


def build_simples_query(csv_path: str, output_path: str) -> str:
    """
    Build complete DuckDB query for Simples Nacional CSV processing.

    Args:
        csv_path: Path to input CSV file
        output_path: Path to output Parquet file

    Returns:
        Complete SQL query string
    """
    column_names = ','.join([f'column{i}' for i in range(7)])

    return f"""
    COPY (
        {simples_cleaning_template()}
        FROM read_csv('{csv_path}',
            delim=';',
            header=false,
            names=[{column_names}],
            all_varchar=true,
            ignore_errors=true
        )
        WHERE LENGTH(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', ''))) >= 6
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


# ============================================================================
# REFERENCE TABLES (Cnaes, Motivos, Municipios, Naturezas, Paises, Qualificacoes)
# ============================================================================

def build_reference_query(csv_path: str, output_path: str, ref_type: str) -> str:
    """
    Build DuckDB query for any 2-column reference table CSV.

    All reference CSVs share the same structure: codigo;descricao
    The primary key type varies:
        - cnaes:          VARCHAR(7)
        - motivos:        VARCHAR(2)  (zero-padded, e.g. '01', '63')
        - municipios:     VARCHAR(4)  (zero-padded, e.g. '0001', '9997')
        - naturezas:      INTEGER
        - paises:         VARCHAR(3)  (zero-padded, e.g. '000', '013', '158')
        - qualificacoes:  VARCHAR(2)  (zero-padded, e.g. '00', '05', '50')

    Args:
        csv_path:  Path to input CSV file
        output_path: Path to output Parquet file
        ref_type:  One of 'cnaes', 'motivos', 'municipios', 'naturezas', 'paises', 'qualificacoes'

    Returns:
        Complete SQL query string
    """
    varchar_refs = {'cnaes'}
    lpad2_refs   = {'qualificacoes', 'motivos'}    # zero-padded 2-char codes
    lpad3_refs   = {'paises'}                       # zero-padded 3-char codes
    lpad4_refs   = {'municipios'}                   # zero-padded 4-char codes

    # cnpj.cnae uses 'cnae_fiscal' as PK column name (not 'codigo')
    pk_alias = 'cnae_fiscal' if ref_type == 'cnaes' else 'codigo'
    # cnpj.municipio and cnpj.pais use 'nome' for the description column (not 'descricao')
    desc_alias = 'nome' if ref_type in ('municipios', 'paises') else 'descricao'

    if ref_type in varchar_refs:
        codigo_expr = f"TRIM(REPLACE(CAST(column0 AS VARCHAR), '\"', '')) AS {pk_alias}"
    elif ref_type in lpad2_refs:
        codigo_expr = f"LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '\"', '')), 2, '0') AS {pk_alias}"
    elif ref_type in lpad3_refs:
        codigo_expr = f"LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '\"', '')), 3, '0') AS {pk_alias}"
    elif ref_type in lpad4_refs:
        codigo_expr = f"LPAD(TRIM(REPLACE(CAST(column0 AS VARCHAR), '\"', '')), 4, '0') AS {pk_alias}"
    else:
        codigo_expr = f"TRY_CAST(TRIM(REPLACE(CAST(column0 AS VARCHAR), '\"', '')) AS INTEGER) AS {pk_alias}"

    return f"""
    COPY (
        SELECT
            {codigo_expr},
            TRIM(REPLACE(CAST(column1 AS VARCHAR), '"', '')) AS {desc_alias}
        FROM read_csv('{csv_path}',
            delim=';',
            header=false,
            names=['column0','column1'],
            all_varchar=true,
            ignore_errors=true
        )
        WHERE LENGTH(TRIM(REPLACE(CAST(column0 AS VARCHAR), '"', ''))) > 0
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
