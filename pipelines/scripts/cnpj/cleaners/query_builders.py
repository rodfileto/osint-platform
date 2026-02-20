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
        
        -- Natureza jurídica (integer)
        COALESCE(
            TRY_CAST(TRIM(REPLACE(CAST(column2 AS VARCHAR), '"', '')) AS INTEGER),
            0
        ) as natureza_juridica,
        
        -- Qualificação do responsável (integer)
        COALESCE(
            TRY_CAST(TRIM(REPLACE(CAST(column3 AS VARCHAR), '"', '')) AS INTEGER),
            0
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
        
        -- Situação cadastral
        TRY_CAST(TRIM(REPLACE(CAST(column5 AS VARCHAR), '"', '')) AS INTEGER) as situacao_cadastral,
        
        -- Data situação cadastral (YYYYMMDD -> DATE)
        CASE 
            WHEN CAST(column6 AS VARCHAR) = '0' OR TRIM(CAST(column6 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column6 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column6 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL 
        END as data_situacao_cadastral,
        
        -- Motivo situação cadastral
        TRY_CAST(TRIM(REPLACE(CAST(column7 AS VARCHAR), '"', '')) AS INTEGER) as motivo_situacao_cadastral,
        
        -- Cidade exterior
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column8 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as nome_cidade_exterior,
        
        -- País code
        TRY_CAST(TRIM(REPLACE(CAST(column9 AS VARCHAR), '"', '')) AS INTEGER) as codigo_pais,
        
        -- Data início atividade
        CASE 
            WHEN CAST(column10 AS VARCHAR) = '0' OR TRIM(CAST(column10 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column10 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column10 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
            ELSE NULL 
        END as data_inicio_atividade,
        
        -- CNAE principal and secondary
        TRY_CAST(TRIM(REPLACE(CAST(column11 AS VARCHAR), '"', '')) AS INTEGER) as cnae_fiscal_principal,
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
        TRY_CAST(TRIM(REPLACE(CAST(column20 AS VARCHAR), '"', '')) AS INTEGER) as codigo_municipio,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column21 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column21 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as municipio,
        
        -- Contact
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column22 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column22 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as ddd_telefone_1,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column23 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column23 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as ddd_telefone_2,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column24 AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST(column24 AS VARCHAR), '"', ''))
            ELSE NULL 
        END as ddd_fax,
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column25 AS VARCHAR), '"', ''))) > 0 
            THEN LOWER(TRIM(REPLACE(CAST(column25 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as correio_eletronico,
        
        -- Situação especial
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST(column26 AS VARCHAR), '"', ''))) > 0 
            THEN UPPER(TRIM(REPLACE(CAST(column26 AS VARCHAR), '"', '')))
            ELSE NULL 
        END as situacao_especial,
        CASE 
            WHEN CAST(column27 AS VARCHAR) = '0' OR TRIM(CAST(column27 AS VARCHAR)) = '' THEN NULL
            WHEN LENGTH(TRIM(CAST(column27 AS VARCHAR))) = 8 THEN
                TRY_CAST(TRY_STRPTIME(LPAD(TRIM(CAST(column27 AS VARCHAR)), 8, '0'), '%Y%m%d') AS DATE)
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
