"""
CNPJ Data Cleaning - SQL Equivalents for DuckDB

This module provides SQL template strings equivalent to the Python cleaners.
Use these for high-performance data transformation in DuckDB/SQL engines.

Each function returns a SQL expression string that can be embedded in queries.
Performance: 10-100x faster than Python UDFs due to vectorization.

Usage:
    from cleaners_sql import clean_cnpj_digits_sql, clean_capital_social_sql
    
    query = f'''
        SELECT 
            {clean_cnpj_digits_sql('cnpj_basico')} as cnpj_clean,
            {clean_capital_social_sql('capital_social')} as capital_clean
        FROM empresas
    '''
"""

# ============================================================================
# CNPJ FORMATTING AND VALIDATION
# ============================================================================

def clean_cnpj_digits_sql(column: str) -> str:
    """
    SQL: Extract only digits from CNPJ, validate 14-digit length.
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning 14-digit CNPJ or NULL
        
    Example:
        >>> clean_cnpj_digits_sql('cnpj_basico')
        "CASE WHEN LENGTH(REGEXP_REPLACE(...)) = 14 THEN ... ELSE NULL END"
    """
    return f"""CASE 
        WHEN LENGTH(REGEXP_REPLACE(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), '[^0-9]', '', 'g')) = 14 
        THEN REGEXP_REPLACE(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), '[^0-9]', '', 'g')
        ELSE NULL 
    END"""


def clean_cnpj_basico_sql(column: str) -> str:
    """
    SQL: Clean CNPJ básico (8 digits), pad with zeros, remove formatting.
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning 8-digit CNPJ básico or NULL
    """
    return f"""CASE 
        WHEN LENGTH(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))) > 0 
        THEN LPAD(REGEXP_REPLACE(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), '[^0-9]', '', 'g'), 8, '0')
        ELSE NULL 
    END"""


def format_cnpj_sql(cnpj_column: str) -> str:
    """
    SQL: Format 14-digit CNPJ as XX.XXX.XXX/XXXX-XX
    
    Args:
        cnpj_column: Column with 14-digit CNPJ
        
    Returns:
        SQL expression returning formatted CNPJ
    """
    return f"""CASE 
        WHEN LENGTH({cnpj_column}) = 14 THEN
            SUBSTRING({cnpj_column}, 1, 2) || '.' ||
            SUBSTRING({cnpj_column}, 3, 3) || '.' ||
            SUBSTRING({cnpj_column}, 6, 3) || '/' ||
            SUBSTRING({cnpj_column}, 9, 4) || '-' ||
            SUBSTRING({cnpj_column}, 13, 2)
        ELSE NULL 
    END"""


def format_cnpj_basico_sql(basico: str, ordem: str, dv: str) -> str:
    """
    SQL: Build and format complete CNPJ from 3 components.
    
    Args:
        basico: CNPJ básico column (8 digits)
        ordem: CNPJ ordem column (4 digits)
        dv: CNPJ DV column (2 digits)
        
    Returns:
        SQL expression returning formatted CNPJ
    """
    cnpj_concat = f"LPAD(CAST({basico} AS VARCHAR), 8, '0') || LPAD(CAST({ordem} AS VARCHAR), 4, '0') || LPAD(CAST({dv} AS VARCHAR), 2, '0')"
    return format_cnpj_sql(cnpj_concat)


def validate_cnpj_sql(column: str) -> str:
    """
    SQL: Validate CNPJ using check digit algorithm.
    
    This is the most complex transformation - validates both check digits
    using the official CNPJ algorithm.
    
    Args:
        column: Column with 14-digit CNPJ string
        
    Returns:
        SQL expression returning TRUE/FALSE
        
    Note:
        For performance, consider validating in Python post-load for
        non-critical validations. This SQL is ~50 lines and slower.
    """
    return f"""CASE 
        -- Must be 14 digits
        WHEN LENGTH({column}) != 14 THEN FALSE
        -- Cannot be all same digit
        WHEN REGEXP_MATCHES({column}, '^(\\d)\\1{{13}}$') THEN FALSE
        -- Validate first check digit (position 12)
        WHEN (
            MOD(
                CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 5 +
                CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 4 +
                CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 3 +
                CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 2 +
                CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 9 +
                CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 8 +
                CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 7 +
                CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 6 +
                CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 5 +
                CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 4 +
                CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 3 +
                CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 2,
                11
            ) < 2 AND CAST(SUBSTRING({column}, 13, 1) AS INTEGER) = 0
        ) OR (
            MOD(
                CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 5 +
                CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 4 +
                CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 3 +
                CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 2 +
                CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 9 +
                CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 8 +
                CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 7 +
                CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 6 +
                CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 5 +
                CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 4 +
                CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 3 +
                CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 2,
                11
            ) >= 2 AND CAST(SUBSTRING({column}, 13, 1) AS INTEGER) = (
                11 - MOD(
                    CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 2 +
                    CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 9 +
                    CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 8 +
                    CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 7 +
                    CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 6 +
                    CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 2,
                    11
                )
            )
        ) THEN
            -- Validate second check digit (position 13)
            CASE WHEN (
                MOD(
                    CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 6 +
                    CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 2 +
                    CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 9 +
                    CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 8 +
                    CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 7 +
                    CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 6 +
                    CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 13, 1) AS INTEGER) * 2,
                    11
                ) < 2 AND CAST(SUBSTRING({column}, 14, 1) AS INTEGER) = 0
            ) OR (
                MOD(
                    CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 6 +
                    CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 2 +
                    CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 9 +
                    CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 8 +
                    CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 7 +
                    CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 6 +
                    CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 5 +
                    CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 4 +
                    CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 3 +
                    CAST(SUBSTRING({column}, 13, 1) AS INTEGER) * 2,
                    11
                ) >= 2 AND CAST(SUBSTRING({column}, 14, 1) AS INTEGER) = (
                    11 - MOD(
                        CAST(SUBSTRING({column}, 1, 1) AS INTEGER) * 6 +
                        CAST(SUBSTRING({column}, 2, 1) AS INTEGER) * 5 +
                        CAST(SUBSTRING({column}, 3, 1) AS INTEGER) * 4 +
                        CAST(SUBSTRING({column}, 4, 1) AS INTEGER) * 3 +
                        CAST(SUBSTRING({column}, 5, 1) AS INTEGER) * 2 +
                        CAST(SUBSTRING({column}, 6, 1) AS INTEGER) * 9 +
                        CAST(SUBSTRING({column}, 7, 1) AS INTEGER) * 8 +
                        CAST(SUBSTRING({column}, 8, 1) AS INTEGER) * 7 +
                        CAST(SUBSTRING({column}, 9, 1) AS INTEGER) * 6 +
                        CAST(SUBSTRING({column}, 10, 1) AS INTEGER) * 5 +
                        CAST(SUBSTRING({column}, 11, 1) AS INTEGER) * 4 +
                        CAST(SUBSTRING({column}, 12, 1) AS INTEGER) * 3 +
                        CAST(SUBSTRING({column}, 13, 1) AS INTEGER) * 2,
                        11
                    )
                )
            ) THEN TRUE
            ELSE FALSE END
        ELSE FALSE 
    END"""


# ============================================================================
# CURRENCY AND NUMERIC CLEANING
# ============================================================================

def clean_capital_social_sql(column: str) -> str:
    """
    SQL: Clean Brazilian currency format to decimal.
    
    Handles:
    - "1.234,56" -> 1234.56
    - "100,00" -> 100.0
    - Quoted strings
    - Empty/NULL -> 0.0
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning DECIMAL(15,2)
    """
    return f"""COALESCE(
        TRY_CAST(
            REPLACE(
                REPLACE(
                    REPLACE(
                        TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')),
                        '.', ''
                    ),
                    ',', '.'
                ),
                ' ', ''
            ) AS DECIMAL(15,2)
        ),
        0.0
    )"""


def clean_decimal_sql(column: str, default: str = "0.0") -> str:
    """
    SQL: Generic decimal cleaner with custom default.
    
    Args:
        column: Column name or SQL expression
        default: Default value as string (e.g., "0.0", "NULL")
        
    Returns:
        SQL expression returning DECIMAL
    """
    return f"""COALESCE(
        TRY_CAST(
            REPLACE(
                REPLACE(
                    TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')),
                    ',', '.'
                ),
                ' ', ''
            ) AS DECIMAL(15,2)
        ),
        {default}
    )"""


# ============================================================================
# INTEGER CLEANING
# ============================================================================

def clean_integer_sql(column: str) -> str:
    """
    SQL: Clean and convert to integer, return NULL if invalid.
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning INTEGER or NULL
    """
    return f"""TRY_CAST(
        TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))
        AS INTEGER
    )"""


def clean_int_with_default_sql(column: str, default: int = 0) -> str:
    """
    SQL: Clean integer with default for failures.
    
    Args:
        column: Column name or SQL expression
        default: Default integer value
        
    Returns:
        SQL expression returning INTEGER (never NULL)
    """
    return f"""COALESCE(
        TRY_CAST(
            TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))
            AS INTEGER
        ),
        {default}
    )"""


# ============================================================================
# STRING CLEANING
# ============================================================================

def clean_string_sql(column: str) -> str:
    """
    SQL: Clean string - remove quotes, trim whitespace, NULL if empty.
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning VARCHAR or NULL
    """
    return f"""CASE 
        WHEN LENGTH(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))) > 0 
        THEN TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))
        ELSE NULL 
    END"""


def clean_string_upper_sql(column: str) -> str:
    """
    SQL: Clean string and convert to uppercase.
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning uppercase VARCHAR or NULL
    """
    return f"""CASE 
        WHEN LENGTH(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))) > 0 
        THEN UPPER(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')))
        ELSE NULL 
    END"""


def clean_string_with_default_sql(column: str, default: str = "") -> str:
    """
    SQL: Clean string with default for empty/NULL.
    
    Args:
        column: Column name or SQL expression
        default: Default string value (will be SQL-escaped)
        
    Returns:
        SQL expression returning VARCHAR (never NULL)
    """
    # Escape single quotes in default value
    escaped_default = default.replace("'", "''")
    return f"""COALESCE(
        CASE 
            WHEN LENGTH(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))) > 0 
            THEN TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))
            ELSE NULL 
        END,
        '{escaped_default}'
    )"""


# ============================================================================
# DATE CLEANING
# ============================================================================

def clean_cnpj_date_sql(column: str) -> str:
    """
    SQL: Convert CNPJ date (YYYYMMDD integer) to DATE.
    
    Handles:
    - 20200131 -> 2020-01-31
    - "20200131" -> 2020-01-31
    - 0 or empty -> NULL
    - Non-numeric strings (e.g. 'NT@NT.NT', 'NAO  TEM') -> NULL
    - Out-of-range dates (e.g. '37981131') -> NULL
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning DATE or NULL
    """
    return f"""CASE 
        WHEN CAST({column} AS VARCHAR) = '0' OR TRIM(CAST({column} AS VARCHAR)) = '' THEN NULL
        WHEN LENGTH(TRIM(CAST({column} AS VARCHAR))) = 8 THEN
            TRY_CAST(
                TRY_STRPTIME(LPAD(TRIM(CAST({column} AS VARCHAR)), 8, '0'), '%Y%m%d')
                AS DATE
            )
        ELSE NULL 
    END"""


def clean_cnpj_date_iso_sql(column: str) -> str:
    """
    SQL: Convert CNPJ date to ISO string format (YYYY-MM-DD).
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning VARCHAR in ISO format or NULL
    """
    return f"""CASE 
        WHEN CAST({column} AS VARCHAR) = '0' OR TRIM(CAST({column} AS VARCHAR)) = '' THEN NULL
        WHEN LENGTH(TRIM(CAST({column} AS VARCHAR))) = 8 THEN
            CAST(
                TRY_CAST(
                    TRY_STRPTIME(LPAD(TRIM(CAST({column} AS VARCHAR)), 8, '0'), '%Y%m%d')
                    AS DATE
                )
                AS VARCHAR
            )
        ELSE NULL 
    END"""


# ============================================================================
# ENUMERATION CLEANING
# ============================================================================

def clean_porte_empresa_sql(column: str) -> str:
    """
    SQL: Clean and standardize company size code.
    
    Valid codes: 00, 01, 03, 05
    Default: 00 (not informed)
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning 2-char VARCHAR (always valid code)
    """
    return f"""CASE 
        WHEN LPAD(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), 2, '0') IN ('00', '01', '03', '05')
        THEN LPAD(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), 2, '0')
        ELSE '00' 
    END"""


def clean_situacao_cadastral_sql(column: str) -> str:
    """
    SQL: Clean registration status code (pad to 2 digits).
    
    Args:
        column: Column name or SQL expression
        
    Returns:
        SQL expression returning 2-char VARCHAR or NULL
    """
    return f"""CASE 
        WHEN LENGTH(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', ''))) > 0 
        THEN LPAD(TRIM(REPLACE(CAST({column} AS VARCHAR), '"', '')), 2, '0')
        ELSE NULL 
    END"""


# ============================================================================
# COMPOSITE TEMPLATES
# ============================================================================

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


# ============================================================================
# QUERY BUILDER HELPERS
# ============================================================================

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
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # CNPJ functions
    'clean_cnpj_digits_sql',
    'clean_cnpj_basico_sql',
    'format_cnpj_sql',
    'format_cnpj_basico_sql',
    'validate_cnpj_sql',
    
    # Currency/numeric
    'clean_capital_social_sql',
    'clean_decimal_sql',
    
    # Integer
    'clean_integer_sql',
    'clean_int_with_default_sql',
    
    # String
    'clean_string_sql',
    'clean_string_upper_sql',
    'clean_string_with_default_sql',
    
    # Date
    'clean_cnpj_date_sql',
    'clean_cnpj_date_iso_sql',
    
    # Enumerations
    'clean_porte_empresa_sql',
    'clean_situacao_cadastral_sql',
    
    # Templates
    'empresas_cleaning_template',
    'estabelecimentos_cleaning_template',
    
    # Query builders
    'build_empresas_query',
    'build_estabelecimentos_query',
]
