"""
CNPJ Formatting and Validation SQL Functions

Functions for cleaning, formatting, and validating Brazilian CNPJ numbers.
"""


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
