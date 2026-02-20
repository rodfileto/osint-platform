"""
String Cleaning SQL Functions

Functions for cleaning and normalizing string values.
"""


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
