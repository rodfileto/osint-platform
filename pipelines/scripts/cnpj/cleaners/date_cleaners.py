"""
Date Cleaning SQL Functions

Functions for cleaning and converting CNPJ date formats.
"""


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
