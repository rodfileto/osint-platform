"""
Numeric and Currency Cleaning SQL Functions

Functions for cleaning Brazilian currency formats and numeric values.
"""


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
