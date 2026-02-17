"""
CNPJ Data Processing Scripts

This package contains reusable scripts and utilities for processing 
Brazilian CNPJ (Cadastro Nacional da Pessoa Jurídica) data.

Modules:
- cleaners: Data cleaning and validation functions (Python/Pandas)
- cleaners_sql: SQL template strings for DuckDB/high-performance ETL

Usage:
    # Python cleaners (default import)
    from cnpj import clean_cnpj_digits, clean_capital_social
    
    # SQL cleaners (explicit import)
    from cnpj.cleaners_sql import clean_cnpj_digits_sql, empresas_cleaning_template
"""

from .cleaners import (
    clean_cnpj_digits,
    format_cnpj,
    validate_cnpj,
    clean_capital_social,
    clean_integer,
    clean_string,
    clean_cnpj_date,
    clean_porte_empresa,
    apply_cleaners_to_dataframe,
)

__all__ = [
    'clean_cnpj_digits',
    'format_cnpj',
    'validate_cnpj',
    'clean_capital_social',
    'clean_integer',
    'clean_string',
    'clean_cnpj_date',
    'clean_porte_empresa',
    'apply_cleaners_to_dataframe',
]
