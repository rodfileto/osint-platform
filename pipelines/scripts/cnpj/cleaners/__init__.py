"""
CNPJ Data Cleaning Modules

Modular SQL template functions for high-performance data transformation in DuckDB.
Each module focuses on a specific type of cleaning operation.
"""

from .cnpj_cleaners import (
    clean_cnpj_digits_sql,
    clean_cnpj_basico_sql,
    format_cnpj_sql,
    format_cnpj_basico_sql,
    validate_cnpj_sql,
)

from .numeric_cleaners import (
    clean_capital_social_sql,
    clean_decimal_sql,
    clean_integer_sql,
    clean_int_with_default_sql,
)

from .string_cleaners import (
    clean_string_sql,
    clean_string_upper_sql,
    clean_string_with_default_sql,
    clean_porte_empresa_sql,
    clean_situacao_cadastral_sql,
)

from .date_cleaners import (
    clean_cnpj_date_sql,
    clean_cnpj_date_iso_sql,
)

from .query_builders import (
    empresas_cleaning_template,
    estabelecimentos_cleaning_template,
    build_empresas_query,
    build_estabelecimentos_query,
)

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
    'clean_integer_sql',
    'clean_int_with_default_sql',
    
    # String
    'clean_string_sql',
    'clean_string_upper_sql',
    'clean_string_with_default_sql',
    'clean_porte_empresa_sql',
    'clean_situacao_cadastral_sql',
    
    # Date
    'clean_cnpj_date_sql',
    'clean_cnpj_date_iso_sql',
    
    # Templates
    'empresas_cleaning_template',
    'estabelecimentos_cleaning_template',
    'build_empresas_query',
    'build_estabelecimentos_query',
]
