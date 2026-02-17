"""
CNPJ Data Cleaning Utilities

This module provides reusable data cleaning functions for CNPJ data processing.
Extracted from legacy Django commands for use in independent ETL pipelines.

Functions are designed to be:
- Pure (no side effects)
- Composable (can be chained)
- Pandas-compatible (work with Series.apply())
- Type-safe (handle None/NaN gracefully)
"""

import re
from decimal import Decimal, InvalidOperation
from typing import Optional, Union
import pandas as pd
from datetime import datetime


# ============================================================================
# CNPJ FORMATTING AND VALIDATION
# ============================================================================

def clean_cnpj_digits(cnpj: Union[str, int, None]) -> Optional[str]:
    """
    Extract only digits from CNPJ string.
    
    Args:
        cnpj: Raw CNPJ value (may contain formatting)
        
    Returns:
        String with only digits, or None if invalid
        
    Examples:
        >>> clean_cnpj_digits("12.345.678/0001-90")
        '12345678000190'
        >>> clean_cnpj_digits(12345678000190)
        '12345678000190'
        >>> clean_cnpj_digits("")
        None
    """
    if pd.isna(cnpj) or cnpj is None:
        return None
    
    # Convert to string and extract digits
    digits = re.sub(r'[^\d]', '', str(cnpj))
    
    # Must have exactly 14 digits
    if len(digits) != 14:
        return None
        
    return digits


def format_cnpj(cnpj: Union[str, int, None]) -> Optional[str]:
    """
    Format CNPJ in standard Brazilian format: XX.XXX.XXX/XXXX-XX
    
    Args:
        cnpj: Raw CNPJ (with or without formatting)
        
    Returns:
        Formatted CNPJ string or None if invalid
        
    Examples:
        >>> format_cnpj("12345678000190")
        '12.345.678/0001-90'
        >>> format_cnpj(12345678000190)
        '12.345.678/0001-90'
    """
    digits = clean_cnpj_digits(cnpj)
    if not digits:
        return None
        
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"


def format_cnpj_basico(cnpj_basico: Union[str, int, None], 
                       cnpj_ordem: Union[str, int, None],
                       cnpj_dv: Union[str, int, None]) -> Optional[str]:
    """
    Build formatted CNPJ from its three components.
    
    Args:
        cnpj_basico: First 8 digits (base CNPJ)
        cnpj_ordem: Next 4 digits (order/establishment)
        cnpj_dv: Last 2 digits (check digits)
        
    Returns:
        Formatted CNPJ or None if any component is invalid
        
    Examples:
        >>> format_cnpj_basico("12345678", "0001", "90")
        '12.345.678/0001-90'
    """
    if pd.isna(cnpj_basico) or pd.isna(cnpj_ordem) or pd.isna(cnpj_dv):
        return None
        
    try:
        # Ensure proper padding
        basico = str(cnpj_basico).zfill(8)
        ordem = str(cnpj_ordem).zfill(4)
        dv = str(cnpj_dv).zfill(2)
        
        # Combine and format
        full_cnpj = basico + ordem + dv
        return format_cnpj(full_cnpj)
    except:
        return None


def validate_cnpj(cnpj: Union[str, int, None]) -> bool:
    """
    Validate CNPJ using check digit algorithm.
    
    Args:
        cnpj: CNPJ to validate (with or without formatting)
        
    Returns:
        True if valid, False otherwise
    """
    digits = clean_cnpj_digits(cnpj)
    if not digits:
        return False
    
    # Check for known invalid patterns (all same digit)
    if len(set(digits)) == 1:
        return False
    
    # Validate check digits
    def calculate_digit(cnpj_partial, weights):
        total = sum(int(d) * w for d, w in zip(cnpj_partial, weights))
        remainder = total % 11
        return 0 if remainder < 2 else 11 - remainder
    
    # First check digit
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    digit1 = calculate_digit(digits[:12], weights1)
    if digit1 != int(digits[12]):
        return False
    
    # Second check digit
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    digit2 = calculate_digit(digits[:13], weights2)
    if digit2 != int(digits[13]):
        return False
    
    return True


# ============================================================================
# CURRENCY AND NUMERIC CLEANING
# ============================================================================

def clean_capital_social(value: Union[str, float, int, None]) -> float:
    """
    Clean and convert capital_social (monetary value).
    
    Handles:
    - Quoted strings: "100,00"
    - Decimal comma: "1.234,56"
    - Decimal point: "1234.56"
    - Missing/NaN values
    
    Args:
        value: Raw capital social value
        
    Returns:
        Float value or 0.0 if invalid/missing
        
    Examples:
        >>> clean_capital_social('"1.234,56"')
        1234.56
        >>> clean_capital_social("100,00")
        100.0
        >>> clean_capital_social(None)
        0.0
    """
    if pd.isna(value) or value is None:
        return 0.0
    
    try:
        # Remove quotes and whitespace
        cleaned = str(value).strip().strip('"')
        
        # Extract only digits, dots, and commas
        cleaned = ''.join(c for c in cleaned if c.isdigit() or c in '.,')
        
        if not cleaned:
            return 0.0
        
        # Replace decimal comma with dot
        cleaned = cleaned.replace(',', '.')
        
        return float(cleaned)
    except (ValueError, AttributeError):
        return 0.0


def clean_decimal(value: Union[str, float, int, None], 
                  default: float = 0.0) -> float:
    """
    Generic decimal/float cleaner.
    
    Args:
        value: Raw numeric value
        default: Default value if cleaning fails
        
    Returns:
        Cleaned float value
    """
    if pd.isna(value) or value is None:
        return default
    
    try:
        cleaned = str(value).strip().strip('"')
        cleaned = ''.join(c for c in cleaned if c.isdigit() or c in '.,')
        if not cleaned:
            return default
        cleaned = cleaned.replace(',', '.')
        return float(cleaned)
    except (ValueError, AttributeError):
        return default


# ============================================================================
# INTEGER CLEANING
# ============================================================================

def clean_integer(value: Union[str, int, None]) -> Optional[int]:
    """
    Clean and convert to integer.
    
    Handles:
    - Quoted strings: "123"
    - Whitespace
    - Empty strings
    - Non-numeric values
    
    Args:
        value: Raw integer value
        
    Returns:
        Integer or None if invalid/missing
        
    Examples:
        >>> clean_integer('"123"')
        123
        >>> clean_integer("  456  ")
        456
        >>> clean_integer("")
        None
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        cleaned = str(value).strip().strip('"')
        if not cleaned:
            return None
        return int(cleaned)
    except (ValueError, AttributeError):
        return None


def clean_int_with_default(value: Union[str, int, None], 
                           default: int = 0) -> int:
    """
    Clean integer with default value for failures.
    
    Args:
        value: Raw integer value
        default: Default value if cleaning fails
        
    Returns:
        Cleaned integer
    """
    result = clean_integer(value)
    return result if result is not None else default


# ============================================================================
# STRING CLEANING
# ============================================================================

def clean_string(value: Union[str, None]) -> Optional[str]:
    """
    Clean string values.
    
    Handles:
    - Quoted strings
    - Whitespace (leading/trailing)
    - Empty strings -> None
    - NaN/None values
    
    Args:
        value: Raw string value
        
    Returns:
        Cleaned string or None if empty/missing
        
    Examples:
        >>> clean_string('"  Hello  "')
        'Hello'
        >>> clean_string("")
        None
        >>> clean_string(None)
        None
    """
    if pd.isna(value) or value is None:
        return None
    
    cleaned = str(value).strip().strip('"')
    return cleaned if cleaned else None


def clean_string_upper(value: Union[str, None]) -> Optional[str]:
    """
    Clean string and convert to uppercase.
    
    Useful for standardizing names, addresses, etc.
    
    Args:
        value: Raw string value
        
    Returns:
        Uppercase cleaned string or None
    """
    cleaned = clean_string(value)
    return cleaned.upper() if cleaned else None


def clean_string_with_default(value: Union[str, None], 
                              default: str = "") -> str:
    """
    Clean string with default value for empty/missing.
    
    Args:
        value: Raw string value
        default: Default value if empty/missing
        
    Returns:
        Cleaned string or default
    """
    result = clean_string(value)
    return result if result is not None else default


# ============================================================================
# DATE CLEANING
# ============================================================================

def clean_cnpj_date(value: Union[str, int, None]) -> Optional[str]:
    """
    Clean CNPJ date format (YYYYMMDD as integer or string).
    
    CNPJ datasets store dates as 8-digit integers (e.g., 20200131 for Jan 31, 2020).
    This function converts them to ISO format (YYYY-MM-DD).
    
    Args:
        value: Date as YYYYMMDD integer or string
        
    Returns:
        ISO format date string (YYYY-MM-DD) or None if invalid
        
    Examples:
        >>> clean_cnpj_date(20200131)
        '2020-01-31'
        >>> clean_cnpj_date("20200131")
        '2020-01-31'
        >>> clean_cnpj_date(0)
        None
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        # Convert to string and clean
        date_str = str(value).strip().strip('"')
        
        # Handle empty or zero dates
        if not date_str or date_str == '0':
            return None
        
        # Ensure 8 digits
        date_str = date_str.zfill(8)
        
        if len(date_str) != 8:
            return None
        
        # Parse and validate
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        
        # Basic validation
        if year < 1900 or year > 2100:
            return None
        if month < 1 or month > 12:
            return None
        if day < 1 or day > 31:
            return None
        
        # Create date object to validate (e.g., Feb 31 would fail)
        date_obj = datetime(year, month, day)
        
        return date_obj.strftime('%Y-%m-%d')
    except (ValueError, AttributeError):
        return None


def clean_date_to_datetime(value: Union[str, int, None]) -> Optional[datetime]:
    """
    Convert CNPJ date to Python datetime object.
    
    Args:
        value: Date as YYYYMMDD integer or string
        
    Returns:
        datetime object or None if invalid
    """
    date_str = clean_cnpj_date(value)
    if not date_str:
        return None
    
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return None


# ============================================================================
# ENUMERATION CLEANING
# ============================================================================

def clean_porte_empresa(value: Union[str, int, None]) -> str:
    """
    Clean and standardize porte_empresa (company size) code.
    
    Possible values:
    - 00: Não informado
    - 01: Micro empresa
    - 03: Empresa de pequeno porte
    - 05: Demais (medium/large)
    
    Args:
        value: Raw porte code
        
    Returns:
        Standardized 2-digit code (always returns valid code, defaulting to '00')
        
    Examples:
        >>> clean_porte_empresa("1")
        '01'
        >>> clean_porte_empresa("5")
        '05'
        >>> clean_porte_empresa("")
        '00'
    """
    if pd.isna(value) or value is None:
        return '00'
    
    try:
        # Clean and zero-pad to 2 digits
        cleaned = str(value).strip().strip('"')
        if not cleaned:
            return '00'
        
        code = cleaned.zfill(2)
        
        # Validate against known codes
        valid_codes = {'00', '01', '03', '05'}
        return code if code in valid_codes else '00'
    except:
        return '00'


def clean_situacao_cadastral(value: Union[str, int, None]) -> Optional[str]:
    """
    Clean situação cadastral (registration status) code.
    
    Common values:
    - 01: NULA
    - 02: ATIVA
    - 03: SUSPENSA
    - 04: INAPTA
    - 08: BAIXADA
    
    Args:
        value: Raw status code
        
    Returns:
        2-digit status code or None if invalid
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        cleaned = str(value).strip().strip('"')
        if not cleaned:
            return None
        return cleaned.zfill(2)
    except:
        return None


# ============================================================================
# BATCH PROCESSING HELPERS
# ============================================================================

def apply_cleaners_to_dataframe(df: pd.DataFrame, 
                               column_cleaners: dict) -> pd.DataFrame:
    """
    Apply multiple cleaning functions to a DataFrame.
    
    Args:
        df: Input DataFrame
        column_cleaners: Dict mapping column names to cleaner functions
                        Example: {'capital_social': clean_capital_social}
    
    Returns:
        DataFrame with cleaned columns (creates copy)
        
    Example:
        >>> cleaners = {
        ...     'capital_social': clean_capital_social,
        ...     'razao_social': clean_string,
        ...     'cnpj': clean_cnpj_digits
        ... }
        >>> df_clean = apply_cleaners_to_dataframe(df, cleaners)
    """
    df_clean = df.copy()
    
    for column, cleaner_func in column_cleaners.items():
        if column in df_clean.columns:
            df_clean[column] = df_clean[column].apply(cleaner_func)
    
    return df_clean


# ============================================================================
# VALIDATION HELPERS
# ============================================================================

def validate_empresa_record(record: dict) -> tuple[bool, list[str]]:
    """
    Validate a complete empresa (company) record.
    
    Args:
        record: Dictionary with empresa fields
        
    Returns:
        Tuple of (is_valid, list_of_errors)
        
    Example:
        >>> record = {'cnpj_basico': '12345678', 'razao_social': 'ACME LTDA'}
        >>> valid, errors = validate_empresa_record(record)
    """
    errors = []
    
    # Required fields
    if not record.get('cnpj_basico'):
        errors.append("Missing cnpj_basico")
    
    if not record.get('razao_social'):
        errors.append("Missing razao_social")
    
    # Validate CNPJ format if present
    cnpj = record.get('cnpj_basico')
    if cnpj and len(str(cnpj)) != 8:
        errors.append(f"Invalid cnpj_basico length: {cnpj}")
    
    # Validate capital_social is non-negative
    capital = record.get('capital_social', 0)
    if capital < 0:
        errors.append(f"Negative capital_social: {capital}")
    
    return len(errors) == 0, errors


def validate_estabelecimento_record(record: dict) -> tuple[bool, list[str]]:
    """
    Validate a complete estabelecimento (establishment) record.
    
    Args:
        record: Dictionary with estabelecimento fields
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Required fields
    required_fields = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']
    for field in required_fields:
        if not record.get(field):
            errors.append(f"Missing {field}")
    
    # Validate full CNPJ if components are present
    if all(record.get(f) for f in required_fields):
        full_cnpj = (
            str(record['cnpj_basico']).zfill(8) +
            str(record['cnpj_ordem']).zfill(4) +
            str(record['cnpj_dv']).zfill(2)
        )
        if not validate_cnpj(full_cnpj):
            errors.append(f"Invalid CNPJ: {full_cnpj}")
    
    return len(errors) == 0, errors


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # CNPJ functions
    'clean_cnpj_digits',
    'format_cnpj',
    'format_cnpj_basico',
    'validate_cnpj',
    
    # Currency/numeric
    'clean_capital_social',
    'clean_decimal',
    
    # Integer
    'clean_integer',
    'clean_int_with_default',
    
    # String
    'clean_string',
    'clean_string_upper',
    'clean_string_with_default',
    
    # Date
    'clean_cnpj_date',
    'clean_date_to_datetime',
    
    # Enumerations
    'clean_porte_empresa',
    'clean_situacao_cadastral',
    
    # Batch processing
    'apply_cleaners_to_dataframe',
    
    # Validation
    'validate_empresa_record',
    'validate_estabelecimento_record',
]
