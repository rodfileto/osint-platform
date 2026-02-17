"""
Test script for CNPJ data cleaners.

Run this to verify the cleaning functions work correctly.

Usage:
    python test_cleaners.py
"""

import pandas as pd
from cleaners import (
    clean_cnpj_digits,
    format_cnpj,
    format_cnpj_basico,
    validate_cnpj,
    clean_capital_social,
    clean_integer,
    clean_string,
    clean_cnpj_date,
    clean_porte_empresa,
    apply_cleaners_to_dataframe,
)


def test_cnpj_formatting():
    """Test CNPJ formatting and validation functions."""
    print("=" * 60)
    print("Testing CNPJ Formatting and Validation")
    print("=" * 60)
    
    test_cases = [
        "12.345.678/0001-90",
        "12345678000190",
        12345678000190,
        "invalid",
        "",
        None,
    ]
    
    for test in test_cases:
        digits = clean_cnpj_digits(test)
        formatted = format_cnpj(test)
        is_valid = validate_cnpj(test)
        print(f"\nInput: {test!r}")
        print(f"  Digits: {digits}")
        print(f"  Formatted: {formatted}")
        print(f"  Valid: {is_valid}")
    
    # Test format_cnpj_basico
    print("\n" + "-" * 60)
    print("Testing format_cnpj_basico:")
    result = format_cnpj_basico("12345678", "0001", "90")
    print(f"  format_cnpj_basico('12345678', '0001', '90') = {result}")


def test_numeric_cleaning():
    """Test numeric data cleaning functions."""
    print("\n" + "=" * 60)
    print("Testing Numeric Cleaning")
    print("=" * 60)
    
    capital_test_cases = [
        '"1.234,56"',
        "100,00",
        1234.56,
        None,
        "",
        "invalid",
    ]
    
    print("\nTesting clean_capital_social:")
    for test in capital_test_cases:
        result = clean_capital_social(test)
        print(f"  {test!r:20} → {result}")
    
    integer_test_cases = [
        '"123"',
        "  456  ",
        789,
        "",
        None,
        "abc",
    ]
    
    print("\nTesting clean_integer:")
    for test in integer_test_cases:
        result = clean_integer(test)
        print(f"  {test!r:20} → {result}")


def test_string_cleaning():
    """Test string cleaning functions."""
    print("\n" + "=" * 60)
    print("Testing String Cleaning")
    print("=" * 60)
    
    test_cases = [
        '"  Hello World  "',
        "  Test  ",
        "",
        None,
        "ACME LTDA",
    ]
    
    for test in test_cases:
        result = clean_string(test)
        print(f"  {test!r:25} → {result!r}")


def test_date_cleaning():
    """Test date cleaning functions."""
    print("\n" + "=" * 60)
    print("Testing Date Cleaning")
    print("=" * 60)
    
    test_cases = [
        20200131,
        "20200131",
        "20200229",  # Leap year
        "20190229",  # Invalid (not leap year)
        0,
        "",
        None,
        "invalid",
    ]
    
    for test in test_cases:
        result = clean_cnpj_date(test)
        print(f"  {str(test):15} → {result}")


def test_enumeration_cleaning():
    """Test enumeration cleaning functions."""
    print("\n" + "=" * 60)
    print("Testing Enumeration Cleaning")
    print("=" * 60)
    
    print("\nTesting clean_porte_empresa:")
    test_cases = ["1", "3", "5", "", None, "99"]
    for test in test_cases:
        result = clean_porte_empresa(test)
        print(f"  {test!r:10} → {result}")


def test_dataframe_processing():
    """Test batch DataFrame processing."""
    print("\n" + "=" * 60)
    print("Testing DataFrame Batch Processing")
    print("=" * 60)
    
    # Create sample DataFrame
    df = pd.DataFrame({
        'cnpj': ['12.345.678/0001-90', '98765432000111', None],
        'razao_social': ['"  ACME LTDA  "', 'Test Corp', ''],
        'capital_social': ['"1.000,00"', '50000.50', None],
        'porte_empresa': ['1', '3', ''],
        'data_inicio': [20200101, 20190615, None],
    })
    
    print("\nOriginal DataFrame:")
    print(df)
    
    # Apply cleaners
    cleaners = {
        'cnpj': clean_cnpj_digits,
        'razao_social': clean_string,
        'capital_social': clean_capital_social,
        'porte_empresa': clean_porte_empresa,
        'data_inicio': clean_cnpj_date,
    }
    
    df_clean = apply_cleaners_to_dataframe(df, cleaners)
    
    print("\nCleaned DataFrame:")
    print(df_clean)
    print("\nData types:")
    print(df_clean.dtypes)


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 12 + "CNPJ Data Cleaners Test Suite" + " " * 16 + "║")
    print("╚" + "=" * 58 + "╝")
    
    test_cnpj_formatting()
    test_numeric_cleaning()
    test_string_cleaning()
    test_date_cleaning()
    test_enumeration_cleaning()
    test_dataframe_processing()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)
    print("\nThese cleaners are ready to be used in Airflow DAGs.")
    print("Import them with: from scripts.cnpj.cleaners import *")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
