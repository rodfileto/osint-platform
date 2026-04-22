# CNPJ Data Cleaners

Reusable data cleaning functions extracted from legacy Django commands for use in independent ETL pipelines.

## Overview

The `cleaners.py` module provides a comprehensive set of pure, composable functions for cleaning Brazilian CNPJ data. These functions are:

- **Pure**: No side effects, same input always produces same output
- **Composable**: Can be chained and combined
- **Pandas-compatible**: Work seamlessly with `Series.apply()`
- **Type-safe**: Handle None/NaN values gracefully
- **Well-documented**: Clear docstrings with examples

## Function Categories

### 1. CNPJ Formatting and Validation

```python
from scripts.cnpj.cleaners import clean_cnpj_digits, format_cnpj, validate_cnpj

# Extract digits from formatted CNPJ
cnpj_digits = clean_cnpj_digits("12.345.678/0001-90")  # → "12345678000190"

# Format CNPJ
formatted = format_cnpj(12345678000190)  # → "12.345.678/0001-90"

# Validate CNPJ using check digit algorithm
is_valid = validate_cnpj("12.345.678/0001-90")  # → True/False

# Build formatted CNPJ from components
full = format_cnpj_basico("12345678", "0001", "90")  # → "12.345.678/0001-90"
```

### 2. Currency and Numeric Cleaning

```python
from scripts.cnpj.cleaners import clean_capital_social, clean_decimal

# Clean monetary values (handles Brazilian format with comma)
capital = clean_capital_social('"1.234,56"')  # → 1234.56
capital = clean_capital_social("100,00")      # → 100.0
capital = clean_capital_social(None)          # → 0.0

# Generic decimal cleaner
value = clean_decimal("123,45", default=0.0)  # → 123.45
```

### 3. Integer Cleaning

```python
from scripts.cnpj.cleaners import clean_integer, clean_int_with_default

# Clean integers (returns None if invalid)
number = clean_integer('"123"')       # → 123
number = clean_integer("")            # → None

# With default value
number = clean_int_with_default("", default=0)  # → 0
```

### 4. String Cleaning

```python
from scripts.cnpj.cleaners import clean_string, clean_string_upper

# Basic string cleaning (removes quotes, whitespace)
name = clean_string('"  ACME LTDA  "')  # → "ACME LTDA"
name = clean_string("")                  # → None

# Clean and uppercase
name = clean_string_upper("  acme  ")    # → "ACME"
```

### 5. Date Cleaning

CNPJ datasets store dates as 8-digit integers (YYYYMMDD format).

```python
from scripts.cnpj.cleaners import clean_cnpj_date, clean_date_to_datetime

# Convert CNPJ date to ISO format
date = clean_cnpj_date(20200131)       # → "2020-01-31"
date = clean_cnpj_date("20200229")     # → "2020-02-29" (leap year)
date = clean_cnpj_date(0)              # → None

# Convert to datetime object
dt = clean_date_to_datetime(20200131)  # → datetime(2020, 1, 31)
```

### 6. Enumeration Cleaning

```python
from scripts.cnpj.cleaners import clean_porte_empresa, clean_situacao_cadastral

# Company size (porte) - always returns valid code
porte = clean_porte_empresa("1")   # → "01" (Micro empresa)
porte = clean_porte_empresa("3")   # → "03" (Pequeno porte)
porte = clean_porte_empresa("5")   # → "05" (Demais)
porte = clean_porte_empresa("")    # → "00" (Não informado)

# Registration status
status = clean_situacao_cadastral("2")  # → "02" (ATIVA)
```

## Batch Processing with DataFrames

The most powerful feature is batch processing entire DataFrames:

```python
import pandas as pd
from scripts.cnpj.cleaners import apply_cleaners_to_dataframe, *

# Load raw data
df = pd.read_csv('empresas.csv', sep=';', encoding='latin1')

# Define cleaners for each column
cleaners = {
    'cnpj_basico': clean_cnpj_digits,
    'razao_social': clean_string,
    'capital_social': clean_capital_social,
    'porte_empresa': clean_porte_empresa,
    'data_inicio_atividade': clean_cnpj_date,
    'natureza_juridica': clean_integer,
    'qualificacao_responsavel': clean_integer,
}

# Apply all cleaners in one call
df_clean = apply_cleaners_to_dataframe(df, cleaners)
```

## Validation Functions

Validate complete records before inserting into database:

```python
from scripts.cnpj.cleaners import validate_empresa_record, validate_estabelecimento_record

# Validate empresa record
record = {
    'cnpj_basico': '12345678',
    'razao_social': 'ACME LTDA',
    'capital_social': 10000.0,
}
is_valid, errors = validate_empresa_record(record)
if not is_valid:
    print(f"Validation errors: {errors}")

# Validate estabelecimento record
record = {
    'cnpj_basico': '12345678',
    'cnpj_ordem': '0001',
    'cnpj_dv': '90',
}
is_valid, errors = validate_estabelecimento_record(record)
```

## Usage in Airflow DAGs

Example of using cleaners in an Airflow ETL task:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from scripts.cnpj.cleaners import apply_cleaners_to_dataframe, *

def extract_and_clean_empresas(**context):
    """Extract and clean empresa data."""
    
    # Extract
    df = pd.read_csv(
        '/data/cnpj/Empresas0.csv',
        sep=';',
        encoding='latin1',
        dtype=str,
        chunksize=10000
    )
    
    # Define cleaners
    cleaners = {
        'cnpj_basico': clean_cnpj_digits,
        'razao_social': clean_string,
        'capital_social': clean_capital_social,
        'porte_empresa': clean_porte_empresa,
        'natureza_juridica': clean_integer,
    }
    
    cleaned_chunks = []
    for chunk in df:
        # Clean chunk
        chunk_clean = apply_cleaners_to_dataframe(chunk, cleaners)
        
        # Validate
        chunk_clean['is_valid'] = chunk_clean.apply(
            lambda row: validate_empresa_record(row.to_dict())[0],
            axis=1
        )
        
        # Filter only valid records
        chunk_clean = chunk_clean[chunk_clean['is_valid']]
        cleaned_chunks.append(chunk_clean)
    
    # Combine all chunks
    df_final = pd.concat(cleaned_chunks, ignore_index=True)
    
    # Save to staging
    df_final.to_parquet('/data/staging/empresas_clean.parquet')

# Create DAG task
clean_task = PythonOperator(
    task_id='clean_empresas',
    python_callable=extract_and_clean_empresas,
    dag=dag,
)
```

## Testing

Run the test suite to verify all functions work correctly:

```bash
cd /media/mynewdrive/osint-platform/pipelines/scripts/cnpj
python test_cleaners.py
```

## Performance Considerations

1. **Use `apply_cleaners_to_dataframe`** for batch operations instead of applying cleaners individually
2. **Process in chunks** when dealing with large files (use `pd.read_csv(..., chunksize=10000)`)
3. **Validate after cleaning** to catch any data quality issues
4. **Use DuckDB** for heavy transformations after basic cleaning

## Integration with DuckDB

After cleaning with pandas, you can load into DuckDB for advanced transformations:

```python
import duckdb

# Clean with pandas
df_clean = apply_cleaners_to_dataframe(df_raw, cleaners)

# Load into DuckDB for further processing
con = duckdb.connect()
con.execute("CREATE TABLE empresas_clean AS SELECT * FROM df_clean")

# Complex transformations in DuckDB
result = con.execute("""
    SELECT 
        porte_empresa,
        COUNT(*) as count,
        SUM(capital_social) as total_capital
    FROM empresas_clean
    WHERE capital_social > 0
    GROUP BY porte_empresa
    ORDER BY total_capital DESC
""").fetchdf()
```

## Complete Field Mapping

### Empresa (Companies)
```python
empresa_cleaners = {
    'cnpj_basico': clean_cnpj_digits,
    'razao_social': clean_string,
    'natureza_juridica': clean_integer,
    'qualificacao_responsavel': clean_integer,
    'capital_social': clean_capital_social,
    'porte_empresa': clean_porte_empresa,
    'ente_federativo_responsavel': clean_string,
}
```

### Estabelecimento (Establishments)
```python
estabelecimento_cleaners = {
    'cnpj_basico': clean_cnpj_digits,
    'cnpj_ordem': lambda x: str(clean_integer(x) or '').zfill(4),
    'cnpj_dv': lambda x: str(clean_integer(x) or '').zfill(2),
    'identificador_matriz_filial': clean_integer,
    'nome_fantasia': clean_string,
    'situacao_cadastral': clean_situacao_cadastral,
    'data_situacao_cadastral': clean_cnpj_date,
    'motivo_situacao_cadastral': clean_integer,
    'nome_cidade_exterior': clean_string,
    'pais': clean_integer,
    'data_inicio_atividade': clean_cnpj_date,
    'cnae_fiscal_principal': clean_integer,
    'cnae_fiscal_secundaria': clean_string,
    'tipo_logradouro': clean_string,
    'logradouro': clean_string,
    'numero': clean_string,
    'complemento': clean_string,
    'bairro': clean_string,
    'cep': clean_integer,
    'uf': clean_string,
    'municipio': clean_integer,
    'ddd_1': clean_integer,
    'telefone_1': clean_integer,
    'ddd_2': clean_integer,
    'telefone_2': clean_integer,
    'ddd_fax': clean_integer,
    'fax': clean_integer,
    'correio_eletronico': clean_string,
    'situacao_especial': clean_string,
    'data_situacao_especial': clean_cnpj_date,
}
```

## Next Steps

1. **Step 2C**: Use these cleaners in the Airflow DAG (next task)
2. **Step 3**: Integrate with DuckDB for heavy transformations
3. **Step 4**: Load cleaned data into PostgreSQL and Neo4j

---

**Created**: February 2026  
**Purpose**: Modular, reusable data cleaning for OSINT Platform  
**Architecture**: Follows the "separate processing from application" principle
