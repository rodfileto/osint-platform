# CNPJ Ingestion DAG

Airflow DAG for ingesting Brazilian CNPJ (Cadastro Nacional da Pessoa Jurídica) data from Receita Federal into PostgreSQL and Neo4j.

## Overview

This DAG implements a high-performance ETL pipeline for CNPJ data processing:

- **Extract**: Unzips downloaded CNPJ files from Receita Federal
- **Transform**: Cleans and validates data using DuckDB pure SQL (10-100x faster than Python)
- **Load**: Inserts into PostgreSQL (relational) and Neo4j (graph)

## Architecture

```
┌─────────────┐
│  Raw ZIPs   │  /data/cnpj/raw/{YYYY-MM}/
└──────┬──────┘
       │ Extract (parallel)
       ▼
┌─────────────┐
│ Staging CSVs│  /data/cnpj/staging/{YYYY-MM}/
└──────┬──────┘
       │ Transform with DuckDB SQL (4M rows/sec)
       ▼
┌─────────────┐
│  Parquet    │  /data/cnpj/processed/{YYYY-MM}/
└──────┬──────┘
       │ Load (bulk insert)
       ▼
┌─────────────┬─────────────┐
│ PostgreSQL  │   Neo4j     │
│  (cnpj.*) │  (Graphs)   │
└─────────────┴─────────────┘
```

## Task Groups

### 1. `process_empresas_group`
Processes 10 Empresas files (Empresas0.zip - Empresas9.zip) containing:
- CNPJ básico (8 digits)
- Razão social (company name)
- Legal nature
- Share capital
- Company size

**Output Tables:**
- PostgreSQL: `cnpj.empresas`
- Neo4j: `:Empresa` nodes

### 2. `process_estabelecimentos_group`
Processes 10 Estabelecimentos files containing:
- Complete CNPJ (14 digits: básico + ordem + DV)
- Trade name
- Registration status
- Address (street, city, state, ZIP)
- Economic activity codes (CNAE)
- Contact information

**Output Tables:**
- PostgreSQL: `cnpj.estabelecimentos`
- Neo4j: `:Estabelecimento` nodes with `[:PERTENCE_A]` relationships to `:Empresa`

### 3. `process_socios_group` (Future)
Will process partner/shareholder data and create relationships.

## Performance

### Benchmark Results (Empresas0.zip, ~5M rows):
- **Extract**: 2-5 seconds (unzip)
- **Transform (DuckDB SQL)**: 3-4 seconds (~4M rows/sec)
- **Load (PostgreSQL)**: 10-20 seconds (bulk insert)
- **Load (Neo4j)**: 30-60 seconds (MERGE batches)

**Total per file**: ~45-90 seconds  
**Total for 10 files (parallel)**: ~2-3 minutes

### Storage:
- Raw ZIP: ~300 MB (Empresas0) → 6 GB (Estabelecimentos0)
- Extracted CSV: ~900 MB → 18 GB
- Parquet (compressed): ~250 MB → 5 GB (70% reduction)

## Configuration

### DAG Parameters (runtime override):
```python
{
    'reference_month': '2024-02',      # Which month to process
    'process_empresas': True,          # Enable/disable empresas
    'process_estabelecimentos': True,  # Enable/disable estabelecimentos
    'process_socios': False            # Not implemented yet
}
```

### Environment Variables:
Set in Airflow connections or environment:
```bash
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=osint_platform
POSTGRES_USER=osint_user
POSTGRES_PASSWORD=osint_password

# Neo4j
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=osint_password
```

### Directory Structure:
```
/opt/airflow/
├── data/cnpj/
│   ├── raw/           # Downloaded ZIP files (by month)
│   │   └── 2024-02/
│   │       ├── Empresas0.zip
│   │       ├── Empresas1.zip
│   │       └── ...
│   ├── staging/       # Extracted CSVs (temporary)
│   │   └── 2024-02/
│   │       ├── empresas_0/
│   │       └── ...
│   └── processed/     # Transformed Parquet (permanent)
│       └── 2024-02/
│           ├── empresas_0.parquet
│           └── ...
└── scripts/cnpj/      # Python modules
    ├── __init__.py
    ├── cleaners.py         # Python cleaners (for reference)
    └── cleaners_sql.py     # SQL templates (used in DAG)
```

## Usage

### Trigger Manually:
```bash
# Process default month (2024-02)
airflow dags trigger cnpj_ingestion

# Process specific month
airflow dags trigger cnpj_ingestion \
  --conf '{"reference_month": "2024-03"}'

# Process only Empresas
airflow dags trigger cnpj_ingestion \
  --conf '{"process_estabelecimentos": false}'
```

### Backfill Historical Data:
```bash
# Process all months from 2022-08 to 2025-06
for month in 2022-08 2022-09 2022-10 ... 2025-06; do
  airflow dags trigger cnpj_ingestion \
    --conf "{\"reference_month\": \"$month\"}"
  sleep 300  # Wait 5 minutes between runs
done
```

### Monitor Progress:
```bash
# Watch DAG runs
airflow dags list-runs -d cnpj_ingestion

# View task logs
airflow tasks logs cnpj_ingestion process_empresas_group.transform_empresas_duckdb EXECUTION_DATE

# Check database
psql -h localhost -U osint_user -d osint_platform \
  -c "SELECT reference_month, COUNT(*) FROM cnpj.empresas GROUP BY reference_month"
```

## SQL Cleaners

The DAG uses pure SQL for transformation (defined in `cleaners_sql.py`):

### Key Functions:
- `clean_cnpj_basico_sql()` - Clean and pad CNPJ to 8 digits
- `clean_capital_social_sql()` - Convert Brazilian currency format (1.234,56 → 1234.56)
- `clean_porte_empresa_sql()` - Validate company size codes (00, 01, 03, 05)
- `clean_cnpj_date_sql()` - Convert YYYYMMDD integer to DATE
- `clean_string_sql()` - Remove quotes, trim whitespace

### Complete Templates:
- `empresas_cleaning_template()` - Full SELECT for all 7 Empresas columns
- `estabelecimentos_cleaning_template()` - Full SELECT for all 28 Estabelecimentos columns
- `build_empresas_query()` - Complete CSV → Parquet transformation
- `build_estabelecimentos_query()` - Complete CSV → Parquet transformation

## Database Schema

### PostgreSQL:

```sql
-- Empresas table
CREATE TABLE cnpj.empresas (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DECIMAL(15,2),
    porte_empresa VARCHAR(2),
    ente_federativo_responsavel TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_month VARCHAR(7)
);

-- Estabelecimentos table
CREATE TABLE cnpj.estabelecimentos (
    cnpj_basico VARCHAR(8),
    cnpj_ordem VARCHAR(4),
    cnpj_dv VARCHAR(2),
    cnpj_completo VARCHAR(14) GENERATED ALWAYS AS (cnpj_basico || cnpj_ordem || cnpj_dv) STORED,
    nome_fantasia TEXT,
    situacao_cadastral INTEGER,
    data_situacao_cadastral DATE,
    municipio TEXT,
    uf VARCHAR(2),
    -- ... 20+ more columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_month VARCHAR(7),
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
    FOREIGN KEY (cnpj_basico) REFERENCES cnpj.empresas(cnpj_basico)
);

-- Indexes for performance
CREATE INDEX idx_empresas_razao_social ON cnpj.empresas USING GIN (to_tsvector('portuguese', razao_social));
CREATE INDEX idx_estabelecimentos_municipio ON cnpj.estabelecimentos(municipio);
CREATE INDEX idx_estabelecimentos_uf ON cnpj.estabelecimentos(uf);
```

### Neo4j:

```cypher
// Empresa nodes
CREATE (e:Empresa {
    cnpj_basico: "12345678",
    razao_social: "ACME LTDA",
    natureza_juridica: 2135,
    capital_social: 5000.00,
    porte_empresa: "01"
})

// Estabelecimento nodes with relationship
CREATE (est:Estabelecimento {
    cnpj: "12345678000190",
    nome_fantasia: "ACME Store",
    municipio: "São Paulo",
    uf: "SP"
})
CREATE (est)-[:PERTENCE_A]->(e)

// Indexes
CREATE INDEX FOR (e:Empresa) ON (e.cnpj_basico);
CREATE INDEX FOR (est:Estabelecimento) ON (est.cnpj);
CREATE FULLTEXT INDEX empresa_names FOR (e:Empresa) ON EACH [e.razao_social];
```

## Troubleshooting

### Common Issues:

1. **DuckDB encoding errors**:
   - Solution: Files should be auto-detected, but if issues persist, check `ignore_errors=true` in CSV reader

2. **PostgreSQL connection timeout**:
   - Check: `docker-compose ps postgres`
   - Verify: Connection settings in DAG match `docker-compose.yml`

3. **Neo4j batch too large**:
   - Reduce `batch_size` in `load_to_neo4j()` function (default: 1000)

4. **Out of memory**:
   - DuckDB processes in chunks automatically
   - If still an issue, split files or increase Airflow worker memory

5. **Slow load performance**:
   - Check: Are PostgreSQL/Neo4j indexes created?
   - Verify: Parallel workers configured in Airflow
   - Monitor: `docker stats` during load

## Next Steps

1. **Implement Socios processing** - Partner/shareholder data with relationships
2. **Add download task** - Scrape Receita Federal website automatically
3. **Incremental updates** - Only process new/changed files
4. **Data quality checks** - Add Great Expectations validation
5. **Monitoring** - Add Prometheus metrics and Grafana dashboards
6. **Cross-references** - Link to Sanctions, Contracts, etc.

## References

- [Receita Federal - CNPJ Open Data](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj)
- [CNPJ File Layout](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Architecture Plan](../../ARCHITECTURE_PLAN.md)
- [Implementation Steps](../../IMPLEMENTATION_STEPS.md)
