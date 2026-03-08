# CNPJ Ingestion DAGs

Airflow DAGs for ingesting Brazilian CNPJ (Cadastro Nacional da Pessoa Jurídica) data from Receita Federal into PostgreSQL and Neo4j.

## Overview

This pipeline implements a high-performance ETL pipeline for CNPJ data processing:

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

Airflow DAG chain (typical run):

- `cnpj_download` (scheduled) → downloads ZIPs and updates `cnpj.download_manifest`
- `cnpj_transform` (manual or triggered) → extract + DuckDB SQL transform to Parquet
- `cnpj_load_postgres` (triggered) → bulk load to PostgreSQL
- `cnpj_matview_refresh` (triggered) → refresh `cnpj.mv_company_search`
- `cnpj_load_neo4j` (triggered) → load graph entities to Neo4j
```

## Download DAGs

### `cnpj_download`

Monitora o repositório da Receita Federal (WebDAV) e baixa automaticamente novos meses quando disponíveis.

- **Schedule:** mensal, dia 2 às 02:00 (cron: `0 2 2 * *`)
- **Data dir (raw ZIPs):** `/opt/airflow/data/cnpj/raw/{YYYY-MM}/`
- **Manifest:** atualiza a tabela `cnpj.download_manifest` ao final do fluxo

**Optional parameter:**

- `reference_month` (format `YYYY-MM`, e.g., `2026-02`)
  - default: `auto` (baixa o último mês disponível no servidor, somente se ainda não existir no `cnpj.download_manifest`)
  - when set: baixa/verifica somente o mês informado

**Tasks:**

- `check_new_months` → detecta meses novos (preferencialmente via `cnpj.download_manifest`, com fallback para filesystem)
- `download_new_months` → baixa os arquivos do(s) mês(es) novo(s)
- `verify_downloads` → verifica se o mês está completo (todos os ZIPs esperados)
- `repair_downloads` → tenta rebaixar arquivos faltantes / com size mismatch
- `update_manifest` → executa `pipelines/scripts/cnpj/populate_manifest.py` para registrar os ZIPs no Postgres
- `generate_report` → imprime sumário (tamanho total, meses disponíveis, último mês)

**Manual trigger examples:**

```bash
# Auto mode (default): download latest month if absent in manifest
airflow dags trigger cnpj_download

# Force download a specific month
airflow dags trigger cnpj_download \
  --conf '{"reference_month": "2026-02"}'
```

### `cnpj_download_full_sync`

Full sync manual (útil no setup inicial ou recuperação). Baixa todos os meses e depois:

- `verify_after_sync` → valida integridade
- `update_manifest_after_sync` → popula `cnpj.download_manifest`
- `report_after_sync` → sumário + `df -h` do volume

## Task Groups

### 1. `process_empresas_group`
Processes 10 Empresas files (Empresas0.zip - Empresas9.zip) containing:
- CNPJ básico (8 digits)
- Razão social (company name)
- Legal nature
- Share capital
- Company size

**Output Tables:**
- PostgreSQL: `cnpj.empresa`
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
- PostgreSQL: `cnpj.estabelecimento`
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
POSTGRES_DB=osint_metadata
POSTGRES_USER=osint_admin
POSTGRES_PASSWORD=osint_secure_password

# Neo4j
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=osint_graph_password
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
# Download & update manifest (scheduled monthly, but can be triggered)
airflow dags trigger cnpj_download

# Transform (extract + DuckDB SQL → Parquet). This will trigger the downstream DAGs.
airflow dags trigger cnpj_transform \
  --conf '{"reference_month": "2026-02"}'

# Transform only Empresas
airflow dags trigger cnpj_transform \
  --conf '{"process_estabelecimentos": false, "process_socios": false, "process_simples": false, "process_references": false}'
```

### Backfill Historical Data:
```bash
# Process all months from 2022-08 to 2025-06
for month in 2022-08 2022-09 2022-10 ... 2025-06; do
  airflow dags trigger cnpj_transform \
    --conf "{\"reference_month\": \"$month\"}"
  sleep 300  # Wait 5 minutes between runs
done
```

### Monitor Progress:
```bash
# Watch DAG runs
airflow dags list-runs -d cnpj_transform

# View task logs
airflow tasks logs cnpj_transform transform_empresas_group.process_empresas_file EXECUTION_DATE

# Check database
psql -h localhost -U osint_admin -d osint_metadata \
  -c "SELECT reference_month, COUNT(*) FROM cnpj.empresa GROUP BY reference_month"
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

The authoritative schema is managed by Flyway migrations (see `infrastructure/postgres/migrations/cnpj/`).

Main tables (physical names):

- `cnpj.empresa`
- `cnpj.estabelecimento`
- `cnpj.socio`
- `cnpj.simples_nacional`

Download tracking:

- `cnpj.download_manifest`

Search MatView:

- `cnpj.mv_company_search`

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
