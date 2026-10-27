# CNPJ pipeline DAG

`cnpj_pipeline` ([cnpj/cnpj_pipeline_dag.py](cnpj/cnpj_pipeline_dag.py)) ingests the Receita Federal company registry in one run: download, transform, build gold with dbt, publish to PostgreSQL. It runs monthly on the 2nd at 02:00 (America/Sao_Paulo).

## Flow

```
download_release → update_manifest → select_month
  → transform_file ×N (one per ZIP, 4 at a time)
  → build_gold (dbt-duckdb, silver → gold Parquet in MinIO)
  → publish_gold (gold Parquet → PostgreSQL, build-and-swap)
  → rebuild_geo_mapping
```

| Task | Module | What it does |
|---|---|---|
| `download_release` | `tasks/download.py` | Makes the month (requested, or the latest on the server) complete in MinIO; streams only missing or mismatched ZIPs; fails if files are still missing |
| `update_manifest` | `tasks/download.py` | Registers every ZIP in MinIO in `cnpj.download_manifest`, keeping per-file progress |
| `select_month` | `tasks/download.py` | Picks the month; skips the rest of the run when every main file is already transformed and loaded |
| `transform_file` | `tasks/transform.py` | Downloads one ZIP from MinIO, extracts it, runs the DuckDB SQL from `scripts/cnpj/cleaners.py` to Parquet, copies it to MinIO silver, deletes its staging files |
| `build_gold` | `dbt` (`pipelines/dbt`) | Builds the gold Parquet (latest-month tables + `socio_history` + `company_search`) in MinIO from the silver layer; `gold_guard.py` skips months older than the latest silver |
| `publish_gold` | `scripts/cnpj/gold_publish.py` | Copies the gold Parquet into `*_next` tables and swaps them in atomically (indexes, PKs and dependent views recreated inside one transaction) |
| `rebuild_geo_mapping` | `scripts/geo/load_br_geo_reference.py` | Rebuilds the CNPJ municipality → IBGE mapping, which infers each municipality's state from its establishments |

Each task is idempotent, so a retry repeats only that step. Per-file progress (`extracted_at`, `transformed_at`, `loaded_postgres_at`) lives in `cnpj.download_manifest`; a file is marked loaded only after its table's upsert commits.

## Params

| Param | Default | Meaning |
|---|---|---|
| `reference_month` | `auto` | `YYYY-MM`, or `auto` for the latest release |
| `force_reprocess` | `false` | Transform and load the month again even if it is already done |

```bash
airflow dags trigger cnpj_pipeline                                                  # latest month
airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10"}'           # a specific month
airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10", "force_reprocess": true}'
```

## Storage

```
osint-raw/cnpj/raw/<YYYY-MM>/<File>.zip         MinIO, source of truth
/opt/airflow/temp_ssd/cnpj_staging/<YYYY-MM>/    CSV staging, removed after each file
/opt/airflow/temp_ssd/cnpj_processed/<YYYY-MM>/  Parquet (about 5.5 GB per month), read by the load tasks
osint-processed/cnpj/silver/<entity>/reference_month=<YYYY-MM>/
                                                 MinIO copy of the Parquet, kept for every month
```

A transform that is skipped because the month is already done uploads a Parquet file missing from MinIO, or downloads one missing from the temp volume, so wiping the temp volume does not mean transforming again.

`CNPJ_DUCKDB_MEMORY_LIMIT` (default `4GB`) caps DuckDB memory per transform task; it spills to `/opt/airflow/temp_ssd/duckdb_tmp`.

Benchmark from one file (`Empresas0.zip`, ~5M rows): 2 to 5 s extract, 3 to 4 s DuckDB transform. Whole-month times per step are in the main README, under [How long a full load takes](../../README.md#how-long-a-full-load-takes).

## SQL cleaners

`scripts/cnpj/cleaners.py` builds one DuckDB query per file kind: `build_empresas_query`, `build_estabelecimentos_query`, `build_socios_query`, `build_simples_query` and `build_reference_query`. Each reads the semicolon-separated Latin-1 CSV, normalizes codes, dates and Brazilian decimals (`1.234,56` → `1234.56`), and writes ZSTD Parquet.

## Known limitations

See [docs/repo/DATA_ARCHITECTURE_REVIEW.md](../../docs/repo/DATA_ARCHITECTURE_REVIEW.md): `socio` keeps every loaded month while `empresa` and `estabelecimento` keep only the latest. The ownership-network endpoints read only the latest `socio` month.
