# CNPJ scripts

Helpers imported by the `cnpj_pipeline` DAG (`pipelines/dags/cnpj/cnpj_pipeline_dag.py`). The DAGs add this directory to `sys.path`, so modules are imported by their bare name (`from cleaners import ...`).

| Module | Role |
|---|---|
| `downloader.py` | Lists Receita Federal releases, compares them with MinIO and streams missing ZIPs to `osint-raw/cnpj/raw/<YYYY-MM>/` |
| `manifest_tracker.py` | Reads and updates per-file pipeline progress in `cnpj.download_manifest` (extracted, transformed, loaded) |
| `cleaners.py` | DuckDB SQL builders that turn the raw CSVs into typed Parquet: `build_empresas_query`, `build_estabelecimentos_query`, `build_socios_query`, `build_simples_query`, `build_reference_query` |
| `test_reference_query_builders.py` | Unit test for the reference-table query builder |

Run the test inside the Airflow container:

```bash
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml exec -w /opt/airflow/scripts/cnpj \
  airflow-scheduler python -m unittest test_reference_query_builders
```
