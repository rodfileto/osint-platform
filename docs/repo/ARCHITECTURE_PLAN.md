# Architecture

How the platform is built. Setup and configuration are in the [README](../../README.md) and [`.env.example`](../../.env.example); a critique of the storage design is in [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md).

## Layers

| Layer | Role | Where |
|---|---|---|
| Pipelines | Download, transform and load each source | `pipelines/` (Airflow) |
| Storage | Raw files; relational tables | MinIO; PostgreSQL + PostGIS |
| API | Read-only gateway for the UI | `backend/` (FastAPI) |
| Frontend | Search, detail pages, networks, maps | `frontend/` (React + Vite) |
| Auth | OIDC login for UI and API | Keycloak |

The API only reads what the pipelines wrote.

## Pipelines

One DAG per source. Each records its files in a `download_manifest` table, so nothing is downloaded or loaded twice.

| DAG | Steps | Schedule (America/Sao_Paulo) |
|---|---|---|
| `cnpj_pipeline` | download → select month → DuckDB transform to Parquet (one task per ZIP) → load each table → refresh search views | Monthly, 2nd at 02:00 |
| `finep_pipeline` | download both spreadsheets → load the 7 tables | Monthly, 2nd at 06:00 |
| `inpi_pipeline` | download the 10 CSVs → stage → publish all tables in one transaction → search view | Weekly, Monday 05:00 |

`finep_nlp_topic` (topics and project similarity) is triggered by hand.

## Storage

**MinIO**, bucket `osint-raw`, keeps the downloads as they came: `cnpj/raw/<YYYY-MM>/*.zip` and `inpi/patentes/<YYYYMMDD>/*.csv`. FINEP spreadsheets are on the Airflow data volume; CNPJ CSV and Parquet staging is on the temp volume and can be regenerated.

**PostgreSQL**, one schema per domain, migrations in `infrastructure/postgres/migrations/` (Flyway):

| Schema | Content |
|---|---|
| `cnpj` | Companies, establishments, partners, reference tables, search materialized views |
| `finep` | Contracts, disbursements, NLP topics, project similarity |
| `inpi` | 10 patent tables (applicants with holder history), `mv_patent_search` |
| `geo` | IBGE states and municipalities, CNPJ municipality mapping |
| `public` | Airflow metadata, Flyway history |

Domains join on `cnpj_basico`; INPI applicants are resolved to it at load time.

## Ownership networks

Built from `cnpj.socio` (latest month) by `backend/api/services/cnpj_network.py`: one indexed query per hop, up to depth 3, stopping at `max_edges` (default 200, at most `NETWORK_MAX_EDGES`). It relies on `idx_socio_cpf_nome` and the partial index `idx_socio_pj_cnpj_basico`. Wider analyses (shared partners, holding chains) are SQL joins or recursive CTEs.

On 28.5M partner rows:

| Query | Time |
|---|---|
| Depth 3, p95: companies with most partners / people with most companies / random companies (in PostgreSQL) | 0.7 / 0.6 / 1.2 ms |
| Through the API: hub company depth 1 / ordinary company depth 3 / hub person depth 2 | 44 / 14 / 43 ms |
| FINEP companies → partners → their other companies (3,233 → 9,204 → 36,018) | 0.3 s |

## API and frontend

- **Backend:** routers `auth`, `search`, `cnpj`, `finep`, `inpi` under `/api`; all but `auth` require a Keycloak token. SQLAlchemy async over asyncpg.
- **Frontend:** CNPJ search, company detail, person search and detail, FINEP and INPI panels. Cytoscape, Leaflet, ApexCharts.

## Stack

Docker Compose (`docker-compose.yml` plus `compose.prod.yml` / `compose.dev.yml`), Airflow 2.10 (LocalExecutor), PostgreSQL 16 + PostGIS, MinIO, DuckDB, sentence-transformers / UMAP / HDBSCAN / BERTopic, Keycloak 26.

Container paths are fixed; the host disk behind each is set by the `*_HOST_PATH` variables in `.env`.
