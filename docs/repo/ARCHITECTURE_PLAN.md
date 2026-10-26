# Architecture

How the platform is built today. For a critique of the storage design and the recommended changes, see [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md). Setup and configuration live in the [README](../../README.md) and [`.env.example`](../../.env.example).

## Layers

| Layer | Responsibility | Location |
|---|---|---|
| Pipelines | Download, transform and load each source | `pipelines/` (Airflow DAGs and scripts) |
| Storage | Raw files, relational tables | MinIO, PostgreSQL + PostGIS |
| API | Stateless read-only gateway for the UI | `backend/` (FastAPI) |
| Frontend | Search, detail pages, networks, maps | `frontend/` (React + Vite) |
| Auth | OIDC login for UI and API | Keycloak |

Pipelines never serve HTTP; the API never runs ETL. The API only reads what the pipelines produced.

## Pipelines

Each source runs as one DAG: `cnpj_pipeline`, `finep_pipeline` and `inpi_pipeline`. Download DAGs record every file in a per-source `download_manifest` table and skip or register files already in MinIO, so re-runs never download twice.

| Source | Chain | Schedule |
|---|---|---|
| CNPJ (Receita Federal) | One DAG, `cnpj_pipeline`: download → select month → transform (one task per ZIP) → load each table → refresh search views. A month that is already loaded is skipped. | Monthly, 2nd at 02:00 |
| FINEP | One DAG, `finep_pipeline`: download both spreadsheets → load the 7 tables (each only when a newer file arrived); `finep_nlp_topic` is triggered manually | Monthly, 2nd at 06:00 |
| INPI | One DAG, `inpi_pipeline`: download the 10 CSVs (a snapshot is stored only when its content changed) → stage each table → publish all tables in one transaction → search view | Weekly, Monday 05:00 |

Schedule times are in Airflow's default timezone, America/Sao_Paulo. CNPJ transforms run in DuckDB SQL: ZIPs are extracted to CSV staging and written as Parquet, then bulk-loaded.

## Storage

### MinIO (raw layer)

The bucket `osint-raw` is the source of truth for downloaded files:

- `cnpj/raw/<YYYY-MM>/<File>.zip`: streamed straight from the Receita Federal server
- `inpi/patentes/<YYYYMMDD>/<FILE>.csv`: one immutable snapshot per upstream version

FINEP spreadsheets are kept on the Airflow data volume (`/opt/airflow/data/finep/`). CNPJ CSV staging and Parquet live on the Airflow temp volume and can be regenerated from the raw ZIPs.

### PostgreSQL

One database, one schema per domain, managed by Flyway (`infrastructure/postgres/migrations/`):

| Schema | Content |
|---|---|
| `cnpj` | Companies, establishments, partners, reference tables, download manifest, search materialized views |
| `finep` | Contracts, disbursements, NLP topics and project similarity, download manifest |
| `inpi` | 10 patent tables (applicants keep holder history: one row per holder with `data_inicio` / `data_fim`), `mv_patent_search`, download manifest |
| `geo` | IBGE states and municipalities, CNPJ municipality mapping (the `geo-bootstrap` service loads the geometries; `cnpj_pipeline` rebuilds the mapping) |
| `public` | Airflow metadata and the Flyway history |

Cross-domain joins go through `cnpj_basico`: INPI applicants are resolved to `inpi.patentes_depositantes.cnpj_basico_resolved` at load time.

### Ownership networks and graph analysis

There is no graph database: the company and person networks are built from `cnpj.socio` (latest `reference_month`) by `backend/api/services/cnpj_network.py`, one indexed query per hop, stopping at the requested `max_edges` (default 200, at most `NETWORK_MAX_EDGES`). Depth 3 around the largest hubs takes milliseconds. Co-ownership analyses (FINEP companies sharing partners, holding chains above patent holders) run as SQL joins or recursive CTEs. FINEP project similarity is the table `finep.nlp_project_similarity`. If graph algorithms (communities, centrality) are needed, the plan is to extract the relevant subgraph in a DAG task, run NetworkX or igraph on it and write the results back to PostgreSQL.

Neo4j was removed on 2026-10-25 (the plan, before it was carried out, is `docs/repo/NEO4J_REMOVAL_PLAN.md` in commit `21a5857`). Measured then, on 28.5M `socio` rows:

| Query | Time |
|---|---|
| Ego network, depth 3, p95 over the 20 companies with most partners / 20 people with most companies / 50 random companies (inside PostgreSQL) | 0.7 / 0.6 / 1.2 ms |
| Same through the API: hub company depth 1, ordinary company depth 3, hub person depth 2 (200 edges) | 44 / 14 / 43 ms |
| Hub company depth 3 with `max_edges=2000` | 36 ms |
| FINEP companies → partners → every company those partners own (3,233 → 9,204 → 36,018) | 0.3 s |
| Patent holders sharing a partner (28,629 holders, 3,888 pairs) | 3.6 s |
| Holding chain above patent holders, 4 levels (recursive CTE) | 1.3 s |

The network needs `idx_socio_cpf_nome` and the partial index `idx_socio_pj_cnpj_basico` (companies a company is a partner in); without the latter that lookup scans `socio` (about 1 s).

## API and frontend

- **Backend:** FastAPI routers `auth`, `search`, `cnpj`, `finep`, `inpi` under `/api`; all except `auth` require a Keycloak token. PostgreSQL through SQLAlchemy async over asyncpg.
- **Frontend:** pages for CNPJ search, company detail, person search and detail, FINEP overview and INPI overview. Cytoscape for networks, Leaflet for maps, ApexCharts for charts.

## Stack

| Component | Tool |
|---|---|
| Containers | Docker Compose: shared `docker-compose.yml` plus `compose.prod.yml` / `compose.dev.yml` overlays |
| Orchestration | Apache Airflow 2.10 (LocalExecutor, metadata in PostgreSQL) |
| Relational | PostgreSQL 16 + PostGIS |
| Object storage | MinIO (S3 API) |
| Transforms | DuckDB, Python |
| NLP | sentence-transformers, UMAP, HDBSCAN, BERTopic |
| Auth | Keycloak 26 (OIDC) |

Container paths are fixed (`/opt/airflow/data`, `/opt/airflow/temp_ssd`, `/var/lib/postgresql/data`, `/data` for MinIO); which host disk backs each one is set only by the `*_HOST_PATH` variables in `.env`.
