# Architecture

How the platform is built today. For a critique of the storage design and the recommended changes, see [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md). Setup and configuration live in the [README](../../README.md) and [`.env.example`](../../.env.example).

## Layers

| Layer | Responsibility | Location |
|---|---|---|
| Pipelines | Download, transform and load each source | `pipelines/` (Airflow DAGs and scripts) |
| Storage | Raw files, relational tables, graph | MinIO, PostgreSQL + PostGIS, Neo4j |
| API | Stateless read-only gateway for the UI | `backend/` (FastAPI) |
| Frontend | Search, detail pages, networks, maps | `frontend/` (React + Vite) |
| Auth | OIDC login for UI and API | Keycloak |

Pipelines never serve HTTP; the API never runs ETL. The API only reads what the pipelines produced.

## Pipelines

Each source runs as one DAG: `cnpj_pipeline`, `finep_pipeline` and `inpi_pipeline`. Download DAGs record every file in a per-source `download_manifest` table and skip or register files already in MinIO, so re-runs never download twice.

| Source | Chain | Schedule |
|---|---|---|
| CNPJ (Receita Federal) | One DAG, `cnpj_pipeline`: download → select month → transform (one task per ZIP) → load each table → refresh search views → Neo4j. A month that is already loaded is skipped. | Monthly, 2nd at 02:00 |
| FINEP | One DAG, `finep_pipeline`: download both spreadsheets → load the 7 tables (each only when a newer file arrived); `finep_nlp_topic` is triggered manually | Monthly, 2nd at 06:00 |
| INPI | One DAG, `inpi_pipeline`: download the 10 CSVs (a snapshot is stored only when its content changed) → stage each table → publish all tables in one transaction → search view → Neo4j | Weekly, Monday 05:00 |

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
| `finep` | Contracts, disbursements, NLP topics, download manifest |
| `inpi` | 10 patent tables (applicants keep holder history: one row per holder with `data_inicio` / `data_fim`), `mv_patent_search`, download manifest |
| `geo` | IBGE states and municipalities, CNPJ municipality mapping (the `geo-bootstrap` service loads the geometries; `cnpj_pipeline` rebuilds the mapping) |
| `public` | Airflow metadata and the Flyway history |

Cross-domain joins go through `cnpj_basico`: INPI applicants are resolved to `inpi.patentes_depositantes.cnpj_basico_resolved` at load time.

### Neo4j

| Node | Key | Source |
|---|---|---|
| `Empresa` | `cnpj_basico` | CNPJ |
| `Pessoa` | `pessoa_id` (MD5 of normalized fields) | CNPJ partners |
| `ProjetoFinep` | `id` | FINEP |
| `Patente` | `codigo_interno` | INPI |
| `ClasseIPC` | `simbolo` | INPI |
| `DepositantePF` | `depositante_id` (hash) | INPI individuals |

| Relationship | From → To | Source |
|---|---|---|
| `SOCIO_DE` | `Pessoa → Empresa` | CNPJ |
| `PROPOSTO_POR`, `EXECUTADO_POR` | `ProjetoFinep → Empresa` | FINEP |
| `SIMILAR_TO` | `ProjetoFinep → ProjetoFinep` | FINEP NLP (k-NN cosine) |
| `DEPOSITOU` | `Empresa` or `DepositantePF → Patente` | INPI |
| `CLASSIFICADA_COMO` | `Patente → ClasseIPC` | INPI |
| `VINCULADA_A` | `Patente → Patente` | INPI |

The API reads Neo4j only for the CNPJ company and person networks and a search preview. The INPI and FINEP graph data is loaded but not yet used by the UI.

## API and frontend

- **Backend:** FastAPI routers `auth`, `search`, `cnpj`, `finep`, `inpi` under `/api`; all except `auth` require a Keycloak token. PostgreSQL through SQLAlchemy async over asyncpg, Neo4j through the official driver.
- **Frontend:** pages for CNPJ search, company detail, person search and detail, FINEP overview and INPI overview. Cytoscape for networks, Leaflet for maps, ApexCharts for charts.

## Stack

| Component | Tool |
|---|---|
| Containers | Docker Compose: shared `docker-compose.yml` plus `compose.prod.yml` / `compose.dev.yml` overlays |
| Orchestration | Apache Airflow 2.10 (LocalExecutor, metadata in PostgreSQL) |
| Relational | PostgreSQL 16 + PostGIS |
| Graph | Neo4j 5 Community + APOC |
| Object storage | MinIO (S3 API) |
| Transforms | DuckDB, Python |
| NLP | sentence-transformers, UMAP, HDBSCAN, BERTopic |
| Auth | Keycloak 26 (OIDC) |

Container paths are fixed (`/opt/airflow/data`, `/opt/airflow/temp_ssd`, `/var/lib/postgresql/data`, `/data`); which host disk backs each one is set only by the `*_HOST_PATH` variables in `.env`.
