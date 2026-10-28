# OSINT Platform

[![Tests](https://github.com/rodfileto/osint-platform/actions/workflows/tests.yml/badge.svg)](https://github.com/rodfileto/osint-platform/actions/workflows/tests.yml)

Search Brazil's company registry, see who owns what, and follow public innovation money.

The platform ingests the federal company registry (CNPJ: about 28.5 million partner records), FINEP innovation funding and INPI patents. Airflow and DuckDB clean them every month into PostgreSQL, and a FastAPI + React app serves search, ownership networks and maps over them.

[Leia em português](README.pt-BR.md)

## Quick demo

Needs Docker. Starts a copy of the stack with fictitious companies and people plus the full public FINEP data, on its own ports:

```bash
git clone https://github.com/rodfileto/osint-platform.git && cd osint-platform
cp dev/.env.example dev/.env
./dev/scripts/start.sh
```

After about 7 minutes, open `http://localhost:13000`, sign in as `osint-admin` / `osint123`, search for `acme` and open the company to see its ownership network. More in [Getting started](#getting-started).

**Highlights**

- **Orchestrated ETL:** Apache Airflow DAGs for each source, from scheduled monthly downloads through transform and load to materialized-view refresh.
- **Large-scale processing:** the CNPJ registry has tens of millions of companies and establishments. DuckDB SQL transforms one ~5M-row file in 3 to 4 seconds (about 4M rows/s), and Parquet shrinks it by about 70%.
- **Layered storage:** raw ZIPs and CSVs land in MinIO (S3 API) and are cleaned into Parquet, also kept in MinIO for every month, then loaded into PostgreSQL/PostGIS, which serves search, ownership networks and maps.
- **Idempotent and traceable:** download manifests record every file and reference month, so nothing is downloaded or reprocessed twice. Changes on the source are detected with ETag/Last-Modified checks.
- **Schema as code:** Flyway migrations for each domain schema (`cnpj`, `finep`, `inpi`, `geo`).
- **NLP (experimental, run by hand):** FINEP project texts are topic-modeled with multilingual sentence-transformers, UMAP, HDBSCAN and BERTopic. The k-NN similarity between projects is stored in PostgreSQL.
- **Full stack:** a FastAPI backend, a React + Vite frontend (Cytoscape network graphs, Leaflet maps, ApexCharts) and Keycloak (OIDC) authentication.

## Engineering decisions

- **No graph database.** Ownership networks used to be loaded into Neo4j every month. They are now built in PostgreSQL with one indexed query per hop over the 28.5 million partner rows: depth 3 around the largest hubs takes 0.6 to 1.2 ms (p95) in the database and 14 to 44 ms through the API. One service and 56 GB of disk fewer, and no second copy of the data to keep in sync. Measurements in [ARCHITECTURE_PLAN.md](docs/repo/ARCHITECTURE_PLAN.md#ownership-networks).
- **Search that uses its indexes.** Company search scanned the whole search views (17 to 30 s). Prefix indexes on the CNPJ columns and dropping a `COALESCE` that hid the name column from its index brought it to 3 to 660 ms.
- **One DAG instead of five.** The CNPJ load was five chained DAGs that passed parameters as strings (so `'False'` counted as true), downloaded every ZIP twice and marked files as loaded before the upsert committed. It is now a single `cnpj_pipeline` with typed parameters that builds gold with dbt and publishes it with a table swap; the rewrite deleted 9,547 lines and added 1,510.
- **Keep the raw files.** Downloads stay in MinIO as they came from the source, with a manifest of every file and month. The database can be dropped and rebuilt without downloading again: a full rebuild of the 2026-10 release took 1 h 10 min for CNPJ, 8 min for INPI and under 1 min for FINEP.
- **Tested on every push.** GitHub Actions runs the DuckDB cleaning SQL and FINEP loader tests, the network and search queries against PostgreSQL with the real migrations, and the frontend build.

## Screenshots

Taken from the dev replica. The companies and people are fictitious; the FINEP panel uses the full public FINEP dataset.

**Ownership network:** a company, its partners, and the partners' other companies (depth 2).

![Ownership network of a company](docs/images/network.png)

**Company search** by name or CNPJ.

![Company search results](docs/images/search.png)

**FINEP panel:** funded companies and projects, by funding line and state.

![FINEP funding panel](docs/images/finep.png)

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Public sources"]
        RF["Receita Federal<br/>CNPJ registry"]
        FINEP["FINEP<br/>funding spreadsheets"]
        INPI["INPI<br/>patents open data"]
        OPENALEX["OpenAlex<br/>scholarly works snapshot"]
    end

    subgraph Airflow["Apache Airflow"]
        DL["download DAGs<br/>(cron + manifest)"]
        TR["transform<br/>(DuckDB SQL)"]
        GOLD["CNPJ gold<br/>(dbt-duckdb)"]
        LD["publish / load<br/>(table swap)"]
        NLP["NLP topic DAG<br/>(BERTopic)"]
    end

    RAW[("MinIO / S3 bronze<br/>ZIP, CSV, XLSX, OpenAlex Parquet")]
    PQ[("Parquet silver<br/>CNPJ in MinIO, OpenAlex on NVMe")]
    GQ[("Parquet gold<br/>CNPJ, in MinIO")]
    CAT[("DuckDB catalog<br/>OpenAlex lake.duckdb")]
    PG[("PostgreSQL + PostGIS<br/>cnpj, finep, inpi, geo")]
    API["FastAPI backend"]
    UI["React frontend<br/>search, networks, maps"]
    KC["Keycloak (OIDC)"]

    RF & FINEP & INPI & OPENALEX --> DL --> RAW --> TR --> PQ
    PQ --> GOLD --> GQ --> LD --> PG
    PQ --> CAT
    PG <--> NLP
    PG --> API --> UI
    KC -. tokens .-> UI
    KC -. validates .-> API
```

### The CNPJ pipeline

One DAG, `cnpj_pipeline`, runs monthly (on the 2nd at 02:00):

| Step | Tasks | What it does |
|---|---|---|
| Download | `download_release`, `update_manifest` | Streams the latest release's ZIPs straight to MinIO (`osint-raw/cnpj/raw/{YYYY-MM}/`) and records them in `cnpj.download_manifest` |
| Select | `select_month` | Picks the month to process and skips the rest of the run if it is already loaded |
| Transform | `transform_file` (one per ZIP) | Downloads the ZIP from MinIO, extracts it, and cleans and validates it in pure DuckDB SQL, writing Parquet locally and to `osint-processed/cnpj/silver/` |
| Gold | `build_gold` | `dbt build` over the silver Parquet (dbt-duckdb, [`pipelines/dbt/`](pipelines/dbt/)): latest-month tables with duplicate keys resolved, `socio_history` over every silver month, and `company_search`, written as gold Parquet to `osint-processed/cnpj/gold/`, with 42 data tests |
| Publish | `publish_gold` | Copies gold into empty `cnpj.*_next` tables, indexes them, and swaps them in one transaction (the previous tables stay as `*_old` for rollback); the API never sees a partial load |
| Geo | `rebuild_geo_mapping` | Links CNPJ municipalities to the IBGE geometries |

See the medallion layout in [docs/repo/MEDALLION_MIGRATION_PLAN.md](docs/repo/MEDALLION_MIGRATION_PLAN.md). A monthly run takes about 1 h 10 min once the release is in MinIO (see [How long a full load takes](#how-long-a-full-load-takes)). To process a specific month again:

```bash
airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10", "force_reprocess": true}'
```

FINEP and INPI follow the same pattern: a downloader with a manifest, then loaders for each dataset. See [`pipelines/dags/README_CNPJ_DAG.md`](pipelines/dags/README_CNPJ_DAG.md) for details.

### The OpenAlex lake

`openalex_pipeline` keeps a local copy of [OpenAlex](https://openalex.org/), the open catalogue of scholarly works, as bronze and silver layers for DuckDB queries (no gold or PostgreSQL tables yet; see [docs/repo/OPENALEX_INTEGRATION_PLAN.md](docs/repo/OPENALEX_INTEGRATION_PLAN.md)). OpenAlex publishes a full snapshot quarterly, on the second Wednesday of January, April, July and October, so the DAG runs daily on the 8th–21st of those months and stops at its first task once the release is in.

| Step | Task | What it does |
|---|---|---|
| Check | `check_release` | Compares the upstream manifest's release with `openalex.release` and skips the run when it is already built |
| Bronze | `sync_bronze`, `verify_bronze` | Mirrors `s3://openalex/data/parquet/` (without `authors` and `awards`) into `osint-raw/openalex/parquet/`, then checks every object's size and Parquet record count against the manifest; refuses a bucket that is half replaced |
| Silver | `build_silver` (7 tables, 2 at a time) | Flattens `works` into `works_core`, `work_references`, `work_authorships`, `work_topics`, `work_keywords`, `work_concepts` and `work_abstracts` with DuckDB SQL, one file per bronze file, on the NVMe (`OPENALEX_LAKE_PATH`); a file is rebuilt only when its bronze file changed |
| Catalog | `build_catalog` | Recreates `lake.duckdb`: views over every bronze entity and silver table, and `lake_release` |

Query it from any DuckDB session (the bronze views also need the MinIO secret):

```sql
ATTACH '/srv/osint/openalex/lake.duckdb' AS lake (READ_ONLY);
SELECT release_date FROM lake.lake_release;
SELECT publication_year, count(*) FROM lake.works_core GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

CREATE SECRET osint_minio (TYPE S3, KEY_ID '<MINIO_ROOT_USER>', SECRET '<MINIO_ROOT_PASSWORD>',
                           ENDPOINT 'localhost:<MINIO_API_PORT>', URL_STYLE 'path', USE_SSL false);
SELECT display_name, country_code FROM lake.institutions WHERE country_code = 'BR' LIMIT 5;
```

Operational state (which bronze and silver files exist, and from which upstream version) is in the `openalex` PostgreSQL schema. `work_abstracts` is for internal corpus search only, not for redistribution (for example in a web app): abstracts can be under the publishers' copyright, which is why OpenAlex ships them only as an inverted index.

## Tech stack

| Layer | Tools |
|---|---|
| Orchestration | Apache Airflow 2.10 |
| Processing | Python, DuckDB, dbt (dbt-duckdb), pandas, Parquet |
| Storage | PostgreSQL 16 + PostGIS, MinIO (S3) |
| Schema management | Flyway |
| NLP / ML | sentence-transformers, UMAP, HDBSCAN, BERTopic, scikit-learn |
| Backend | FastAPI, SQLAlchemy / asyncpg |
| Frontend | React 19, Vite, TypeScript, Cytoscape.js, Leaflet, ApexCharts |
| Auth | Keycloak 26 (OIDC) |
| Ops | Docker Compose (shared base plus dev and prod-like overrides), systemd units, GitHub Actions |

## Repository layout

| Directory | Role |
|---|---|
| [`pipelines/`](pipelines/) | Airflow DAGs (`dags/`) and the ETL scripts they call (`scripts/`): downloaders, cleaners, loaders and the NLP pipeline |
| [`backend/`](backend/) | FastAPI API: a stateless gateway over PostgreSQL, with no ETL logic |
| [`frontend/`](frontend/) | React + Vite dashboard: company and person search, network graphs, FINEP and INPI overviews |
| [`infrastructure/`](infrastructure/) | PostgreSQL migrations, Airflow image, Keycloak realm, systemd units |
| [`dev/`](dev/) | A lightweight replica of the stack with a small deterministic sample dataset |
| [`docs/`](docs/) | Architecture and implementation notes |

## Getting started

Requirements: Docker and Docker Compose. Loading the latest CNPJ month plus INPI and FINEP needs about 180 GB of disk (250 GB recommended for headroom), and the default memory settings are sized for a 32 GB machine; see [How long a full load takes](#how-long-a-full-load-takes). The dev replica runs on much less.

### Dev replica (sample data)

The fastest way to try it. It uses separate ports, sample CNPJ data and the full FINEP data:

```bash
cp dev/.env.example dev/.env
chmod +x dev/scripts/*.sh
./dev/scripts/start.sh                 # app stack only
./dev/scripts/start.sh --with-airflow  # plus the Airflow UI and workers
```

Then open `http://localhost:13000` and sign in as `osint-admin` / `osint123`. The first start takes about 7 minutes (image builds and the FINEP download); see [`dev/README.md`](dev/README.md) for the other logins and how to stop it.

### Production-like local stack

```bash
cp .env.example .env
./scripts/start-prod-like.sh --apply-migrations
```

By default all service data (PostgreSQL, MinIO, Airflow) lives under `./data/` in the repo, and the memory settings are sized for a 32 GB machine. To load the full datasets, point the `*_HOST_PATH` variables in `.env` at larger disks and raise the PostgreSQL memory values; `.env.example` lists the 64 GB profile.

This starts the frontend (`http://localhost:3000`), backend (`http://localhost:8000`), Keycloak (`http://localhost:8081`), MinIO console (`http://localhost:9001`), PostgreSQL and Flyway. Migrations are never applied implicitly: the script requires `--apply-migrations` or `--skip-migrations`.

Airflow is kept out of the default startup and can be started separately:

```bash
./scripts/start-prod-like.sh --skip-migrations --with-airflow
```

The Brazilian geography (IBGE states and municipalities) loads into the `geo` schema automatically: the `geo-bootstrap` service runs before the backend on every start and downloads the geometries only the first time. `cnpj_pipeline` rebuilds the CNPJ municipality mapping after each CNPJ load.

To start the stack at boot, install the systemd units (they are rendered with this repository's path):

```bash
sudo sh infrastructure/systemd/install-osint-platform-local.sh --start
sudo systemctl enable --now osint-platform-airflow.service   # optional: Airflow at boot too
```

### Logging in

Open `http://localhost:3000` and sign in with the local user the Keycloak realm imports: `osint-admin` / `osint123`. Change it (see `infrastructure/keycloak/realm/`) before exposing the stack anywhere.

### First data load

The database starts empty and every DAG is created paused. Each source has one pipeline DAG, and unpausing it starts its first run right away:

```bash
C="docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml"
for dag in cnpj_pipeline finep_pipeline inpi_pipeline; do $C exec airflow-scheduler airflow dags unpause $dag; done
```

`cnpj_pipeline` runs download → transform (silver) → dbt gold → publish to PostgreSQL, `finep_pipeline` downloads both spreadsheets and loads the 7 FINEP tables, and `inpi_pipeline` downloads the 10 patent CSVs and loads them. `openalex_pipeline` is separate and much larger (about 745 GB of bronze on the HDD and 225 GB of silver on the NVMe per release); unpause it the same way when the disk is ready.

Downloads are safe to re-run: files already in MinIO are skipped or registered instead of downloaded again.

### How long a full load takes

Measured with the default memory settings from `.env.example`, which are sized for a 32 GB machine (PostgreSQL `shared_buffers` 2 GB and `maintenance_work_mem` 1 GB; DuckDB 4 GB for the CNPJ transform and dbt, 8 GB per OpenAlex silver task). The machine itself has 8 cores and 60 GB of RAM, with PostgreSQL and the OpenAlex silver on NVMe and MinIO on an HDD; the spare RAM serves as operating-system file cache, so a machine with only 32 GB may be somewhat slower. Loading the 2026-10 CNPJ release (the download time is from the first load; the other rows are from a load from scratch with the files already in MinIO):

| Step | Time |
|---|---|
| CNPJ download: about 7.3 GB, served by Receita Federal at roughly 2.5 MB/s | ~1 h |
| CNPJ transform (silver): 37 ZIPs to Parquet, 4 at a time | 4 min |
| CNPJ gold: `dbt build`, 12 models and their tests, including `company_search` and `socio_history` | 13 min |
| CNPJ publish: gold Parquet into `*_next` tables, indexes, then the swap (under 1 s of locks) | 52 min |
| Municipality mapping (`rebuild_geo_mapping`) | 2 min |
| INPI: stage and publish the 10 patent tables, refresh the search view | ~8 min, plus downloading about 2.6 GB from INPI |
| FINEP: both spreadsheets, 7 tables | under 1 min |
| **First run from scratch** | **about 2 h 10 min**; the three pipelines run in parallel and CNPJ is the longest |
| **Monthly CNPJ run with the release already in MinIO** | **about 1 h 10 min** |

Disk used after that load:

| Data | Size |
|---|---|
| CNPJ bronze in MinIO, per monthly release | 7.3 GB |
| INPI bronze in MinIO, per snapshot (stored only when the content changes) | about 2.6 GB |
| CNPJ Parquet on the Airflow temp volume | 5.2 GB |
| CNPJ silver Parquet in MinIO, per month | 5.2 GB |
| CNPJ gold Parquet in MinIO (latest month only) | 8.9 GB |
| dbt working files on the Airflow temp volume | 4 GB |
| PostgreSQL: `cnpj` 88 GB live (tables, indexes, `company_search`), `inpi` 4 GB, `finep` and `geo` under 100 MB | 92 GB |
| PostgreSQL: the previous publish's `cnpj.*_old` tables, kept until the next run for rollback (no indexes) | 55 GB |
| **Total** | **about 180 GB** |

The second publish onwards needs no extra room at its peak: it drops the `_old` tables before building the `_next` ones. The 64 GB memory profile in `.env.example` mostly speeds up the index builds in the publish step.

#### OpenAlex

The first full load of release 2026-10-14 (2,361 files, 744.8 GB upstream), on the same machine:

| Step | Time |
|---|---|
| `sync_bronze`: download to the SSD, upload to MinIO, 8 files at a time, about 31 MB/s | 6 h 38 min |
| `verify_bronze`: sizes and Parquet footers of every object against the manifest | 1.5 min |
| `build_silver`, 2 tables at a time, 1,898 `works` files each: `works_core` 2 h 08, `work_abstracts` 2 h 39, `work_authorships` 1 h 40, `work_references` 1 h 22, `work_keywords` 1 h 09, `work_concepts` 1 h 09, `work_topics` 47 min | about 6 h 40 min |
| `build_catalog` | 4 s |
| **First run from scratch** | **about 13 h 20 min** |

A later release downloads and rebuilds only the files whose size or ETag changed.

| Data | Size |
|---|---|
| Bronze in MinIO (HDD), latest release only | 745 GB |
| Silver on the NVMe: `work_abstracts` 81 GB, `work_authorships` 40, `works_core` 30, `work_concepts` 26, `work_keywords` 22, `work_references` 19, `work_topics` 8 | 225 GB |
| `lake.duckdb` catalog (views only) | under 1 MB |

## Tests

`tests/` holds pytest suites that CI runs on every push ([`.github/workflows/tests.yml`](.github/workflows/tests.yml)), together with the frontend type check and build:

- `tests/pipelines`: the DuckDB cleaning SQL (codes, Brazilian decimals, dates, invalid rows), the Latin-1 extraction of Receita ZIPs, the FINEP loader's header matching, and the dbt gold models and data tests over a three-month silver fixture.
- `tests/backend`: the ownership network, company search and partner history against a PostgreSQL with the Flyway migrations and the dev seed. Set `POSTGRES_HOST` and the other `POSTGRES_*` variables; without them the suite is skipped.
- `tests/pipelines/test_gold_publish.py`: the gold build and the swap into PostgreSQL, against the same database.
- `tests/pipelines/test_openalex_lake.py`: the OpenAlex lake end to end (sync from a fake upstream bucket, verify, silver, catalog, a second release, a half-replaced bucket), against a MinIO and the same database.

`scripts/run-tests.sh` runs them the way CI does. It needs Docker and [uv](https://docs.astral.sh/uv/), starts a throwaway PostGIS and MinIO on free ports, applies the migrations and the seed, and removes both on exit:

```bash
scripts/run-tests.sh            # everything
scripts/run-tests.sh pipelines  # only tests/pipelines, no database
```

## Data sources

All data comes from open-data sources:

- **CNPJ:** the Receita Federal company registry (companies, establishments, shareholders)
- **FINEP:** public spreadsheets of contracts and disbursements (`download.finep.gov.br`)
- **INPI:** patent data from [dadosabertos.inpi.gov.br](https://dadosabertos.inpi.gov.br/index/patentes/)
- **Geography:** IBGE boundaries via [geobr](https://github.com/ipeaGIT/geobr)
- **OpenAlex:** the [OpenAlex](https://openalex.org/) snapshot of scholarly works, institutions, sources and topics (CC0), from its public S3 bucket

The shareholder records include names of real people, which are personal data under Brazil's LGPD even though they are published openly. The repository contains no real datasets: the dev seeds use fictitious people.

## Documentation

| Document | Content |
|---|---|
| [docs/repo/ARCHITECTURE_PLAN.md](docs/repo/ARCHITECTURE_PLAN.md) | Stack, architectural decisions, storage strategy |
| [pipelines/dags/README_CNPJ_DAG.md](pipelines/dags/README_CNPJ_DAG.md) | The CNPJ DAGs in detail, with benchmarks |
| [infrastructure/postgres/README.md](infrastructure/postgres/README.md) | PostgreSQL setup, CNPJ schemas, materialized views |
| [docs/repo/DOCKER_RESET_GUIDE.md](docs/repo/DOCKER_RESET_GUIDE.md) | Troubleshooting and a full container reset |

Most of the documentation in `docs/` is in Portuguese.

## License

Licensed under the [Apache License 2.0](LICENSE). The datasets remain subject to the terms of their publishers.

## Author

Rodrigo Fileto Cuerci Maciel ([@rodfileto](https://github.com/rodfileto))
