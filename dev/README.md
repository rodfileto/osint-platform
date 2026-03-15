# Dev Replica Environment

This folder contains a lightweight replica of the main OSINT stack. It keeps the same service topology used by the root project while replacing production-sized resource settings and full datasets with a deterministic sample bootstrap.

## What It Includes

- PostgreSQL, Neo4j, Redis, MinIO, Flyway, Django, and Next.js by default
- Optional Airflow services when you need DAG and pipeline debugging
- Reduced ports and memory settings so it can run side by side with the main stack
- Sample CNPJ data for:
  - active and inactive company search
  - company detail with matriz and filial data
  - Neo4j shareholder network with both pessoa fisica and pessoa juridica links
- Full FINEP data loaded from the public FINEP spreadsheets into the `finep` schema
- Canonical Brazilian geography loaded into the `geo` schema from Python geobr, plus a CNPJ municipality mapping view

## First Run

```bash
cp dev/.env.example dev/.env
chmod +x dev/scripts/*.sh
./dev/scripts/start.sh
```

`./dev/scripts/start.sh` is still the preferred entrypoint because it prepares host folders and runs the one-shot services in sequence. By default it starts only the app stack for backend and frontend work, and it no longer builds the Airflow image unless you explicitly request Airflow. Plain `docker compose up --build` from `dev/` now also triggers an automatic `dev-bootstrap` service before the backend starts, so the frontend no longer comes up against an empty CNPJ dataset.

To start the Airflow UI and workers too:

```bash
./dev/scripts/start.sh --with-airflow
```

## Basic Workflow

Use this sequence for normal backend and frontend work.

### 1. Start dev

From the project root:

```bash
cp dev/.env.example dev/.env
./dev/scripts/start.sh
```

If you need Airflow too:

```bash
./dev/scripts/start.sh --with-airflow
```

### 2. Make modifications

Edit the application code in the root project folders:

- `backend/`
- `frontend/`

The dev stack mounts those folders directly, so backend and frontend changes are reflected in the running dev environment.

If you changed dependencies or Docker build inputs, rebuild only the affected service:

```bash
docker compose --env-file ./dev/.env -f ./dev/docker-compose.yml up -d --build backend
docker compose --env-file ./dev/.env -f ./dev/docker-compose.yml up -d --build frontend
```

### 3. Run Flyway if needed

Only do this when your change includes PostgreSQL migration files under `infrastructure/postgres/migrations`.

Dev:

```bash
docker compose --env-file ./dev/.env -f ./dev/docker-compose.yml up --build --abort-on-container-exit --exit-code-from flyway flyway
```

Prod-like:

```bash
docker compose --env-file .env up --build --abort-on-container-exit --exit-code-from flyway flyway
```

### 4. Build prod-like

From the project root:

```bash
docker compose --env-file .env up -d --build backend frontend
```

If `.env` does not exist yet at the project root:

```bash
cp .env.example .env
docker compose --env-file .env up -d --build backend frontend
```

## Ports

- Frontend: `http://localhost:13000`
- Backend: `http://localhost:18000`
- Airflow: `http://localhost:18080` when started with `--with-airflow`
- Neo4j Browser: `http://localhost:17474`
- MinIO Console: `http://localhost:19001`
- PostgreSQL: `localhost:15432`

## Reset

```bash
./dev/scripts/reset.sh
```

If you change core PostgreSQL identity settings in `dev/.env`, such as `POSTGRES_DB`, run a full reset before starting again so the container initializes a fresh database cluster with the new values.

## Notes

- The dev bootstrap uses committed sample seed files for CNPJ and Neo4j, but downloads and loads the full public FINEP dataset through the existing pipeline loaders.
- The dev bootstrap also loads canonical Brazilian state and municipality boundaries into `geo.br_state` and `geo.br_municipality`, then rebuilds `geo.cnpj_br_municipality_map` and `geo.v_cnpj_br_municipality`.
- The shared geography loader lives in `pipelines/scripts/geo/load_br_geo_reference.py`, and the dev bootstrap invokes that shared script instead of keeping a dev-only implementation.
- The dev bootstrap runs on a lightweight Python app container and no longer depends on the Airflow image.
- Automatic bootstrap is controlled by `DEV_BOOTSTRAP_FINEP` in `dev/.env`. Leave it enabled to preserve the previous behavior of loading the full FINEP dataset on first startup.
- Automatic geography bootstrap is controlled by `DEV_BOOTSTRAP_GEO`, `GEOBR_YEAR`, and `GEOBR_SIMPLIFIED` in `dev/.env`.
- The FINEP bootstrap requires internet access from the Docker host because it downloads `Contratacao.xlsx` and `Liberacao.xlsx` during startup.
- The geography bootstrap requires internet access from the Docker host because geobr downloads the official IBGE boundary files on first load.
- The main application code still lives in the root `backend`, `frontend`, `pipelines`, and `infrastructure` directories; this folder only owns the dev orchestration layer.