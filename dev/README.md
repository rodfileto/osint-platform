# Dev Replica Environment

This folder contains a lightweight replica of the main OSINT stack. It keeps the same service topology used by the root project while replacing production-sized resource settings and full datasets with a deterministic sample bootstrap.

## What It Includes

- PostgreSQL, Neo4j, Redis, MinIO, Flyway, Django, Next.js, and Airflow
- Reduced ports and memory settings so it can run side by side with the main stack
- Sample CNPJ data for:
  - active and inactive company search
  - company detail with matriz and filial data
  - Neo4j shareholder network with both pessoa fisica and pessoa juridica links
- Full FINEP data loaded from the public FINEP spreadsheets into the `finep` schema

## First Run

```bash
cp dev/.env.example dev/.env
chmod +x dev/scripts/*.sh
./dev/scripts/start.sh
```

## Ports

- Frontend: `http://localhost:13000`
- Backend: `http://localhost:18000`
- Airflow: `http://localhost:18080`
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
- The FINEP bootstrap requires internet access from the Docker host because it downloads `Contratacao.xlsx` and `Liberacao.xlsx` during startup.
- The main application code still lives in the root `backend`, `frontend`, `pipelines`, and `infrastructure` directories; this folder only owns the dev orchestration layer.