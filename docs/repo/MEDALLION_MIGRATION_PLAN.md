# Medallion Migration Plan: Bronze / Silver / Gold, PostgreSQL as Serving, No Neo4j

Date: 2026-10-24. Background: [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md).

Each step below is one reviewable change. Work stops after each step until it is approved.

## Decisions (confirmed 2026-10-24)

1. **Partner history:** publish a compact `cnpj.socio_history` to PostgreSQL (one row per continuous period of a partnership, with `valid_from` / `valid_to`).
2. **Silver retention:** keep all months.
3. **Existing data:** rebuild silver from bronze for every month. The DAGs are adjusted so that silver triggers the gold build and the PostgreSQL publish.

## Target architecture

```
Receita Federal ─▶ BRONZE  s3://osint-raw/cnpj/raw/YYYY-MM/*.zip            (unchanged)
                     │  DuckDB clean + type
                     ▼
                   SILVER  s3://osint-processed/cnpj/silver/<entity>/reference_month=YYYY-MM/*.parquet
                     │  all months kept; history and ad-hoc analysis via DuckDB
                     │  DuckDB SQL models
                     ▼
                   GOLD    s3://osint-processed/cnpj/gold/<table>/*.parquet
                     │  latest-month tables + socio_history (built over all silver months)
                     │  COPY into *_next tables → index → swap in one transaction
                     ▼
                   PostgreSQL schema cnpj  (latest snapshot + socio_history, API-shaped)
                     ▲
                   FastAPI  (search, detail, ownership network via recursive CTE, partner history)
```

Principles:
- **Bronze** is immutable. **Silver** is the system of record for history. **Gold** in PostgreSQL can be thrown away and rebuilt from silver at any time.
- Every publish builds the gold tables from scratch. No upserts, no stubs, no index drop/recreate, no materialized-view refresh.
- Neo4j is removed. The ownership network is served by PostgreSQL.
- INPI and FINEP keep their current bronze → PostgreSQL flow. They are small, and moving them is out of scope. Only their links to Neo4j and to CNPJ tables change.

### Sizing

From the production figures in IMPLEMENTATION_STEPS / CNPJ_IMPLEMENTATION_REVIEW:
- Parquet is about 5 GB/month, about 245 GB for the 49 months already in bronze (174 GB), growing about 60 GB/year. That is about 2.5% of the 10 TB HDD.
- The latest-month gold build reads about 5 GB. The `socio_history` build reads only the `socios` columns of every month (an estimated 1 GB/month, to be measured in Step 1), which is minutes of I/O on the HDD.
- No NVMe cache for silver is planned.

## Gold tables in PostgreSQL (`cnpj` schema)

Table names stay the same where possible so the API and other domains change as little as possible.

| Table | Built from | Notes |
|---|---|---|
| `empresa` | silver empresas (latest month) | Lookup labels joined in |
| `estabelecimento` | silver estabelecimentos (latest month) | All branches (company detail and geo matching need them) |
| `socio` | silver socios (latest month) | Drops the surrogate `id`; indexes on `cnpj_basico` and `cpf_cnpj_socio` for the network CTE |
| `socio_history` | silver socios (**all months**) | See below |
| `simples_nacional` | silver simples (latest month) | |
| `company_search` | empresa + HQ estabelecimento + lookups | **Replaces both materialized views.** One table with an `is_active` flag and partial trigram indexes |
| Lookup tables (`cnae`, `municipio`, `natureza_juridica`, `pais`, `qualificacao_socio`, `motivo_situacao_cadastral`) | silver lookups (latest month) | Kept: API joins and the geo FK use them |
| `download_manifest` + manifest views | unchanged | Operational state, not gold |

### `socio_history` design

- **Partnership key:** `cnpj_basico`, `identificador_socio`, `cpf_cnpj_socio`, `nome_socio_razao_social`, `qualificacao_socio`. A change of qualification (e.g., partner → administrator) opens a new period.
- **Periods:** a gaps-and-islands calculation over the months in which the key appears. A partner who leaves and comes back gets two rows.
- **Columns:**
  - the key;
  - `valid_from` and `valid_to` (first and last month seen);
  - `is_current` (`valid_to` = latest month);
  - `data_entrada_sociedade`, `faixa_etaria`, `pais`, legal representative fields, taken from the last month of the period.
- **Build strategy:** full rebuild from all silver months on every publish. It is one code path, always correct and idempotent, and runs in minutes at the current size. Switch to incremental (apply the new month to the previous state) only if it becomes slow.
- **Known limitation:** the Receita masks CPFs, and names can change spelling between snapshots. These cases show up as one period ending and another starting. They are not merged.
- **Rough size:** tens of millions of rows (well under a month-by-month copy). It will be measured after the backfill.

### Swap and dependent objects

Swapping by renaming creates a new table, but foreign keys and views stay bound to the old one. Current dependents:
- FKs: `geo.br_municipality…` → `cnpj.municipio`.
- Views: `cnpj.estabelecimento_completo`, `geo.v_cnpj_br_municipality`.

Plan: turn those FKs into soft references (indexed columns, no constraint), as was already done for `socio`/`estabelecimento` → `empresa`. The publish step recreates the dependent views inside the swap transaction, from one versioned SQL file.

## DAG flow

```
cnpj_download (monthly, unchanged schedule)
   └─ new month downloaded ─▶ triggers cnpj_silver(reference_month=M)        ← new trigger

cnpj_silver(M)          bronze → silver for one month (today's cnpj_transform, retargeted)
   └─ triggers cnpj_gold_publish(reference_month=M, publish=auto)

cnpj_gold_publish(M)
   ├─ build socio_history gold (all silver months)
   ├─ if M is the latest silver month: build latest-month gold tables
   └─ publish to PostgreSQL (swap); skipped when M is older than what is published

cnpj_silver_backfill (manual; params: from_month, to_month, force)
   ├─ runs bronze → silver for each month in the range, a limited number at a time
   └─ at the end triggers cnpj_gold_publish once with the latest month
```

- During the backfill, the individual `cnpj_silver` runs **do not** trigger a publish (`trigger_publish=False`). Only the final step does, so PostgreSQL is rebuilt once and not 49 times.
- **Replaced:** `cnpj_transform` (becomes `cnpj_silver`), `cnpj_load_postgres` and `cnpj_matview_refresh` (become `cnpj_gold_publish`). `cnpj_load_neo4j` is deleted.
- The manifest keeps tracking `transformed_at` (silver). New columns or a small table record `gold_built_at` / `published_at` per month.

## Steps

### Pre-checks (on the production host, no code changes)
- **A.** Confirm the HDD is really mounted: `mount | grep data10tb`, `df -h /mnt/data10tb`. On the dev machine it is a plain folder on the root filesystem.
- **B.** List which months exist in bronze in MinIO (`osint-raw/cnpj/raw/`). The 174 GB / 49 months were documented on the local disk (`pipelines/data/cnpj/raw`). Any months not yet in MinIO must be uploaded before the backfill.
- **C.** Benchmark the ownership network as a recursive CTE against the current `cnpj.socio` (latest month only), at depth 1–3, on the companies with the most partners. This is the one assumption that removing Neo4j depends on. I will provide the SQL and a small script.

### Step 1: Silver in MinIO + DAG rework
- `cnpj_transform` becomes `cnpj_silver`. It writes Parquet to `s3://osint-processed/cnpj/silver/<entity>/reference_month=M/` (DuckDB `httpfs`/S3 secret) instead of `/opt/airflow/temp_ssd/cnpj_processed`. CSV staging stays on the local SSD.
- Add the `trigger_publish` param, and a trigger to `cnpj_gold_publish` (an empty placeholder DAG until Step 3).
- `cnpj_download` triggers `cnpj_silver` for each new month.
- New `cnpj_silver_backfill` DAG.
- New shared helper for DuckDB + MinIO connections (one place for credentials and paths).
- Create the `osint-processed` bucket on startup (compose init) if missing.
- **Verify:** silver for one month in dev; object listing and row counts against the old local Parquet; measure Parquet size per entity.
- **Rollback:** config flag back to the local path.

### Step 2: Gold build (DuckDB SQL models)
- New `pipelines/scripts/cnpj/gold/` with one `.sql` file per gold table (including `socio_history`) and a small runner that writes gold Parquet to MinIO.
- Data-quality checks in the runner: row counts versus silver, PK uniqueness, share of orphan `cnpj_basico` (logged, not fatal), `socio_history` invariant (current rows = latest-month `socio` rows).
- **Verify:** compare gold `company_search` counts with the current `mv_company_search` + `mv_company_search_inactive`; check `socio_history` on a few known companies across months.

### Step 3: Publish gold to PostgreSQL
- Flyway migration:
  - create `cnpj.company_search` and `cnpj.socio_history` structures;
  - drop the inbound hard FKs listed above;
  - drop `socio.id`;
  - the materialized views are dropped in Step 4, once the API no longer uses them.
- `cnpj_gold_publish` DAG:
  1. `CREATE TABLE cnpj.<t>_next (LIKE …)`;
  2. COPY from gold Parquet (DuckDB `postgres` extension);
  3. build indexes, `ANALYZE`;
  4. `BEGIN` → rename live→`_old`, `_next`→live → recreate dependent views → `COMMIT`;
  5. keep `_old` until the next run for rollback.
- Disable `cnpj_load_postgres` and `cnpj_matview_refresh` (deleted in Step 6).
- **Verify:** publish in dev; API endpoints return the same results as before; the swap holds locks only for the rename.

### Step 4: Backend on the new serving model
- Search, FINEP and INPI routers: two MV queries → one `company_search` query with `is_active`. Then a migration drops the two MVs.
- Ownership network (`/network/{cnpj}`, `/pessoa-network/...`): recursive CTE over `cnpj.socio`, keeping the current response schema so the frontend doesn't change.
- Person detail reads current partnerships from `socio` and past ones from `socio_history`. A new endpoint lists a company's partner history (frontend tab is optional, decide during review).
- Unified search: remove the Neo4j branch.
- **Verify:** compare network responses on sample companies; p95 benchmark on the companies with the most partners (record numbers in this doc).

### Step 5: Remove Neo4j

> Replaced by [NEO4J_REMOVAL_PLAN.md](NEO4J_REMOVAL_PLAN.md), written against the current single-DAG pipelines. The list below names files that no longer exist.

- Pipelines: delete `cnpj_load_neo4j`, `inpi_load_neo4j`, `neo4j_schema.py`, the Neo4j parts of `load_tasks.py`, `inpi/neo4j_loader.py`, `delete_estabelecimentos_neo4j.py`, `create_neo4j_indexes.py`.
- FINEP NLP: write the k-NN `SIMILAR_TO` edges to a new `finep.nlp_project_similarity` table instead of Neo4j.
- Backend: remove the Neo4j driver, settings and the `neo4j` dependency.
- Infra: remove the service from all compose files, `infrastructure/neo4j/`, `.env.example` vars, dev seeds and bootstrap; update the CI guardrail script if it references Neo4j.
- **Verify:** stack starts without Neo4j; `rg -i neo4j` returns only docs history.

### Step 6: Cleanup
- Delete the replaced DAGs and dead code: the stub-company insertion, upsert/staging logic, the three cleaner implementations reduced to the one actually used, `process_all_months.py`, and other leftovers found during Steps 1–5.
- Update README(s), ARCHITECTURE_PLAN, CNPJ DAG README, the mermaid diagram, and the dev replica seeds.

### Step 7: Backfill (operational, on the production host)
- Run `cnpj_silver_backfill` for all months in bronze, then confirm the automatic publish.
- Record actual silver size, `socio_history` row count and build times in this doc.
