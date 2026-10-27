# Medallion Migration Plan: Bronze / Silver / Gold, PostgreSQL as Serving, No Neo4j

Date: 2026-10-24. Background: [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md).

Each step below is one reviewable change. Work stops after each step until it is approved.

## Decisions (confirmed 2026-10-24)

1. **Partner history:** publish a compact `cnpj.socio_history` to PostgreSQL (one row per continuous period of a partnership, with `valid_from` / `valid_to`).
2. **Silver retention:** keep all months.
3. **Existing data:** rebuild silver from bronze for every month. The DAGs are adjusted so that silver triggers the gold build and the PostgreSQL publish.
4. **Gold layer in dbt (confirmed 2026-10-27):** the gold models live in a dbt-duckdb project at [`pipelines/dbt/`](../../pipelines/dbt/): one SQL model per gold table, data tests in YAML, gold written as Parquet to `s3://osint-processed/cnpj/gold/<table>/`. Silver stays the current Airflow per-file DuckDB transform (fan-out and Latin-1 decoding stay in Python). Airflow invokes dbt through the `dbt build` CLI (BashOperator); dbt is installed in its own virtualenv in the Airflow image (`/opt/dbt-venv`, pinned in `pipelines/dbt/requirements.txt`) so it never conflicts with Airflow's dependencies. The PostgreSQL publish (Step 3) stays custom: DuckDB ATTACH + swap, not dbt-postgres.

## Target architecture

```
Receita Federal ─▶ BRONZE  s3://osint-raw/cnpj/raw/YYYY-MM/*.zip            (unchanged)
                     │  DuckDB clean + type
                     ▼
                    SILVER  s3://osint-processed/cnpj/silver/<entity>/reference_month=YYYY-MM/*.parquet
                     │  all months kept; history and ad-hoc analysis via DuckDB
                     │  dbt-duckdb models (pipelines/dbt/models/cnpj_gold)
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

From the production figures (in the since-removed IMPLEMENTATION_STEPS.md and CNPJ_IMPLEMENTATION_REVIEW.md, still in git history):
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
- **Periods:** a gaps-and-islands calculation over the months in which the key appears. A partner who leaves and comes back gets two rows. Months are counted over the ones present in silver, not the calendar, so a month that was never downloaded does not split a period (it also hides an absence during that month).
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
- **Implemented (2026-10-27) as a single DAG:** `cnpj_pipeline` keeps download → transform, then runs `build_gold` (dbt) → `publish_gold` → `rebuild_geo_mapping` → `refresh_search_views` (MVs only until Step 4). A separate `cnpj_silver_backfill` DAG is still pending (Step 7).
- **Replaced:** `cnpj_transform` (becomes `cnpj_silver`), `cnpj_load_postgres` and `cnpj_matview_refresh` (become `cnpj_gold_publish`). `cnpj_load_neo4j` is deleted.
- The manifest keeps tracking `transformed_at` (silver). New columns or a small table record `gold_built_at` / `published_at` per month.

## Steps

### Pre-checks (on the production host, no code changes)
- **A.** Confirm the HDD is really mounted: `mount | grep data10tb`, `df -h /mnt/data10tb`. On the dev machine it is a plain folder on the root filesystem.
- **B.** List which months exist in bronze in MinIO (`osint-raw/cnpj/raw/`). The 174 GB / 49 months were documented on the local disk (`pipelines/data/cnpj/raw`). Any months not yet in MinIO must be uploaded before the backfill.
- **C.** Benchmark the ownership network as a recursive CTE against the current `cnpj.socio` (latest month only), at depth 1–3, on the companies with the most partners. This is the one assumption that removing Neo4j depends on. I will provide the SQL and a small script.

### Step 1: Silver in MinIO + DAG rework

> **Partly done 2026-10-26:** `cnpj_pipeline`'s transform task copies each Parquet file to `s3://osint-processed/cnpj/silver/<entity>/reference_month=M/` and restores missing local files from there; the load still reads the local copy. Measured for 2026-10: estabelecimentos 3.6 GB, empresas 1.1 GB, socios 0.5 GB, simples 0.2 GB (5.5 GB per month). The DAG rework below is not done.

- `cnpj_transform` becomes `cnpj_silver`. It writes Parquet to `s3://osint-processed/cnpj/silver/<entity>/reference_month=M/` (DuckDB `httpfs`/S3 secret) instead of `/opt/airflow/temp_ssd/cnpj_processed`. CSV staging stays on the local SSD.
- Add the `trigger_publish` param, and a trigger to `cnpj_gold_publish` (an empty placeholder DAG until Step 3).
- `cnpj_download` triggers `cnpj_silver` for each new month.
- New `cnpj_silver_backfill` DAG.
- New shared helper for DuckDB + MinIO connections (one place for credentials and paths).
- Create the `osint-processed` bucket on startup (compose init) if missing.
- **Verify:** silver for one month in dev; object listing and row counts against the old local Parquet; measure Parquet size per entity.
- **Rollback:** config flag back to the local path.

### Step 2: Gold build (dbt-duckdb SQL models)

> **Done 2026-10-27, via dbt instead of a hand-rolled runner.** The gold layer is a dbt-duckdb project in [`pipelines/dbt/`](../../pipelines/dbt/): one model per gold table (`empresa`, `estabelecimento`, `simples_nacional`, `socio`, `socio_history`, `company_search`, six lookups) written as `external` materializations to `s3://osint-processed/cnpj/gold/<table>/<table>.parquet`. Data-quality checks are dbt tests (unique/not_null/relationships with warn severity for orphans, `socio_history` invariant, in-month partnership-key uniqueness). The `cnpj_pipeline` DAG runs `dbt build` after the transform (guarded by `gold_guard.py`, which skips months older than the latest silver). The CI runs `dbt build` over a locally generated silver fixture (`tests/pipelines/test_dbt_gold.py`).
>
> Notes: `socio_history` is a gaps-and-islands model over all silver months (attributes from the last month of each period via `arg_max`); `company_search` replaces both materialized views with one table with an `is_active` flag. The old plan below (one `.sql` per table + a small runner) is superseded.

- ~~New `pipelines/scripts/cnpj/gold/` with one `.sql` file per gold table (including `socio_history`) and a small runner that writes gold Parquet to MinIO.~~
- ~~Data-quality checks in the runner: row counts versus silver, PK uniqueness, share of orphan `cnpj_basico` (logged, not fatal), `socio_history` invariant (current rows = latest-month `socio` rows).~~
- **Verify:** compare gold `company_search` counts with the current `mv_company_search` + `mv_company_search_inactive`; check `socio_history` on a few known companies across months.

### Step 3: Publish gold to PostgreSQL

> **Done 2026-10-27.** Flyway `V2026.10.27.001` creates `cnpj.company_search` and `cnpj.socio_history`, drops the inbound geo FK (`geo.cnpj_br_municipality_map → cnpj.municipio`, now a soft reference) and the dead `updated_at` triggers, and detaches `socio_id_seq` from the column so it survives the swaps. The `cnpj_pipeline` DAG now runs `gold_publish.py` after the dbt build instead of the old load tasks: gold Parquet → `*_next` tables (COPY via DuckDB postgres extension) → indexes with `_next` names → one metadata-only transaction that drops the live canonical indexes/PKs, renames `live→_old` / `_next→live`, renames indexes and PK constraints to their canonical names, and recreates the dependent views (`cnpj.estabelecimento_completo`, `geo.v_cnpj_br_municipality`) and the search MVs (empty; refreshed by the existing `refresh_search_views` task) from versioned SQL files. `_old` is kept until the next run.
>
> **Deviations from the plan above:**
> - **`socio.id` is kept for now** (dropped in Step 4 with the backend change): `backend/api/services/cnpj_network.py` still uses it as the BFS seen key, so dropping it here would break the network endpoints. The publish fills it with `row_number()` and syncs the sequence with `setval`.
> - DuckDB's postgres extension writes NULL for omitted columns instead of applying defaults, so `created_at`/`updated_at` (and `socio.id`) are generated in the COPY select.
> - The search MVs must be recreated inside the swap transaction: renaming the swapped tables rewrites their stored queries to the `_old` tables, and the next publish's `DROP *_old CASCADE` would drop them. `serving_mvs.sql` recreates them empty; search results are briefly empty until `refresh_search_views` repopulates them (removed in Step 4).
>
> Verified by `tests/pipelines/test_gold_publish.py` (run against the migrated PostgreSQL in CI): row counts, generated `socio.id`, canonical index/PK names, recreated views, MV refresh after two publishes, and idempotent re-publish.

- ~~Flyway migration:~~
  - create `cnpj.company_search` and `cnpj.socio_history` structures;
  - drop the inbound hard FKs listed above;
  - ~~drop `socio.id`~~ (deferred to Step 4, see the deviation note above);
  - the materialized views are dropped in Step 4, once the API no longer uses them.
- ~~`cnpj_gold_publish` DAG:~~ implemented as the `publish_gold` task inside `cnpj_pipeline`:
  1. `CREATE TABLE cnpj.<t>_next (LIKE …)`;
  2. COPY from gold Parquet (DuckDB `postgres` extension);
  3. build indexes, `ANALYZE`;
  4. `BEGIN` → rename live→`_old`, `_next`→live → recreate dependent views → `COMMIT`;
  5. keep `_old` until the next run for rollback.
- Disable `cnpj_load_postgres` and `cnpj_matview_refresh` (deleted in Step 6): **done**, the load tasks no longer run in the DAG; only `refresh_search_views` remains until Step 4.
- **Verify:** publish in dev; API endpoints return the same results as before; the swap holds locks only for the rename.

### Step 4: Backend on the new serving model

> **Done 2026-10-27.** The API reads `cnpj.company_search` (one table, `is_active` flag) in `search`, `finep` and `inpi`; the `cnpj` search endpoints take an `is_active` filter instead of two MVs. The network service dedups by the natural partnership key instead of `socio.id`. Person detail reads current partnerships from `socio` and past ones from `socio_history` (new `is_current` flag on each company), and a new `/empresa/{cnpj_basico}/socios/history` endpoint lists a company's partner periods. Flyway `V2026.10.27.002` drops the two MVs and `socio.id`. The dev seed now builds `company_search` directly (no pipeline in the replica) and seeds a couple of `socio_history` rows; `dev/scripts/compose_bootstrap.py` validates `company_search` instead of the MVs.
>
> Verified by `tests/backend/` (network, search, person history) against the migrated + seeded PostgreSQL.

- ~~Search, FINEP and INPI routers: two MV queries → one `company_search` query with `is_active`. Then a migration drops the two MVs.~~
- Ownership network (`/network/{cnpj}`, `/pessoa-network/...`): **done 2026-10-25** with the Neo4j removal: level-by-level indexed queries over `cnpj.socio` (latest month), same response schema, edge cap as the `max_edges` request param. Hubs at depth 3 answer in under 40 ms over HTTP.
- Person detail reads current partnerships from `socio` and past ones from `socio_history`. A new endpoint lists a company's partner history (frontend tab is optional, decide during review).
- Unified search: remove the Neo4j branch. **Done 2026-10-25.**
- **Verify:** compare network responses on sample companies; p95 benchmark on the companies with the most partners (record numbers in this doc).

### Step 5: Remove Neo4j

> **Done 2026-10-25**, following a separate step-by-step plan written against the single-DAG pipelines (`docs/repo/NEO4J_REMOVAL_PLAN.md` in commit `2565533`; the measurements are in ARCHITECTURE_PLAN.md). The list below names files that no longer exist. FINEP similarity is in `finep.nlp_project_similarity`; the current design is described in [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md#ownership-networks).

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
