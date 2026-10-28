# OpenAlex Integration Plan: Bronze and Silver

Date: 2026-10-27. Brings the sibling project `openalex-lake` (last commit `fa251ea`) into the platform as a new source with bronze and silver layers. Gold is out of scope and will be planned separately.

Each step below is one reviewable change. Work stops after each step until it is approved.

## Decisions (confirmed 2026-10-27)

1. **Storage:** bronze moves into MinIO (`s3://osint-raw/openalex/...`), next to the CNPJ ZIPs. Silver stays on the NVMe as a bind-mounted folder: MinIO lives on the HDD, and silver exists to make full-table DuckDB scans fast.
2. **Code:** the `openalex-lake` modules are ported to `pipelines/scripts/openalex/`, with a new `openalex_pipeline` DAG. A small CLI stays for manual runs.
3. **History:** the code is copied in one commit that names the `openalex-lake` commit it came from. The old repo stays as an archive.
4. **Catalog:** keep `lake.duckdb` as the query entry point, recreated by the DAG after `verify` and the silver build pass. With bronze in MinIO, queries on the bronze views need the MinIO S3 secret in the caller's DuckDB session; silver views are plain NVMe paths.

## OpenAlex releases

New full releases of the public snapshot (`s3://openalex/data/`) come out quarterly, on the second Wednesday of January, April, July and October (UTC): 2026-10-14, then 2027-01-13, 2027-04-14, 2027-07-14. Each release replaces the bucket contents in place; OpenAlex keeps no older release.

Consequences for the design:
- **Schedule:** the DAG only needs to run around those dates (see DAG flow).
- **A release can be read mid-replacement.** While OpenAlex rewrites the bucket, the manifest, the file listing and the files can disagree. The sync therefore never trusts a partial state: every downloaded file must match the manifest's size and record count and the listing's ETag, and any mismatch fails the run, which the next day's run retries. Stale files are removed only after every file of the new release is in.
- **Our copy is the only copy of the previous release** once OpenAlex replaces it. Bronze is updated in place too (as in openalex-lake), so during a sync our bronze is also a mix of two releases. Silver and the catalog's `lake_release` change only after `verify` passes, so they always describe one release; bronze queries during the sync window are the exception.

## What openalex-lake is today

| Part | What it does | Where |
|---|---|---|
| `lake sync` | Parallel, resumable mirror of OpenAlex's public `s3://openalex/data/parquet/`, checked against the release manifest (size + ETag); keeps the latest release only; skips `authors` and `awards` | 660 GB, HDD: `/mnt/data10tb/openalex_lake/bronze` (release 2026-09-23) |
| `lake verify` | File sizes and Parquet footer record counts against the manifest | |
| `lake silver` | 7 flattened tables from `works`, DuckDB SQL per table (`sql/*.sql`), one zstd output per bronze file, rebuilt when the bronze file is newer | 215 GB, NVMe: `openalex-lake/data/silver` |
| `lake catalog` | `lake.duckdb` with views over bronze (ids shortened to match silver) and silver, plus `lake_release` | NVMe: `openalex-lake/data/lake.duckdb` |

Silver tables: `works_core` (29 GB), `work_abstracts` (79 GB), `work_authorships` (39 GB), `work_concepts` (26 GB), `work_keywords` (20 GB), `work_references` (18 GB), `work_topics` (6.8 GB).

Its roadmap kept object storage out of scope ("no gain on a single machine"). Decision 1 changes that for bronze only, so all raw sources share one store and one backup routine; the bronze layout already matches a bucket, as that roadmap noted.

## Target architecture

```
OpenAlex  s3://openalex/data/parquet/  (public, anonymous HTTPS)
   │  sync: download each file to the SSD, check size, upload, check size + ETag
   ▼
BRONZE   MinIO  s3://osint-raw/openalex/parquet/<entity>/updated_date=…/part_NNN.parquet   latest release only
   │        + s3://osint-raw/openalex/parquet/manifest.json (the release's manifest, written last)
   │  silver: DuckDB SQL per table, one output per bronze file (reads bronze through httpfs)
   ▼
SILVER   NVMe   /srv/osint/openalex/silver/<table>/updated_date=…/part_NNN.parquet       mounted in Airflow
   │  catalog
   ▼
CATALOG  NVMe   /srv/osint/openalex/lake.duckdb   views over bronze (s3://) and silver (paths)
```

Principles, the same as the CNPJ medallion:
- **Bronze** is an exact mirror of one OpenAlex release. Nothing in it is edited.
- **Silver** can be rebuilt from bronze at any time.
- Operational state (which files are synced and built) lives in PostgreSQL, like `cnpj.download_manifest`, so the DAG and a person can both see it. No data goes to PostgreSQL.

### Operational state (Flyway, schema `openalex`)

- `openalex.release`: one row per release seen (`release_date`, `manifest_etag`, `synced_at`, `verified_at`, `silver_built_at`).
- `openalex.bronze_file`: one row per bronze file of the current release (`key` relative to the prefix, `entity`, `size_bytes`, `record_count`, `source_etag` from the OpenAlex listing, `release_date`, `synced_at`).
- `openalex.silver_file`: one row per silver output (`silver_table`, `source_key`, `source_etag` it was built from, `rows`, `built_at`).

Silver is rebuilt for a file when the `source_etag` it was built from differs from the bronze file's, replacing openalex-lake's mtime check, which does not carry over to objects copied into MinIO.

### Sizing

- Bronze: 660 GB per release in MinIO on the HDD (5.0 TB free). Only the latest release is kept; each quarterly release replaces the files whose size or ETag changed. How much of `works` changes between releases is not measured yet (Step 7).
- Silver: 215 GB on the NVMe (2.5 TB free). Not counted against the HDD.
- Download and build times are not recorded from the first sync. Estimates to check in Step 7: a full release download is bound by the network (660 GB), and a full silver rebuild reads all of bronze from MinIO on the HDD, so it is bound by HDD reads.

## DAG flow

```
openalex_pipeline (daily at 06:00 America/Sao_Paulo, the platform's Airflow timezone, on the 8th–21st of Jan, Apr, Jul, Oct; paused when created, like the other DAGs)
   check_release      fetch the OpenAlex manifest; short-circuit when its date equals the synced release
   sync               plan = files missing or with a different size/ETag; download → SSD → MinIO, 8 at a time;
                      stale objects of older releases removed only after every file is in
   verify             sizes and Parquet footer record counts of every object against the manifest
   silver[table]      one mapped task per silver table (7), 2 at a time; each rebuilds only files whose source ETag changed
   catalog            recreate lake.duckdb views and lake_release
```

Params: `force_silver` (rebuild every silver file), `tables` (subset of silver tables), `dry_run` (sync plan only).

Schedule: cron `0 6 8-21 1,4,7,10 *`. "Second Wednesday" cannot be written in cron: Airflow's croniter treats day-of-month and weekday as OR (`0 6 8-14 1,4,7,10 3` fires on 2027-01-06, 01-08, 01-09, …). The schedule runs in the platform's Airflow timezone (America/Sao_Paulo, 09:00 UTC). The second Wednesday always falls on the 8th–14th; running daily until the 21st also covers a late release and retries a sync that hit a mid-replacement bucket. After the release is in, each run ends at `check_release` in seconds. A manual trigger works at any time.

## Steps

### Pre-checks (no code changes)
- **A.** Run the existing `lake verify` against `/mnt/data10tb/openalex_lake/bronze` (pass `OPENALEX_BRONZE`, since the config still points at the old `/run/media/...` mount). A clean result means the import in Step 2 copies a complete release.
- **B.** Fetch the current OpenAlex manifest date. The local copy's manifest says 2026-09-23 (a Wednesday, but not a release day), while the October release came out on 2026-10-14. Either the manifest date is the snapshot's data date, or the October release is newer than the local copy. If it is newer, decide whether to import the old copy and let the DAG update it, or sync the new release directly.
- **C.** Confirm the disk figures above (`df -h /mnt/data10tb /`) on the day of the import.

## Progress

- **Pre-checks (2026-10-28):** A: `lake verify` on the 2026-09-23 mirror, 2583 files, 0 problems. B: OpenAlex already serves release **2026-10-14** (2361 files, 744.8 GB); only 12 files match the local copy and 1110 local files are gone upstream. So the old mirror is **not imported**: the DAG syncs 2026-10-14 straight into MinIO and builds silver from it, and the old bronze (`/mnt/data10tb/openalex_lake/bronze`) and silver (`openalex-lake/data/silver`) stay untouched as a fallback until they are deleted after approval. C: HDD 5.0 TB free, NVMe 2.6 TB free.
- **Step 1** done: code ported (`68bcde6`); the two SQL files with `x -> …` lambdas changed to DuckDB 2 syntax.
- **Steps 2–5** done in code: bronze sync and verify against MinIO, state in PostgreSQL, silver from MinIO to `OPENALEX_LAKE_PATH` (default `/srv/osint/openalex`, mounted at the same path in Airflow), the DAG and the catalog. `tests/pipelines/test_openalex_lake.py` runs the whole flow against a throwaway MinIO and PostgreSQL, including a second release and a bucket caught mid-replacement.

### Step 1: Port the code and tests, behaviour unchanged
- Copy `src/openalex_lake/{config,sync,silver,catalog}.py` and `sql/*.sql` to `pipelines/scripts/openalex/`, and the tests to `tests/pipelines/test_openalex_{sync,silver,catalog}.py`. Commit message names `openalex-lake@fa251ea`.
- Replace the CLI with `python -m openalex {sync,verify,silver,catalog}` in the same folder; paths still come from `OPENALEX_BRONZE` / `OPENALEX_LAKE_HOME`.
- No dependency changes: it needs only DuckDB and the standard library.
- `scripts/run-tests.sh` and CI run the new tests.
- **Verify:** tests pass; `python -m openalex verify` against the current bronze folder gives the same result as `lake verify`.

### Step 2: Bronze in MinIO
- Flyway migration: schema `openalex` with `release` and `file_manifest`.
- Shared storage helper (MinIO client + DuckDB S3 secret), reusing the CNPJ downloader's bucket and credential settings rather than a second copy.
- `sync` writes to MinIO: download with today's resumable range requests to a temp file on the SSD (`/opt/airflow/temp_ssd/openalex_download`), check the size, upload (boto3 multipart), check the object's size, record the file in `file_manifest`, delete the temp file. `manifest.json` is written last, so its presence still means "complete release".
- `verify` reads Parquet footers through DuckDB httpfs instead of the filesystem.
- **One-time import** of the existing 660 GB: copy `/mnt/data10tb/openalex_lake/bronze` into `s3://osint-raw/openalex/parquet/` (same HDD; a few hours), fill `file_manifest` from the local `_state.json` and manifest, then run `verify` against MinIO.
- **Verify:** `verify` passes on MinIO with all record counts; a `sync --dry-run` plans 0 downloads for the same release.
- **Rollback:** the original folder is left untouched until it is deleted, after approval, at the end of Step 4.

### Step 3: Silver on the NVMe
- New host path `OPENALEX_SILVER_HOST_PATH` (default `/srv/osint/openalex`), mounted in the Airflow containers at `/opt/airflow/openalex`; `.env.example` and the compose files updated.
- Move the existing silver and catalog: `mv openalex-lake/data/{silver,lake.duckdb} /srv/osint/openalex/` (same filesystem, so it is a rename, not a copy).
- The silver builder reads bronze from `s3://osint-raw/openalex/...` and decides staleness from `file_manifest.source_etag` instead of mtimes. The first run after the move records the current ETags for the files already built, without rebuilding them.
- DuckDB memory limit and spill directory from the same settings as the CNPJ transform (`CNPJ_DUCKDB_MEMORY_LIMIT` is renamed or paired with `OPENALEX_DUCKDB_MEMORY_LIMIT`; spill to `/opt/airflow/temp_ssd/duckdb_tmp`).
- **Verify:** `silver --limit 5` rebuilds 5 sample files per table from MinIO with the same row counts as the existing outputs; a full run with nothing changed builds 0 files.

### Step 4: `openalex_pipeline` DAG
- The flow above, using the ported functions; long tasks with `execution_timeout=None` and heartbeat logging like the CNPJ load.
- `check_release` is a short-circuit task: the run ends early when the release is already synced and silver is up to date.
- **Verify:** one run end to end in dev with a small entity subset (`continents`, `topics` and 3 `works` files through a test param); then one run in prod-like on the full release that does no work (everything current).
- **Cleanup after approval:** delete `/mnt/data10tb/openalex_lake/bronze` (660 GB freed).

### Step 5: Catalog
- `catalog` task recreates `/srv/osint/openalex/lake.duckdb`: bronze views over `s3://`, silver views over the NVMe paths, `lake_release` from `openalex.release`. A short section in the README shows `ATTACH … (READ_ONLY)` and creating the MinIO secret for bronze queries.

### Step 6: Docs and cleanup
- README (EN and PT-BR): data source entry, the OpenAlex flow, disk table, the DAG.
- ARCHITECTURE_PLAN: storage table and DAG list. `docs/repo/README.md`: link this plan.
- `openalex-lake/ROADMAP.md`: a line saying the project moved into osint-platform (only after approval; it is the user's other repo).

### Step 7: First full run (operational, prod-like)
- Unpause the DAG; record download, verify and silver build times and sizes in this doc.

## Out of scope

- **Gold**, and any PostgreSQL serving tables for OpenAlex.
- Linking OpenAlex to the other sources (e.g. Brazilian institutions to CNPJ, works to FINEP projects or INPI patents): that belongs to gold.
- `authors` and `awards` entities, and keeping older releases (unchanged from openalex-lake).
- Redistributing `work_abstracts` text (OpenAlex terms): it stays internal, as noted in openalex-lake.
