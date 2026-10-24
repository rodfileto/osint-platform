# Data Architecture Review: Lake → PostgreSQL + Neo4j

Date: 2026-10-24. Scope: CNPJ, INPI and FINEP ingestion, how they are stored, and what the API actually reads. The current design itself is described in [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md); this document only covers its problems.

## TL;DR

The layered idea (raw files in MinIO, DuckDB transforms, PostgreSQL for search) is sound and not over-engineered. The problem is **Neo4j for CNPJ**. It holds a second full copy of the registry (66.6M `Empresa`, 17.5M `Pessoa`, 27M+ `SOCIO_DE`) and takes about 12 hours to load. In return it serves three queries: two bounded ego networks (depth ≤ 3, capped at 200 edges) and a search preview that PostgreSQL already does better. No graph algorithms, path finding or cross-domain graph queries are implemented in the API.

Writing to two stores has also caused **real correctness drift**: the two stores, and different tables within PostgreSQL, keep time differently. That matters more than storage cost.

## 1. What each store actually serves

| Store | Used by the API for |
|---|---|
| PostgreSQL | All search (2 materialized views), company/person detail, FINEP, INPI, geography: essentially everything |
| Neo4j | `CnpjNetworkService.get_company_network`, `get_person_network` ([cnpj_network.py](../../backend/api/services/cnpj_network.py)), plus a "preview" branch in unified search ([search.py:143](../../backend/api/routers/search.py#L143)) |

- The finep and inpi routers never query Neo4j. The INPI (`Patente`, `ClasseIPC`, `DEPOSITOU`) and FINEP (`ProjetoFinep`, `SIMILAR_TO`) graph data is loaded but **nothing reads it**.
- `grep` finds no `shortestPath`, `gds.*` or `apoc.*` calls in backend or pipelines. The cross-domain query the earlier architecture plan advertised ("defence patents + FINEP projects on the same topic") does not exist.
- Network depth is capped by `NEO4J_MAX_NETWORK_DEPTH=3` and results by `NEO4J_MAX_NETWORK_EDGES=200`. A recursive CTE over `cnpj.socio` handles this kind of bounded query well, and the indexes it needs (`idx_socio_cnpj_basico`, `idx_socio_cpf_cnpj`) already exist.

## 2. Redundancy map (CNPJ)

The same data is stored in up to six forms:

1. Raw ZIP in MinIO (`osint-raw/cnpj/raw/YYYY-MM/`): **keep**, this is the source of truth.
2. Parquet on the local SSD (`/opt/airflow/temp_ssd/cnpj_processed`): not in MinIO, so not durable. The planned `osint-processed` bucket is unused.
3. PostgreSQL base tables (`empresa`, `estabelecimento`, `socio`, …).
4. `mv_company_search` and 5. `mv_company_search_inactive`: copies of joined base tables. Refreshing them over 135M rows takes hours (`execution_timeout=None`).
6. Neo4j `Empresa` / `Pessoa` / `SOCIO_DE`.

Copies 1 and 3 are justified. Copy 2 should be durable or treated as explicitly disposable. Copies 4 and 5 are worth questioning. Copy 6 is the main question of this review.

## 3. Correctness problems caused by the dual load

These matter more than disk space.

### 3.1 Months pile up in the graph
`SOCIO_DE` is MERGEd with `reference_month` as its key ([load_tasks.py `_create_pessoa_socio_batch`](../../pipelines/dags/tasks/load_tasks.py)). Every monthly load adds a parallel edge for every partnership that still exists, and nothing is ever deleted:
- The network queries (`[:SOCIO_DE*1..n]`) do not filter by month. They return duplicate edges, and partners who left years ago appear as current.
- Duplicates use up the 200-edge cap, so after a few months the visible network shrinks.
- Variable-length expansion cost grows with every month loaded.
- Companies that close or drop out of the registry stay in the graph forever.

### 3.2 PostgreSQL tables keep time differently
- `empresa` / `estabelecimento` have a PK without the month, so they hold **only the latest** snapshot.
- `socio` has a surrogate `id` and is appended per month, so it **accumulates history**.
- Person queries in [cnpj.py](../../backend/api/routers/cnpj.py) (around lines 455–530) do not filter `socio.reference_month`, so they show the same mix of current and past partnerships.

### 3.3 Monthly reload probably fails without `force_reprocess`
In normal mode, `load_to_postgresql` skips a month that is already loaded and otherwise runs a plain `INSERT`. For a **new** month, `empresa` (PK `cnpj_basico`) will hit duplicate keys. The docstring in [load_groups.py:138](../../pipelines/dags/tasks/load_groups.py#L138) says "TRUNCATE + INSERT strategy is used upstream", but no TRUNCATE exists in the CNPJ path. *Not run; inferred from the code.* In practice the pipeline only works with `force_reprocess=True`, which goes through a staging table and `ON CONFLICT`.

### 3.4 Placeholder ("stub") rows
- PostgreSQL still inserts `'EMPRESA NAO CARREGADA'` stub companies to satisfy foreign keys, but migration `V2026.03.08.003` dropped those FKs. The stubs now only pollute search results.
- Neo4j creates `_stub` `Empresa` nodes in three places. A stub is cleaned up only if a matching Empresa row appears later.

### 3.5 Inconsistent lineage
- CNPJ → Neo4j reads **Parquet**. INPI/FINEP → Neo4j read **PostgreSQL**.
- `DEPOSITOU` / `PROPOSTO_POR` silently skip any company not yet in Neo4j, so graph completeness depends on DAG order.

## 4. Neo4j load efficiency

The 12-hour load is mostly self-inflicted:
- Batches are read with `LIMIT 50000 OFFSET n` from Parquet. For `Socio`, every batch also **recomputes `COUNT(*) OVER (PARTITION BY …)` over the whole file**, so total work grows roughly with the square of the file size.
- Every row becomes a Python dict and goes through `MERGE` over Bolt. For a **full monthly snapshot**, MERGE is the wrong tool. `neo4j-admin database import full` from CSV/Parquet-derived files into a fresh database usually takes minutes to tens of minutes at this scale, then you swap databases.
- All 66.6M companies are loaded, but only companies that touch an edge (`SOCIO_DE`, patents, FINEP) are reachable in the graph. A large share of the registry is MEIs and sole proprietors with no partners.

## 5. Search duplication

Unified search runs PostgreSQL and Neo4j in parallel. The Neo4j branch does `MATCH (n) WHERE … CONTAINS …`: a scan of **every node** with no label. It ignores the `empresa_names` / `pessoa_names` full-text indexes that already exist. It is the slowest and least useful part of the request. PostgreSQL (GIN/trigram on the materialized views) should be the only search engine.

## 6. What is *not* over-engineered

- MinIO raw zone + download manifest + ETag checks: cheap, idempotent, auditable.
- DuckDB SQL transforms to Parquet: the right tool, and fast.
- PostgreSQL with schemas per domain + Flyway.
- Airflow is heavy for a single-node monthly batch, but defensible given the multiple sources. Five CNPJ DAGs chained with `TriggerDagRunOperator` could be one DAG with task groups, which would make failures and retries easier to follow.

## 7. Recommendations (in order)

1. **Pick a temporal model first, then make every store follow it.** Either *latest snapshot only* (simplest, and matches `empresa`) or *history kept separately* (e.g., `socio_history` with `valid_from`/`valid_to`, and serving tables/graph built from the latest snapshot only). Until this is decided, every other fix sits on unstable ground.

2. **Benchmark PostgreSQL for the ego networks before investing more in Neo4j.** Write the company and person network as recursive CTEs over `cnpj.socio` (latest month, depth 1–3, `LIMIT 201`) and measure p95 on hub companies.
   - If it is fast enough, **drop CNPJ from Neo4j**. That removes 12 hours of loading, a second copy of the data and a class of drift bugs.
   - Keep Neo4j only for things PostgreSQL does badly, and only once they exist in the product: path finding between two entities, ownership chains of unknown depth, communities/centrality (GDS), and cross-domain exploration.

3. **If Neo4j stays, treat it as a disposable projection you rebuild, not a store you keep updating.**
   - Rebuild it monthly from one source (Parquet in MinIO) with `neo4j-admin import` into a new database, then swap. No MERGE, no edges keyed by month, no stubs, nothing to delete.
   - Load only the connected subgraph: companies with partners, patents or FINEP links, not all 66.6M.
   - Make INPI/FINEP export to the same Parquet layer so the graph has a single source and no dependency on load order.

4. **Remove the Neo4j branch from unified search.**

5. **Make the processed layer real.** Write Parquet to `osint-processed` in MinIO. Then the loaders become re-runnable without re-transforming, and ad-hoc analytics can run in DuckDB over Parquet instead of growing PostgreSQL.

6. **Replace `REFRESH MATERIALIZED VIEW` with build-and-swap.** Build the search table with `CREATE TABLE … AS` into a new table, index it, and swap it in with `ALTER TABLE … RENAME` in one transaction. Reconsider whether active and inactive need two separate views or one table with a partial index.

7. **Remove dead and duplicate code:** the PostgreSQL stub insertion, the deprecated `Estabelecimento` Neo4j path (`NEO4J_ESTABELECIMENTO_FIELDS`, `_create_estabelecimento_nodes`, `delete_estabelecimentos_neo4j.py`), and the three cleaner implementations (`cleaners.py`, `cleaners_sql.py`, `cleaners/`). Fix the TRUNCATE docstring, or implement what it describes.

## Open questions for the owner

- Is history of partnerships a product requirement (e.g., "who were the partners in 2022?"), or is the current snapshot enough?
- Are graph-native features (path between two people, ownership chains, GDS) on the roadmap? If not, Neo4j's cost is hard to justify today.
- What is the observed p95 latency of the network endpoints on large companies right now?
