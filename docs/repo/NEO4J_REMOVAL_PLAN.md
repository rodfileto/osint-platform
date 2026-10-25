# Neo4j Removal Plan

Date: 2026-10-25. Replaces Step 5 of [MEDALLION_MIGRATION_PLAN.md](MEDALLION_MIGRATION_PLAN.md), which predates the single-DAG pipelines.

## Why

Neo4j holds a second copy of the CNPJ registry (70.5M `Empresa`, ~17M `Pessoa`, ~28M `SOCIO_DE`), plus INPI patents and FINEP projects. Loading it is about two-thirds of the CNPJ pipeline (≈1–1.5 h for companies, an estimated 1.5–3 h for partners, against ≈1.5 h for everything else). In return the API uses it for two bounded queries (company and person ego networks, depth ≤ 3, at most 200 edges) and a search preview; the INPI and FINEP graph data is not read by anything ([DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md)).

The co-ownership analyses that motivate a graph run in PostgreSQL already (measured 2026-10-25, warm cache):

| Analysis | Result | Time |
|---|---|---|
| FINEP companies → partners → every company those partners own | 3,233 → 9,204 → 36,018 | 0.3 s (1.2 s before `idx_socio_cpf_nome`) |
| FINEP companies sharing a partner | 375 pairs | 0.07 s |
| Patent holders sharing a partner | 28,629 holders, 3,888 pairs | 3.6 s |
| Holding chain above patent holders, 4 levels (recursive CTE) | 6,598 / 2,394 / 979 / 651 links | 1.3 s |

Graph algorithms (communities, centrality) or open-ended path search, if they become product needs, will run on an extracted subgraph (NetworkX or igraph in a DAG task, results written to PostgreSQL), not on a permanently synchronized second store.

## Scope

In: every Neo4j dependency in pipelines, backend, frontend labels, infrastructure, dev environment and current docs. Out: graph analytics themselves (separate work once needed), and historical docs under `docs/repo/` that describe past states.

## Gate before anything is deleted

**Step 0 — Benchmark the ego networks as recursive CTEs.** The only real dependency on Neo4j is the two network endpoints. Write both queries over `cnpj.socio` (latest `reference_month`), following `SOCIO_DE` in both directions (company ↔ person, company ↔ company for corporate partners), depth 1–3, capped at 201 edges. Measure p95 on:
- the 20 companies with the most partners,
- the 20 people (masked CPF + name) with the most companies,
- a sample of ordinary companies.

Pass: p95 under ~1 s at depth 2 and under a few seconds at depth 3. If it fails, stop here and decide between better indexes, a precomputed adjacency table, or keeping a small disposable graph.

`idx_socio_cpf_nome (cpf_cnpj_socio, nome_socio_razao_social)` (migration `V2026.10.25.002`) already serves the person side of these joins.

## Steps

Each step is one commit, verified before the next starts.

### Step 1 — Move the network endpoints to PostgreSQL

- `backend/api/services/cnpj_network.py`: replace the two Cypher queries with the Step 0 CTEs. Keep the response shape exactly (`nodes`, `edges`, `metadata` with `core_*`, `depth`, `total_nodes`, `total_edges`, `total_relationships`, `truncated`, `max_edges`) so `CompanyNetworkResponse` / `PersonNetworkResponse` and the Cytoscape components do not change.
- `backend/api/routers/cnpj.py`: endpoints `/api/cnpj/network/{cnpj_basico}`, `/api/cnpj/pessoa-network/detail`, `/api/cnpj/pessoa-network/{cpf}` keep their paths and params. Rename the settings `neo4j_max_network_depth` / `neo4j_max_network_edges` to `network_max_depth` / `network_max_edges`.
- Edges come from the latest month only, which also fixes the duplicate-edge problem of month-keyed `SOCIO_DE` (review §3.1).
- **Verify:** the same company and person return the same set of neighbors as Neo4j today (compare a few by hand, allowing for removed duplicates); frontend graphs render.

### Step 2 — Remove Neo4j from search

- `backend/api/routers/search.py`: drop `_search_neo4j_preview` and the `neo4j` part of the concurrent gather; `backend/api/schemas/search.py`: drop the `neo4j` field from `UnifiedSearchResponse`.
- Check the frontend for any use of `neo4j` in the search response before removing the field.
- **Verify:** unified search returns the PostgreSQL results unchanged.

### Step 3 — FINEP project similarity to PostgreSQL

- New migration: `finep.nlp_project_similarity (run_id, projeto_id, similar_projeto_id, similarity)` with FKs to `finep.nlp_model_run` and `finep.projetos_operacao_direta` (`ON DELETE CASCADE`, like `nlp_document_topic`).
- `pipelines/scripts/finep/nlp_topic_pipeline.py`: replace `load_neo4j_nodes` / `load_neo4j_edges` with one `store_similarity(run_id)` that writes the k-NN pairs; topic and UMAP coordinates already live in `finep.nlp_document_topic`.
- `pipelines/dags/finep/finep_nlp_topic_dag.py`: replace the two Neo4j tasks with that one.
- In the same migration, `COMMENT ON` the NLP tables whose comments say edges live in Neo4j (`V2026.03.17.001` is applied and is not edited).
- **Verify:** a `finep_nlp_topic` run fills the new table; edge count matches the k-NN parameters.

### Step 4 — Remove the graph loads from the pipelines

- `cnpj_pipeline`: delete `neo4j_enabled`, `ensure_neo4j_indexes`, `load_neo4j_empresas`, `load_neo4j_socios`, the `load_neo4j` param; delete `tasks/load_neo4j.py`, `tasks/neo4j_schema.py` and the Neo4j settings in `tasks/config.py`.
- `inpi_pipeline`: delete `neo4j_enabled`, `ensure_inpi_schema` and the five graph tasks, the `load_neo4j` param; delete `scripts/inpi/neo4j_loader.py`.
- `pipelines/dags/test_connection.py`: drop the Neo4j check.
- `pipelines/requirements.txt`: drop `neo4j`.
- **Verify:** both DAGs parse; a forced run of each finishes without Neo4j tasks; the CNPJ run time drops to ≈1.5 h.

### Step 5 — Remove Neo4j from the backend

- `backend/core/database.py`: drop the driver, `neo4j_session`, `run_neo4j_query` and the Neo4j entry of `/health/ready`.
- `backend/core/config.py`: drop the `neo4j_*` settings; `backend/requirements.txt`: drop `neo4j`.
- **Verify:** backend starts; `/health/ready` reports PostgreSQL only; all API paths still listed in `openapi.json`.

### Step 6 — Remove the service and its configuration

- `docker-compose.yml`: delete the `neo4j` service and the `NEO4J_*` env; `compose.dev.yml` / `compose.prod.yml`: delete `neo4j` from `depends_on`.
- `scripts/start-prod-like.sh`: drop `neo4j` from the started services, the readiness wait and the summary line.
- `.env.example`, `dev/.env.example`: drop `NEO4J_*`; remind users to drop them from their own `.env`.
- `infrastructure/neo4j/` (plugins/apoc.jar, Cypher init scripts, `apply-indexes.sh`, `MIGRATION_GUIDE.md`): delete.
- `dev/`: drop `seed_sample.cypher`, the Neo4j parts of `scripts/compose_bootstrap.py`, `start.sh`, `bootstrap-sample.sh`, `reset.sh`.
- `.gitignore`: drop the Neo4j entry.
- **Verify:** `./scripts/check-compose-guardrails.sh` passes; a fresh `start-prod-like.sh --apply-migrations --with-airflow` brings the stack up without Neo4j; the dev replica starts.

### Step 7 — Frontend labels and docs

- `CompanyNetworkGraph.tsx`, `EmpresaDetail.tsx`, `PersonNetworkGraph.tsx`, `PessoaDetail.tsx`: drop "(Neo4j)" from the card descriptions.
- `README.md` / `README.pt-BR.md`: architecture diagram, highlights, tech stack, layout, pipeline tables, timing note; `docs/repo/ARCHITECTURE_PLAN.md`; `pipelines/dags/README_CNPJ_DAG.md` (flow, tasks, params, the per-file benchmark); `DOCKER_RESET_GUIDE.md`; `pipelines/scripts/README.md`; `infrastructure/postgres/README.md`.
- **Verify:** `git grep -i neo4j` returns only historical docs and applied migrations.

### Step 8 — Reclaim the data

After the stack has run without Neo4j for a while: delete the Neo4j data directory (`NEO4J_DATA_HOST_PATH`, `/srv/osint/neo4j/data` on the main machine). This is the only irreversible step; take it last.

## Risks

- **Network query cost on hub companies** — covered by the Step 0 gate.
- **Person identity.** Receita masks CPFs, so a person is the pair (masked CPF, name). That is the same key Neo4j uses (`pessoa_id` is a hash of it), so results do not get worse, but homonyms with the same masked digits stay merged.
- **Edges that only Neo4j has.** The graph's `SOCIO_DE` edges are built from the same `socio` Parquet that loads `cnpj.socio`; nothing is lost. INPI `DEPOSITOU`/`VINCULADA_A` edges come from `inpi.*` tables, and FINEP `SIMILAR_TO` moves to PostgreSQL in Step 3.
