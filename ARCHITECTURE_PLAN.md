# OSINT Platform Architecture Plan

## Core Architectural Principle

**Separate:**
*   Data Processing Layer (Pipelines)
*   Data Storage Layer
*   Application/API Layer
*   Frontend (Presentation Layer)

**Do NOT mix them.** Your CNPJ processing must not live inside your Django app.

## 1. Data Processing Layer (Pipelines)

Mask processing into independent data engineering jobs. Each project (CNPJ, Sanctions, Contracts) has:
*   Its own repository (or subfolder).
*   Its own ETL pipeline.
*   Its own schema and data model.

**Location:** `/pipelines/{project_name}`

**Tools:** DuckDB, Python, Spark (if needed), Apache Airflow.
**Outputs:** Clean tables, Graph edges, Derived datasets.

## 2. Storage Strategy

**Goal:** Avoid multiple isolated databases. Use a unified storage pattern.

### A. PostgreSQL (Metadata Layer)
Single Postgres cluster with schema separation:
*   `public.users`, `public.auth` (Shared Auth)
*   `cnpj.empresas`, `cnpj.estabelecimentos`
*   `sanctions.entities`
*   `contracts.contracts`

### B. Neo4j (Graph Layer)
**Unified Graph (Recommended):** All projects load into the same graph to enable cross-domain intelligence.
*   **Nodes:** `Empresa`, `Pessoa`, `Contract`, `Politico`, `SanctionedEntity`
*   **Relationships:** `SOCIO_DE`, `CONTRATOU`, `DOADOR_DE`, `LOCALIZADO_EM`

This allows finding connections like: *"A partner of Company A is also in a Sanctions list."*

## 3. Django API (Modular Architecture)

**Role:** Orchestrator, Stateless, Clean.
**Location:** `/backend`

**Structure:**
*   `/apps/cnpj`
*   `/apps/sanctions`
*   `/apps/core`

Django must **not** know how data was processed. It simply consumes the DBs exposed by the pipelines.

## 4. Frontend (Next.js)

**Role:** Modular UI.
**Location:** `/frontend`

**Features:**
*   Unified search bar (CNPJ, CPF, Name).
*   Common graph viewer component.
*   Modules: `/modules/cnpj`, `/modules/sanctions`.

## 5. Entity Resolution (The "Secret Sauce")

To achieve real OSINT power, build a global **"Entity Resolution"** layer.
*   One `Person` node per real person.
*   One `Company` node per real company.
*   All datasets connect to these central identity nodes.

## Infrastructure
*   **Docker Compose:** Orchestrates Postgres, Neo4j, Redis, Django, Celery.
*   **Airflow:** Orchestrates the ETL pipelines independently of the web app.

## Database Sizes & Performance Considerations

### Current Database Sizes (as of February 2026)

**PostgreSQL (osint_metadata):** 106 GB
*   `cnpj.empresa`: 83 GB (company base data)
*   `cnpj.estabelecimento`: 24 GB (establishment locations)
*   `cnpj.download_manifest`: 888 KB (pipeline tracking)
*   Other schemas: ~4 MB (Airflow metadata)

**Neo4j:** 5.2 GB
*   Graph data for CNPJ relationships and entities

**Total Storage:** ~111 GB

### Memory Requirements & Actual Configuration

**Current Hardware:**
*   **RAM:** 64 GB (i7-9700K, 8 cores)
*   **Storage:** 
    *   PostgreSQL: 106 GB on HDD (9.1TB `/media/bigdata`)
    *   Neo4j: 5.2 GB on SSD (384GB `/`, 98 GB free after migration)
*   **Database sizes cannot fit entirely in RAM** — tuning focused on maximizing cache efficiency

**Applied Tuning:**
*   **PostgreSQL** (optimized for 64GB RAM + HDD):
    *   `shared_buffers=16GB` (25% RAM)
    *   `effective_cache_size=48GB` (75% RAM)
    *   `work_mem=64MB`, `maintenance_work_mem=2GB`
    *   `random_page_cost=3.0`, `effective_io_concurrency=2` (HDD settings)
    *   `max_parallel_workers_per_gather=4` (8 cores)
*   **Neo4j** (on SSD for fast I/O):
    *   `pagecache=6GB` (fits entire 5.2GB dataset)
    *   `heap=4GB` (initial + max)
    *   `dbms.memory.transaction.total.max=2GB`

**For full in-memory operation:**
*   **Minimum RAM:** 128 GB (106 GB PostgreSQL + 10 GB Neo4j + 12 GB system)
*   **Recommended RAM:** 192 GB+ for production with headroom

With 64 GB RAM, PostgreSQL uses OS page cache for the ~90 GB that doesn't fit in shared_buffers. Performance depends on workload's hot data size.

### Scaling Considerations
*   Current dataset covers 48+ months of CNPJ data (2021-2025)
*   Database grows ~2-3 GB per month with new CNPJ updates
*   Consider partitioning by `reference_month` for tables > 50 GB
*   Neo4j graph relationships scale well up to billions of edges

---
*Based on the "Multi-Project OSINT Platform" architecture.*
