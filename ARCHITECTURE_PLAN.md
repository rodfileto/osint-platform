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

---
*Based on the "Multi-Project OSINT Platform" architecture.*
