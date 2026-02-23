# Unified OSINT Platform

This repository contains the source code for a multi-engine OSINT intelligence platform.

## Architecture

The project is divided into 4 distinct layers:

1.  **Backend (`/backend`)**: Django application acting as the API Gateway and Orchestrator. It connects to the database layer but contains NO heavy data processing logic.
2.  **Frontend (`/frontend`)**: Next.js application for the user interface.
3.  **Pipelines (`/pipelines`)**: Apache Airflow DAGs and ETL scripts. This is where data processing happens (CNPJ, Sanctions, Contracts, etc.).
4.  **Infrastructure (`/infrastructure`)**: Configuration for Postgres, Neo4j, Airflow, and other core services.

## Prerequisites

-   Docker & Docker Compose
-   16GB+ RAM recommended (for Airflow + Neo4j)

## Getting Started

### Quick Start

1.  Copy `.env.example` to `.env`.
2.  Run `docker-compose up -d --build`.

### Reset Environment from Scratch

Para testar o workflow global do zero ou resolver problemas de containers:

```bash
./reset-docker-from-scratch.sh
```

Este script automatiza todo o processo de cleanup e reinicialização dos containers Docker.

📖 **Documentação detalhada**: [DOCKER_RESET_GUIDE.md](DOCKER_RESET_GUIDE.md)

---

## Documentation

- 📦 **[Docker Reset Guide](DOCKER_RESET_GUIDE.md)** - Troubleshooting e reset completo do ambiente
- 🗄️ **[PostgreSQL Setup](infrastructure/postgres/README.md)** - Schema CNPJ, tabelas e queries
- 🔀 **[CNPJ DAGs](pipelines/dags/README_CNPJ_DAG.md)** - Pipeline de dados CNPJ
- 🏗️ **[Architecture Plan](ARCHITECTURE_PLAN.md)** - Visão geral da arquitetura
