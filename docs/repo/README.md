# Repository Documentation

This folder contains the canonical repository-level documentation that used to live at the project root.

## Core Docs

- [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md): architecture decisions, stack, and storage strategy
- [DOCKER_RESET_GUIDE.md](DOCKER_RESET_GUIDE.md): container reset and troubleshooting steps

## Plans And Reviews

- [DATA_ARCHITECTURE_REVIEW.md](DATA_ARCHITECTURE_REVIEW.md): critique of the storage design that led to the plans below
- [MEDALLION_MIGRATION_PLAN.md](MEDALLION_MIGRATION_PLAN.md): bronze / silver / gold layers with PostgreSQL serving

## Historical Docs

These describe earlier states of the project (including Redis and Neo4j) and are kept for reference.

- [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md): rollout history and hardware notes up to 2026-03
- [DOCKER_SERVICES.md](DOCKER_SERVICES.md): service overview before Redis and Neo4j were removed
- [BACKEND_FASTAPI_MIGRATION_PLAN.md](BACKEND_FASTAPI_MIGRATION_PLAN.md)
- [FRONTEND_VITE_MIGRATION_PLAN.md](FRONTEND_VITE_MIGRATION_PLAN.md)
- [CNPJ_IMPLEMENTATION_REVIEW.md](CNPJ_IMPLEMENTATION_REVIEW.md)
- [CONFIG_CHECK_REPORT.md](CONFIG_CHECK_REPORT.md)
- [SOCIOS_NEO4J_ANALYSIS.md](SOCIOS_NEO4J_ANALYSIS.md)

## Deployment And Environment Docs

- [CLOUDFLARE_TUNNEL.md](CLOUDFLARE_TUNNEL.md)

## Notes

- [README.md](../../README.md) remains the root entry point for the repository.
