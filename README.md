# Unified OSINT Platform

Plataforma de inteligência OSINT multi-engine para análise de dados públicos brasileiros (CNPJ, sanções, contratos).

## Estrutura do Projeto

| Diretório | Papel |
|-----------|-------|
| `/backend` | API Django — gateway stateless, sem lógica de ETL |
| `/frontend` | Next.js — dashboard de busca e visualização |
| `/pipelines` | Airflow DAGs e scripts ETL (DuckDB, Python) |
| `/infrastructure` | Configurações de PostgreSQL, Neo4j, Airflow |
| `/data` | Dados raw e processados (CNPJ Parquet/ZIP) |

## Pré-requisitos

- Docker & Docker Compose
- 32 GB+ RAM (64 GB recomendado para dataset completo)
- 150 GB+ de espaço em disco

## Quick Start

```bash
# 1. Configurar variáveis de ambiente
cp .env.example .env

# 2. Subir todos os serviços
docker-compose up -d --build
```

### Reset Completo do Ambiente

```bash
./reset-docker-from-scratch.sh
```

---

## Documentação

| Documento | Conteúdo |
|-----------|---------|
| [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md) | Stack, decisões arquiteturais, estratégia de storage |
| [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md) | Ambiente, hardware, status de implementação, comandos operacionais |
| [infrastructure/postgres/README.md](infrastructure/postgres/README.md) | Setup PostgreSQL, schemas CNPJ, MatViews no SSD |
| [pipelines/dags/README_CNPJ_DAG.md](pipelines/dags/README_CNPJ_DAG.md) | DAGs de pipeline CNPJ — uso e parâmetros |
| [DOCKER_RESET_GUIDE.md](DOCKER_RESET_GUIDE.md) | Troubleshooting e reset detalhado dos containers |
