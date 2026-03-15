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

## Ambiente Python (dev)

Para evitar conflito entre dependências do Django (`backend`) e Airflow/ETL (`pipelines`), use ambientes separados:

- `backend/.venv`
- `pipelines/.venv`

Bootstrap rápido:

```bash
chmod +x ./setup-python-envs.sh
./setup-python-envs.sh
```

No VS Code, selecione o interpretador conforme o contexto:

- Backend: `backend/.venv/bin/python`
- Pipelines: `pipelines/.venv/bin/python`

## Quick Start

```bash
# 1. Configurar variáveis de ambiente
cp .env.example .env

# 2. Subir todos os serviços
docker-compose up -d --build
```

### Executar Pipeline CNPJ Completo

```bash
# Detecta o mês mais recente e executa a cadeia:
# transform → load_postgres → matview → neo4j
./pipelines/scripts/cnpj/run_pipeline.sh
```

### Ambiente Dev com Amostra Reduzida

Para subir uma réplica leve do stack completo, com portas separadas e base de amostra para CNPJ, Neo4j e FINEP:

```bash
cp dev/.env.example dev/.env
chmod +x dev/scripts/*.sh
./dev/scripts/start.sh
```

Documentação específica: [dev/README.md](dev/README.md)

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
| [pipelines/dags/cnpj/](pipelines/dags/cnpj/) | DAGs do pipeline CNPJ (`transform → postgres → matview → neo4j`) |
| [DOCKER_RESET_GUIDE.md](DOCKER_RESET_GUIDE.md) | Troubleshooting e reset detalhado dos containers |
