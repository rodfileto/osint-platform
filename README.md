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

# 2. Subir stack principal (app + infra compartilhada)
docker compose --env-file .env up -d --build
```

O `Quick Start` do compose raiz sobe o stack principal do produto: frontend, backend, PostgreSQL, Neo4j, Redis, MinIO e Flyway. Airflow fica fora do startup padrão e pode ser iniciado separadamente com o profile `airflow`.

### Ambiente Local Production-Like

Para rodar localmente com Django em Gunicorn, frontend em build de producao, WhiteNoise para static e MinIO para media/exports, use o compose raiz do projeto:

```bash
cp .env.example .env
docker compose --env-file .env up -d --build
```

URLs esperadas nesse modo:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000`
- MinIO Console: `http://localhost:9001`

Nesse modo:

- static do Django sai via WhiteNoise
- media e exports gerados pelo backend usam MinIO via API S3
- o ambiente dev continua isolado em `dev/`
- Airflow fica separado do startup padrão e não sobe junto com o app

Para subir Airflow separadamente quando precisar:

```bash
docker compose --env-file .env --profile airflow up -d airflow-init airflow-webserver airflow-scheduler airflow-triggerer
```

Para parar apenas o grupo do Airflow:

```bash
docker compose --env-file .env stop airflow-webserver airflow-scheduler airflow-triggerer
```

Para carregar ou atualizar manualmente a geografia canônica brasileira no schema `geo`:

```bash
sh infrastructure/postgres/run-geo-bootstrap.sh
```

Esse comando executa o loader compartilhado em [pipelines/scripts/geo/load_br_geo_reference.py](pipelines/scripts/geo/load_br_geo_reference.py) contra o stack raiz e atualiza `geo.br_state`, `geo.br_municipality`, `geo.cnpj_br_municipality_map` e `geo.v_cnpj_br_municipality`.

Para inicializar automaticamente no boot via systemd:

```bash
sudo sh infrastructure/systemd/install-osint-platform-local.sh --start
```

Isso instala [infrastructure/systemd/osint-platform-local.service](infrastructure/systemd/osint-platform-local.service) e [infrastructure/systemd/osint-platform-airflow.service](infrastructure/systemd/osint-platform-airflow.service) em `/etc/systemd/system/`, executa `systemctl daemon-reload`, habilita o serviço principal para boot e inicia o stack principal imediatamente.

Se você quiser Airflow no boot também, habilite separadamente:

```bash
sudo systemctl enable osint-platform-airflow.service
sudo systemctl start osint-platform-airflow.service
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
| [infrastructure/systemd/osint-platform-local.service](infrastructure/systemd/osint-platform-local.service) | Template de unidade systemd para subir o stack local-production no boot |
| [infrastructure/systemd/osint-platform-airflow.service](infrastructure/systemd/osint-platform-airflow.service) | Template de unidade systemd para subir apenas o grupo do Airflow |
