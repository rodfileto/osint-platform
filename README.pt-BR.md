# Unified OSINT Platform

Plataforma de inteligência OSINT multi-engine para análise de dados públicos brasileiros (CNPJ, sanções, contratos).

## Estrutura do Projeto

| Diretório | Papel |
|-----------|-------|
| `/backend` | API Django — gateway stateless, sem lógica de ETL |
| `/frontend` | React + Vite — dashboard de busca e visualização |
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

# 2. Subir stack principal (decisão explícita sobre migrations)
./scripts/start-prod-like.sh --apply-migrations
```

Por padrão, os dados de todos os serviços (PostgreSQL, Neo4j, MinIO, Airflow) ficam em `./data/` dentro do repositório, e a memória está dimensionada para um host de 16-32 GB. Para carregar os datasets completos, aponte as variáveis `*_HOST_PATH` do `.env` para discos maiores e aumente a memória do PostgreSQL e do Neo4j; o `.env.example` traz o perfil de 64 GB.

O `Quick Start` do compose raiz sobe o stack principal do produto: frontend, backend, PostgreSQL, Neo4j, MinIO e Flyway. Airflow fica fora do startup padrão e pode ser iniciado separadamente com o profile `airflow`.

### Ambiente Local Production-Like

Para rodar localmente com Django em Gunicorn, frontend em build de producao, WhiteNoise para static e MinIO para media/exports, use o compose raiz do projeto:

```bash
cp .env.example .env
./scripts/start-prod-like.sh --apply-migrations
```

URLs esperadas nesse modo:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000`
- Keycloak: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`

Nesse modo:

- static do Django sai via WhiteNoise
- media e exports gerados pelo backend usam MinIO via API S3
- login do frontend usa Keycloak via OIDC com bearer token para a API FastAPI
- `KEYCLOAK_URL` controla o acesso local ao Keycloak; `KEYCLOAK_PUBLIC_URL` e `VITE_KEYCLOAK_URL` so devem apontar para o dominio publico quando o tunnel estiver habilitado
- dados locais do Keycloak ficam em um volume Docker gerenciado pelo compose
- o ambiente dev continua isolado em `dev/`
- Airflow fica separado do startup padrão e não sobe junto com o app
- o startup prod-like exige decisão explícita sobre Flyway via `--apply-migrations` ou `--skip-migrations`

Credenciais locais importadas por padrao no realm bootstrap:

- usuario: `osint-admin`
- senha: `osint123`

Para subir Airflow separadamente quando precisar:

```bash
./scripts/start-prod-like.sh --skip-migrations --with-airflow
```

Ou, se o app já estiver de pé:

```bash
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml --profile airflow up -d airflow-init airflow-webserver airflow-scheduler airflow-triggerer
```

Para parar apenas o grupo do Airflow:

```bash
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml stop airflow-webserver airflow-scheduler airflow-triggerer
```

A geografia brasileira (estados e municípios do IBGE) é carregada automaticamente no schema `geo`: o serviço `geo-bootstrap` roda antes do backend a cada start e baixa as geometrias só na primeira vez. O `cnpj_pipeline` reconstrói o mapeamento de municípios do CNPJ após cada carga.

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

### Primeira carga de dados

O banco começa vazio e todas as DAGs são criadas pausadas. Cada fonte tem uma DAG de pipeline, e despausá-la já inicia a primeira execução:

```bash
C="docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml"
for dag in cnpj_pipeline finep_pipeline inpi_pipeline; do $C exec airflow-scheduler airflow dags unpause $dag; done
```

`cnpj_pipeline` faz download → transform → PostgreSQL → views de busca → Neo4j, `finep_pipeline` baixa as duas planilhas e carrega as 7 tabelas FINEP, e `inpi_pipeline` baixa os 10 CSVs de patentes, carrega e monta o grafo de patentes.

O release do CNPJ tem cerca de 7 GB e o servidor da Receita Federal entrega por volta de 2,5 MB/s, então o download leva cerca de uma hora; `cnpj_pipeline` transforma e carrega o mês na mesma execução. Para reprocessar um mês: `airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10", "force_reprocess": true}'`. Os downloads podem ser repetidos com segurança: arquivos que já estão no MinIO são ignorados ou registrados sem novo download.

### Ambiente Dev com Amostra Reduzida

Para subir uma réplica leve do stack completo, com portas separadas e base de amostra para CNPJ, Neo4j e FINEP:

```bash
cp dev/.env.example dev/.env
chmod +x dev/scripts/*.sh
./dev/scripts/start.sh
```

Documentação específica: [dev/README.md](dev/README.md)

O compose raiz agora define apenas a base compartilhada. As diferenças de execução ficam em [compose.prod.yml](compose.prod.yml) e [compose.dev.yml](compose.dev.yml), reduzindo drift entre dev e prod-like para serviços como Flyway e dependências de infraestrutura.

Para reduzir risco operacional, o prod-like não aplica mais migrations implicitamente pelo startup do backend. O runner [scripts/start-prod-like.sh](scripts/start-prod-like.sh) exige uma decisão explícita: `--apply-migrations` ou `--skip-migrations`.

### Reset Completo do Ambiente

Veja [docs/repo/DOCKER_RESET_GUIDE.md](docs/repo/DOCKER_RESET_GUIDE.md).

---

## Documentação

| Documento | Conteúdo |
|-----------|---------|
| [docs/repo/ARCHITECTURE_PLAN.md](docs/repo/ARCHITECTURE_PLAN.md) | Stack, decisões arquiteturais, estratégia de storage |
| [docs/repo/IMPLEMENTATION_STEPS.md](docs/repo/IMPLEMENTATION_STEPS.md) | Ambiente, hardware, status de implementação, comandos operacionais |
| [infrastructure/postgres/README.md](infrastructure/postgres/README.md) | Setup PostgreSQL, schemas CNPJ, MatViews no SSD |
| [pipelines/dags/cnpj/](pipelines/dags/cnpj/) | DAGs do pipeline CNPJ (`transform → postgres → matview → neo4j`) |
| [docs/repo/DOCKER_RESET_GUIDE.md](docs/repo/DOCKER_RESET_GUIDE.md) | Troubleshooting e reset detalhado dos containers |
| [infrastructure/systemd/osint-platform-local.service](infrastructure/systemd/osint-platform-local.service) | Template de unidade systemd para subir o stack local-production no boot |
| [infrastructure/systemd/osint-platform-airflow.service](infrastructure/systemd/osint-platform-airflow.service) | Template de unidade systemd para subir apenas o grupo do Airflow |

## Licença

Distribuído sob a [Apache License 2.0](LICENSE). Os datasets continuam sujeitos aos termos de seus publicadores.
