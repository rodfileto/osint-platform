# OSINT Platform — Passos de Implementação

> Documento **operacional**: ambiente, configuração de hardware, histórico de execução e próximos passos.
> Para decisões arquiteturais e stack, veja [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md).

---

## Ambiente & Infraestrutura

### Hardware

| Recurso | Especificação |
|---------|--------------|
| CPU | Intel i7-9700K — 8 cores |
| RAM | 64 GB |
| NVMe | 4 TB — `/` + `/srv/osint` — PostgreSQL, `fast_ssd`, Neo4j e temp rápido |
| HDD | 10 TB — `/mnt/data10tb` — dados raw/staging/processed e MinIO |
| HDD | 2 TB — `/mnt/data2tb` — backups e retenção operacional |

### Layout Atual de Volumes

**Diretórios quentes no NVMe (`/srv/osint`):**
- PostgreSQL data dir: `/srv/osint/postgres/data`
- Tablespace `fast_ssd`: `/srv/osint/postgres/ssd_tablespace`
- Neo4j data dir: `/srv/osint/neo4j/data`
- Airflow temp SSD: `/srv/osint/airflow/temp_ssd`

**Diretórios bulk e object storage:**
- Dados do pipeline: `/mnt/data10tb/osint/pipelines/data`
- MinIO data dir: `/mnt/data10tb/osint/minio`
- Backups: `/mnt/data2tb/osint/backups`

**Observação operacional:** os mounts do host são nativos em `/mnt` (via `fstab`), e o automount do GNOME foi desabilitado para evitar remount automático em `/media`.

### Banco de Dados (estado atual — fev/2026)

**PostgreSQL 16 (`osint_metadata`):** 106 GB total
- `cnpj.empresa`: 83 GB — 66.936.364 registros
- `cnpj.estabelecimento`: 24 GB — 69.446.909 registros
- `cnpj.mv_company_search`: 6.785 MB no NVMe — 27.795.796 registros (ativos)
- `cnpj.download_manifest`: ~1 MB

**Neo4j:** 5.2 GB (localizado no NVMe)
- `Empresa`: 66.682.246 nós
- `Estabelecimento`: 69.177.350 nós

**Cobertura de dados:** 49+ meses de CNPJ (2021-10 a 2026-02)

### Tuning Aplicado

**PostgreSQL** (configuração atual para 64 GB RAM e workload híbrido):
```
shared_buffers           = 16GB   (25% da RAM)
effective_cache_size     = 48GB   (75% da RAM)
work_mem                 = 64MB
maintenance_work_mem     = 2GB
random_page_cost         = 3.0    (HDD)
effective_io_concurrency = 2      (HDD)
max_parallel_workers_per_gather = 4
```

**Tablespace `fast_ssd`** (para MatViews no NVMe):
```
random_page_cost = 1.1
seq_page_cost    = 1.0
location         = /var/lib/postgresql/ssd_tablespace
host path        = /srv/osint/postgres/ssd_tablespace
```

**Neo4j** (no NVMe):
```
pagecache = 6GB   (comporta os 5.2 GB inteiros)
heap      = 4GB
dbms.memory.transaction.total.max = 2GB
```

**Constraint Docker (shm):** O `/dev/shm` do container PostgreSQL é limitado.
Operações pesadas de MatView devem usar antes de executar:
```sql
SET max_parallel_workers_per_gather = 0;
```

---

## Serviços e Access Points

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Frontend (React + Vite) | http://localhost:3000 | — |
| Backend API (Django) | http://localhost:8000 | — |
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Neo4j Browser | http://localhost:7474 | neo4j / osint_password |
| MinIO API | http://localhost:9000 | ver `.env` (`MINIO_ROOT_USER`) |
| MinIO Console | http://localhost:9001 | ver `.env` (`MINIO_ROOT_USER`) |
| PostgreSQL | localhost:5432 | osint_admin / (ver .env) |

**Containers principais:**
- `osint_postgres` — PostgreSQL 16
- `osint-platform-airflow-webserver-1` — Airflow 2.10.4
- `osint-platform-airflow-scheduler-1`
- `osint_minio` — MinIO object storage

---

## Status de Implementação

### ✅ Infraestrutura Base
- Estrutura de diretórios (`backend`, `frontend`, `pipelines`, `infrastructure`)
- `docker-compose.yml` com PostgreSQL, Neo4j, Redis, MinIO e Airflow
- `.env` com defaults seguros
- Flyway para inicialização e evolução do schema PostgreSQL
- Bind mounts parametrizados para `/srv/osint`, `/mnt/data10tb` e `/mnt/data2tb`
- Healthcheck do `airflow-scheduler` ajustado para `airflow jobs check --job-type SchedulerJob --local`

### ✅ Backend (Django)
- Dockerfile em `/backend`
- Projeto Django com estrutura `config/`
- Banco configurado para PostgreSQL 16
- `apps/` com `sys.path` configurado para imports diretos
- Serviço rodando em http://localhost:8000
- Migrations iniciais aplicadas

### ✅ Frontend (React + Vite)
- Template TailAdmin em `/frontend`
- Dockerfile com Node 20
- React 19 com Vite rodando em http://localhost:3000
- Variável `VITE_API_URL` configurada

### ✅ Airflow & Pipelines
- Webserver, scheduler e triggerer rodando em http://localhost:8080
- DAG de teste de conexão em `pipelines/dags/test_connection.py`
- Logs com ownership compatível com `AIRFLOW_UID=50000`

### ✅ Pipeline CNPJ — Limpeza e Transformação
- `/pipelines/scripts/cnpj/cleaners.py` — 650+ linhas, 15+ funções Python
- `/pipelines/scripts/cnpj/cleaners_sql.py` — equivalentes Pure SQL para DuckDB
- Benchmark: ~4M rows/sec; Pure SQL 10–100x mais rápido que Python UDFs
- Formato Parquet validado como intermediário (70% compressão vs CSV)

### ✅ DAGs Airflow CNPJ

Todas as DAGs ficam em `pipelines/dags/cnpj/` e se encadeiam automaticamente via `TriggerDagRunOperator(wait_for_completion=True)`:

```
cnpj_transform → cnpj_load_postgres → cnpj_matview_refresh → cnpj_load_neo4j
```

**`cnpj_transform`**
- Descompacta ZIPs e transforma CSVs raw em Parquet via DuckDB
- Suporta `force_reprocess` para regeração de Parquets existentes
- Ao terminar, dispara `cnpj_load_postgres` automaticamente

**`cnpj_load_postgres`**
- Carrega Parquet → PostgreSQL via UPSERT (`ON CONFLICT`)
- Cria stubs de empresa para CNPJs órfãos (garante integridade FK)
- `execution_timeout=None` — UPSERT de ~135M rows pode levar várias horas
- Ao terminar, dispara `cnpj_matview_refresh` automaticamente

**`cnpj_matview_refresh`**
- Cria `mv_company_search` se não existe, `REFRESH CONCURRENTLY` se já existe
- 4 índices criados no tablespace `fast_ssd`
- Ao terminar, dispara `cnpj_load_neo4j` automaticamente

**`cnpj_load_neo4j`**
- Carrega Parquet → Neo4j via `MERGE` (idempotente)
- Nós: `Empresa` (66.6M) e `Estabelecimento` (69.1M)
- Estimativa de tempo: ~4–5h para carga completa do mês mais recente

**`cnpj_download`**
- Download dos ZIPs mensais da Receita Federal
- Acionado manualmente quando um novo dump estiver disponível

### ✅ PostgreSQL Performance Layer (MatViews no SSD)
- Tablespace `fast_ssd` → `/var/lib/postgresql/ssd_tablespace`
- `cnpj.mv_company_search` — 27.795.796 registros (estabelecimentos ativos), 6.785 MB
- Índices no SSD:
  - `idx_mv_cnpj14` — UNIQUE, lookup exato por CNPJ 14 dígitos
  - `idx_mv_razao_social_trgm` — GIN trigram, busca por razão social
  - `idx_mv_nome_fantasia_trgm` — GIN trigram, busca por nome fantasia
  - `idx_mv_uf_municipio` — btree, filtro geográfico
- Extensão `pg_trgm` habilitada

### ✅ Validação de Performance
- Busca `ILIKE '%petrobras%'` → **30ms** (`Bitmap Index Scan on idx_mv_razao_social_trgm`)
- `GROUP BY uf` em 27.7M registros → **511ms** (sequential scan — esperado para agregação full)
- SP lidera com 8.437.482 estabelecimentos ativos

### ✅ Dados Históricos CNPJ
- 49+ meses disponíveis em `/mnt/data10tb/osint/pipelines/data/cnpj/raw/` (2021-10 a 2026-02, 174+ GB)
- Dumps CNPJ são **full snapshots** — apenas o mês mais recente (2026-02) é processado
- Parquets transformados em `/mnt/data10tb/osint/pipelines/data/cnpj/processed/2026-02/`

### ✅ Object Storage (MinIO)
- Serviço `osint_minio` rodando em `http://localhost:9000` (API) e `http://localhost:9001` (console)
- Volume persistente em `/mnt/data10tb/osint/minio`
- Bucket inicial validado: `osint-raw`

### ✅ Backend API (Django) — Endpoints CNPJ
- `GET /api/cnpj/search/` — busca paginada de empresas
  - `?q=petrobras` → 806 results (text search c/ accent normalization)
  - `?q=47.960.950/0001-21` → Magazine Luiza (CNPJ parsing com formatação)
  - `?q=petrobras&uf=RJ` → 260 results (combined filters)
  - `?municipio=São Paulo` → filter por município
  - `?cnae=4713004` → 11.508 results (match exato por CNAE fiscal principal)
- `GET /api/cnpj/search/{cnpj_14}/` — detalhe por CNPJ
- `GET /api/cnpj/empresa/{cnpj_basico}/` — detalhe completo de empresa + estabelecimentos
  - Retorna dados da empresa (razão social, capital social, natureza jurídica, porte)
  - Inclui `total_estabelecimentos` e `estabelecimentos_ativos` (situacao_cadastral=2)
  - Array completo de estabelecimentos com endereços e dados de contato
  - Performance: ~355ms para empresa com 654 estabelecimentos (Petrobras)
- Paginação automática via DRF (`next`/`previous` links)
- Accent normalization: "São Paulo" encontra "SAO PAULO" na base

---

## Comandos Operacionais

### Iniciar Ambiente
```bash
cd /home/rfileto/projects/osint-platform
./scripts/start-prod-like.sh --apply-migrations --with-airflow
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml ps
```

### Reset Completo
```bash
./reset-docker-from-scratch.sh
```

### Disparar Pipeline CNPJ Completo
```bash
# Detecta o mês mais recente automaticamente e executa a cadeia completa:
# cnpj_transform → cnpj_load_postgres → cnpj_matview_refresh → cnpj_load_neo4j
./pipelines/scripts/cnpj/run_pipeline.sh

# Especificar mês
./pipelines/scripts/cnpj/run_pipeline.sh 2026-02

# Forçar reprocessamento (mesmo que Parquets já existam)
./pipelines/scripts/cnpj/run_pipeline.sh 2026-02 true
```

### Disparar DAG Individual
```bash
# Apenas transform
docker exec osint-platform-airflow-webserver-1 airflow dags trigger cnpj_transform \
  --conf '{"reference_month": "2026-02"}'

# Apenas load postgres
docker exec osint-platform-airflow-webserver-1 airflow dags trigger cnpj_load_postgres \
  --conf '{"reference_month": "2026-02", "entity_type": "all"}'

# Refresh manual da MatView
docker exec osint-platform-airflow-webserver-1 airflow dags trigger cnpj_matview_refresh
```

### Validar Infraestrutura de Storage
```bash
# Mounts nativos dos HDDs
mount | grep -E 'data10tb|data2tb'

# Saúde dos serviços principais
docker compose ps
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:9000/minio/health/live

# Neo4j
docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password 'RETURN 1 AS ok;'
```

### Verificar Counts Principais
```bash
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "
SELECT 'empresa'           AS tabela, COUNT(*) FROM cnpj.empresa
UNION ALL
SELECT 'estabelecimento',             COUNT(*) FROM cnpj.estabelecimento
UNION ALL
SELECT 'mv_company_search',           COUNT(*) FROM cnpj.mv_company_search;
"
```

### Testar Backend API
```bash
# Busca por texto
curl "http://localhost:8000/api/cnpj/search/?q=petrobras" | python3 -m json.tool

# Busca por CNPJ formatado
curl "http://localhost:8000/api/cnpj/search/?q=47.960.950/0001-21" | python3 -m json.tool

# Busca com filtro de UF
curl "http://localhost:8000/api/cnpj/search/?q=petrobras&uf=RJ" | python3 -m json.tool

# Busca com filtro de CNAE (match exato)
curl "http://localhost:8000/api/cnpj/search/?cnae=4713004" | python3 -m json.tool

# Detalhe de empresa por CNPJ 14 dígitos
curl "http://localhost:8000/api/cnpj/search/47960950000121/" | python3 -m json.tool

# Detalhe completo de empresa + todos estabelecimentos (por CNPJ básico)
curl "http://localhost:8000/api/cnpj/empresa/47960950/" | python3 -m json.tool
curl "http://localhost:8000/api/cnpj/empresa/33000167/" | python3 -m json.tool  # Petrobras
```

### Matar Run Travado no Airflow (emergência)
```bash
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "
  UPDATE dag_run SET state='failed'
    WHERE dag_id IN ('cnpj_transform','cnpj_load_postgres','cnpj_matview_refresh','cnpj_load_neo4j')
    AND state IN ('running','queued');
  UPDATE task_instance SET state='failed'
    WHERE dag_id IN ('cnpj_transform','cnpj_load_postgres')
    AND state IN ('running','queued','up_for_retry');
"
```

---

## Próximos Passos

### Backend API (Django)
- [x] Endpoint de busca em `cnpj.mv_company_search` (razão social, nome fantasia, CNPJ)
  - `GET /api/cnpj/search/?q=<texto|cnpj>` — text/CNPJ search com accent normalization
  - `GET /api/cnpj/search/{cnpj_14}/` — detalhe por CNPJ de 14 dígitos
- [x] Paginação via DRF (next/previous links automáticos)
- [x] Filtros por UF, município e CNAE (`?uf=SP&municipio=São Paulo&cnae=4713004`)
- [x] **Limitação:** Busca apenas empresas ATIVAS (situacao_cadastral=2)
  - MatView atual contém apenas empresas ativas (~28M registros, rápido)
  - Buscar empresas inativas requer MatView adicional (69M estabelecimentos, lento sem índices)
- [x] Endpoint de detalhe de empresa (JOIN empresa + estabelecimentos)
  - `GET /api/cnpj/empresa/{cnpj_basico}/` — retorna empresa + array de estabelecimentos
  - Contadores: total_estabelecimentos, estabelecimentos_ativos
  - ~355ms para empresa com 654 estabelecimentos
- [ ] Autenticação JWT e rate limiting

### Frontend
- [ ] Barra de busca conectada ao endpoint da API
- [ ] Página de resultado com dados da empresa + estabelecimentos
- [ ] Visualizador de grafo (Neo4j)
- [ ] Filtros geográficos e setoriais

### Pipeline — Expansão
- [ ] Carregar dados de sócios (`cnpj_socios`) → Neo4j (relationships `SOCIO_DE`)
- [ ] Relacionamentos Neo4j entre Empresa e Estabelecimento
- [ ] Pipeline de sanções
- [ ] Pipeline de contratos públicos (PNCP)
- [ ] **MatView para empresas inativas** — Para suportar busca de empresas baixadas/suspensas
  - Opção 1: MatView separada `mv_company_inactive` (situacao_cadastral IN (1,3,4,8))
  - Opção 2: Expandir `mv_company_search` para incluir todas as situações (aumenta de 28M para 69M)
  - Requer índices GIN similares ao MatView atual para performance

### Operacional
- [ ] Refresh semanal automático da MatView (`schedule_interval='@weekly'`)
- [ ] Alertas por e-mail em falha de pipeline
- [ ] Backup automatizado do tablespace SSD
- [ ] Buckets definitivos do MinIO (`osint-processed`, `osint-airflow-logs`, `osint-backups`)

---

## Scaling (Referência)

Com 64 GB RAM e dados no HDD, o PostgreSQL usa o page cache do OS para os ~90 GB que não cabem em `shared_buffers`. Performance depende do hot data do workload.

Para operação full in-memory:
- Mínimo: 128 GB RAM (106 GB PostgreSQL + 10 GB Neo4j + 12 GB sistema)
- Recomendado: 192 GB+ em produção

O dataset CNPJ cresce ~2–3 GB/mês. Considerar particionamento por `reference_month` para tabelas > 100 GB.
