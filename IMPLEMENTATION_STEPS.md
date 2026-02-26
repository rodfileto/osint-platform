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
| HDD | 9.1 TB — `/media/bigdata` — dados raw e banco PostgreSQL |
| SSD | 384 GB — `/` — MatViews, índices de busca e Neo4j |

### Banco de Dados (estado atual — fev/2026)

**PostgreSQL 16 (`osint_metadata`):** 106 GB total
- `cnpj.empresa`: 83 GB — 66.936.364 registros
- `cnpj.estabelecimento`: 24 GB — 69.446.909 registros
- `cnpj.mv_company_search`: 6.785 MB no SSD — 27.795.796 registros (ativos)
- `cnpj.download_manifest`: ~1 MB

**Neo4j:** 5.2 GB (localizado no SSD)
- `Empresa`: 66.682.246 nós
- `Estabelecimento`: 69.177.350 nós

**Cobertura de dados:** 49+ meses de CNPJ (2021-10 a 2026-02)

### Tuning Aplicado

**PostgreSQL** (otimizado para 64 GB RAM + HDD):
```
shared_buffers           = 16GB   (25% da RAM)
effective_cache_size     = 48GB   (75% da RAM)
work_mem                 = 64MB
maintenance_work_mem     = 2GB
random_page_cost         = 3.0    (HDD)
effective_io_concurrency = 2      (HDD)
max_parallel_workers_per_gather = 4
```

**Tablespace `fast_ssd`** (para MatViews no SSD):
```
random_page_cost = 1.1
seq_page_cost    = 1.0
location         = /var/lib/postgresql/ssd_tablespace
host path        = /home/rfileto/osint_pg_ssd/
```

**Neo4j** (no SSD):
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
| Frontend (Next.js) | http://localhost:3000 | — |
| Backend API (Django) | http://localhost:8000 | — |
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Neo4j Browser | http://localhost:7474 | neo4j / osint_password |
| PostgreSQL | localhost:5432 | osint_admin / (ver .env) |

**Containers principais:**
- `osint_postgres` — PostgreSQL 16
- `osint-platform-airflow-webserver-1` — Airflow 2.10.4
- `osint-platform-airflow-scheduler-1`

---

## Status de Implementação

### ✅ Infraestrutura Base
- Estrutura de diretórios (`backend`, `frontend`, `pipelines`, `infrastructure`)
- `docker-compose.yml` com PostgreSQL, Neo4j, Redis e Airflow
- `.env` com defaults seguros
- Script `init-db.sh` para inicialização do PostgreSQL

### ✅ Backend (Django)
- Dockerfile em `/backend`
- Projeto Django com estrutura `config/`
- Banco configurado para PostgreSQL 16
- `apps/` com `sys.path` configurado para imports diretos
- Serviço rodando em http://localhost:8000
- Migrations iniciais aplicadas

### ✅ Frontend (Next.js)
- Template TailAdmin em `/frontend`
- Dockerfile com Node 20
- Next.js com Turbopack rodando em http://localhost:3000
- Variável `NEXT_PUBLIC_API_URL` configurada

### ✅ Airflow & Pipelines
- Webserver, scheduler e triggerer rodando em http://localhost:8080
- DAG de teste de conexão em `pipelines/dags/test_connection.py`

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
- 49+ meses disponíveis em `data/cnpj/raw/` (2021-10 a 2026-02, 174+ GB)
- Dumps CNPJ são **full snapshots** — apenas o mês mais recente (2026-02) é processado
- Parquets transformados em `data/cnpj/processed/2026-02/`

---

## Comandos Operacionais

### Iniciar Ambiente
```bash
cd /media/bigdata/osint-platform
docker-compose up -d postgres neo4j redis airflow-webserver airflow-scheduler
docker-compose ps
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
- [ ] Endpoint de busca em `cnpj.mv_company_search` (razão social, nome fantasia, CNPJ)
- [ ] Paginação e filtros por UF / município / CNAE / situação cadastral
- [ ] Endpoint de detalhe de empresa (JOIN empresa + estabelecimentos)
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

### Operacional
- [ ] Refresh semanal automático da MatView (`schedule_interval='@weekly'`)
- [ ] Alertas por e-mail em falha de pipeline
- [ ] Backup automatizado do tablespace SSD

---

## Scaling (Referência)

Com 64 GB RAM e dados no HDD, o PostgreSQL usa o page cache do OS para os ~90 GB que não cabem em `shared_buffers`. Performance depende do hot data do workload.

Para operação full in-memory:
- Mínimo: 128 GB RAM (106 GB PostgreSQL + 10 GB Neo4j + 12 GB sistema)
- Recomendado: 192 GB+ em produção

O dataset CNPJ cresce ~2–3 GB/mês. Considerar particionamento por `reference_month` para tabelas > 100 GB.
