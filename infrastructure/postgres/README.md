# PostgreSQL Setup

## ⚡ Reset Completo (RECOMENDADO)

Para testar o workflow global do zero, use o script automatizado:

```bash
# Na raiz do projeto
chmod +x reset-docker-from-scratch.sh
./reset-docker-from-scratch.sh
```

Este script:
- ✅ Para e remove todos os containers
- ✅ Limpa todos os volumes de dados
- ✅ Recria containers do zero
- ✅ Aguarda inicialização correta (com retry logic)
- ✅ Aplica migrations Flyway automaticamente
- ✅ Verifica instalação (PKs, constraints, conexões)
- ✅ Opcionalmente inicia Airflow

## Versionamento com Flyway

O Flyway é a fonte única de verdade para schema PostgreSQL neste projeto.

As migrations versionadas ficam em:

- `infrastructure/postgres/migrations/cnpj/`
- `infrastructure/postgres/migrations/finep/`
- `infrastructure/postgres/migrations/geo/`

Comandos principais:

```bash
./dev/scripts/run-flyway.sh info
./dev/scripts/run-flyway.sh validate
./dev/scripts/run-flyway.sh migrate
./infrastructure/postgres/run-flyway.sh --env prod-like info
./infrastructure/postgres/run-flyway.sh --env prod-like validate
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
sh ./infrastructure/postgres/run-geo-bootstrap.sh
```

Regras práticas:

- Banco vazio: `flyway migrate`
- Banco existente: `flyway migrate`
- Não edite migrations já aplicadas; crie uma nova migration
- Use versões globais únicas, por exemplo `V2026.03.07.001__cnpj_add_new_index.sql`
- O schema `geo` segue o mesmo fluxo de Flyway e deve ser tratado como fonte canônica para geografia brasileira compartilhada

---

## 📋 Setup Manual (Passo a Passo)

### 1. Iniciar PostgreSQL
```bash
docker compose up -d postgres
```

### 2. Aplicar migrations
```bash
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

### 3. Verificar instalação
```bash
# Verificar tabelas criadas
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "\dt cnpj.*"

# Verificar PRIMARY KEYs
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "
SELECT 
    tc.table_name, 
    tc.constraint_name, 
    STRING_AGG(kcu.column_name, ', ') AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'cnpj' 
    AND tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_name IN ('empresa', 'estabelecimento')
GROUP BY tc.table_name, tc.constraint_name
ORDER BY tc.table_name;
"
```

### 4. Carregar geografia canônica brasileira

Quando quiser popular ou atualizar o schema `geo` fora do ambiente dev, execute o loader compartilhado:

```bash
sh ./infrastructure/postgres/run-geo-bootstrap.sh
```

Esse runner garante que PostgreSQL e Flyway estejam prontos e depois executa [pipelines/scripts/geo/load_br_geo_reference.py](pipelines/scripts/geo/load_br_geo_reference.py) no stack raiz.

**Saída esperada:**
```
 table_name      | constraint_name       | columns
-----------------+-----------------------+----------------------------------
 empresa         | empresa_pkey          | cnpj_basico
 estabelecimento | estabelecimento_pkey  | cnpj_basico, cnpj_ordem, cnpj_dv
```

---

## 🔧 Troubleshooting

### Problema: migrations falharam

```bash
./infrastructure/postgres/run-flyway.sh --env prod-like info
./infrastructure/postgres/run-flyway.sh --env prod-like validate
docker logs osint_flyway --tail 100
```

### Problema: Tabelas sem PRIMARY KEY

Se as tabelas foram criadas sem PRIMARY KEY, recrie-as:

```bash
# Drop tabelas existentes
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<EOF
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;
DROP TABLE IF EXISTS cnpj.download_manifest CASCADE;
EOF

# Recria com migrations
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

### Problema: Container não inicia

```bash
# Verifica logs
docker logs osint_postgres --tail 50

# Verifica se a porta está em uso
sudo lsof -i :5432

# Se necessário, mata processos na porta
sudo pkill -9 postgres
```

### Reset Rápido (Apenas PostgreSQL)

```bash
# Para apenas o PostgreSQL
docker compose stop postgres
docker rm -f osint_postgres

# Limpa dados
sudo rm -rf infrastructure/postgres/data/*

# Reinicia
docker compose up -d postgres

# Aguarda ficar pronto
until docker exec osint_postgres pg_isready -U osint_admin; do
  sleep 2
done

# Recria via Flyway
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

---

## 📊 Estrutura das Tabelas

### cnpj.empresa
- **PRIMARY KEY**: `cnpj_basico`
- Contém dados básicos da empresa (razão social, capital social, etc.)
- Campos principais: `razao_social`, `capital_social`, `porte_empresa`, `natureza_juridica`

### cnpj.estabelecimento  
- **PRIMARY KEY**: `(cnpj_basico, cnpj_ordem, cnpj_dv)`
- **FOREIGN KEY**: `cnpj_basico` → `cnpj.empresa(cnpj_basico) ON DELETE CASCADE`
- Contém dados dos estabelecimentos (filiais, endereços, CNAEs, etc.)
- Campos principais: `nome_fantasia`, `situacao_cadastral`, `municipio`, `uf`, `cnae_fiscal_principal`

### cnpj.download_manifest
- **PRIMARY KEY**: `id` (SERIAL)
- **UNIQUE**: `(reference_month, file_name)`
- Rastreia status completo do pipeline de downloads e processamento
- Campos de tracking: `processing_status`, `extracted_at`, `transformed_at`, `loaded_postgres_at`, `loaded_neo4j_at`
- Métricas: `rows_extracted`, `rows_transformed`, `rows_loaded_postgres`, `processing_duration_seconds`

## 🕐 Colunas Timestamp

Todas as tabelas principais têm timestamps automáticos:
- `created_at`: Data de criação do registro (DEFAULT CURRENT_TIMESTAMP)
- `updated_at`: Data da última atualização (atualizada automaticamente em UPSERTs via trigger)

## 📈 Views Disponíveis

| View | Descrição |
|------|-----------|
| `cnpj.estabelecimento_completo` | JOIN completo de estabelecimento com empresa, inclui CNPJ 14 dígitos |
| `cnpj.download_progress` | Resumo de progresso de processamento por mês e tipo de arquivo |
| `cnpj.incomplete_months` | Meses com processamento incompleto (< 37 arquivos ou com falhas) |
| `cnpj.files_ready_to_ingest` | Arquivos baixados prontos para próxima etapa do pipeline |

### Exemplos de Queries

```sql
-- Ver progresso de um mês específico
SELECT * FROM cnpj.download_progress WHERE reference_month = '2024-02';

-- Ver meses incompletos
SELECT * FROM cnpj.incomplete_months;

-- Ver arquivos prontos para processar
SELECT * FROM cnpj.files_ready_to_ingest LIMIT 10;

-- Buscar empresa por razão social
SELECT * FROM cnpj.empresa WHERE razao_social ILIKE '%petrobras%' LIMIT 10;

-- Buscar estabelecimentos por município
SELECT * FROM cnpj.estabelecimento WHERE municipio = 'SAO PAULO' AND situacao_cadastral = 2 LIMIT 10;
```

### Geo: queries de validação

```sql
-- Contagem das tabelas canônicas
SELECT COUNT(*) AS total_states FROM geo.br_state;
SELECT COUNT(*) AS total_municipalities FROM geo.br_municipality;

-- Cobertura do mapa CNPJ -> município brasileiro canônico
SELECT
  COUNT(*) AS mapped_rows
FROM geo.cnpj_br_municipality_map;

SELECT
  issue_type,
  COUNT(*) AS total
FROM geo.cnpj_br_municipality_map_issue
GROUP BY issue_type
ORDER BY total DESC;

-- Conferir o join pronto para consumidores do backend
SELECT
  cnpj_municipality_code,
  cnpj_municipality_name,
  municipality_ibge_code,
  municipality_name,
  state_abbrev
FROM geo.v_cnpj_br_municipality
ORDER BY cnpj_municipality_code
LIMIT 20;
```

---

## 🔍 Comandos Úteis

```bash
# Conectar ao PostgreSQL
docker exec -it osint_postgres psql -U osint_admin -d osint_metadata

# Listar todas as tabelas do schema cnpj
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "\dt cnpj.*"

# Ver tamanho das tabelas
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables 
WHERE schemaname = 'cnpj'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# Ver índices criados
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "\di cnpj.*"

# Count de registros
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c "
SELECT 'empresa' as tabela, COUNT(*) FROM cnpj.empresa
UNION ALL
SELECT 'estabelecimento', COUNT(*) FROM cnpj.estabelecimento
UNION ALL
SELECT 'download_manifest', COUNT(*) FROM cnpj.download_manifest;
"
```

---

## 🚀 Implementação: Tablespace SSD + Materialized Views

### Contexto e Motivação

O banco de dados CNPJ (106 GB) reside no HDD (`/media/bigdata`). Para acelerar as queries do frontend, o objetivo é armazenar **Materialized Views e seus índices no SSD**, utilizando a feature de **Tablespaces** do PostgreSQL.

**Hardware:**
- HDD (`/media/bigdata`): 9.1 TB — dados brutos (empresa, estabelecimento)
- SSD (`/`): 384 GB, ~98 GB livres — MatViews e índices de busca

**Estimativa do tablespace SSD:** 10–20 GB para uma MatView de busca com índices GIN/trigram.

---

### ✅ Fase 1 — Preparar Diretório no SSD (CONCLUÍDA)

Diretório `/home/rfileto/osint_pg_ssd` criado no SSD com ownership correto (UID 999 — usuário `postgres` do container). Volume mapeado no `docker-compose.yml`.

**Resultado:** Diretório montado em `/var/lib/postgresql/ssd_tablespace` dentro do container com owner `postgres`.

**Verificar:**
```bash
# Confirmar que o volume está montado no container
docker exec osint_postgres ls -la /var/lib/postgresql/ssd_tablespace

# Saída esperada: diretório vazio com owner postgres
```

---

### ✅ Fase 2 — Criar o Tablespace no PostgreSQL (CONCLUÍDA)

Conectar ao banco e registrar o tablespace apontando para o diretório no SSD:

```sql
-- Criar tablespace
CREATE TABLESPACE fast_ssd
    OWNER osint_admin
    LOCATION '/var/lib/postgresql/ssd_tablespace';

-- Ajustar custo de I/O para SSD (query planner usará índices agressivamente)
ALTER TABLESPACE fast_ssd
    SET (random_page_cost = 1.1, seq_page_cost = 1.0);
```

**Verificar:**
```sql
-- Confirmar tablespace criado
SELECT spcname, pg_tablespace_location(oid) AS location
FROM pg_tablespace
WHERE spcname = 'fast_ssd';
```

**Executar via terminal:**
```bash
docker exec -it osint_postgres psql -U osint_admin -d osint_metadata
```

**Resultado (24/02/2026):** Tablespace `fast_ssd` criado e configurado com `random_page_cost = 1.1` e `seq_page_cost = 1.0`. Verificado via `pg_tablespace`:
```
 spcname  |              location
----------+------------------------------------
 fast_ssd | /var/lib/postgresql/ssd_tablespace
(1 row)
```

---

### ✅ Fase 3 — Carregar a Tabela estabelecimento (CONCLUÍDA)

Antes de criar a MatView, a tabela `cnpj.estabelecimento` precisa estar populada.

Disparar o DAG via Airflow com os parâmetros:

```json
{
  "reference_month": "2026-02",
  "entity_type": "estabelecimentos",
  "force_reprocess": false
}
```

**Ou via CLI do Airflow:**
```bash
docker exec osint-platform-airflow-webserver-1 airflow dags trigger cnpj_load_postgres \
  --conf '{"reference_month": "2026-02", "entity_type": "estabelecimentos"}'
```

**Acompanhar carga:**
```bash
# Monitorar registros sendo inseridos
watch -n 10 'docker exec osint_postgres psql -U osint_admin -d osint_metadata -c \
  "SELECT COUNT(*) FROM cnpj.estabelecimento;"'
```

**Resultado (24/02/2026):** DAG `cnpj_load_postgres` concluído com sucesso. 6.689 stubs de empresa criados para CNPJs órfãos antes da carga.
```
 tabela          | registros
-----------------+------------
 empresa         | 66.675.557
 estabelecimento | 69.177.350
```
Duração total: ~3h (10.813s) — 6.397 rows/seg.

---

### ✅ Fase 4 — Criar Materialized View + Índices no SSD (CONCLUÍDA)

> **Pré-requisito:** Fases 2 e 3 concluídas.

#### ✅ 4.1 — Habilitar extensão pg_trgm (CONCLUÍDA)

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

#### ✅ 4.2 — Criar a Materialized View no tablespace SSD (CONCLUÍDA)

```sql
CREATE MATERIALIZED VIEW cnpj.mv_company_search
TABLESPACE fast_ssd
AS
SELECT
    emp.cnpj_basico,
    emp.razao_social,
    est.nome_fantasia,
    est.cnpj_ordem,
    est.cnpj_dv,
    -- CNPJ completo 14 dígitos
    (emp.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj_14,
    est.situacao_cadastral,
    est.municipio,
    est.uf,
    est.cnae_fiscal_principal,
    emp.porte_empresa,
    emp.natureza_juridica,
    emp.capital_social
FROM cnpj.empresa emp
JOIN cnpj.estabelecimento est
    ON emp.cnpj_basico = est.cnpj_basico
WHERE est.situacao_cadastral = 2  -- apenas estabelecimentos ATIVOS
WITH DATA;
```

> ⚠️ Este comando pode levar **20–40 minutos** dependendo do volume de dados.

**Resultado (24/02/2026):** MatView criada com **27.795.796 registros** (estabelecimentos ativos) via DAG `cnpj_matview_refresh`.

#### ✅ 4.3 — Criar Índices no SSD (CONCLUÍDA)

```sql
-- Lookup exato por CNPJ 14 dígitos
CREATE UNIQUE INDEX idx_mv_cnpj14
    ON cnpj.mv_company_search(cnpj_14)
    TABLESPACE fast_ssd;

-- Busca textual por razão social (frontend search bar)
CREATE INDEX idx_mv_razao_social_trgm
    ON cnpj.mv_company_search
    USING gin (razao_social gin_trgm_ops)
    TABLESPACE fast_ssd;

-- Busca por nome fantasia
CREATE INDEX idx_mv_nome_fantasia_trgm
    ON cnpj.mv_company_search
    USING gin (nome_fantasia gin_trgm_ops)
    TABLESPACE fast_ssd;

-- Filtro por UF + município
CREATE INDEX idx_mv_uf_municipio
    ON cnpj.mv_company_search(uf, municipio)
    TABLESPACE fast_ssd;
```

#### ✅ 4.4 — Verificar tamanho e localização no SSD (CONCLUÍDA)

**Resultado (24/02/2026):**
```
      relname      | total_size
-------------------+------------
 mv_company_search | 6785 MB

         indexname         | tablespace
---------------------------+------------
 idx_mv_cnpj14             | fast_ssd
 idx_mv_razao_social_trgm  | fast_ssd
 idx_mv_nome_fantasia_trgm | fast_ssd
 idx_mv_uf_municipio       | fast_ssd
```

```sql
-- Tamanho da MatView e índices
SELECT
    relname,
    pg_size_pretty(pg_total_relation_size('cnpj.' || relname)) AS total_size,
    spcname AS tablespace
FROM pg_class c
LEFT JOIN pg_tablespace t ON c.reltablespace = t.oid
WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cnpj')
  AND relname LIKE 'mv_%' OR relname LIKE 'idx_mv_%'
ORDER BY pg_total_relation_size('cnpj.' || relname) DESC;

-- Confirmar no SSD fisicamente
```

```bash
du -sh /home/rfileto/osint_pg_ssd/
```

---

### ✅ Fase 5 — Configurar Refresh Automático via Airflow (CONCLUÍDA)

DAG dedicada `cnpj_matview_refresh` criada em `pipelines/dags/cnpj_matview_dag.py`.

- Fluxo: `start → ensure_pg_trgm → ensure_mv_exists → ensure_indexes → analyze_mv → end`
- `ensure_mv_exists`: **cria** a MatView se não existir, ou faz **REFRESH CONCURRENTLY** se já existir
- `ensure_indexes`: idempotente (`IF NOT EXISTS`) — todos os 4 índices no `fast_ssd`
- DAG `cnpj_load_postgres` encadeia automaticamente via `TriggerDagRunOperator` ao final de cada carga

Refresh manual:

```python
# Task de refresh no final do pipeline
from airflow.operators.postgres_operator import PostgresOperator

refresh_matview = PostgresOperator(
    task_id='refresh_mv_company_search',
    postgres_conn_id='osint_postgres',
    sql="REFRESH MATERIALIZED VIEW CONCURRENTLY cnpj.mv_company_search;",
    execution_timeout=timedelta(hours=2),
)
```

---

### 📋 Checklist de Validação Final

```bash
# 1. Tablespace no SSD
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c \
  "SELECT spcname, pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = 'fast_ssd';"

# 2. MatView populada
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c \
  "SELECT COUNT(*) FROM cnpj.mv_company_search;"

# 3. Índices criados no SSD
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c \
  "SELECT indexname, tablespace FROM pg_indexes WHERE tablename = 'mv_company_search';"

# 4. Teste de performance (deve usar índice GIN — Bitmap Index Scan)
docker exec osint_postgres psql -U osint_admin -d osint_metadata -c \
  "EXPLAIN ANALYZE SELECT * FROM cnpj.mv_company_search WHERE razao_social ILIKE '%petrobras%' LIMIT 10;"

# 5. Tamanho físico no SSD
du -sh /home/rfileto/osint_pg_ssd/
```
