# PostgreSQL Setup - CNPJ Schema

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
- ✅ Cria schemas e tabelas automaticamente
- ✅ Verifica instalação (PKs, constraints, conexões)
- ✅ Opcionalmente inicia Airflow

---

## 📋 Setup Manual (Passo a Passo)

### 1. Iniciar PostgreSQL
```bash
docker-compose up -d postgres
```

### 2. Aguardar inicialização
```bash
# Aguarda até PostgreSQL estar ready
until docker exec osint_postgres pg_isready -U osint_admin; do
  echo "Aguardando PostgreSQL..."
  sleep 2
done
```

### 3. Criar Schemas Base

**Importante:** O arquivo `init-db.sh` só executa automaticamente na primeira criação do container (quando o volume está vazio). Para execução manual:

```bash
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS airflow;
    CREATE SCHEMA IF NOT EXISTS cnpj;
    CREATE SCHEMA IF NOT EXISTS naturalization;
    CREATE SCHEMA IF NOT EXISTS sanctions;
    CREATE SCHEMA IF NOT EXISTS contracts;
    
    GRANT ALL PRIVILEGES ON SCHEMA cnpj TO osint_admin;
    GRANT ALL PRIVILEGES ON ALL OTHER SCHEMAS TO osint_admin;
EOSQL
```

### 4. Criar Tabelas CNPJ com PRIMARY KEYs
```bash
cat infrastructure/postgres/init-cnpj-schema.sql | \
  docker exec -i osint_postgres psql -U osint_admin -d osint_metadata
```

### 5. Verificar Instalação
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

**Saída esperada:**
```
 table_name      | constraint_name       | columns
-----------------+-----------------------+----------------------------------
 empresa         | empresa_pkey          | cnpj_basico
 estabelecimento | estabelecimento_pkey  | cnpj_basico, cnpj_ordem, cnpj_dv
```

---

## 🔧 Troubleshooting

### Problema: init-db.sh não executa

**Causa:** Scripts em `/docker-entrypoint-initdb.d/` só rodam quando o diretório de dados PostgreSQL está vazio.

**Solução:** Execute manualmente ou use o script de reset completo.

### Problema: Tabelas sem PRIMARY KEY

Se as tabelas foram criadas sem PRIMARY KEY, recrie-as:

```bash
# Drop tabelas existentes
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<EOF
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;
DROP TABLE IF EXISTS cnpj.download_manifest CASCADE;
EOF

# Recria com constraints corretas
cat infrastructure/postgres/init-cnpj-schema.sql | \
  docker exec -i osint_postgres psql -U osint_admin -d osint_metadata
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
docker-compose stop postgres
docker rm -f osint_postgres

# Limpa dados
sudo rm -rf infrastructure/postgres/data/*

# Reinicia
docker-compose up -d postgres

# Aguarda ficar pronto
until docker exec osint_postgres pg_isready -U osint_admin; do
  sleep 2
done

# Recria schemas e tabelas
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS cnpj;
    GRANT ALL PRIVILEGES ON SCHEMA cnpj TO osint_admin;
EOSQL

cat infrastructure/postgres/init-cnpj-schema.sql | \
  docker exec -i osint_postgres psql -U osint_admin -d osint_metadata
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
