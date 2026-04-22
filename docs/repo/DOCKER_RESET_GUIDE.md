# Guia de Reset do Docker - OSINT Platform

> Atenção: este guia contém contexto histórico do fluxo antigo baseado em scripts de init do PostgreSQL.
> O fluxo atual usa compose em camadas e Flyway com runners por ambiente.
> Use `./dev/scripts/run-flyway.sh migrate` para dev, `./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate` para prod-like,
> e consulte `infrastructure/postgres/README.md` e `README.md` como referências principais.

## 🎯 Problema Identificado

Você estava enfrentando dificuldades para reiniciar os containers Docker do zero devido a:

1. **Scripts de init só executam uma vez**: Os scripts em `/docker-entrypoint-initdb.d/` do PostgreSQL só rodam quando o volume de dados está vazio (primeira criação)

2. **Múltiplos scripts inconsistentes**: Existiam vários scripts (`force-reset-databases.sh`, `wait-and-recreate-schemas.sh`, etc.) com abordagens diferentes

3. **Dependência de timing**: Sleeps fixos sem verificação se o banco realmente está pronto

4. **README desatualizado**: Instruções não correspondiam aos scripts reais

## ✅ Solução Implementada

Foi criado um script unificado e robusto: **`reset-docker-from-scratch.sh`**

### Recursos do Novo Script

- ✅ **Cleanup completo**: Remove containers, volumes e dados
- ✅ **Retry logic inteligente**: Aguarda bancos ficarem prontos de verdade (até 60s)
- ✅ **Criação automática**: Schemas + tabelas + constraints + indexes
- ✅ **Verificação pós-instalação**: Confirma PKs, FKs e conexões
- ✅ **Interativo**: Pergunta confirmação e opção de iniciar Airflow
- ✅ **Error handling**: Exit on error, mensagens claras

---

## 🚀 Como Usar

### Reset Completo (Recomendado)

```bash
# Na raiz do projeto
./reset-docker-from-scratch.sh
```

O script irá:
1. Solicitar confirmação
2. Limpar tudo (containers + volumes)
3. Recriar do zero
4. Aguardar inicialização (com retry)
5. Criar schemas e tabelas
6. Verificar instalação
7. Opcionalmente iniciar Airflow

**Tempo estimado**: 2-3 minutos

---

## 📋 Alternativas Rápidas

### Reset Apenas PostgreSQL

```bash
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml stop postgres
docker rm -f osint_postgres
sudo rm -rf /srv/osint/postgres/data/*
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml up -d postgres

# Aguarda ficar pronto
until docker exec osint_postgres pg_isready -U osint_admin; do sleep 2; done

# Recria estrutura
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS cnpj;
    GRANT ALL PRIVILEGES ON SCHEMA cnpj TO osint_admin;
EOSQL

./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

### Reset Apenas Neo4j

```bash
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml stop neo4j
docker rm -f osint_neo4j
sudo rm -rf /srv/osint/neo4j/data/*
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml up -d neo4j

# Aguarda ficar pronto
until docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password "RETURN 1" 2>/dev/null; do sleep 2; done

# Cria schema
cat infrastructure/neo4j/init-cnpj-schema.cypher | \
    docker exec -i osint_neo4j cypher-shell -u neo4j -p osint_graph_password
```

### Drop e Recriar Apenas Tabelas CNPJ (Sem Reset de Container)

```bash
# Drop tabelas
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<EOF
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;
DROP TABLE IF EXISTS cnpj.download_manifest CASCADE;
EOF

# Recria
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

---

## 🔍 Verificações

### PostgreSQL

```bash
# Conectar
docker exec -it osint_postgres psql -U osint_admin -d osint_metadata

# Dentro do psql:
\dt cnpj.*;              -- Lista tabelas
\d+ cnpj.empresa;        -- Descreve tabela empresa
\d+ cnpj.estabelecimento; -- Descreve tabela estabelecimento

# Verifica PRIMARY KEYs
SELECT 
    tc.table_name, 
    tc.constraint_name, 
    STRING_AGG(kcu.column_name, ', ') AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'cnpj' 
    AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.table_name, tc.constraint_name
ORDER BY tc.table_name;
```

**Saída esperada:**
```
 table_name      | constraint_name       | columns
-----------------+-----------------------+----------------------------------
 empresa         | empresa_pkey          | cnpj_basico
 estabelecimento | estabelecimento_pkey  | cnpj_basico, cnpj_ordem, cnpj_dv
```

### Neo4j

```bash
# Via cypher-shell
docker exec -it osint_neo4j cypher-shell -u neo4j -p osint_graph_password

# Via browser
open http://localhost:7474
# User: neo4j
# Pass: osint_graph_password
```

### Containers

```bash
# Listar containers rodando
docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml ps

# Ver logs
docker logs osint_postgres --tail 50
docker logs osint_neo4j --tail 50
docker logs osint-platform-airflow-webserver-1 --tail 50
```

---

## 🐛 Troubleshooting

### Erro: "PostgreSQL não inicializou a tempo"

```bash
# Verifica logs
docker logs osint_postgres --tail 100

# Possíveis causas:
# 1. Porta 5432 em uso
sudo lsof -i :5432
sudo pkill -9 postgres  # Se necessário

# 2. Permissões do volume
sudo chown -R 999:999 infrastructure/postgres/data/

# 3. Memória insuficiente
docker stats osint_postgres
```

### Erro: "Neo4j não inicializou a tempo"

```bash
# Verifica logs
docker logs osint_neo4j --tail 100

# Possíveis causas:
# 1. Porta 7687 ou 7474 em uso
sudo lsof -i :7687
sudo lsof -i :7474

# 2. Permissões do volume SSD
sudo chown -R 7474:7474 /home/rfileto/osint_neo4j/data/

# 3. Heap memory insuficiente (ajustar em docker-compose.yml)
```

### Erro: "Tabelas criadas mas sem PRIMARY KEY"

Isso acontece se você rodou um script antigo ou criou tabelas manualmente sem constraints.

**Solução:**
```bash
# Drop e recria com constraints
docker exec osint_postgres psql -U osint_admin -d osint_metadata <<EOF
DROP TABLE IF EXISTS cnpj.estabelecimento CASCADE;
DROP TABLE IF EXISTS cnpj.empresa CASCADE;
EOF

./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

### Container não para

```bash
# Force kill
docker kill osint_postgres osint_neo4j
docker rm -f osint_postgres osint_neo4j

# Se ainda persistir
docker ps -a | grep osint
docker rm -f $(docker ps -aq)  # Remove TODOS containers (cuidado!)
```

---

## 📁 Arquivos Relacionados

| Arquivo | Descrição |
|---------|-----------|
| `reset-docker-from-scratch.sh` | **Script principal** - Reset completo automatizado |
| `infrastructure/postgres/README.md` | Documentação detalhada PostgreSQL |
| `infrastructure/postgres/run-flyway.sh` | Executa Flyway no ambiente escolhido, com confirmação explícita para mutações em `prod-like` |
| `dev/scripts/run-flyway.sh` | Runner Flyway do ambiente dev |
| `infrastructure/postgres/migrations/` | Fonte de verdade do schema PostgreSQL |
| `infrastructure/neo4j/init-cnpj-schema.cypher` | Cria constraints e indexes Neo4j |
| `docker-compose.yml` | Configuração de todos os serviços |
| `force-reset-databases.sh` | ⚠️ Script antigo - use o novo |
| `wait-and-recreate-schemas.sh` | ⚠️ Script antigo - use o novo |

---

## 🎓 Conceitos Importantes

### Por que migramos para Flyway?

O fluxo antigo baseado em scripts de inicialização do Docker só funcionava automaticamente quando o diretório de dados estava vazio. Flyway elimina essa diferença: o mesmo comando aplica schema em banco vazio e em banco existente.

### Qual a ordem correta de inicialização?

1. **Migrations versionadas** (`infrastructure/postgres/migrations/`): Criam e evoluem a estrutura de dados
2. **Flyway** (`./dev/scripts/run-flyway.sh migrate` ou `./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate`): Aplica o que falta no banco
3. **Dados** (DAGs do Airflow): Popula com dados reais

### Por que múltiplos scripts?

- **Flyway**: Fonte única de verdade para CNPJ, FINEP e schemas auxiliares
- **Migrations**: Arquivos versionados e incrementais, sem bootstrap paralelo
- Modularidade permite adicionar novos módulos sem quebrar existentes

---

## ✅ Checklist de Validação Pós-Reset

Use este checklist para garantir que tudo está funcionando:

- [ ] Containers rodando: `docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml ps`
- [ ] PostgreSQL aceita conexões: `docker exec osint_postgres pg_isready -U osint_admin`
- [ ] Schemas criados: `\dn` no psql deve mostrar `cnpj`, `airflow`, etc.
- [ ] Tabelas criadas: `\dt cnpj.*` deve mostrar `empresa`, `estabelecimento`, `download_manifest`
- [ ] PRIMARY KEYs: Query de verificação retorna 2 PKs (empresa + estabelecimento)
- [ ] FOREIGN KEYs: `\d+ cnpj.estabelecimento` mostra FK para empresa
- [ ] Indexes: `\di cnpj.*` mostra múltiplos indexes
- [ ] Views: `\dv cnpj.*` mostra 4 views
- [ ] Neo4j conecta: `docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password "RETURN 1"`
- [ ] Airflow UI: http://localhost:8080 (se iniciado)

---

## 🆘 Suporte Adicional

Se após seguir este guia ainda tiver problemas:

1. **Capture logs completos:**
   ```bash
    docker compose --env-file .env -f docker-compose.yml -f compose.prod.yml logs > docker-logs.txt
   docker ps -a >> docker-logs.txt
   ```

2. **Verifique recursos do sistema:**
   ```bash
   free -h
   df -h
   docker stats --no-stream
   ```

3. **Revise configurações:**
   - Arquivo `.env` está correto?
   - Permissões de diretórios estão corretas? (`/media/bigdata/osint-platform` acessível?)
   - Ports 5432, 7474, 7687, 8080 disponíveis?

4. **Consulte documentação específica:**
    - PostgreSQL: [infrastructure/postgres/README.md](../../infrastructure/postgres/README.md)
    - DAGs: [pipelines/dags/README_CNPJ_DAG.md](../../pipelines/dags/README_CNPJ_DAG.md)
    - Arquitetura: [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md)
