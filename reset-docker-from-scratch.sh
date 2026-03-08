#!/bin/bash
# Script unificado para reset completo do ambiente Docker OSINT Platform
# Garante estado limpo para testes do workflow global

set -e  # Exit on error

echo "🔥 RESET COMPLETO DO AMBIENTE DOCKER"
echo "====================================="
echo ""
echo "Este script irá:"
echo "  1. Parar e remover todos os containers"
echo "  2. Limpar todos os volumes de dados"
echo "  3. Recriar containers do zero"
echo "  4. Inicializar schemas e tabelas"
echo ""
read -p "Continuar? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operação cancelada."
    exit 1
fi

# ==============================================
# STEP 1: Cleanup completo
# ==============================================
echo ""
echo "📦 STEP 1: Limpando ambiente..."
echo "----------------------------------------------"

# Kill tmux sessions relacionadas
echo "  → Encerrando sessões tmux..."
tmux kill-session -t drop_postgres 2>/dev/null || true
tmux kill-session -t drop_neo4j 2>/dev/null || true

# Para todos os containers do compose
echo "  → Parando containers..."
docker compose down --remove-orphans 2>/dev/null || true

# Force remove se ainda existirem
echo "  → Removendo containers remanescentes..."
docker rm -f osint_postgres osint_neo4j osint-platform-airflow-webserver-1 osint-platform-airflow-scheduler-1 2>/dev/null || true

# Limpa volumes de dados
echo "  → Limpando volumes de dados..."
sudo rm -rf infrastructure/postgres/data/*
sudo rm -rf infrastructure/neo4j/data/*
sudo rm -rf /home/rfileto/osint_neo4j/data/* 2>/dev/null || true

echo "✓ Cleanup completo"

# ==============================================
# STEP 2: Criar containers
# ==============================================
echo ""
echo "🐳 STEP 2: Criando containers..."
echo "----------------------------------------------"

docker compose up -d postgres neo4j

echo "✓ Containers criados"

# ==============================================
# STEP 3: Aguardar inicialização
# ==============================================
echo ""
echo "⏳ STEP 3: Aguardando inicialização dos bancos..."
echo "----------------------------------------------"

# Aguarda PostgreSQL ficar pronto
echo "  → Aguardando PostgreSQL..."
RETRIES=30
until docker exec osint_postgres pg_isready -U osint_admin > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo -n "."
  sleep 2
  RETRIES=$((RETRIES-1))
done
echo ""

if [ $RETRIES -eq 0 ]; then
  echo "❌ ERRO: PostgreSQL não inicializou a tempo"
  exit 1
fi

echo "✓ PostgreSQL pronto"

# Aguarda Neo4j ficar pronto
echo "  → Aguardando Neo4j..."
RETRIES=30
until docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password "RETURN 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo -n "."
  sleep 2
  RETRIES=$((RETRIES-1))
done
echo ""

if [ $RETRIES -eq 0 ]; then
  echo "❌ ERRO: Neo4j não inicializou a tempo"
  exit 1
fi

echo "✓ Neo4j pronto"

# ==============================================
# STEP 4: Aplicar migrations
# ==============================================
echo ""
echo "🗃️  STEP 4: Aplicando migrations do PostgreSQL..."
echo "----------------------------------------------"

echo "  → Executando Flyway migrate..."
bash infrastructure/postgres/run-flyway.sh migrate

echo "✓ Migrations PostgreSQL aplicadas"

# Neo4j - Schema e constraints
echo "  → Criando schema Neo4j..."
cat infrastructure/neo4j/init-cnpj-schema.cypher | docker exec -i osint_neo4j cypher-shell -u neo4j -p osint_graph_password 2>&1 | grep -E "(Added|Created|ERROR)" || true

echo "✓ Schema Neo4j criado"

# ==============================================
# STEP 5: Verificação
# ==============================================
echo ""
echo "✅ STEP 5: Verificando instalação..."
echo "----------------------------------------------"

# Verifica tabelas PostgreSQL
echo "  → Verificando tabelas PostgreSQL..."
TABLES=$(docker exec osint_postgres psql -U osint_admin -d osint_metadata -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'cnpj';")
echo "    Tabelas CNPJ criadas: $TABLES"

# Verifica PRIMARY KEYs
echo "  → Verificando PRIMARY KEYs..."
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
" | grep -E "(empresa|estabelecimento|rows)" || echo "    ⚠️  Nenhuma PRIMARY KEY encontrada"

# Verifica Neo4j
echo "  → Verificando Neo4j..."
NEO4J_CHECK=$(docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password "RETURN 'OK' as status" 2>&1 | grep -c "OK" || echo "0")
if [ "$NEO4J_CHECK" -gt 0 ]; then
    echo "    Neo4j: ✓ Conectado"
else
    echo "    Neo4j: ⚠️  Problema de conexão"
fi

# ==============================================
# STEP 6: Iniciar Airflow (opcional)
# ==============================================
echo ""
echo "🚁 STEP 6: Airflow..."
echo "----------------------------------------------"
read -p "Iniciar Airflow também? (y/N) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "  → Iniciando Airflow..."
    docker compose up -d airflow-webserver airflow-scheduler
    echo "✓ Airflow iniciado"
    echo "    Acesse: http://localhost:8080"
else
    echo "  → Pulando Airflow (pode iniciar depois com: docker-compose up -d)"
fi

# ==============================================
# Conclusão
# ==============================================
echo ""
echo "🎉 RESET COMPLETO!"
echo "====================================="
echo ""
echo "Ambiente pronto para testes. Próximos passos:"
echo ""
echo "1. Verificar containers:"
echo "   docker compose ps"
echo ""
echo "2. Testar conexão PostgreSQL:"
echo "   docker exec -it osint_postgres psql -U osint_admin -d osint_metadata -c '\dt cnpj.*'"
echo ""
echo "3. Testar conexão Neo4j:"
echo "   http://localhost:7474 (neo4j / osint_graph_password)"
echo ""
echo "4. Iniciar processamento CNPJ:"
echo "   docker exec -it osint-platform-airflow-webserver-1 airflow dags test cnpj_batch_controller"
echo ""
