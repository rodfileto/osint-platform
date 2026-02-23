#!/bin/bash
#  Força reset completo das bases de dados

echo "🔥 RESET FORÇADO DOS BANCOS DE DADOS"
echo "===================================="
echo ""

# Kill tmux sessions
echo "1. Matando sessões tmux..."
tmux kill-session -t drop_postgres 2>/dev/null
tmux kill-session -t drop_neo4j 2>/dev/null
echo "✓ Sessões tmux encerradas"
echo ""

# Force kill containers
echo "2. Forçando parada de containers..."
docker kill osint_postgres osint_neo4j 2>/dev/null
docker rm osint_postgres osint_neo4j 2>/dev/null
echo "✓ Containers removidos"
echo ""

# Remove data directories
echo "3. Removendo dados dos volumes..."
sudo rm -rf infrastructure/postgres/data/*
sudo rm -rf infrastructure/neo4j/data/*
echo "✓ Volumes limpos"
echo ""

# Recreate containers
echo "4. Recriando containers..."
docker-compose up -d postgres neo4j
echo "✓ Containers criados"
echo ""

# Wait for databases
echo "5. Aguardando bancos ficarem prontos..."
sleep 15
echo "✓ Bancos inicializados"
echo ""

# Create schemas
echo "6. Criando schema PostgreSQL..."
cat infrastructure/postgres/init-cnpj-schema.sql | docker exec -i osint_postgres psql -U osint_admin osint_metadata
echo "✓ Schema PostgreSQL criado"
echo ""

echo "7. Criando schema Neo4j..."
sleep 5
cat infrastructure/neo4j/init-cnpj-schema.cypher | docker exec -i osint_neo4j cypher-shell -u neo4j -p osint_graph_password
echo "✓ Schema Neo4j criado"
echo ""

echo "🎉 Reset completo! Bancos prontos para uso."
echo ""
echo "Teste com:"
echo "  docker exec -it osint-platform-airflow-webserver-1 airflow dags test cnpj_load_postgres -c '{\"reference_month\": \"2024-02\", \"entity_type\": \"empresas\"}'"
