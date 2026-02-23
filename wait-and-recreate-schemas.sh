#!/bin/bash
# Aguarda o drop do PostgreSQL completar e recria os schemas

echo "Aguardando drop do PostgreSQL completar..."
echo "Você pode acompanhar com: tmux attach -t drop_postgres"
echo ""

while tmux has-session -t drop_postgres 2>/dev/null; do
    echo -n "."
    sleep 10
done

echo ""
echo "✓ Drop do PostgreSQL completo!"
echo ""
echo "Recriando schemas..."
echo ""

# PostgreSQL
echo "📊 Criando schema PostgreSQL..."
cat infrastructure/postgres/init-cnpj-schema.sql | docker exec -i osint_postgres psql -U osint_admin osint_metadata
echo "✓ Schema PostgreSQL criado"
echo ""

# Neo4j
echo "🔗 Criando schema Neo4j..."
cat infrastructure/neo4j/init-cnpj-schema.cypher | docker exec -i osint_neo4j cypher-shell -u neo4j -p osint_graph_password
echo "✓ Schema Neo4j criado"
echo ""

echo "🎉 Bancos de dados prontos para uso!"
echo ""
echo "Teste com:"
echo "docker exec -it osint-platform-airflow-webserver-1 airflow dags test cnpj_load_postgres -c '{\"reference_month\": \"2024-02\", \"entity_type\": \"empresas\"}'"
