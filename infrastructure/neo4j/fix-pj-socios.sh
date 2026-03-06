#!/bin/bash
# ============================================================================
# Script de Correção: Migrar Pessoa nodes PJ para Empresa→Empresa relationships
# ============================================================================
#
# Este script corrige o problema onde sócios Pessoa Jurídica (identificador = 1)
# foram incorretamente criados como nós :Pessoa em vez de relacionamentos
# diretos (:Empresa)-[:SOCIO_DE]->(:Empresa).
#
# Uso:
#   ./infrastructure/neo4j/fix-pj-socios.sh
#
# ============================================================================

set -e  # Exit on error

CONTAINER_NAME="osint_neo4j"
NEO4J_USER="neo4j"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-osint_graph_password}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CYPHER_SCRIPT="$SCRIPT_DIR/fix-pj-pessoa-nodes.cypher"

echo "=========================================="
echo "Correção de Sócios PJ no Neo4j"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Erro: Container Neo4j '$CONTAINER_NAME' não está rodando."
    echo "   Execute: docker start $CONTAINER_NAME"
    exit 1
fi

echo "✓ Container Neo4j encontrado: $CONTAINER_NAME"
echo ""

# Check if APOC plugin is available
echo "Verificando plugin APOC..."
if ! docker exec "$CONTAINER_NAME" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    "RETURN apoc.version() AS version" 2>/dev/null | grep -q "version"; then
    echo "⚠️  APOC não encontrado. Instalando..."
    docker exec "$CONTAINER_NAME" sh -c 'echo "dbms.security.procedures.unrestricted=apoc.*" >> /var/lib/neo4j/conf/neo4j.conf'
    echo "   É necessário reiniciar o Neo4j para ativar APOC."
    echo "   Execute: docker restart $CONTAINER_NAME"
    echo "   Depois execute este script novamente."
    exit 1
fi
echo "✓ APOC plugin disponível"
echo ""

# Show current state
echo "Estado atual do banco:"
docker exec "$CONTAINER_NAME" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    "MATCH (p:Pessoa) WHERE p.identificador_socio = 1 RETURN COUNT(p) AS pj_pessoa_nodes"
echo ""

# Confirm before proceeding
read -p "Deseja prosseguir com a migração? (s/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Ss]$ ]]; then
    echo "Operação cancelada."
    exit 0
fi

echo ""
echo "Iniciando migração..."
echo "=========================================="

# Copy script to container
docker cp "$CYPHER_SCRIPT" "$CONTAINER_NAME:/tmp/fix-pj-pessoa-nodes.cypher"

# Execute the migration script
docker exec "$CONTAINER_NAME" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    -f /tmp/fix-pj-pessoa-nodes.cypher

echo ""
echo "=========================================="
echo "✓ Migração concluída!"
echo ""
echo "Verificando resultado final..."
docker exec "$CONTAINER_NAME" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    "MATCH (p:Pessoa) WHERE p.identificador_socio = 1 RETURN COUNT(p) AS remaining_pj_nodes"
echo ""

# Cleanup
docker exec "$CONTAINER_NAME" rm -f /tmp/fix-pj-pessoa-nodes.cypher

echo "=========================================="
echo "Script finalizado com sucesso!"
echo "=========================================="
