#!/bin/bash
#
# Apply Neo4j indexes and constraints from init-cnpj-schema.cypher
# Can be run while data is being loaded — indexes build in background
#

set -e

NEO4J_HOST="${NEO4J_HOST:-localhost}"
NEO4J_PORT="${NEO4J_PORT:-7474}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-osint_graph_password}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"

BASE_URL="http://${NEO4J_HOST}:${NEO4J_PORT}"
TX_URL="${BASE_URL}/db/${NEO4J_DATABASE}/tx/commit"

echo "=== Applying Neo4j Indexes and Constraints ==="
echo "Target: ${BASE_URL}"
echo "Database: ${NEO4J_DATABASE}"
echo ""

# Function to execute a Cypher statement
execute_cypher() {
    local statement="$1"
    local description="$2"
    
    echo -n "  → ${description}... "
    
    # Escape double quotes in the Cypher statement
    escaped_statement=$(echo "$statement" | sed 's/"/\\"/g')
    
    response=$(curl -s -u "${NEO4J_USER}:${NEO4J_PASSWORD}" \
        -H "Content-Type: application/json" \
        -d "{\"statements\":[{\"statement\":\"${escaped_statement}\"}]}" \
        "${TX_URL}")
    
    # Check for errors in response
    errors=$(echo "$response" | grep -o '"errors":\[\]' || echo "has_errors")
    
    if [ "$errors" = '"errors":[]' ]; then
        echo "✓"
    else
        echo "✗"
        echo "$response" | grep -o '"message":"[^"]*"' || echo "Unknown error"
    fi
}

# =====================================================
# CONSTRAINTS (create first — they also create indexes)
# =====================================================

echo "Creating constraints..."

execute_cypher \
    "CREATE CONSTRAINT empresa_cnpj_basico IF NOT EXISTS FOR (e:Empresa) REQUIRE e.cnpj_basico IS UNIQUE" \
    "empresa_cnpj_basico (UNIQUE)"

execute_cypher \
    "CREATE CONSTRAINT pessoa_id IF NOT EXISTS FOR (p:Pessoa) REQUIRE p.pessoa_id IS UNIQUE" \
    "pessoa_id (UNIQUE)"

# =====================================================
# INDEXES — Empresa
# =====================================================

echo ""
echo "Creating Empresa indexes..."

execute_cypher \
    "CREATE INDEX empresa_razao_social IF NOT EXISTS FOR (e:Empresa) ON (e.razao_social)" \
    "empresa_razao_social"

execute_cypher \
    "CREATE INDEX empresa_porte IF NOT EXISTS FOR (e:Empresa) ON (e.porte_empresa)" \
    "empresa_porte"

execute_cypher \
    "CREATE INDEX empresa_natureza IF NOT EXISTS FOR (e:Empresa) ON (e.natureza_juridica)" \
    "empresa_natureza"

execute_cypher \
    "CREATE INDEX empresa_created_at IF NOT EXISTS FOR (e:Empresa) ON (e.created_at)" \
    "empresa_created_at"

execute_cypher \
    "CREATE INDEX empresa_updated_at IF NOT EXISTS FOR (e:Empresa) ON (e.updated_at)" \
    "empresa_updated_at"

execute_cypher \
    "CREATE FULLTEXT INDEX empresa_names IF NOT EXISTS FOR (e:Empresa) ON EACH [e.razao_social]" \
    "empresa_names (FULLTEXT)"

# =====================================================
# INDEXES — Pessoa
# =====================================================

echo ""
echo "Creating Pessoa indexes..."

execute_cypher \
    "CREATE INDEX pessoa_cpf_cnpj IF NOT EXISTS FOR (p:Pessoa) ON (p.cpf_cnpj_socio)" \
    "pessoa_cpf_cnpj"

execute_cypher \
    "CREATE INDEX pessoa_identificador IF NOT EXISTS FOR (p:Pessoa) ON (p.identificador_socio)" \
    "pessoa_identificador"

execute_cypher \
    "CREATE INDEX pessoa_faixa_etaria IF NOT EXISTS FOR (p:Pessoa) ON (p.faixa_etaria)" \
    "pessoa_faixa_etaria"

execute_cypher \
    "CREATE FULLTEXT INDEX pessoa_names IF NOT EXISTS FOR (p:Pessoa) ON EACH [p.nome]" \
    "pessoa_names (FULLTEXT)"

# =====================================================
# INDEXES — Relationships
# =====================================================

echo ""
echo "Creating relationship indexes..."

execute_cypher \
    "CREATE INDEX rel_socio_de_month IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.reference_month)" \
    "rel_socio_de_month"

execute_cypher \
    "CREATE INDEX rel_socio_de_entrada IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.data_entrada_sociedade)" \
    "rel_socio_de_entrada"

execute_cypher \
    "CREATE INDEX rel_socio_de_qualificacao IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.qualificacao_socio)" \
    "rel_socio_de_qualificacao"

# =====================================================
# Verify final state
# =====================================================

echo ""
echo "=== Verification ==="

constraints=$(curl -s -u "${NEO4J_USER}:${NEO4J_PASSWORD}" \
    -H "Content-Type: application/json" \
    -d '{"statements":[{"statement":"SHOW CONSTRAINTS YIELD name RETURN count(name) as count"}]}' \
    "${TX_URL}" | grep -o '"row":\[[0-9]*\]' | grep -o '[0-9]*')

indexes=$(curl -s -u "${NEO4J_USER}:${NEO4J_PASSWORD}" \
    -H "Content-Type: application/json" \
    -d '{"statements":[{"statement":"SHOW INDEXES YIELD name RETURN count(name) as count"}]}' \
    "${TX_URL}" | grep -o '"row":\[[0-9]*\]' | grep -o '[0-9]*')

echo "  Constraints: ${constraints}"
echo "  Indexes: ${indexes}"
echo ""
echo "✓ Done! Indexes will build in background."
echo "  Use 'SHOW INDEXES' to check population progress."
