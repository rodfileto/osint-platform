#!/bin/bash

set -euo pipefail

DEV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$DEV_DIR/.." && pwd)"
ENV_FILE="$DEV_DIR/.env"
BASE_COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
DEV_COMPOSE_FILE="$ROOT_DIR/compose.dev.yml"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing $ENV_FILE"
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

compose() {
    docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" "$@"
}

echo "Applying Neo4j schema..."
compose exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" < "$ROOT_DIR/infrastructure/neo4j/init-cnpj-schema.cypher"

echo "Loading PostgreSQL sample..."
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 < "$DEV_DIR/data/postgres/seed_sample.sql"

echo "Validating PostgreSQL sample and materialized views..."
sample_counts=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SELECT
    (SELECT COUNT(*) FROM cnpj.empresa),
    (SELECT COUNT(*) FROM cnpj.estabelecimento),
    (SELECT COUNT(*) FROM cnpj.mv_company_search),
    (SELECT COUNT(*) FROM cnpj.mv_company_search_inactive);")
IFS='|' read -r empresa_count estabelecimento_count active_mv_count inactive_mv_count <<< "$sample_counts"

if [[ "$empresa_count" -eq 0 || "$estabelecimento_count" -eq 0 || "$active_mv_count" -eq 0 ]]; then
    echo "Bootstrap validation failed: PostgreSQL sample or active materialized view is empty."
    echo "Counts => empresas: $empresa_count, estabelecimentos: $estabelecimento_count, mv_company_search: $active_mv_count, mv_company_search_inactive: $inactive_mv_count"
    exit 1
fi

echo "PostgreSQL sample loaded successfully."
echo "Counts => empresas: $empresa_count, estabelecimentos: $estabelecimento_count, mv_company_search: $active_mv_count, mv_company_search_inactive: $inactive_mv_count"

echo "Loading Neo4j sample..."
compose exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" < "$DEV_DIR/data/neo4j/seed_sample.cypher"

echo "Loading full FINEP dataset..."
compose run --rm \
    -e DEV_BOOTSTRAP_FINEP_OUTPUT_DIR=/bootstrap-data/finep/raw \
    dev-bootstrap python /workspace/pipelines/scripts/finep/bootstrap_full_load.py --output-dir /bootstrap-data/finep/raw

echo "Bootstrap complete: sample CNPJ + sample Neo4j + full FINEP."
