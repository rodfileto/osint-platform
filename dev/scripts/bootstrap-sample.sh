#!/bin/bash

set -euo pipefail

DEV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$DEV_DIR/.." && pwd)"
ENV_FILE="$DEV_DIR/.env"
COMPOSE_FILE="$DEV_DIR/docker-compose.yml"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing $ENV_FILE"
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

compose() {
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

echo "Applying Neo4j schema..."
compose exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" < "$ROOT_DIR/infrastructure/neo4j/init-cnpj-schema.cypher"

echo "Loading PostgreSQL sample..."
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 < "$DEV_DIR/data/postgres/seed_sample.sql"

echo "Loading Neo4j sample..."
compose exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" < "$DEV_DIR/data/neo4j/seed_sample.cypher"

echo "Loading full FINEP dataset..."
compose run --rm airflow-init python /opt/airflow/scripts/finep/bootstrap_full_load.py

echo "Bootstrap complete: sample CNPJ + sample Neo4j + full FINEP."
