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

echo "Loading PostgreSQL sample..."
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 < "$DEV_DIR/data/postgres/seed_sample.sql"

echo "Validating PostgreSQL sample..."
sample_counts=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SELECT
    (SELECT COUNT(*) FROM cnpj.empresa),
    (SELECT COUNT(*) FROM cnpj.estabelecimento),
    (SELECT COUNT(*) FROM cnpj.company_search);")
IFS='|' read -r empresa_count estabelecimento_count search_count <<< "$sample_counts"

if [[ "$empresa_count" -eq 0 || "$estabelecimento_count" -eq 0 || "$search_count" -eq 0 ]]; then
    echo "Bootstrap validation failed: PostgreSQL sample or company search is empty."
    echo "Counts => empresas: $empresa_count, estabelecimentos: $estabelecimento_count, company_search: $search_count"
    exit 1
fi

echo "PostgreSQL sample loaded successfully."
echo "Counts => empresas: $empresa_count, estabelecimentos: $estabelecimento_count, company_search: $search_count"

echo "Loading full FINEP dataset..."
compose run --rm \
    -e DEV_BOOTSTRAP_FINEP_OUTPUT_DIR=/bootstrap-data/finep/raw \
    dev-bootstrap python /workspace/pipelines/scripts/finep/bootstrap_full_load.py --output-dir /bootstrap-data/finep/raw

echo "Bootstrap complete: sample CNPJ + full FINEP."
