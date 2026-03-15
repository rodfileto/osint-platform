#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
BASE_COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
PROD_COMPOSE_FILE="$ROOT_DIR/compose.prod.yml"
APPLY_MIGRATIONS=false
SKIP_MIGRATIONS=false
START_AIRFLOW=false

usage() {
    cat <<'EOF'
Usage: ./scripts/start-prod-like.sh [--apply-migrations | --skip-migrations] [--with-airflow]

Options:
  --apply-migrations  Explicitly run Flyway migrate before starting the app stack.
  --skip-migrations   Start the app stack without applying Flyway migrations.
  --with-airflow      Start the Airflow services after the app stack is up.
  -h, --help          Show this help message.

This script forces an explicit migration decision for the prod-like environment.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply-migrations)
            APPLY_MIGRATIONS=true
            ;;
        --skip-migrations)
            SKIP_MIGRATIONS=true
            ;;
        --with-airflow)
            START_AIRFLOW=true
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

if [[ "$APPLY_MIGRATIONS" == "true" && "$SKIP_MIGRATIONS" == "true" ]]; then
    echo "Choose only one of --apply-migrations or --skip-migrations."
    exit 1
fi

if [[ "$APPLY_MIGRATIONS" != "true" && "$SKIP_MIGRATIONS" != "true" ]]; then
    echo "A prod-like start requires an explicit migration decision."
    usage
    exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing $ENV_FILE"
    echo "Copy $ROOT_DIR/.env.example to $ENV_FILE and adjust values if needed."
    exit 1
fi

compose() {
    docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE_FILE" -f "$PROD_COMPOSE_FILE" "$@"
}

echo "Starting shared infrastructure..."
compose up -d postgres neo4j redis minio

if [[ "$APPLY_MIGRATIONS" == "true" ]]; then
    echo "Running Flyway migrations for prod-like..."
    "$ROOT_DIR/infrastructure/postgres/run-flyway.sh" --env prod-like --yes migrate
else
    echo "Skipping Flyway migrations for prod-like."
fi

echo "Starting prod-like application services..."
compose up -d --build minio-init backend frontend

if [[ "$START_AIRFLOW" == "true" ]]; then
    echo "Initializing Airflow metadata..."
    compose --profile airflow up --build --abort-on-container-exit --exit-code-from airflow-init airflow-init

    echo "Starting Airflow services..."
    compose --profile airflow up -d --build airflow-webserver airflow-scheduler airflow-triggerer
fi

echo
echo "Prod-like stack is up."
echo "Frontend: http://localhost:${FRONTEND_PORT:-3000}"
echo "Backend:  http://localhost:${BACKEND_PORT:-8000}"
if [[ "$START_AIRFLOW" == "true" ]]; then
    echo "Airflow:  http://localhost:${AIRFLOW_PORT:-8080}"
else
    echo "Airflow:  not started (use --with-airflow)"
fi
echo "Neo4j:    http://localhost:${NEO4J_HTTP_PORT:-7474}"
echo "MinIO:    http://localhost:${MINIO_CONSOLE_PORT:-9001}"