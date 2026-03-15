#!/bin/bash

set -euo pipefail

DEV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$DEV_DIR/.." && pwd)"
ENV_FILE="$DEV_DIR/.env"
BASE_COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
DEV_COMPOSE_FILE="$ROOT_DIR/compose.dev.yml"
START_AIRFLOW=false

usage() {
    cat <<'EOF'
Usage: ./dev/scripts/start.sh [--with-airflow]

Options:
  --with-airflow  Start the Airflow webserver, scheduler, and triggerer.
  -h, --help      Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
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

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing $ENV_FILE"
    echo "Copy $DEV_DIR/.env.example to $ENV_FILE and adjust values if needed."
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

compose() {
    docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" "$@"
}

ensure_host_dirs() {
    mkdir -p \
        "$DEV_DIR/volumes/postgres/data" \
        "$DEV_DIR/volumes/neo4j/data" \
        "$DEV_DIR/volumes/minio/data" \
        "$DEV_DIR/volumes/airflow/data" \
        "$DEV_DIR/volumes/airflow/temp" \
        "$DEV_DIR/volumes/redis/data"
}

wait_for_postgres() {
    echo "Waiting for PostgreSQL..."
    for _ in $(seq 1 60); do
        if compose exec -T postgres pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done

    echo "PostgreSQL did not become ready in time."
    return 1
}

wait_for_neo4j() {
    echo "Waiting for Neo4j..."
    for _ in $(seq 1 60); do
        if compose exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" 'RETURN 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done

    echo "Neo4j did not become ready in time."
    return 1
}

ensure_host_dirs

echo "Starting infrastructure services..."
compose up -d postgres neo4j redis minio

wait_for_postgres
wait_for_neo4j

echo "Running Flyway migrations..."
compose up --build --abort-on-container-exit --exit-code-from flyway flyway

echo "Bootstrapping dev sample data..."
compose up --build --abort-on-container-exit --exit-code-from dev-bootstrap dev-bootstrap

echo "Starting application services..."
compose up -d --build backend frontend

if [[ "$START_AIRFLOW" == "true" ]]; then
    echo "Initializing Airflow metadata..."
    compose --profile airflow up --build --abort-on-container-exit --exit-code-from airflow-init airflow-init

    echo "Starting Airflow services..."
    compose --profile airflow up -d --build airflow-webserver airflow-scheduler airflow-triggerer
fi

echo
echo "Dev stack is up."
echo "Frontend: http://localhost:${FRONTEND_PORT}"
echo "Backend:  http://localhost:${BACKEND_PORT}"
if [[ "$START_AIRFLOW" == "true" ]]; then
    echo "Airflow:  http://localhost:${AIRFLOW_PORT}"
else
    echo "Airflow:  not started (use --with-airflow)"
fi
echo "Neo4j:    http://localhost:${NEO4J_HTTP_PORT}"
echo "MinIO:    http://localhost:${MINIO_CONSOLE_PORT}"
