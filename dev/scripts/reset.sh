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

compose() {
    docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" "$@"
}

wipe_dir() {
    local dir_path="$1"

    mkdir -p "$dir_path"
    docker run --rm -v "$dir_path:/target" alpine:3.20 sh -c 'rm -rf /target/* /target/.[!.]* /target/..?* 2>/dev/null || true'
}

echo "Stopping dev stack..."
compose down --remove-orphans

echo "Removing bind-mounted state..."
wipe_dir "$DEV_DIR/volumes/postgres/data"
wipe_dir "$DEV_DIR/volumes/neo4j/data"
wipe_dir "$DEV_DIR/volumes/minio/data"
wipe_dir "$DEV_DIR/volumes/airflow/data"
wipe_dir "$DEV_DIR/volumes/airflow/temp"
wipe_dir "$DEV_DIR/volumes/redis/data"

echo "Dev state cleared. Run dev/scripts/start.sh to recreate the environment."
