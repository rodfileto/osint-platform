#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"

"$ROOT_DIR/infrastructure/postgres/run-flyway.sh" --env prod-like --yes migrate
docker compose --env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker-compose.yml" -f "$ROOT_DIR/compose.prod.yml" run --rm geo-bootstrap
