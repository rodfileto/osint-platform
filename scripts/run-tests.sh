#!/usr/bin/env bash
# Run the test suites the way CI does (.github/workflows/tests.yml).
#
#   scripts/run-tests.sh            pipelines, backend and gold publish tests
#   scripts/run-tests.sh pipelines  only the pipelines tests (no database)
#
# The database suites run against a throwaway PostGIS container on a free local
# port, with the Flyway migrations and the dev seed; it is removed on exit.
# Needs Docker and uv (which provides the CI Python versions).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mode="${1:-all}"
if [[ "$mode" != "all" && "$mode" != "pipelines" ]]; then
  echo "Usage: $0 [pipelines]"
  exit 1
fi

for cmd in uv docker; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd not found"
    exit 1
  fi
done

PIPELINES_DEPS=(--python 3.12 --with-requirements pipelines/dbt/requirements.txt --with openpyxl
  --with psycopg2-binary --with requests --with tqdm --with pytest)

echo "[1/4] Pipelines tests"
uv run "${PIPELINES_DEPS[@]}" pytest tests/pipelines -p no:cacheprovider

if [[ "$mode" == "pipelines" ]]; then
  exit 0
fi

container="osint-test-pg-$$"
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

echo "[2/4] Starting PostgreSQL on 127.0.0.1:$port (migrations + dev seed)"
docker run -d --rm --name "$container" -p "127.0.0.1:$port:5432" \
  -e POSTGRES_USER=osint_admin -e POSTGRES_PASSWORD=ci_password -e POSTGRES_DB=osint_test \
  postgis/postgis:16-3.4 >/dev/null
# The image restarts the server once after its init scripts; wait for the final one.
until docker exec "$container" pg_isready -U osint_admin -q 2>/dev/null; do sleep 1; done
sleep 3
until docker exec "$container" pg_isready -U osint_admin -q 2>/dev/null; do sleep 1; done

docker run --rm --network host -v "$ROOT_DIR/infrastructure/postgres/migrations:/flyway/sql:ro" \
  -e FLYWAY_URL="jdbc:postgresql://127.0.0.1:$port/osint_test" \
  -e FLYWAY_USER=osint_admin -e FLYWAY_PASSWORD=ci_password \
  -e FLYWAY_DEFAULT_SCHEMA=public -e FLYWAY_TABLE=flyway_schema_history \
  -e FLYWAY_SCHEMAS=cnpj,finep,geo,airflow,naturalization,sanctions,contracts,inpi \
  -e FLYWAY_LOCATIONS=filesystem:/flyway/sql/cnpj,filesystem:/flyway/sql/finep,filesystem:/flyway/sql/geo,filesystem:/flyway/sql/inpi \
  -e FLYWAY_VALIDATE_MIGRATION_NAMING=true \
  flyway/flyway:11-alpine migrate | grep -E "Successfully applied|ERROR"

docker exec -i "$container" psql -U osint_admin -d osint_test -v ON_ERROR_STOP=1 -q \
  < dev/data/postgres/seed_sample.sql

export POSTGRES_HOST=127.0.0.1 POSTGRES_PORT="$port" POSTGRES_DB=osint_test
export POSTGRES_USER=osint_admin POSTGRES_PASSWORD=ci_password KEYCLOAK_AUTH_ENABLED=false

# Backend first: the publish test replaces the seed tables with its fixture data.
echo "[3/4] Backend tests"
uv run --python 3.11 --with-requirements backend/requirements.txt --with pytest \
  pytest tests/backend -p no:cacheprovider

echo "[4/4] Gold build + publish test"
uv run "${PIPELINES_DEPS[@]}" pytest tests/pipelines/test_gold_publish.py -p no:cacheprovider

echo "All suites passed."
