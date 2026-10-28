#!/usr/bin/env bash
# Run the test suites the way CI does (.github/workflows/tests.yml).
#
#   scripts/run-tests.sh            pipelines, backend, gold publish and OpenAlex lake tests
#   scripts/run-tests.sh pipelines  only the pipelines tests (no database)
#
# The database suites run against a throwaway PostGIS container (Flyway migrations and the dev
# seed) and a throwaway MinIO, each on a free local port; both are removed on exit.
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
  --with psycopg2-binary --with requests --with tqdm --with boto3 --with pytest)

echo "[1/4] Pipelines tests"
uv run "${PIPELINES_DEPS[@]}" pytest tests/pipelines -p no:cacheprovider

if [[ "$mode" == "pipelines" ]]; then
  exit 0
fi

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}
container="osint-test-pg-$$"
minio="osint-test-minio-$$"
port="$(free_port)"
minio_port="$(free_port)"
trap 'docker rm -f "$container" "$minio" >/dev/null 2>&1 || true' EXIT

echo "[2/4] Starting PostgreSQL on 127.0.0.1:$port (migrations + dev seed) and MinIO"
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
  -e FLYWAY_SCHEMAS=cnpj,finep,geo,airflow,naturalization,sanctions,contracts,inpi,openalex \
  -e FLYWAY_LOCATIONS=filesystem:/flyway/sql/cnpj,filesystem:/flyway/sql/finep,filesystem:/flyway/sql/geo,filesystem:/flyway/sql/inpi,filesystem:/flyway/sql/openalex \
  -e FLYWAY_VALIDATE_MIGRATION_NAMING=true \
  flyway/flyway:11-alpine migrate | grep -E "Successfully applied|ERROR"

docker exec -i "$container" psql -U osint_admin -d osint_test -v ON_ERROR_STOP=1 -q \
  < dev/data/postgres/seed_sample.sql

export POSTGRES_HOST=127.0.0.1 POSTGRES_PORT="$port" POSTGRES_DB=osint_test
export POSTGRES_USER=osint_admin POSTGRES_PASSWORD=ci_password KEYCLOAK_AUTH_ENABLED=false

docker run -d --rm --name "$minio" -p "127.0.0.1:$minio_port:9000" \
  -e MINIO_ROOT_USER=test_minio -e MINIO_ROOT_PASSWORD=test_minio_password \
  minio/minio:latest server /data >/dev/null
until curl -sf "http://127.0.0.1:$minio_port/minio/health/live" >/dev/null; do sleep 1; done
export MINIO_ENDPOINT_URL="http://127.0.0.1:$minio_port" MINIO_ROOT_USER=test_minio
export MINIO_ROOT_PASSWORD=test_minio_password

# Backend first: the publish test replaces the seed tables with its fixture data.
echo "[3/4] Backend tests"
uv run --python 3.11 --with-requirements backend/requirements.txt --with pytest \
  pytest tests/backend -p no:cacheprovider

echo "[4/4] Gold build + publish and OpenAlex lake tests"
uv run "${PIPELINES_DEPS[@]}" pytest tests/pipelines/test_gold_publish.py tests/pipelines/test_openalex_lake.py \
  -p no:cacheprovider

echo "All suites passed."
