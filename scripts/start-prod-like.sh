#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
BASE_COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
PROD_COMPOSE_FILE="$ROOT_DIR/compose.prod.yml"
TUNNEL_COMPOSE_FILE="$ROOT_DIR/compose.tunnel.yml"
APPLY_MIGRATIONS=false
SKIP_MIGRATIONS=false
START_AIRFLOW=false
START_CLOUDFLARED=false

env_value() {
    local key="$1"
    local fallback="${2:-}"
    local value="${!key:-}"

    if [[ -n "$value" ]]; then
        printf '%s\n' "$value"
        return 0
    fi

    if [[ -f "$ENV_FILE" ]]; then
        value="$(sed -n "s/^${key}=//p" "$ENV_FILE" | head -n1)"
        if [[ -n "$value" ]]; then
            printf '%s\n' "$value"
            return 0
        fi
    fi

    printf '%s\n' "$fallback"
}

set_env_file_value() {
    local key="$1"
    local value="$2"

    if grep -q "^${key}=" "$ENV_FILE"; then
        python3 - <<'PY' "$ENV_FILE" "$key" "$value"
from pathlib import Path
import sys

env_path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = env_path.read_text(encoding="utf-8").splitlines()
updated = []
for line in lines:
    if line.startswith(f"{key}="):
        updated.append(f"{key}={value}")
    else:
        updated.append(line)
env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY
    else
        printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}

wait_for_public_url() {
    local label="$1"
    local url="$2"
    local timeout_seconds="$3"
    local start_time
    local http_code
    start_time=$(date +%s)

    if [[ -z "$url" ]]; then
        return 0
    fi

    while true; do
        http_code="$(curl -k -L -sS -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null || true)"
        case "$http_code" in
            200|204|301|302|303|307|308|401|403)
                echo "$label is reachable at $url (HTTP $http_code)."
                return 0
                ;;
        esac

        if (( $(date +%s) - start_time >= timeout_seconds )); then
            echo "Timed out waiting for $label at $url. Last HTTP code: ${http_code:-none}."
            echo "If you are using a token-managed Cloudflare tunnel, confirm the public hostname exists in the Cloudflare Zero Trust tunnel config."
            return 1
        fi

        sleep 2
    done
}

wait_for_cloudflared_hostname() {
    local expected_hostname="$1"
    local timeout_seconds="$2"
    local start_time
    local logs
    start_time=$(date +%s)

    if [[ -z "$expected_hostname" ]]; then
        return 0
    fi

    while true; do
        logs="$(compose logs --tail=200 cloudflared 2>/dev/null || true)"
        if grep -Fq "\\\"hostname\\\":\\\"${expected_hostname}\\\"" <<< "$logs"; then
            echo "cloudflared is advertising hostname ${expected_hostname}."
            return 0
        fi

        if grep -Fq "\\\"hostname\\\":\\\"${expected_hostname}." <<< "$logs"; then
            echo "cloudflared advertised a malformed hostname based on ${expected_hostname}."
            echo "$logs" | tail -n 40
            return 1
        fi

        if (( $(date +%s) - start_time >= timeout_seconds )); then
            echo "Timed out waiting for cloudflared to advertise hostname ${expected_hostname}."
            echo "$logs" | tail -n 40
            return 1
        fi

        sleep 2
    done
}

usage() {
    cat <<'EOF'
Usage: ./scripts/start-prod-like.sh [--apply-migrations | --skip-migrations] [--with-airflow] [--with-cloudflared]

Options:
  --apply-migrations  Explicitly run Flyway migrate before starting the app stack.
  --skip-migrations   Start the app stack without applying Flyway migrations.
  --with-airflow      Start the Airflow services after the app stack is up.
    --with-cloudflared  Start the Cloudflare tunnel overlay after the app stack is healthy.
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
        --with-cloudflared)
            START_CLOUDFLARED=true
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

if [[ "$START_CLOUDFLARED" == "true" && ! -f "$TUNNEL_COMPOSE_FILE" ]]; then
    echo "Missing $TUNNEL_COMPOSE_FILE"
    exit 1
fi

if [[ "$START_CLOUDFLARED" == "true" && -z "${CLOUDFLARE_TUNNEL_TOKEN:-}" ]]; then
    if ! grep -q '^CLOUDFLARE_TUNNEL_TOKEN=' "$ENV_FILE"; then
        echo "Cloudflare tunnel requested, but CLOUDFLARE_TUNNEL_TOKEN is not set in $ENV_FILE"
        exit 1
    fi
fi

compose() {
    local args=(--env-file "$ENV_FILE" -f "$BASE_COMPOSE_FILE" -f "$PROD_COMPOSE_FILE")
    if [[ "$START_CLOUDFLARED" == "true" ]]; then
        args+=( -f "$TUNNEL_COMPOSE_FILE" )
    fi
    docker compose "${args[@]}" "$@"
}

service_container_id() {
    compose ps -q "$1"
}

print_service_logs() {
    local service="$1"
    if [[ -n "$(service_container_id "$service")" ]]; then
        echo
        echo "Recent logs for $service:"
        compose logs --tail=40 "$service" || true
    fi
}

wait_for_service_state() {
    local service="$1"
    local expected_state="$2"
    local timeout_seconds="$3"
    local start_time
    local container_id
    local current_state
    start_time=$(date +%s)

    while true; do
        container_id="$(service_container_id "$service")"
        if [[ -n "$container_id" ]]; then
            current_state="$(docker inspect --format '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
            if [[ "$current_state" == "$expected_state" ]]; then
                echo "$service is $expected_state."
                return 0
            fi
        fi

        if (( $(date +%s) - start_time >= timeout_seconds )); then
            echo "Timed out waiting for $service to reach state '$expected_state'."
            print_service_logs "$service"
            return 1
        fi

        sleep 2
    done
}

wait_for_service_health() {
    local service="$1"
    local timeout_seconds="$2"
    local start_time
    local container_id
    local health_status
    start_time=$(date +%s)

    while true; do
        container_id="$(service_container_id "$service")"
        if [[ -n "$container_id" ]]; then
            health_status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$container_id" 2>/dev/null || true)"
            if [[ "$health_status" == "healthy" ]]; then
                echo "$service is healthy."
                return 0
            fi
            if [[ "$health_status" == "unhealthy" ]]; then
                echo "$service became unhealthy."
                print_service_logs "$service"
                return 1
            fi
        fi

        if (( $(date +%s) - start_time >= timeout_seconds )); then
            echo "Timed out waiting for $service to become healthy."
            print_service_logs "$service"
            return 1
        fi

        sleep 2
    done
}

echo "Starting shared infrastructure..."
compose up -d postgres neo4j redis minio keycloak

echo "Waiting for shared infrastructure readiness..."
wait_for_service_health postgres 180
wait_for_service_state neo4j running 120
wait_for_service_health redis 120
wait_for_service_state minio running 120
wait_for_service_health keycloak 180

if [[ "$APPLY_MIGRATIONS" == "true" ]]; then
    echo "Running Flyway migrations for prod-like..."
    "$ROOT_DIR/infrastructure/postgres/run-flyway.sh" --env prod-like --yes migrate
else
    echo "Skipping Flyway migrations for prod-like."
fi

local_keycloak_url="http://localhost:$(env_value KEYCLOAK_PORT 8081)"
if [[ "$START_CLOUDFLARED" == "true" ]]; then
    auth_domain="$(env_value CF_AUTH_DOMAIN)"
    auth_public_url_default=""
    if [[ -n "$auth_domain" ]]; then
        auth_public_url_default="https://${auth_domain}"
    fi

    set_env_file_value KEYCLOAK_PUBLIC_URL "${auth_public_url_default:-$(env_value KEYCLOAK_PUBLIC_URL "$local_keycloak_url")}" 
    set_env_file_value VITE_KEYCLOAK_URL "${auth_public_url_default:-$(env_value VITE_KEYCLOAK_URL "$local_keycloak_url")}" 
else
    set_env_file_value KEYCLOAK_PUBLIC_URL "$local_keycloak_url"
    set_env_file_value VITE_KEYCLOAK_URL "$local_keycloak_url"
fi

echo "Starting prod-like application services..."
compose up -d --build minio-init backend frontend

echo "Waiting for application services readiness..."
wait_for_service_health backend 240
wait_for_service_health frontend 240

if [[ "$START_AIRFLOW" == "true" ]]; then
    echo "Initializing Airflow metadata..."
    compose --profile airflow up --build --abort-on-container-exit --exit-code-from airflow-init airflow-init

    echo "Starting Airflow services..."
    compose --profile airflow up -d --build airflow-webserver airflow-scheduler airflow-triggerer

    echo "Waiting for Airflow readiness..."
    wait_for_service_health airflow-webserver 240
    wait_for_service_health airflow-scheduler 240
    wait_for_service_health airflow-triggerer 240
fi

if [[ "$START_CLOUDFLARED" == "true" ]]; then
    echo "Starting Cloudflare tunnel overlay..."
    compose up -d cloudflared
    wait_for_service_state cloudflared running 120

    auth_public_base_url="$(env_value KEYCLOAK_PUBLIC_URL)"
    auth_public_url="${auth_public_base_url%/}/realms/$(env_value KEYCLOAK_REALM osint-platform)"
    frontend_public_url="https://$(env_value CF_APP_DOMAIN)"
    backend_public_url="https://$(env_value CF_API_DOMAIN)/health"

    echo "Validating Cloudflare public hostnames..."
    wait_for_cloudflared_hostname "$(env_value CF_APP_DOMAIN)" 120
    wait_for_cloudflared_hostname "$(env_value CF_API_DOMAIN)" 120
    wait_for_public_url "Frontend public hostname" "$frontend_public_url" 120
    wait_for_public_url "Backend public hostname" "$backend_public_url" 120
    if [[ -n "$auth_public_base_url" && "$auth_public_base_url" == https://* ]]; then
        wait_for_cloudflared_hostname "$(env_value CF_AUTH_DOMAIN)" 120
        wait_for_public_url "Keycloak public hostname" "$auth_public_url" 120
    fi
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
if [[ "$START_CLOUDFLARED" == "true" ]]; then
    echo "Tunnel:   started (Cloudflare tunnel overlay enabled)"
else
    echo "Tunnel:   not started (use --with-cloudflared)"
fi
echo "Neo4j:    http://localhost:${NEO4J_HTTP_PORT:-7474}"
echo "MinIO:    http://localhost:${MINIO_CONSOLE_PORT:-9001}"
echo "Keycloak: http://localhost:${KEYCLOAK_PORT:-8081}"