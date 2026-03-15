#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_ENV_FILE="$ROOT_DIR/.env"
DEV_ENV_FILE="$ROOT_DIR/dev/.env"
CREATED_ROOT_ENV=false
CREATED_DEV_ENV=false

cleanup() {
    if [[ "$CREATED_ROOT_ENV" == "true" ]]; then
        rm -f "$ROOT_ENV_FILE"
    fi

    if [[ "$CREATED_DEV_ENV" == "true" ]]; then
        rm -f "$DEV_ENV_FILE"
    fi
}

trap cleanup EXIT

ensure_env_file() {
    local target_file="$1"
    local example_file="$2"
    local created_flag_name="$3"

    if [[ -f "$target_file" ]]; then
        return 0
    fi

    cp "$example_file" "$target_file"
    printf -v "$created_flag_name" '%s' "true"
}

assert_fails_with_message() {
    local expected_message="$1"
    shift

    set +e
    local output
    output=$("$@" 2>&1)
    local status=$?
    set -e

    if [[ $status -eq 0 ]]; then
        echo "Expected command to fail, but it succeeded: $*"
        exit 1
    fi

    if [[ "$output" != *"$expected_message"* ]]; then
        echo "Command failed, but not with the expected message."
        echo "Expected substring: $expected_message"
        echo "Actual output:"
        echo "$output"
        exit 1
    fi
}

ensure_env_file "$ROOT_ENV_FILE" "$ROOT_DIR/.env.example" CREATED_ROOT_ENV
ensure_env_file "$DEV_ENV_FILE" "$ROOT_DIR/dev/.env.example" CREATED_DEV_ENV

echo "Validating merged dev compose config..."
docker compose \
    --env-file "$DEV_ENV_FILE" \
    -f "$ROOT_DIR/docker-compose.yml" \
    -f "$ROOT_DIR/compose.dev.yml" \
    config >/dev/null

echo "Validating merged prod-like compose config..."
docker compose \
    --env-file "$ROOT_ENV_FILE" \
    -f "$ROOT_DIR/docker-compose.yml" \
    -f "$ROOT_DIR/compose.prod.yml" \
    config >/dev/null

echo "Checking for stale dev compose references..."
if rg --fixed-strings \
    --glob '!dev/.env' \
    --glob '!dev/.env.example' \
    --glob '!scripts/check-compose-guardrails.sh' \
    'dev/docker-compose.yml' "$ROOT_DIR" >/dev/null; then
    echo "Found stale references to dev/docker-compose.yml"
    exit 1
fi

echo "Validating prod-like Flyway guardrail..."
assert_fails_with_message \
    "Refusing to run 'migrate' against prod-like without --yes." \
    "$ROOT_DIR/infrastructure/postgres/run-flyway.sh" --env prod-like migrate

echo "Validating prod-like startup guardrail..."
assert_fails_with_message \
    "A prod-like start requires an explicit migration decision." \
    "$ROOT_DIR/scripts/start-prod-like.sh"

echo "Compose guardrails passed."