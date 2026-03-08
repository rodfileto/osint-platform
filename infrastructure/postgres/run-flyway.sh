#!/bin/bash
set -euo pipefail

ACTION="${1:-migrate}"
shift || true

docker compose up -d postgres
docker compose run --rm flyway "$ACTION" "$@"