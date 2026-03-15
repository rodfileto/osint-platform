#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENVIRONMENT="prod-like"
REQUIRE_CONFIRMATION=false
CONFIRMED=false

usage() {
	cat <<'EOF'
Usage: ./infrastructure/postgres/run-flyway.sh [--env dev|prod-like] [--yes] <action> [flyway-args...]

Examples:
  ./infrastructure/postgres/run-flyway.sh --env dev migrate
  ./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
  ./infrastructure/postgres/run-flyway.sh --env prod-like validate

Notes:
  - prod-like defaults to a guarded mode for mutating actions like migrate and repair.
  - use --yes to confirm those actions explicitly in prod-like.
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--env)
			ENVIRONMENT="${2:-}"
			shift 2
			;;
		--yes)
			CONFIRMED=true
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			break
			;;
	esac
done

ACTION="${1:-migrate}"
if [[ $# -gt 0 ]]; then
	shift
fi

case "$ENVIRONMENT" in
	dev)
		ENV_FILE="$ROOT_DIR/dev/.env"
		COMPOSE_FILES=("$ROOT_DIR/docker-compose.yml" "$ROOT_DIR/compose.dev.yml")
		;;
	prod-like)
		ENV_FILE="$ROOT_DIR/.env"
		COMPOSE_FILES=("$ROOT_DIR/docker-compose.yml" "$ROOT_DIR/compose.prod.yml")
		REQUIRE_CONFIRMATION=true
		;;
	*)
		echo "Unknown environment: $ENVIRONMENT"
		usage
		exit 1
		;;
esac

if [[ ! -f "$ENV_FILE" ]]; then
	echo "Missing environment file: $ENV_FILE"
	exit 1
fi

if [[ "$REQUIRE_CONFIRMATION" == "true" ]]; then
	case "$ACTION" in
		migrate|repair)
			if [[ "$CONFIRMED" != "true" ]]; then
				echo "Refusing to run '$ACTION' against prod-like without --yes."
				exit 1
			fi
			;;
	esac
fi

compose() {
	docker compose --env-file "$ENV_FILE" -f "${COMPOSE_FILES[0]}" -f "${COMPOSE_FILES[1]}" "$@"
}

compose up -d postgres
compose run --rm flyway "$ACTION" "$@"