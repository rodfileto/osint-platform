#!/bin/sh
set -eu

docker compose up -d postgres
docker compose up --build --abort-on-container-exit --exit-code-from flyway flyway
docker compose run --rm geo-bootstrap
