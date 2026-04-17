#!/bin/sh

set -eu

exec uvicorn main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers "${UVICORN_WORKERS:-4}" \
  --log-level "${UVICORN_LOG_LEVEL:-info}" \
  --proxy-headers \
  --forwarded-allow-ips "${UVICORN_FORWARDED_ALLOW_IPS:-*}" \
  --timeout-keep-alive "${UVICORN_TIMEOUT_KEEP_ALIVE:-120}"
