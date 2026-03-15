#!/bin/sh

set -eu

python manage.py collectstatic --noinput

exec gunicorn \
  --bind 0.0.0.0:8000 \
  --workers "${GUNICORN_WORKERS:-4}" \
  --timeout "${GUNICORN_TIMEOUT:-120}" \
  config.wsgi:application
