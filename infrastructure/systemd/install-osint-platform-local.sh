#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
UNIT_NAME=osint-platform-local.service
SOURCE_UNIT="$SCRIPT_DIR/$UNIT_NAME"
TARGET_UNIT="/etc/systemd/system/$UNIT_NAME"
AIRFLOW_UNIT_NAME=osint-platform-airflow.service
AIRFLOW_SOURCE_UNIT="$SCRIPT_DIR/$AIRFLOW_UNIT_NAME"
AIRFLOW_TARGET_UNIT="/etc/systemd/system/$AIRFLOW_UNIT_NAME"

if [ ! -f "$SOURCE_UNIT" ]; then
  echo "Missing unit file: $SOURCE_UNIT" >&2
  exit 1
fi

if [ ! -f "$AIRFLOW_SOURCE_UNIT" ]; then
  echo "Missing unit file: $AIRFLOW_SOURCE_UNIT" >&2
  exit 1
fi

install_unit() {
  sed "s#@REPO_DIR@#$REPO_DIR#g" "$1" > "$2"
  chmod 0644 "$2"
}

install_unit "$SOURCE_UNIT" "$TARGET_UNIT"
install_unit "$AIRFLOW_SOURCE_UNIT" "$AIRFLOW_TARGET_UNIT"
systemctl daemon-reload
systemctl enable "$UNIT_NAME"
systemctl enable "$AIRFLOW_UNIT_NAME"

if [ "${1:-}" = "--start" ]; then
  systemctl restart "$UNIT_NAME"
  systemctl restart "$AIRFLOW_UNIT_NAME"
fi

systemctl status "$UNIT_NAME" --no-pager
systemctl status "$AIRFLOW_UNIT_NAME" --no-pager
