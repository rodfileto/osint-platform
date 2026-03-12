#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
PIPELINES_DIR="$ROOT_DIR/pipelines"

python_cmd="${PYTHON:-python3}"

if ! command -v "$python_cmd" >/dev/null 2>&1; then
  echo "Python interpreter not found: $python_cmd"
  exit 1
fi

echo "[1/4] Creating backend virtual environment..."
"$python_cmd" -m venv "$BACKEND_DIR/.venv"


echo "[2/4] Installing backend requirements..."
"$BACKEND_DIR/.venv/bin/python" -m pip install --upgrade pip setuptools wheel
"$BACKEND_DIR/.venv/bin/python" -m pip install -r "$BACKEND_DIR/requirements.txt"


echo "[3/4] Creating pipelines virtual environment..."
"$python_cmd" -m venv "$PIPELINES_DIR/.venv"


echo "[4/4] Installing pipelines requirements..."
"$PIPELINES_DIR/.venv/bin/python" -m pip install --upgrade pip setuptools wheel
"$PIPELINES_DIR/.venv/bin/python" -m pip install -r "$PIPELINES_DIR/requirements.txt"

cat <<EOF

Done.

Use one of these interpreters in VS Code:
- Backend:   $BACKEND_DIR/.venv/bin/python
- Pipelines: $PIPELINES_DIR/.venv/bin/python

EOF
