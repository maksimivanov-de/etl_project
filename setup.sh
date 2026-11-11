#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo 'Нужен Python 3.12. Установите его или задайте PYTHON_BIN=/путь/к/python3.12.' >&2
    exit 1
fi
"$PYTHON_BIN" -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else "Нужен Python 3.12.")'
if [ ! -d .venv ]; then
    "$PYTHON_BIN" -m venv .venv
fi
.venv/bin/python -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else "Существующее .venv должно использовать Python 3.12.")'
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/setup_database.py
