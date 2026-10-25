#!/usr/bin/env bash
set -Eeuo pipefail

# Run from the project directory or set PROJECT_DIR explicitly.
PROJECT_DIR="${PROJECT_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_DIR}/venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Python executable not found: ${PYTHON_BIN}" >&2
    echo "Create the virtual environment and install requirements first." >&2
    exit 1
fi

cd "${PROJECT_DIR}"
exec "${PYTHON_BIN}" "${PROJECT_DIR}/main.py"
