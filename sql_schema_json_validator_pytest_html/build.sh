#!/usr/bin/env bash
set -euo pipefail

# Configurable vars (override with env vars if needed)
PY=${PY:-python}
PIP=${PIP:-pip}
VENVDIR=${VENVDIR:-.venv}
CONF=${CONF:-config.yaml}
REPORT=${REPORT:-reports/pytest_schema_report.html}

echo "==> Creating virtualenv in ${VENVDIR} (if missing)"
if [ ! -d "${VENVDIR}" ]; then
  ${PY} -m venv "${VENVDIR}"
fi

# Activate venv
if [ -f "${VENVDIR}/bin/activate" ]; then
  # Unix
  source "${VENVDIR}/bin/activate"
else
  # Windows (Git Bash/CI may use this path)
  source "${VENVDIR}/Scripts/activate"
fi

echo "==> Upgrading pip & installing requirements"
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "==> Exporting Sandbox schema snapshots"
python export_schema_json.py --config "${CONF}"

echo "==> Running pytest (HTML report -> ${REPORT})"
mkdir -p "$(dirname "${REPORT}")"
pytest --html="${REPORT}" --self-contained-html

echo "==> Done. Open ${REPORT} in your browser."
