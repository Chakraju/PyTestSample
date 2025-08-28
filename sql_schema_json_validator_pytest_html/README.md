# PostgreSQL Schema Validator — **pytest** Edition (Beginner Friendly)

This project helps you **check that your Dev database schema matches your Sandbox schema**.  
It does **not** validate data; it validates **structure only** (tables, columns, primary keys, unique constraints, foreign keys, views, functions, roles, role memberships, sequences, index definitions, triggers, table owners, and table privileges).

You will:
1. **Export** the Sandbox schema to JSON files.
2. **Run tests** against Dev using those JSON snapshots.
3. **Read an HTML report** of pass/fail results (from `pytest-html`).

## 📦 What’s in the box
What’s inside (plain-English tour)

README.md — hand-holding guide: what the project does, step-by-step setup, and FAQs.

requirements.txt — the Python packages to install (pytest, pytest-html, SQLAlchemy, DeepDiff, etc.).

config.yaml — where you put your Sandbox/Dev database connection URLs and any ignore rules.

export_schema_json.py — exports your Sandbox schema as JSON snapshots into the snapshots/ folder.

utils_sql.py — all the SQL queries used by the exporter and the tests (tables, columns, PK/UK/FK, views, functions, roles, sequences, indexes, triggers, owners, privileges).

pytest.ini — small config to make pytest output quieter.

snapshots/ — created after you export; contains one JSON per table under snapshots/tables/ and “global” JSONs (views, functions, etc.).

reports/ — pytest writes an HTML test report here.

tests/

conftest.py — shared helpers for tests (reads config.yaml, opens Dev DB connection, loads snapshots).

test_schema.py — the schema tests:

One test per table JSON (columns, PK, UNIQUE, FKs).

Global tests for views, functions, roles, role memberships, sequences & sequence ownerships, indexes, triggers, table owners, table privileges.

## 🚀 Quick Start
```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python export_schema_json.py --config config.yaml
pytest --html=reports/pytest_schema_report.html --self-contained-html
```

How to run (copy/paste)
python -m venv .venv
source .venv/bin/activate    # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1) Edit config.yaml with your Sandbox/Dev connection URLs
# 2) Export Sandbox snapshots
python export_schema_json.py --config config.yaml

# 3) Test Dev vs snapshots and make an HTML report
pytest --html=reports/pytest_schema_report.html --self-contained-html


Open reports/pytest_schema_report.html in your browser for a clean pass/fail summary.

pom.xml

Uses exec-maven-plugin to run the shell script during the test phase.

Usage:

mvn -Prun test


(Profile run keeps it opt-in; or just mvn test if you’re fine with default execution.)

You can also customize via properties:

${build.script} (defaults to build.sh)

${report.path} (defaults to reports/pytest_schema_report.html)

build.sh

Creates/uses a virtualenv (.venv), installs requirements, exports Sandbox snapshots, runs pytest with an HTML report.

Usage:

chmod +x build.sh
./build.sh


Env overrides:

PY=python3 PIP=pip3 VENVDIR=.venv CONF=config.yaml REPORT=reports/pytest_schema_