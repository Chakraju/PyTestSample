# PostgreSQL Schema Validator — SQL-based JSON Snapshots (No Data)

Exports schema from **Sandbox** using `information_schema`/`pg_catalog` into **JSON** (one file per table + global JSONs),
then validates **Dev** against that snapshot. Structure-only: tables, columns, PK, unique constraints, FKs, views, functions,
roles, role memberships, sequences, sequence ownerships, **indexes**, **triggers**, **table owners**, and **table privileges**.

> **PK/UNIQUE bug fix:** column lists are read via `Row._mapping['colnames']` and coerced to lists.


## Quick Start
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1) Export Sandbox
python export_schema_json.py --config config.yaml

# 2) Validate Dev
python test_schema_json.py --config config.yaml

# 3) HTML diff (snapshot JSON vs live Dev)
python generate_html_diff.py --config config.yaml --snapshots snapshots --out reports/json_vs_live_diff.html
```
