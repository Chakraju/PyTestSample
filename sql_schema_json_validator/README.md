# PostgreSQL Schema Validator — SQL-based JSON Snapshots (No Data)

Exports **schema snapshots** from *Sandbox* using **information_schema/pg_catalog** as **JSON files**, then
validates *Dev* against them — **structure only** (no data). Each table is saved to its own JSON file, and
other object types (views, functions, roles, sequences) are saved to separate JSON files.

## Quick Start
1) `pip install -r requirements.txt`
2) Edit `config.yaml`
3) Export: `python export_schema_json.py --config config.yaml`
4) Validate: `python test_schema_json.py --config config.yaml`
