# DB Release Test with **Two Python Programs** + DBUnit

Exactly two Python programs:

1. `export_sandbox.py` — reads the **Sandbox** database and exports **one DBUnit Flat XML** file per table to `datasets/`.
2. `test_release_dbunit.py` — runs **DBUnit** against the **Dev** database using those XML files.

DB target: PostgreSQL (easy to adapt).

---

## Quick Start

### 0) Prerequisites
- Python 3.9+
- Java 17+ and Maven
- Network access to both Sandbox and Dev DBs

### 1) Configure connections and discovery filters
Edit `config.yaml`:
```yaml
sandbox:
  url: postgresql+psycopg2://user:pass@host:5432/sandbox_db
  schema: public
  include_schemas: [public]      # optional; defaults to [public]
  exclude_schemas: [pg_catalog, information_schema]  # optional
  table_like: "%"                # optional; SQL LIKE pattern (e.g., "cust%")
  row_limit: null                # optional; export only first N rows for each table

dev:
  url: postgresql+psycopg2://user:pass@host:5432/dev_db
  schema: public
```

> The exporter now discovers **tables and their columns using SQL** against `information_schema`.
> You can still list specific tables via `--only_tables schema.table schema2.table2` if you wish.

### 2) Install Python deps
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3) Export Sandbox → Flat XML using SQL-driven discovery
```bash
python export_sandbox.py --config config.yaml --outdir datasets
# or restrict to specific tables:
python export_sandbox.py --config config.yaml --outdir datasets --only_tables public.customers public.orders
```

### 4) Build the DBUnit runner (once)
```bash
cd dbunit_runner
mvn -q -DskipTests package
cd ..
```

### 5) Test Dev using DBUnit
```bash
python test_release_dbunit.py --config config.yaml --datasets_dir datasets
```
