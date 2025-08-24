# PostgreSQL Release Schema Test — **YAML Snapshot** (No Data)

This POC exports a **YAML schema snapshot** from **Sandbox** and validates **Dev** against it.
It checks **tables/columns/PK/UK/FK**, **views**, **functions**, **roles/memberships**, and **sequences** (incl. owned-by).

> Note: This is **schema-only**. For data checks, keep a separate lane (e.g., DBUnit).

---

## Files
- `export_schema_yaml.py` — builds YAML snapshot from Sandbox using `information_schema` and `pg_catalog`.
- `test_schema_yaml.py` — validates Dev matches the snapshot.
- `utils_sql.py` — shared SQL.
- `config.yaml` — connection strings and discovery filters.
- `snapshots/schema_snapshot.yaml` — output.

## YAML Structure (top-level keys)
```yaml
tables:
  - schema: public
    name: customers
    columns:
      - name: customer_id
        data_type: int4
        is_nullable: "NO"
        char_max: null
        num_precision: "32"
        num_scale: null
        datetime_precision: null
        default: nextval('public.customers_customer_id_seq'::regclass)
        is_identity: "NO"
    primary_key:
      name: customers_pkey
      columns: [customer_id]
    uniques:
      - name: customers_email_key
        columns: [email]
    foreign_keys:
      - name: orders_customer_id_fkey
        ref_schema: public
        ref_table: orders
        columns:
          - {local: customer_id, remote: customer_id}

views:
  - {schema: public, name: active_customers, definition_norm: "select ..."}

functions:
  - {schema: public, name: do_stuff, args: "integer,text", returns: "void", language: "plpgsql", definition_hash: "<sha256>"}

roles:
  - {name: app_user, can_login: true, superuser: false, inherit: true, createrole: false, createdb: false, replication: false}

role_memberships:
  - {role: app_user, member: ci_runner}

sequences:
  - {schema: public, name: customers_customer_id_seq, data_type: bigint, start: "1", min: "1", max: "9223372036854775807", increment: "1", cycle: "false"}

sequence_ownerships:
  - {schema: public, sequence: customers_customer_id_seq, table_schema: public, table: customers, column: customer_id}
```

## Quick Start

### 0) Prereqs
- Python 3.9+

### 1) Configure
```yaml
sandbox:
  url: postgresql+psycopg2://user:pass@host:5432/sandbox_db
  include_schemas: [public]
  exclude_schemas: [pg_catalog, information_schema]
  table_like: "%"

dev:
  url: postgresql+psycopg2://user:pass@host:5432/dev_db
```

### 2) Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Export YAML snapshot (Sandbox → YAML)
```bash
python export_schema_yaml.py --config config.yaml
# writes snapshots/schema_snapshot.yaml
```

### 4) Validate Dev matches the snapshot
```bash
python test_schema_yaml.py --config config.yaml
```

### Options to extend easily
- Add **indexes, triggers, privileges/owners** by dropping in more queries + snapshot fields.
- Add **ignore lists** (e.g., audit columns) or **allow lists** per schema/table.
