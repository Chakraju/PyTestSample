# SchemaCrawler + JSON/YAML + pytest — Release Schema Test (PostgreSQL)

This POC uses **SchemaCrawler** to export **schema snapshots** from *Sandbox* and *Dev*,
then uses **pytest** to **assert equivalence** with configurable normalization (ignore keys,
whitespace normalization, etc.). It also creates a **side-by-side HTML diff report**.

---

## Quick start

### 0) Install Python deps
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 1) Configure `config.yaml`
```yaml
schemacrawler:
  executable: schemacrawler   # If not on PATH, set absolute path to schemacrawler launcher
  server: postgresql
  info_level: maximum         # brief|standard|maximum
  command: details            # details|brief|schema
  schemas: "public"           # optional regex e.g., "public|sales"
  output_formats: [json, yaml]

sandbox:
  host: localhost
  port: 5432
  database: sandbox_db
  user: sandbox_user
  password: sandbox_pass

dev:
  host: localhost
  port: 5432
  database: dev_db
  user: dev_user
  password: dev_pass

compare:
  ignore_keys: ["remarks", "columnOrdinalPosition", "definitionExpression", "definitionText", "definition", "lookupKey"]
  ignore_sections: ["databaseInfo", "jdbcDriverInfo", "serverInfo"]
  normalize_sql_keys: ["definition", "definitionText", "viewDefinition", "routineBody"]
  include_root_keys: ["schemas", "tables", "columns", "tableConstraints", "routines", "views", "sequences"]
```

### 2) Export snapshots (Sandbox & Dev)
```bash
python tools/export_snapshots.py --config config.yaml
# -> snapshots/sandbox.json(.yaml), snapshots/dev.json(.yaml)
```

### 3) Generate HTML diff
```bash
python tools/diff_report.py --config config.yaml --out reports/schema_diff.html
# open reports/schema_diff.html in your browser
```

### 4) Run tests
```bash
pytest -q
```

### 5) One-shot helper (export + diff + tests)
```bash
python tools/runner.py --config config.yaml
```

---

## Tips
- Reduce noise by limiting schemas via `schemacrawler.schemas` (e.g., `public|myschema`).
- Prefer JSON if you want smaller diffs; YAML also supported.
- Tweak `compare.ignore_keys` and `compare.normalize_sql_keys` to reduce spurious differences.
