import json
import os
import yaml
import pytest
from sqlalchemy import create_engine
from utils_sql import (
    SQL_LIST_TABLES, SQL_LIST_COLUMNS, SQL_PK, SQL_UNIQUES, SQL_FKS,
    SQL_VIEWS, SQL_FUNCTIONS, SQL_ROLES, SQL_ROLE_MEMBERS, SQL_SEQUENCES,
    SQL_SEQUENCE_OWNED_BY, SQL_INDEXES, SQL_TRIGGERS, SQL_TABLE_OWNERS,
    SQL_TABLE_PRIVILEGES,
)

def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="session")
def cfg():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@pytest.fixture(scope="session")
def include_exclude(cfg):
    sb = cfg.get("sandbox", {})
    include = sb.get("include_schemas", ["public"])
    exclude = sb.get("exclude_schemas", ["pg_catalog", "information_schema"])
    return include, exclude

@pytest.fixture(scope="session")
def dev_engine(cfg):
    engine = create_engine(cfg["dev"]["url"], future=True)
    try:
        with engine.connect() as c:
            c.execute("SELECT 1")
    except Exception as e:
        pytest.skip(f"Cannot connect to Dev DB: {e}")
    return engine

@pytest.fixture(scope="session")
def ignore(cfg):
    ig = cfg.get("ignore", {}) or {}
    return {
        "tables": ig.get("tables", []),
        "columns": ig.get("columns", {}),
        "column_fields": set(ig.get("column_fields", [])),
        "grants_ignore_grantees": set((ig.get("grants", {}) or {}).get("ignore_grantees", [])),
        "grants_ignore_privs": set((ig.get("grants", {}) or {}).get("ignore_privileges", [])),
    }

def _ignore_table(ignore, schema, table):
    import fnmatch
    target = f"{schema}.{table}"
    return any(fnmatch.fnmatch(target, pat) for pat in ignore["tables"])

@pytest.fixture(scope="session")
def snapshots():
    snap_dir = "snapshots"
    tables = os.path.join(snap_dir, "tables")
    table_files = [os.path.join(tables, f) for f in os.listdir(tables) if f.endswith(".json")] if os.path.isdir(tables) else []
    globals_paths = {name: os.path.join(snap_dir, f"{name}.json")
                     for name in ["views","functions","roles","role_memberships",
                                  "sequences","sequence_ownerships","indexes",
                                  "triggers","table_owners","table_privileges"]
                     if os.path.exists(os.path.join(snap_dir, f"{name}.json"))}
    return {"dir": snap_dir, "table_files": table_files, "globals": globals_paths, "_loader": _load_json, "_ignore_table": _ignore_table}
