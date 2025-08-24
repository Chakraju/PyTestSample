import argparse
import os
import yaml
import hashlib
from sqlalchemy import create_engine
from utils_sql import *

def norm(s: str) -> str:
    if s is None:
        return ""
    return " ".join(s.split())

def export_snapshot(engine, include_schemas, exclude_schemas, table_like):
    snap = {
        "tables": [],
        "views": [],
        "functions": [],
        "roles": [],
        "role_memberships": [],
        "sequences": [],
        "sequence_ownerships": []
    }
    with engine.connect() as conn:
        # roles
        for r in conn.execute(SQL_ROLES):
            snap["roles"].append({
                "name": r.rolname,
                "can_login": bool(r.rolcanlogin),
                "superuser": bool(r.rolsuper),
                "inherit": bool(r.rolinherit),
                "createrole": bool(r.rolcreaterole),
                "createdb": bool(r.rolcreatedb),
                "replication": bool(r.rolreplication),
            })
        for m in conn.execute(SQL_ROLE_MEMBERS):
            snap["role_memberships"].append({"role": m.role, "member": m.member})

        # tables & columns
        tables = conn.execute(SQL_LIST_TABLES, {
            "include_schemas": include_schemas,
            "exclude_schemas": tuple(exclude_schemas),
            "table_like": table_like
        }).all()

        for t in tables:
            schema, name = t.table_schema, t.table_name
            entry = {"schema": schema, "name": name, "columns": []}

            cols = conn.execute(SQL_LIST_COLUMNS, {"schema": schema, "table": name}).all()
            for c in cols:
                entry["columns"].append({
                    "name": c.column_name,
                    "data_type": c.udt_name or c.data_type or "",
                    "is_nullable": (c.is_nullable or ""),
                    "char_max": c.character_maximum_length,
                    "num_precision": c.numeric_precision,
                    "num_scale": c.numeric_scale,
                    "datetime_precision": c.datetime_precision,
                    "default": c.column_default,
                    "is_identity": (c.is_identity or ""),
                })

            # PK
            pk = conn.execute(SQL_PK, {"schema": schema, "table": name}).all()
            if pk:
                entry["primary_key"] = {"name": pk[0].constraint_name, "columns": list(pk[0].columns)}

            # uniques
            uqs = conn.execute(SQL_UNIQUES, {"schema": schema, "table": name}).all()
            if uqs:
                entry["uniques"] = [{"name": u.constraint_name, "columns": list(u.columns)} for u in uqs]

            # fks
            fk_rows = conn.execute(SQL_FKS, {"schema": schema, "table": name}).all()
            if fk_rows:
                grouped = {}
                for r in fk_rows:
                    grouped.setdefault(r.constraint_name, []).append(r)
                fks = []
                for cname, rows in grouped.items():
                    rows = sorted(rows, key=lambda x: x.ordinal_position or 0)
                    fks.append({
                        "name": cname,
                        "ref_schema": rows[0].foreign_table_schema,
                        "ref_table": rows[0].foreign_table_name,
                        "columns": [{"local": r.column_name, "remote": r.foreign_column_name} for r in rows]
                    })
                entry["foreign_keys"] = fks

            snap["tables"].append(entry)

        # views
        views = conn.execute(SQL_VIEWS, {
            "include_schemas": include_schemas,
            "exclude_schemas": tuple(exclude_schemas)
        }).all()
        for v in views:
            snap["views"].append({
                "schema": v.table_schema,
                "name": v.table_name,
                "definition_norm": norm(v.definition)
            })

        # functions
        funcs = conn.execute(SQL_FUNCTIONS, {
            "include_schemas": include_schemas,
            "exclude_schemas": tuple(exclude_schemas)
        }).all()
        for f in funcs:
            def_hash = hashlib.sha256(norm(f.definition).encode("utf-8")).hexdigest()
            snap["functions"].append({
                "schema": f.schema,
                "name": f.name,
                "args": f.args or "",
                "returns": f.returns or "",
                "language": f.language or "",
                "definition_hash": def_hash
            })

        # sequences
        seqs = conn.execute(SQL_SEQUENCES, {
            "include_schemas": include_schemas,
            "exclude_schemas": tuple(exclude_schemas)
        }).all()
        for s in seqs:
            snap["sequences"].append({
                "schema": s.sequence_schema,
                "name": s.sequence_name,
                "data_type": s.data_type,
                "start": str(s.start_value),
                "min": str(s.minimum_value),
                "max": str(s.maximum_value),
                "increment": str(s.increment),
                "cycle": str(s.cycle_option).lower()
            })

        owns = conn.execute(SQL_SEQUENCE_OWNED_BY).all()
        for o in owns:
            if o.schema_name is None:
                continue
            snap["sequence_ownerships"].append({
                "schema": o.schema_name,
                "sequence": o.sequence_name,
                "table_schema": o.table_schema or "",
                "table": o.table_name or "",
                "column": o.column_name or ""
            })

    return snap

def main():
    ap = argparse.ArgumentParser(description="Export Sandbox schema to YAML snapshot (no data).")
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", default="snapshots/schema_snapshot.yaml")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    sb = cfg["sandbox"]
    engine = create_engine(sb["url"], future=True)
    snap = export_snapshot(engine,
                           include_schemas=sb.get("include_schemas", ["public"]),
                           exclude_schemas=sb.get("exclude_schemas", ["pg_catalog", "information_schema"]),
                           table_like=sb.get("table_like", "%"))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        yaml.safe_dump(snap, f, sort_keys=False)
    print(f"[export] wrote {args.out}")

if __name__ == "__main__":
    main()
