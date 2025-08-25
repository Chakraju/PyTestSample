import argparse, os, json, hashlib, yaml
from sqlalchemy import create_engine
from utils_sql import *

def norm_sql(s: str) -> str:
    if s is None: return ""
    return " ".join(s.split())

def export_table(conn, schema: str, table: str, out_dir: str):
    entry = {"schema": schema, "name": table, "columns": []}

    cols = conn.execute(SQL_LIST_COLUMNS, {"schema": schema, "table": table}).all()
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

    pk = conn.execute(SQL_PK, {"schema": schema, "table": table}).all()
    if pk:
        entry["primary_key"] = {"name": pk[0].constraint_name, "columns": list(pk[0].columns)}

    uqs = conn.execute(SQL_UNIQUES, {"schema": schema, "table": table}).all()
    if uqs:
        entry["uniques"] = [{"name": u.constraint_name, "columns": list(u.columns)} for u in uqs]

    fk_rows = conn.execute(SQL_FKS, {"schema": schema, "table": table}).all()
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

    fname = f"{schema}.{table}.json"
    path = os.path.join(out_dir, "tables", fname)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(entry, f, indent=2, ensure_ascii=False)
    print(f"[export] table -> {path}")

def export_globals(conn, include_schemas, exclude_schemas, out_dir: str):
    # views
    views = []
    for v in conn.execute(SQL_VIEWS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}):
        views.append({"schema": v.table_schema, "name": v.table_name, "definition_norm": norm_sql(v.definition)})
    open(os.path.join(out_dir, "views.json"), "w").write(json.dumps(views, indent=2, ensure_ascii=False))

    # functions
    funcs = []
    for f in conn.execute(SQL_FUNCTIONS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}):
        funcs.append({
            "schema": f.schema, "name": f.name, "args": f.args or "", "returns": f.returns or "",
            "language": f.language or "",
            "definition_hash": hashlib.sha256(norm_sql(f.definition).encode("utf-8")).hexdigest()
        })
    open(os.path.join(out_dir, "functions.json"), "w").write(json.dumps(funcs, indent=2, ensure_ascii=False))

    # roles
    roles = []
    for r in conn.execute(SQL_ROLES):
        roles.append({
            "name": r.rolname,
            "can_login": bool(r.rolcanlogin),
            "superuser": bool(r.rolsuper),
            "inherit": bool(r.rolinherit),
            "createrole": bool(r.rolcreaterole),
            "createdb": bool(r.rolcreatedb),
            "replication": bool(r.rolreplication),
        })
    open(os.path.join(out_dir, "roles.json"), "w").write(json.dumps(roles, indent=2, ensure_ascii=False))

    rms = [{"role": m.role, "member": m.member} for m in conn.execute(SQL_ROLE_MEMBERS)]
    open(os.path.join(out_dir, "role_memberships.json"), "w").write(json.dumps(rms, indent=2, ensure_ascii=False))

    # sequences
    seqs = []
    for s in conn.execute(SQL_SEQUENCES, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}):
        seqs.append({
            "schema": s.sequence_schema, "name": s.sequence_name, "data_type": s.data_type,
            "start": str(s.start_value), "min": str(s.minimum_value), "max": str(s.maximum_value),
            "increment": str(s.increment), "cycle": str(s.cycle_option).lower()
        })
    open(os.path.join(out_dir, "sequences.json"), "w").write(json.dumps(seqs, indent=2, ensure_ascii=False))

    # ownerships
    owns = []
    for o in conn.execute(SQL_SEQUENCE_OWNED_BY):
        if o.schema_name is None:
            continue
        owns.append({
            "schema": o.schema_name, "sequence": o.sequence_name,
            "table_schema": o.table_schema or "", "table": o.table_name or "", "column": o.column_name or ""
        })
    open(os.path.join(out_dir, "sequence_ownerships.json"), "w").write(json.dumps(owns, indent=2, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser(description="Export Sandbox schema as JSON (per table + global files).")
    ap.add_argument("--config", required=True)
    ap.add_argument("--outdir", default="snapshots")
    ap.add_argument("--only_tables", nargs="*", help="Optional explicit list schema.table")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    sb = cfg["sandbox"]
    engine = create_engine(sb["url"], future=True)
    include_schemas = sb.get("include_schemas", ["public"])
    exclude_schemas = sb.get("exclude_schemas", ["pg_catalog", "information_schema"])
    table_like = sb.get("table_like", "%")

    out_dir = args.outdir
    os.makedirs(os.path.join(out_dir, "tables"), exist_ok=True)

    with engine.connect() as conn:
        if args.only_tables:
            targets = []
            for fq in args.only_tables:
                s, t = fq.split(".", 1) if "." in fq else (sb.get("schema", "public"), fq)
                targets.append((s, t))
        else:
            targets = [(r.table_schema, r.table_name) for r in conn.execute(SQL_LIST_TABLES, {
                "include_schemas": include_schemas,
                "exclude_schemas": tuple(exclude_schemas),
                "table_like": table_like
            }).all()]

        for schema, table in targets:
            export_table(conn, schema, table, out_dir)

        export_globals(conn, include_schemas, exclude_schemas, out_dir)

    print(f"Done. JSON snapshots under: {out_dir}")

if __name__ == "__main__":
    from sqlalchemy import create_engine
    main()
